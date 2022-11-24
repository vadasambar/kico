package corednsrunner

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	logrus "github.com/sirupsen/logrus"
	"github.com/vadasambar/kico/pkg/interfaces"
	"gopkg.in/yaml.v3"
	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	log              *logrus.Logger
	ignoredPodLabels = []string{
		"pod-template-hash",
	}
)

const (
	corednsNamespace        = "kube-system"
	corednsPodLabels        = "k8s-app=kube-dns"
	logNotFound      string = "%s: waited %v for the relevant log to appear but it didn't"
	fqdnSuffix              = ".svc.cluster.local."
)

type ConnectionLog struct {
	FromIP     string
	ToHostname string
	Status     string
	FromPort   string
}

type Runner struct {
	toPod             *v1.Pod
	toPodNamespace    string
	toPodServiceFQDNs []string

	coreDNSPods          *v1.PodList
	clientset            *kubernetes.Clientset
	allNamespaces        *v1.NamespaceList
	allEndpoints         map[string]*v1.EndpointsList
	connectionLogs       []*ConnectionLog
	hostnamePodMapping   map[string][]*Mapping
	suggestNetworkPolicy bool
	concurrency          int
	waitForLogsDuration  time.Duration
}

type Mapping struct {
	podname   string
	namespace string
}

type InitConfig struct {
	ToPodName            string
	ToPodNamespace       string
	Config               *rest.Config
	SuggestNetworkPolicy bool
	Concurrency          int
	WaitForLogsDuration  time.Duration
}

func init() {
	log = logrus.New()
	level := os.Getenv("LOG_LEVEL")
	if level == "" {
		level = "info"
	}
	l, err := logrus.ParseLevel(level)
	if err != nil {
		panic(err)
	}
	log.SetLevel(l)
}

func Initialize(ic *InitConfig) (interfaces.RunnerInterface, error) {
	clientset, err := kubernetes.NewForConfig(ic.Config)
	if err != nil {
		return nil, err
	}

	toPod, err := clientset.CoreV1().Pods(ic.ToPodNamespace).Get(context.Background(), ic.ToPodName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	nsList, err := clientset.CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	allEps := map[string]*v1.EndpointsList{}
	for _, n := range nsList.Items {
		eList, err := clientset.CoreV1().Endpoints(n.Name).List(context.Background(), metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		allEps[n.Name] = eList
	}

	ctx := context.Background()
	podList, err := clientset.CoreV1().Pods(corednsNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: corednsPodLabels,
	})
	if err != nil {
		return nil, err
	}

	r := &Runner{
		toPod:                toPod,
		toPodNamespace:       ic.ToPodNamespace,
		coreDNSPods:          podList,
		clientset:            clientset,
		allNamespaces:        nsList,
		allEndpoints:         allEps,
		hostnamePodMapping:   map[string][]*Mapping{},
		suggestNetworkPolicy: ic.SuggestNetworkPolicy,
		concurrency:          ic.Concurrency,
		waitForLogsDuration:  ic.WaitForLogsDuration,
	}

	toPodServiceFQDNs, err := r.findToPodServiceFQDNs()
	if err != nil {
		return nil, err
	}

	r.toPodServiceFQDNs = toPodServiceFQDNs

	if err := r.waitForLogs(); err != nil {
		return nil, err
	}

	connLogList, err := r.parseConnectionLogs()
	if err != nil {
		return nil, err
	}

	r.connectionLogs = connLogList

	return r, nil
}

func (r *Runner) Run() error {
	fmt.Println("INCOMING CONNECTIONS")
	fmt.Println("--------------------")
	if err := r.processConnectionLogs(); err != nil {
		return err
	}

	if r.suggestNetworkPolicy {
		return r.suggestNetPol()
	}
	return nil
}

// waitForLogs waits for the connection logs to show up
// in coredns pods
func (r *Runner) waitForLogs() error {
	var wg sync.WaitGroup
	var e error
	var mu sync.Mutex
	wg.Add(1)
	go func() {
		defer wg.Done()
		var wg2 sync.WaitGroup

		for _, pod := range r.coreDNSPods.Items {
			wg2.Add(1)
			// why? check
			// 1. https://github.com/golang/go/wiki/CommonMistakes#using-reference-to-loop-iterator-variable
			// 2. https://github.com/golang/go/wiki/CommonMistakes#using-goroutines-on-loop-iterator-variables
			pod := pod
			go func() {
				tStart := time.Now()
				defer wg2.Done()
				ctx2 := context.Background()
				tailLines := new(int64)
				*tailLines = 5
				req := r.clientset.CoreV1().Pods("kube-system").GetLogs(pod.Name, &v1.PodLogOptions{Follow: true, TailLines: tailLines})
				stream, err := req.Stream(ctx2)
				if err != nil {
					mu.Lock()
					log.Errorf(logNotFound, pod.Name, r.waitForLogsDuration)
					e = err
					mu.Unlock()
				}
				defer stream.Close()

				scanner := bufio.NewScanner(stream)
				// scanner has a limitation where it can read max 65536 characters
				// More info and solution: https://stackoverflow.com/a/16615559/6874596

				log.Debugf("%s: looking for relevant logs in the coredns pod logs\n", pod.Name)
				for scanner.Scan() {
					t := scanner.Text()
					tEnd := time.Now()
					if tEnd.Sub(tStart) > r.waitForLogsDuration {
						log.Infof("%s: giving up... :(\n", pod.Name)

						mu.Lock()
						log.Errorf(logNotFound, pod.Name, r.waitForLogsDuration)
						e = fmt.Errorf(logNotFound, pod.Name, r.waitForLogsDuration)
						mu.Unlock()
						return
					}
					if !relevantLogMsg(t) {
						continue
					} else {
						log.Debug(t)
						log.Debugf("%s: relevant logs found :)\n", pod.Name)
						return
					}

				}

				if err := scanner.Err(); err != nil {
					log.Fatal(err)
				}

			}()

		}
		wg2.Wait()
	}()

	wg.Wait()
	return e
}

// findToPodServiceFQDNs finds K8s Service associated with the toPod
// and creates FQDNs out of them
func (r *Runner) findToPodServiceFQDNs() ([]string, error) {
	toPodServices := []v1.Service{}

	sList, err := r.clientset.CoreV1().Services(r.toPodNamespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, s := range sList.Items {
		selector := s.Spec.Selector
		for k, v := range selector {
			if r.toPod.GetLabels()[k] != v {
				break
			} else {
				toPodServices = append(toPodServices, s)
			}
		}
	}

	toPodServiceFQDNs := []string{}
	for _, s := range toPodServices {
		fqdn := fmt.Sprintf("%s.%s.svc.cluster.local.", s.Name, s.Namespace)
		toPodServiceFQDNs = append(toPodServiceFQDNs, fqdn)
	}

	return toPodServiceFQDNs, nil
}

// parseConnectionLogs reads logs and parses them into
// ConnectionLog struct
func (r *Runner) parseConnectionLogs() ([]*ConnectionLog, error) {
	connLogList := []*ConnectionLog{}
	ctx2 := context.Background()
	for _, pod := range r.coreDNSPods.Items {
		req := r.clientset.CoreV1().Pods("kube-system").GetLogs(pod.Name, &v1.PodLogOptions{})
		stream, err := req.Stream(ctx2)
		if err != nil {
			return nil, err
		}
		defer stream.Close()

		scanner := bufio.NewScanner(stream)
		// scanner has a limitation where it can read max 65536 characters
		// More info and solution: https://stackoverflow.com/a/16615559/6874596
		for scanner.Scan() {
			t := scanner.Text()
			c, err, success := parseLogMsg(t)
			if err != nil {
				return nil, err
			}

			if success {
				connLogList = append(connLogList, c)
			}

		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
	}

	return connLogList, nil
}

// relevantLogMsg returns true if the log message is relevant for us i.e.,
// it is the log message we want
func relevantLogMsg(rawText string) bool {
	// Check for substring in the order in which they appear in the raw text
	// because Go uses short-circuit evaluation of `&&`. That is,
	// `don't go to the next && if the current one is not true`
	// More info: https://go.dev/ref/spec#Logical_operators
	// Sample log that we want are looking for looks like this:
	// [INFO] 10.42.2.90:59003 - 9687 "AAAA IN user-db.sock-shop.svc.cluster.local. udp 53 false 512" NOERROR qr,aa,rd 146 0.000428325s
	// It follows the default logging format of the CoreDNS `log` plugin
	// More info: https://coredns.io/plugins/log/#log-format
	return strings.HasPrefix(rawText, "[INFO]") &&
		strings.Contains(rawText, fqdnSuffix) &&
		// NOERROR indicates success
		// https://www.iana.org/assignments/dns-parameters/dns-parameters.xhtml#dns-parameters-6
		strings.Contains(rawText, "NOERROR") &&
		// to match IP:PORT e.g., 10.42.2.90:59003
		strings.Contains(rawText, ":")
}

func parseLogMsg(rawText string) (*ConnectionLog, error, bool) {
	var c *ConnectionLog

	if !relevantLogMsg(rawText) {
		return c, nil, false
	}

	si := strings.Index(rawText, fqdnSuffix)

	var fqdn string
	// PoC: https://go.dev/play/p/xb3wDprPdOT
	for i := si; i >= 0; i-- {
		if rawText[i:i+1] == " " {
			fqdn = rawText[i+1 : si]
			break
		}
	}

	if fqdn == "" {
		return c, fmt.Errorf("FQDN not found in the log '%v'", rawText), false
	}

	fqdn = fqdn + fqdnSuffix

	eiText := strings.Split(rawText, " ")[1]
	var ip string
	var port string
	// PoC: https://go.dev/play/p/xb3wDprPdOT
	for i := len(eiText) - 1; i >= 0; i-- {
		if eiText[i:i+1] == ":" {
			ip = eiText[0:i]
			port = eiText[i+1:]
			break
		}
	}

	if ip == "" {
		return c, fmt.Errorf("pod ip not found in the log '%v'", rawText), false
	}
	if port == "" {
		return c, fmt.Errorf("pod port not found in the log '%v'", rawText), false
	}

	c = &ConnectionLog{
		FromIP:     ip,
		FromPort:   port,
		ToHostname: fqdn,
	}

	return c, nil, true
}

// processConnectionLogs processes connection logs
// and prints useful info around connection logs
func (r *Runner) processConnectionLogs() error {
	chans := []chan string{}
	mu := &sync.Mutex{}

	l := len(r.connectionLogs)
	segments := l / r.concurrency
	for i := 0; i < segments; i++ {
		from := i * r.concurrency
		to := (i + 1) * r.concurrency

		c := make(chan string)
		chans = append(chans, c)
		go r.processConnectionLogsSegment(r.connectionLogs[from:to], mu, c)
	}

	if m := l % r.concurrency; m != 0 {
		c := make(chan string)
		chans = append(chans, c)
		go r.processConnectionLogsSegment(r.connectionLogs[l-m:l], mu, c)
	}

	for _, ch := range chans {
		// wait for go routines to finish in order
		<-ch
	}

	return nil
}

// processConnectionLogsSegment processes a segment/piece of logs to distribute work
func (r *Runner) processConnectionLogsSegment(connectionLogsSegment []*ConnectionLog, m *sync.Mutex, ch chan string) error {

	for _, c := range connectionLogsSegment {
		m.Lock()
		err := r.processConnectionLog(c)
		m.Unlock()

		if err != nil {
			log.Error(err)
			ch <- "errored"
			return err
		}

	}
	ch <- "done"
	return nil
}

// processConnectionLog processes a single connection log
func (r *Runner) processConnectionLog(c *ConnectionLog) error {
	var fromPodName string
	var fromNs string
	var found bool

	for _, f := range r.toPodServiceFQDNs {

		if c.ToHostname == f {

			for _, n := range r.allNamespaces.Items {

				for _, e := range r.allEndpoints[n.Name].Items {
					for _, es := range e.Subsets {
						for _, ea := range es.Addresses {
							if ea.IP == c.FromIP && ea.TargetRef.Kind == "Pod" {
								fromPodName = ea.TargetRef.Name
								fromNs = ea.TargetRef.Namespace
								found = true
								break
							}
						}
						if found {
							break
						}
					}
					if found {
						break
					}
				}
				if found {
					break
				}
			}

			if r.hostnamePodMapping[c.ToHostname] == nil {
				r.hostnamePodMapping[c.ToHostname] = []*Mapping{}
			}

			var present bool
			for _, p := range r.hostnamePodMapping[c.ToHostname] {
				if p.podname == fromPodName {
					present = true
					break
				}
			}
			if !present {

				r.hostnamePodMapping[c.ToHostname] = append(r.hostnamePodMapping[c.ToHostname], &Mapping{podname: fromPodName, namespace: fromNs})

				log.Infof("pod: %s, ns: %s via svc: %s\n", fromPodName, fromNs, c.ToHostname)
			}

			break

		}
	}

	return nil
}

// suggestNetPol suggests a NetworkPolicy K8s resource
func (r *Runner) suggestNetPol() error {

	netPolPeers := []networkingv1.NetworkPolicyPeer{}

	fmt.Println("")
	fmt.Println("creating a NetworkPolicy suggestion...")

	// TODO: this code has a lot of loops and duplicate get pod api calls
	for _, mappings := range r.hostnamePodMapping {
		for _, mapping := range mappings {
			fromPod, err := r.clientset.CoreV1().Pods(mapping.namespace).Get(context.Background(), mapping.podname, metav1.GetOptions{})
			if err != nil {
				log.Errorf("couldn't get pod: %w", err)
			}

			l := fromPod.GetLabels()

			for _, ignoredLabel := range ignoredPodLabels {
				delete(l, ignoredLabel)
			}

			var found bool
			for _, netPolPeer := range netPolPeers {
				if reflect.DeepEqual(netPolPeer.PodSelector.MatchLabels, l) {
					found = true
				}
			}

			if !found {
				netPolPeers = append(netPolPeers, networkingv1.NetworkPolicyPeer{
					PodSelector: &metav1.LabelSelector{
						MatchLabels: l,
					},
				})
			}

		}

	}

	toPodLabels := r.toPod.GetLabels()
	for _, ignoredLabel := range ignoredPodLabels {
		delete(toPodLabels, ignoredLabel)
	}

	n := networkingv1.NetworkPolicy{
		TypeMeta: metav1.TypeMeta{
			Kind:       "NetworkPolicy",
			APIVersion: "networking.k8s.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-ingress", r.toPod.Name),
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: toPodLabels,
			},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{
					From: netPolPeers,
				},
			},
		},
	}

	y, err := json.Marshal(n)
	if err != nil {
		return err
	}

	v := map[string]interface{}{}
	err = json.Unmarshal(y, &v)
	if err != nil {
		return err
	}

	// for spacing of 2 chars
	var b bytes.Buffer
	yamlEncoder := yaml.NewEncoder(&b)
	yamlEncoder.SetIndent(2)
	err = yamlEncoder.Encode(&v)
	if err != nil {
		return err
	}

	fmt.Println("")
	fmt.Println("SUGGESTED NetworkPolicy")
	fmt.Println("-----------------------")
	fmt.Printf("%s", string(b.String()))
	return nil
}
