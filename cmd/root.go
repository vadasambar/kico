/*
Copyright © 2022 Suraj Banakar surajrbanakar@gmail.com
*/
package cmd

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/vadasambar/kico/pkg/runners/corednsrunner"
	"k8s.io/client-go/tools/clientcmd"
)

const defaultConcurrency = 4
const defaultWaitDurationForLogs = "60s"

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "kico <pod-name>",
	Short: "`kico` shows which pods are connecting to <pod-name>",
	Long: `kico shows which pods are connecting to <pod-name>, prints the labels of such pods and suggests a NetworkPolicy to allow incoming connections to <pod-name>. For example:

$ kico user-db-b8dfb847c-wvkgf -nsock-shop --suggest-netpol
INCOMING CONNECTIONS
--------------------
INFO[0000] pod: user-79dddf5cc9-bzvhd, ns: sock-shop via svc: user-db.sock-shop.svc.cluster.local. 

creating a NetworkPolicy suggestion...

SUGGESTED NetworkPolicy
-----------------------
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  creationTimestamp: null
  name: user-db-b8dfb847c-wvkgf-ingress
spec:
  ingress:
    - from:
        - podSelector:
            matchLabels:
              name: user
  podSelector:
    matchLabels:
      name: user-db
status: {}
`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("args", args)
		if len(args) < 1 || strings.TrimSpace(args[0]) == "" {
			log.Fatal("please provide a pod name")
		}
		ns, err := cmd.Flags().GetString("namespace")
		if err != nil {
			log.Printf("err: %v namespace not provided, defaulting to `default`", err)
		}

		suggestNetPol, err := cmd.Flags().GetBool("suggest-netpol")
		if err != nil {
			log.Printf("err: %v error parsing `suggest-netpol` flag", err)
			log.Printf("defaulting to %v", false)
			suggestNetPol = false
		}

		concurrency, err := cmd.Flags().GetInt("concurrency")
		if err != nil {
			log.Printf("err: %v error parsing `concurrency` flag", err)
			log.Printf("defaulting to %d", defaultConcurrency)
			concurrency = defaultConcurrency
		}

		waitForLogs, err := cmd.Flags().GetString("wait-for-logs")
		if err != nil {
			log.Printf("err: %v error parsing `wait-for-logs` flag", err)
			log.Printf("defaulting to %d", defaultWaitDurationForLogs)
			waitForLogs = defaultWaitDurationForLogs
		}

		waitDuration, err := time.ParseDuration(waitForLogs)
		if err != nil {
			log.Printf("err: %v error parsing time duration specified for `wait-for-logs` flag", err)
			log.Printf("defaulting to %d", defaultWaitDurationForLogs)
			waitDuration = time.Second * 60
		}

		if err := run(args[0], ns, suggestNetPol, concurrency, waitDuration); err != nil {
			log.Fatal(err)
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.kico.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	rootCmd.Flags().StringP("namespace", "n", "", "Namespace where the pod exists (default uses current namespace)")
	rootCmd.Flags().BoolP("suggest-netpol", "s", false, "Suggests a NetworkPolicy if the flag is set (default false)")
	rootCmd.Flags().IntP("concurrency", "c", defaultConcurrency, "Sets concurrency for processing logs")
	rootCmd.Flags().StringP("wait-for-logs", "w", defaultWaitDurationForLogs, "Waits for relevant logs to appear")
}

func run(toPodName string, toPodNamespace string, suggestNetPol bool, concurrency int, waitForLogs time.Duration) error {
	apiConfig, err := clientcmd.NewDefaultClientConfigLoadingRules().Load()
	if err != nil {
		return err
	}

	restConfig, err := clientcmd.NewDefaultClientConfig(*apiConfig, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return err
	}

	if toPodNamespace == "" {

		toPodNamespace = apiConfig.Contexts[apiConfig.CurrentContext].Namespace
		if toPodNamespace == "" {
			toPodNamespace = "default"
		}
	}

	r, err := corednsrunner.Initialize(&corednsrunner.InitConfig{
		ToPodName:            toPodName,
		ToPodNamespace:       toPodNamespace,
		Config:               restConfig,
		SuggestNetworkPolicy: suggestNetPol,
		Concurrency:          concurrency,
		WaitForLogsDuration:  waitForLogs,
	})
	if err != nil {
		return err
	}

	if err := r.Run(); err != nil {
		return err
	}

	return nil
}
