package interfaces

type RunnerInterface interface {
	// Process is for processing raw connection data
	// and printing it in a format that is easy to make sense
	Run() error
}
