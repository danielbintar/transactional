package transactional

type Consumer interface {
	Run()
	Shutdown()
}
