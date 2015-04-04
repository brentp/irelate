package irelate

type IFifo struct {
	cache       chan interface{}
	constructor func() interface{}
}

func NewIFifo(max int, init func() interface{}) *IFifo {
	return &IFifo{make(chan interface{}, max), init}
}

func (s *IFifo) Get() interface{} {
	select {
	case iv := <-s.cache:
		return iv
	default:
		return s.constructor()
	}
}

func (s *IFifo) Put(i interface{}) {
	select {
	case s.cache <- i:
	default:
	}
}
