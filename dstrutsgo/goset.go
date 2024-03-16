package dstrutsgo

import "sync"

type set[T string | int | float64] struct{
	data *(map[T]bool)
	smux *sync.Mutex
}

type ISet[T string | int | float64] interface {
	Add(ele T) 
	Delete(ele T) 
	Contains(ele T) (cont bool)
}
func (s *set[T]) Add(element T) {
	s.smux.Lock()
	defer s.smux.Unlock()

	(*s.data)[element] = true
}

func (s *set[T]) Delete(element T) {
	s.smux.Lock()
	defer s.smux.Unlock()

	delete(*s.data, element)
}

func (s *set[T]) Contains(element T) bool {
	_, ok := (*s.data)[element]
	return ok
}