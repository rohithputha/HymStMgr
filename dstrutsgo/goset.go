package dstrutsgo

import (
	"errors"
	"sync"
)

type set[T string | int | float64] struct{
	data *(map[T]bool)
	smux *sync.Mutex
}


type ISet[T string | int | float64] interface {
	Add(ele T) 
	Delete(ele T) 
	Contains(ele T) (cont bool)
	GetSize() int
	GetAvailableElement() (ele T,err error)
}

func GetNewSet[T string | int | float64] () ISet[T] {
	newMap := make(map[T]bool)
	return &set[T]{
		data: &newMap,
		smux: &sync.Mutex{},
	}
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

func (s *set[T]) GetSize() int {
	return len(*s.data)
}

func (s *set[T]) GetAvailableElement() (ele T,err error){
	if s.GetSize() == 0 {
		return ele, errors.New("set is empty")
	}
	for key := range *s.data {
		return key, nil
	} 
	return ele, errors.New("set is empty")
}