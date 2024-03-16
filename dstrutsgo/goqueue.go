package dstrutsgo

import (
	"errors"
	"sync"
)


type queue[T any] struct{
	data *[]T
	head int
	tail int
	maxqsize int
	qmux *sync.Mutex
}

type IQueue[T any] interface{
	Push(ele T) (err error)
	Pop()(ele T,err error) 
	Get(i int) (ele T, err error)
	collapse() 
}

func GetNewQueue[T any](maxsize int) IQueue[T]{
	d := make([]T, 0, maxsize)
	q :=  queue[T]{
		data: &d,
		head: 0, 
		tail: -1, 
		maxqsize: maxsize,
	}
	return &q
}

func (q *queue[T]) Push(ele T)(err error){
	q.qmux.Lock()
	defer q.qmux.Unlock()
	*q.data = append(*q.data, ele)
	q.tail++
	if len(*q.data)>=q.maxqsize-1 {
		if q.tail - q.head >= q.maxqsize - 1 {
			return errors.New("queue is full")
		}
		q.collapse()	
	}
	return nil
}

func (q *queue[T]) Pop() (ele T, err error){
	q.qmux.Lock()
	defer q.qmux.Unlock()
	if q.head > q.tail{
		var e T
		return e, errors.New("queue is empty")
	}
	val:= (*q.data)[q.head]
	q.head++
	return val, nil
}
func (q *queue[T]) Get(i int) (ele T, err error ){
	q.qmux.Lock()
	defer q.qmux.Unlock()

	if q.head+i>q.tail {
		var e T
		return e, errors.New("index is not beyond the queue size")
	}
	return (*q.data)[q.head+i], nil 
}

func (q *queue[T]) collapse() {
	newData := make([]T,0,q.maxqsize)
	copy(newData, (*q.data)[q.head:q.tail+1])
	q.data = &newData
	q.head = 0
	q.tail = len(newData)-1
	
}