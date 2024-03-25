package dstrutsgo

import (
	"errors"

	"sync"
)


type Queue[T any] struct{
	data *[]T
	head int
	tail int
	maxqsize int
	qmux *sync.Mutex
}

type IQueue[T any] interface{
	Push(ele T) (err error)
	ForcePush(ele T)
	Pop()(ele T,err error) 
	Get(i int) (ele T, err error)
	GetFirst() (ele T, err error)
	GetLast() (ele T, err error)
	Update(i int, ele T)(err error)
	GetSize()(size int)
}

func GetNewQueue[T any](maxsize int) IQueue[T]{
	d := make([]T, 0, maxsize)
	q :=  Queue[T]{
		data: &d,
		head: 0, 
		tail: -1, 
		maxqsize: maxsize,
		qmux: &sync.Mutex{},
	}
	return &q
}

func (q *Queue[T]) Push(ele T)(err error){
	q.qmux.Lock()
	defer q.qmux.Unlock()
	if len(*q.data)>=q.maxqsize {
		if q.tail - q.head + 1 >= q.maxqsize  {
			return errors.New("queue is full")
		}
		if q.head!=0 {
			q.collapse()

		}
	}
	*q.data = append(*q.data, ele)
	q.tail++
	return nil
}

func (q *Queue[T]) ForcePush(ele T){
	q.qmux.Lock()
	defer q.qmux.Unlock()

	if len(*q.data)>=q.maxqsize{
		if (q.tail-q.head+1 >= q.maxqsize) {
			q.head++
		}
		q.collapse()
	}
	*q.data = append(*q.data, ele)
	q.tail++
}

func (q *Queue[T]) Pop() (ele T, err error){
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
func (q *Queue[T]) Get(i int) (ele T, err error ){
	q.qmux.Lock()
	defer q.qmux.Unlock()

	if q.head+i>q.tail {
		var e T
		return e, errors.New("index is beyond the queue size")
	}
	return (*q.data)[q.head+i], nil 
}

func (q *Queue[T]) GetFirst()(ele T, err error){
	q.qmux.Lock()
	defer q.qmux.Unlock()

	if q.head > q.tail {
		var e T
		return e, errors.New("queue is empty")
	}
	return (*q.data)[q.head], nil	
}

func (q *Queue[T]) GetLast()(ele T, err error){
	q.qmux.Lock()
	defer q.qmux.Unlock()

	if q.head > q.tail {
		var e T
		return e, errors.New("queue is empty")
	}
	return (*q.data)[q.tail], nil
}

func (q *Queue[T]) Update(i int, ele T)(err error){
	q.qmux.Lock()
	defer q.qmux.Unlock()

	if q.head+i>q.tail {
		return errors.New("index is not beyond the queue size")
	}

	(*q.data)[q.head+i] = ele
	return nil
}
func (q *Queue[T]) getSize() int {
	if q.head > q.tail { //redundant condition but still kept for safety
		return 0
	}
	return q.tail-q.head+1
}

func (q *Queue[T]) GetSize() int {
	q.qmux.Lock()
	defer q.qmux.Unlock()

	return q.getSize()
}

func (q *Queue[T]) collapse() {
	newData := make([]T,q.getSize(),q.maxqsize)

	if q.getSize() > 0{
		copy(newData, (*q.data)[q.head:q.tail+1])
	}
	
	q.data = &newData
	q.head = 0
	q.tail = len(newData)-1
}
