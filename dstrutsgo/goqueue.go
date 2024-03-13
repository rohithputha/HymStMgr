package dstrutsgo

type Queue[T any] struct{
	data []T
	head int
	tail int
}

type QueueMgr[T any] interface{
	AddQueue(q *Queue[T])
}

type QueueFunc[T any] interface{
	Push(ele T) (err error)
	Pop()(ele T)
	Get(i int) (ele T, err error)
}

func (q *Queue[T]) Push(ele T){
	q.data = append(q.data, ele)
	q.tail++
}
func (q *Queue[T]) Pop() (ele T){
	val:= q.data[q.head]
	q.head++;
	return val
}
func (q *Queue[T]) Get(i int) (ele T){
	return q.data[q.head+i]
}