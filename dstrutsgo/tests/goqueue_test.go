package dstrutsgo

import (
	"errors"
	"testing"

	"github.com/rohithputha/hymStMgr/dstrutsgo"
)

func TestGetNewQueue(test *testing.T){
	queue := dstrutsgo.GetNewQueue[int](10)
	if queue.GetSize()!=0 {
		test.Errorf("queue not formed error")
	}
}

func TestPush(test *testing.T){
	queue:= dstrutsgo.GetNewQueue[int](1)
	queue.Push(1)
	if queue.GetSize() !=1 {
		test.Errorf("queue size not same as expected")
	}
}

func TestPushFullQueue(test *testing.T){
	queue:= dstrutsgo.GetNewQueue[int](1)
	queue.Push(1)
	
	if err := queue.Push(2); err==nil || err.Error() != errors.New("queue is full").Error() {
		test.Errorf("queue full error not thrown")
	}
}

func TestPushPushPopCombiniation(test *testing.T){
	queue:= dstrutsgo.GetNewQueue[int](2);
	queue.Push(1)
	queue.Push(2)
	queue.Push(3)
	queue.Pop()
	if queue.GetSize()!=1{
		test.Errorf("queue size not expected after push pop combo")
	}

	queue.Push(4)
	queue.Push(5)
	if queue.GetSize()!=2{
		test.Errorf("queue size not expected after push pop combo")
	}
}

func TestPop(test *testing.T){
	queue:= dstrutsgo.GetNewQueue[int](1)
	ele :=1
	queue.Push(ele)
	acEle,_ := queue.Pop()
	if acEle!= ele{
		test.Errorf("queue pop element not matching the head element")
	}

}

func TestPopEmptyQueue(test *testing.T){
	queue:= dstrutsgo.GetNewQueue[int](1)
	_, err := queue.Pop()
	if err == nil || err.Error() != errors.New("queue is empty").Error(){
		test.Errorf("queue empty error not thrown")
	}
}


func TestGet(test *testing.T){
	queue := dstrutsgo.GetNewQueue[int](3)
	queue.Push(1)
	queue.Push(2)
	queue.Push(3)

	if ele,err := queue.Get(1); err!=nil || ele !=2{
		test.Errorf("queue get not working as expected")
	}
}

func TestGetOutOfBounds(test *testing.T){
	queue := dstrutsgo.GetNewQueue[int](3)
	queue.Push(1)
	queue.Push(2)
	queue.Push(3)
	if _,err := queue.Get(3); err == nil || err.Error() != errors.New("index is beyond the queue size").Error() {
		test.Errorf("out of bounds error not thrown")
	}
}

func TestGetFirst(test *testing.T){
	queue:= dstrutsgo.GetNewQueue[int](2)
	queue.Push(1)
	queue.Push(2)
	if ele, err := queue.GetFirst(); err != nil || ele !=1{
		test.Errorf("get first not working as expected")
	} 
}

func TestGetFirstEmptyQueue(test *testing.T){
	queue:= dstrutsgo.GetNewQueue[int](2)
	if _, err := queue.GetFirst(); err == nil || err.Error() != errors.New("queue is empty").Error(){
		test.Errorf("get first not working as expected")
	} 
}

func TestGetLast(test *testing.T){
	queue:= dstrutsgo.GetNewQueue[int](2)
	queue.Push(1)
	queue.Push(2)
	if ele, err := queue.GetLast(); err != nil || ele !=2{
		test.Errorf("get last not working as expected")
	} 
}

func TestGetLastEmptyQueue(test *testing.T){
	queue:= dstrutsgo.GetNewQueue[int](2)
	if _, err := queue.GetLast(); err == nil || err.Error() != errors.New("queue is empty").Error(){
		test.Errorf("get first not working as expected")
	} 
}

func TestGetSize(test *testing.T){
	queue:= dstrutsgo.GetNewQueue[int](1)
	queue.Push(1)
	queue.Push(2)
	if queue.GetSize() != 1{
		test.Errorf("get size not working as expected")
	}
}

func TestForcePush(test *testing.T){
	queue:= dstrutsgo.GetNewQueue[int](1)
	queue.ForcePush(1)
	if queue.GetSize()!=1{
		test.Errorf("force push not working as expected")
	}
	queue.ForcePush(2)
	if ele,_:= queue.GetFirst(); ele != 2 && queue.GetSize()!=1{
		test.Errorf("force push not working as expected")
	}
}
