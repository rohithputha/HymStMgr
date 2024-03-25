package dstrutsgo

import (
	"errors"
	"testing"

	"github.com/rohithputha/hymStMgr/dstrutsgo"
)

func TestGetNewSet(test *testing.T){
	set := dstrutsgo.GetNewSet[int]()
	if set.GetSize() != 0 {
		test.Errorf("get new set failed")
	}
}

func TestAdd(test *testing.T){
	set := dstrutsgo.GetNewSet[int]()
	set.Add(1)
	set.Add(1)
	if set.GetSize() != 1 && !set.Contains(1){
		test.Errorf("set add does not work as expected")
	}
}

func TestDelete(test *testing.T){
	set:= dstrutsgo.GetNewSet[int]()
	set.Add(1)
	set.Add(2)
	set.Delete(1)
	if set.GetSize()!=1 && set.Contains(1){
		test.Errorf("set delete does not work as expected")
	}
}

func TestDeleteNonExists(test *testing.T){
	set:= dstrutsgo.GetNewSet[int]()
	set.Delete(1)

	if set.GetSize()!=0{
		test.Errorf("set delete on non existant element does not work as expected")
	}
}

func TestContains(test *testing.T){
	set:= dstrutsgo.GetNewSet[int]()
	set.Add(1)
	if set.Contains(0) || !set.Contains(1){
		test.Errorf("set contains does not work as expected")
	}
}

func TestGetAvailableElement(test *testing.T){
	set:= dstrutsgo.GetNewSet[int]()
	set.Add(1)
	set.Add(2)
	set.Add(0)
	if ele, err := set.GetAvailableElement(); err !=nil  || !set.Contains(ele){
			test.Errorf("get element does not work as expected")

	}
}

func TestGetAvailableElementEmptySet(test *testing.T){
	set:= dstrutsgo.GetNewSet[int]()
	if _, err := set.GetAvailableElement(); err ==nil  || err.Error()!= errors.New("set is empty").Error() {
		
		test.Errorf("get element empty set does not work as expected")
	
}
}