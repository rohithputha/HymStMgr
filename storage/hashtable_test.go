package storage

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestGetExtensibleHashTable(t *testing.T) {
	hashtable := GetExtensibleHashTable[int, int]()
	assert.Equal(t, hashtable.GetGlobalDepth(), 1)
	assert.Equal(t, hashtable.GetNumBuckets(), 2)
}

func TestExtensibleHashTable_Insert(t *testing.T) {
	hashtable := GetExtensibleHashTable[int, int]()

	testVal := 10
	testArr := []*int{&testVal}
	hashtable.Insert(10, &testVal)
	hashtable.Insert(305, &testVal)

	assert.Equal(t, hashtable.Find(10), testArr)
}
func TestExtensibleHashTable_Insert2(t *testing.T) {
	hashtable := GetExtensibleHashTable[int, int]()

	testVal := 10
	testArr := []*int{&testVal}
	for i := 0; i < 12; i++ {
		hashtable.Insert(10, &testVal)
	}
	hashtable.Insert(11, &testVal)
	assert.Equal(t, hashtable.Find(11), testArr)
	assert.Equal(t, hashtable.GetNumBuckets(), 2)
}

func TestExtensibleHashTable_Insert3(t *testing.T) {
	hashtable := GetExtensibleHashTable[int, int]()

	testVal := 10
	testArr := []*int{&testVal}
	for i := 0; i < 22; i++ {
		hashtable.Insert(rand.Intn(100), &testVal)
	}
	hashtable.Insert(11, &testVal)
	assert.Equal(t, testArr, hashtable.Find(11))
	assert.Equal(t, 2, hashtable.GetGlobalDepth())
}
func TestExtensibleHashTable_Find(t *testing.T) {
	hashtable := GetExtensibleHashTable[int, int]()
	testVal := 10
	testArr := []*int{&testVal, &testVal}
	hashtable.Insert(10, &testVal)
	hashtable.Insert(10, &testVal)
	assert.Equal(t, hashtable.Find(10), testArr)
}
