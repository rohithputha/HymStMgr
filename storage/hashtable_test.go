package storage

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestGetExtensibleHashTable(t *testing.T) {
	hashtable := GetExtensibleHashTable[int64, int64]()
	assert.Equal(t, hashtable.GetGlobalDepth(), int64(1))
	assert.Equal(t, hashtable.GetNumBuckets(), int64(2))
}

func TestExtensibleHashTable_Insert(t *testing.T) {
	hashtable := GetExtensibleHashTable[int64, int64]()

	testVal := int64(10)
	testArr := []*int64{&testVal}
	hashtable.Insert(10, &testVal)
	hashtable.Insert(305, &testVal)

	assert.Equal(t, hashtable.Find(10), testArr)
}
func TestExtensibleHashTable_Insert2(t *testing.T) {
	hashtable := GetExtensibleHashTable[int64, int64]()

	testVal := int64(10)
	testArr := []*int64{&testVal}
	for i := 0; i < 12; i++ {
		hashtable.Insert(10, &testVal)
	}
	hashtable.Insert(11, &testVal)
	assert.Equal(t, hashtable.Find(11), testArr)
	assert.Equal(t, hashtable.GetNumBuckets(), int64(2))
}

func TestExtensibleHashTable_Insert3(t *testing.T) {
	hashtable := GetExtensibleHashTable[int64, int64]()

	testVal := int64(10)
	testArr := []*int64{&testVal}
	for i := 0; i < 22; i++ {
		hashtable.Insert(int64(rand.Intn(1000)), &testVal)
	}
	hashtable.Insert(11, &testVal)
	assert.Equal(t, testArr, hashtable.Find(11))
	assert.Equal(t, int64(2), hashtable.GetGlobalDepth())
}
func TestExtensibleHashTable_Find(t *testing.T) {
	hashtable := GetExtensibleHashTable[int64, int64]()
	testVal := int64(10)
	testArr := []*int64{&testVal, &testVal}
	hashtable.Insert(10, &testVal)
	hashtable.Insert(10, &testVal)
	assert.Equal(t, hashtable.Find(10), testArr)
}
