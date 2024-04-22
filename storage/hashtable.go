package storage

import (
	"fmt"
	"github.com/cespare/xxhash/v2"
	"github.com/rohithputha/HymStMgr/constants"
	"github.com/rohithputha/HymStMgr/utils"
	"math"
	"sync"
)

type HashTableMgr[K string | int64, V any] interface {
	Find(key K) (v []*V)
	Insert(key K, v *V)
	Remove(key K)
}

type ExtensibleHashTableMgr[K string | int64, V any] interface {
	HashTableMgr[K, V]
	GetGlobalDepth() int64
	GetLocalDepth(index int64) int64
	GetNumBuckets() int64
}
type ExtensibleHashTable[K string | int64, V any] struct {
	hashTable   []*bucket[K, V]
	globalDepth int64
	htMux       *sync.RWMutex
}

type kvPair[K string | int64, V any] struct {
	hash int64
	key  K
	val  *V
}

type bucket[K string | int64, V any] struct {
	hash        int64
	localDepth  int64
	bucketArray []*kvPair[K, V]
	keySet      utils.ISet[K]
}

func GetExtensibleHashTable[K string | int64, V any]() ExtensibleHashTableMgr[K, V] {
	initHashTable := []*bucket[K, V]{getNewBucket[K, V](0, 1), getNewBucket[K, V](1, 1)}
	return &ExtensibleHashTable[K, V]{
		hashTable:   initHashTable,
		globalDepth: 1,
		htMux:       &sync.RWMutex{},
	}
}

func getHashValue[K string | int64](key K, depth int64) int64 {
	digest := xxhash.Digest{}
	_, digErr := digest.WriteString(fmt.Sprint(key))
	hashVal := digest.Sum64()
	if digErr != nil {
		return -1
	}
	mask := (1 << depth) - 1
	return int64(hashVal) & int64(mask) // is this an issue?
}

func getNewBucket[K string | int64, V any](hash int64, localDepth int64) *bucket[K, V] {
	return &bucket[K, V]{hash: hash, localDepth: localDepth, bucketArray: make([]*kvPair[K, V], 0), keySet: utils.GetNewSet[K]()}
}

func (eh *ExtensibleHashTable[K, V]) newHashTable(fullBucket *bucket[K, V]) {
	newHashTable := make([]*bucket[K, V], int64(math.Pow(2, float64(eh.globalDepth+1))))
	for oldHash, bucket := range eh.hashTable {
		if int64(oldHash) == fullBucket.hash {
			continue
		}
		newHash1 := oldHash << 1
		newHash2 := (oldHash << 1) | 1
		newHashTable[newHash1] = bucket
		newHashTable[newHash2] = bucket
	}
	eh.hashTable = newHashTable
}

func (eh *ExtensibleHashTable[K, V]) reDistributeFullBucket(fullBucket *bucket[K, V]) {
	presentHash := fullBucket.hash
	newBucket1 := getNewBucket[K, V](presentHash<<1, fullBucket.localDepth+1)
	newBucket2 := getNewBucket[K, V]((presentHash<<1)|1, fullBucket.localDepth+1)
	for _, kvp := range fullBucket.bucketArray {
		newHash := getHashValue[K](kvp.key, fullBucket.localDepth+1)
		if newHash == newBucket1.hash {
			newBucket1.bucketArray = append(newBucket1.bucketArray, kvp)
			newBucket1.keySet.Add(kvp.key)
		} else {
			newBucket2.bucketArray = append(newBucket2.bucketArray, kvp)
			newBucket2.keySet.Add(kvp.key)
		}
	}
	eh.hashTable[presentHash<<1] = newBucket1
	eh.hashTable[(presentHash<<1)|1] = newBucket2
}
func (eh *ExtensibleHashTable[K, V]) Find(key K) (val []*V) {
	eh.htMux.RLock()
	defer eh.htMux.RUnlock()
	hash := getHashValue[K](key, eh.globalDepth)
	bucket := eh.hashTable[hash]
	results := make([]*V, 0)
	for _, kvp := range bucket.bucketArray {
		if kvp.key == key {
			results = append(results, kvp.val)
		}
	}
	return results
}
func (eh *ExtensibleHashTable[K, V]) Insert(key K, val *V) {
	eh.htMux.Lock()
	defer eh.htMux.Unlock()
	hash := getHashValue[K](key, eh.globalDepth)
	insertionBucket := eh.hashTable[hash]
	if insertionBucket.keySet.GetSize() > constants.MaxBucketSize {
		if insertionBucket.localDepth == eh.globalDepth {
			eh.newHashTable(insertionBucket)
			eh.globalDepth += 1
		}
		eh.reDistributeFullBucket(insertionBucket)
		insertionBucket = eh.hashTable[getHashValue[K](key, eh.globalDepth)]
	}
	insertionBucket.bucketArray = append(insertionBucket.bucketArray, &kvPair[K, V]{key: key, val: val})
	insertionBucket.keySet.Add(key)
}

func (eh *ExtensibleHashTable[K, V]) Remove(key K) {
	eh.htMux.Lock()
	defer eh.htMux.Unlock()
	hash := getHashValue[K](key, eh.globalDepth)
	bucket := eh.hashTable[hash]
	for i, kvp := range bucket.bucketArray {
		if kvp.key == key {
			bucket.bucketArray = append(bucket.bucketArray[:i], bucket.bucketArray[i+1:]...)
			bucket.keySet.Delete(key)
			return
		}
	}
}

func (eh *ExtensibleHashTable[K, V]) GetGlobalDepth() int64 {
	eh.htMux.RLock()
	defer eh.htMux.RUnlock()
	return eh.globalDepth
}

func (eh *ExtensibleHashTable[K, V]) GetLocalDepth(index int64) int64 {
	eh.htMux.RLock()
	defer eh.htMux.RUnlock()
	return eh.hashTable[index].localDepth
}

func (eh *ExtensibleHashTable[K, V]) GetNumBuckets() int64 {
	eh.htMux.RLock()
	defer eh.htMux.RUnlock()
	set := utils.GetNewSet[*bucket[K, V]]()
	for _, bucketPointer := range eh.hashTable {
		set.Add(bucketPointer)
	}
	return int64(set.GetSize())
}
