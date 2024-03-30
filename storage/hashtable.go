package storage

type HashTableMgr[K any, V any] interface {
	Find(key K) (v *V)
	Insert(key K, v *V)
	Remove(key K)
}

type ExtensibleHashTableMgr[K any, V any] interface {
	HashTableMgr[K, V]
	GetGlobalDepth() int
	GetLocalDepth(index int) int
	GetNumBuckets() int
}

type bucket[K string | int, V any] struct {
	bucketMap map[K]*V
}

type ExtensibleHashTable struct {
}
