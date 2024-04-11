package index

import (
	"bytes"
	"encoding/binary"
	"github.com/rohithputha/HymStMgr/constants"
	"github.com/rohithputha/HymStMgr/storage"
)

var btreeInnerPageHeaders = []string{"TotalKV", "ParentId", "NextPtr"}
var btreeLeafPageHeaders = []string{"TotalKV", "ParentId", "PrevPtr", "NextPtr"}

// buffer readers interfaces and logic
// separate buffer reader could be present for different types...?
type bufferReader[T string | int] struct {
	numBytesPerRead int
}
type bufferReaderMgr[T string | int] interface {
	next(b *bytes.Buffer) T
	nextInt(b *bytes.Buffer) int
	nextInt16(b *bytes.Buffer) int16
}

func (br bufferReader[T]) next(buffer *bytes.Buffer) T {
	var n T
	err := binary.Read(buffer, binary.BigEndian, &n)
	if err != nil {
		return n // handles this error...
	}
	return n
}
func (br bufferReader[T]) nextInt(buffer *bytes.Buffer) int {
	var n int
	err := binary.Read(buffer, binary.BigEndian, &n)
	if err != nil {
		return n // handles this error...
	}
	return n
}
func (br bufferReader[T]) nextInt16(buffer *bytes.Buffer) int16 {
	var n int16
	err := binary.Read(buffer, binary.BigEndian, &n)
	if err != nil {
		return n // handles this error...
	}
	return n
}

//-----------------------------------------------------------------

//btree Page wise logic :- innerpage and leafpage

type Value struct {
	pageId int
	slotId int16
}

func initInnerPageValue(pageId int) *Value {
	v := Value{}
	v.pageId = pageId
	v.slotId = -1
	return &v
}

func initLeafPageValue(pageId int, slotId int16) *Value {
	v := Value{}
	v.pageId = pageId
	v.slotId = slotId
	return &v
}

func (v *Value) getValuePid() int {
	return v.pageId
}
func (v *Value) getValueSid() int16 {
	return v.slotId
}

type btreePage[K string | int] struct {
	bp                *storage.BasePage
	additionalHeaders map[string]int
	keys              []K
	values            []*Value
	brK               bufferReader[K]
}

type btreeInnerPage[K string | int] struct {
	btreePage[K] // V is int for inner pages
}

type btreeLeafPage[K string | int] struct {
	btreePage[K] // V is [2]int for leaf pages
}

type btreePageMgr[K string | int] interface {
	decode()
	getPageType() int
	getPageKeys() []K
	getPageValues() []*Value
	getNextPtr() int
	getMaxSize() int
	setKeys(keys []K)
	setValues(values []*Value)

	//page iterator functions
	next(key K) (value *Value, index int)
	isNext() bool
	parent() int
}

func (bpage *btreePage[K]) setKeys(keys []K) {
	bpage.keys = keys
}
func (bpage *btreePage[K]) setValues(values []*Value) {
	bpage.values = values
}

func (bpage *btreePage[K]) getMaxSize() int {
	return bpage.bp.MaxSize
}

func (bpage *btreePage[K]) getNextPtr() int {
	return bpage.additionalHeaders["NextPtr"]
}

func (bpage *btreePage[K]) getPageType() int {
	return bpage.bp.PageType
}

func (bpage *btreePage[K]) getPageKeys() []K {
	return bpage.keys
}

func (bpage *btreePage[K]) getPageValues() []*Value {
	return bpage.values
}

func (bpage *btreeInnerPage[K]) next(key K) (*Value, int) {
	ind := bpage.binarySearchKey(key)
	if ind != -1 {
		return bpage.values[ind], ind
	}
	return initInnerPageValue(bpage.additionalHeaders["NextPtr"]), -1
}

func (bpage *btreeLeafPage[K]) next(key K) (*Value, int) {
	ind := bpage.binarySearchKey(key)
	if ind != -1 {
		return bpage.values[ind], ind
	}
	return nil, -1
}

func (bpage *btreePage[K]) isNext() bool {
	return bpage.bp.PageType == constants.InnerPageType
}

func (bpage *btreePage[K]) parent() int {
	return bpage.additionalHeaders["ParentId"]
}

func (bi *btreeInnerPage[K]) decode() {
	buffer := bytes.NewBuffer(bi.bp.DataArea)
	for _, v := range btreeInnerPageHeaders {
		bi.additionalHeaders[v] = bi.brK.nextInt(buffer)
	}
	// it is assumed that the all the struct elements are declared and made...
	for i := 0; i < bi.additionalHeaders["TotalKV"]; i++ {
		bi.keys = append(bi.keys, bi.brK.next(buffer))
		bi.values = append(bi.values, initInnerPageValue(bi.brK.nextInt(buffer)))
	}
}

func (bi *btreeLeafPage[K]) decode() {
	buffer := bytes.NewBuffer(bi.bp.DataArea[:])
	for _, v := range btreeLeafPageHeaders {
		bi.additionalHeaders[v] = bi.brK.nextInt(buffer)
	}
	// it is assumed that the all the struct elements are declared and made...

	for i := 0; i < bi.additionalHeaders["TotalKV"]; i++ {
		bi.keys = append(bi.keys, bi.brK.next(buffer))
		bi.values = append(bi.values, initLeafPageValue(bi.brK.nextInt(buffer), bi.brK.nextInt16(buffer)))
	}
}

// ------------------------------------------------------------------------

//	btree index logic

type btreeIndexMgr[K string | int] interface {
	requestPage(pageId int) btreePageMgr[K]
	requestNewPage(pageType int) btreePageMgr[K]
	insert(key K, pageId int, slotId int16)
	search(key K) (pageId int, slotId int16) // simple search
	//replacePage(oldPage btreePageMgr[K], newPage btreePageMgr[K])
}

type btreeIndex[K string | int] struct {
	bufPool   *storage.BuffPoolMgrStr
	indexName string
}

func (bti *btreeIndex[K]) requestPage(pageId int) btreePageMgr[K] {
	page, _ := bti.bufPool.FetchPage(pageId)
	page.Decode()
	if page.GetDecodedBasePage().PageType == constants.InnerPageType {
		innerPage := &btreeInnerPage[K]{
			btreePage: btreePage[K]{
				bp:                page.GetDecodedBasePage(),
				additionalHeaders: make(map[string]int),
				keys:              make([]K, 0),
				values:            make([]*Value, 0),
				brK:               bufferReader[K]{8}, // Adjust size based on K type
			},
		}
		return innerPage
	} else if page.GetDecodedBasePage().PageType == constants.LeafPageType {
		leafPage := &btreeLeafPage[K]{
			btreePage: btreePage[K]{
				bp:                page.GetDecodedBasePage(),
				additionalHeaders: make(map[string]int),
				keys:              make([]K, 0),
				values:            make([]*Value, 0),
				brK:               bufferReader[K]{8}, // Adjust size based on K type
			},
		}
		return leafPage
	}
	return nil
}
func (bti *btreeIndex[K]) requestNewPage(pageType int) btreePageMgr[K] {
	panic("Not Implemented requestNewPage")
	return nil
}

func (bti *btreeIndex[K]) iterateTree(page btreePageMgr[K], key K) {
	for page.isNext() {
		if page.getPageType() == constants.InnerPageType {
			value, _ := page.next(key)
			page = bti.requestPage(value.getValuePid())
		}
	}
}

func (bti *btreeIndex[K]) search(key K) (pageId int, slotId int16) {
	rootPageId := bti.getRootPageId()
	page := bti.requestPage(rootPageId)
	bti.iterateTree(page, key)
	val, _ := page.next(key)
	return val.getValuePid(), val.getValueSid()
}

func (bti *btreeIndex[K]) insert(key K, presentPageId int, pageId int, slotId int16) {
	if presentPageId == -1 {
		presentPageId = bti.getRootPageId()
	}
	page := bti.requestPage(presentPageId)
	bti.iterateTree(page, key)
	bti.insertTuple(key, page, pageId, slotId)
}

func (bti *btreeIndex[K]) insertTuple(key K, page btreePageMgr[K], pageId int, slotId int16) {
	_, insertIndex := page.next(key)
	keys := page.getPageKeys()
	vals := page.getPageValues()
	if insertIndex != -1 {
		keys = append(keys[:insertIndex], append([]K{key}, keys[insertIndex:]...)...)
		vals = append(vals[:insertIndex], append([]*Value{initLeafPageValue(pageId, slotId)}, vals[insertIndex:]...)...)
	} else {
		keys = append(keys, key)
		vals = append(vals, initLeafPageValue(pageId, slotId))
	}
	if len(keys) > page.getMaxSize() {
		bti.iterativeSplit(page)
	}
}

func (bti *btreeIndex[K]) iterativeSplit(page btreePageMgr[K]) {
	newPage, splitKey := bti.splitPage(page)
	parentPage := bti.requestPage(page.parent())
	bti.insertTuple(splitKey, parentPage, newPage.getPageType(), -1)
}

func (bti *btreeIndex[K]) splitPage(page btreePageMgr[K]) (newPage btreePageMgr[K], splitKey K) {
	newPage = bti.requestNewPage(page.getPageType())
	keys := page.getPageKeys()
	vals := page.getPageValues()
	keySplit1 := keys[0 : len(keys)/2]
	keysSplit2 := keys[len(keys)/2:]
	valsSplit1 := vals[0 : len(vals)/2]
	valsSplit2 := vals[len(vals)/2:]
	splitKey = keys[len(keys)/2]
	page.setKeys(keySplit1)
	page.setValues(valsSplit1)
	newPage.setKeys(keysSplit2)
	newPage.setValues(valsSplit2)

	// ** need to rewrite additional headers
	return newPage, newPage.getPageKeys()[len(newPage.getPageKeys())-1]
}

// -------------------------------------------------------------------------
// get the index metadata stored somewhere....

func (bti *btreeIndex[K]) getIndexKeyType() string {
	return "int" //** need to implement this
}

func (bti *btreeIndex[K]) getRootPageId() int {
	return 0 //** need to implement this
}

// -----------------------------------------------------------------------

// core binary search logic

func (bpage *btreePage[K]) binarySearchKey(key K) (resIndex int) {
	low := 0
	high := len(bpage.keys)
	resIndex = -1
	for low < high {
		mid := low + (high-low)/2
		if key < bpage.keys[mid] {
			resIndex = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return resIndex
}

func (bpage *btreePage[K]) rangeBinarySearchKey(key K) (resIndex int) {
}
