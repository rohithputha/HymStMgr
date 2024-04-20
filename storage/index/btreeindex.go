package index

import (
	"bytes"
	"encoding/binary"
	"github.com/rohithputha/HymStMgr/constants"
	"github.com/rohithputha/HymStMgr/diskmgr"
	"github.com/rohithputha/HymStMgr/storage"
)

var btreeInnerPageHeaders = []string{"TotalKV", "ParentId", "NextPtr"}
var btreeLeafPageHeaders = []string{"TotalKV", "ParentId", "PrevPtr", "NextPtr"}

type customBufferReader[T string | int64] struct {
	numBytesPerCustomRead int64
}
type bufferReaderMgr[T string | int64] interface {
	next(b *bytes.Buffer) T
	nextInt64(b *bytes.Buffer) int64
	nextInt16(b *bytes.Buffer) int16
}

func (br customBufferReader[T]) next(buffer *bytes.Buffer) T {
	var n T
	err := binary.Read(buffer, binary.BigEndian, &n)
	if err != nil {
		panic(err) // handles this error..
	}
	return n
}
func (br customBufferReader[T]) nextInt64(buffer *bytes.Buffer) int64 {
	var n int64
	err := binary.Read(buffer, binary.BigEndian, &n)
	if err != nil {
		return n // handles this error...
	}
	return n
}
func (br customBufferReader[T]) nextInt16(buffer *bytes.Buffer) int16 {
	var n int16
	err := binary.Read(buffer, binary.BigEndian, &n)
	if err != nil {
		return n // handles this error...
	}
	return n
}

type Value struct {
	pageId int64
	slotId int16
}

func initInnerPageValue(pageId int64) *Value {
	v := Value{}
	v.pageId = pageId
	v.slotId = -1
	return &v
}

func initLeafPageValue(pageId int64, slotId int16) *Value {
	v := Value{}
	v.pageId = pageId
	v.slotId = slotId
	return &v
}

func (v *Value) getValuePid() int64 {
	return v.pageId
}
func (v *Value) getValueSid() int16 {
	return v.slotId
}

/*
*************************************************
*	B+ tree base page logic and data access
*************************************************
 */

type btreePage[K string | int64] struct {
	bp                *storage.BasePage
	bpMgr             storage.PageInterface
	additionalHeaders map[string]int64
	keys              []K
	values            []*Value
	brK               customBufferReader[K]
}
type btreeMetaDataAccess[K string | int64] interface {
	getPageType() int64
	getPageKeys() []K
	getPageValues() []*Value
	getMaxSize() int64
	getSize() int64
	getParent() int64
	getPageId() int64
	getPageMetadataInterface() storage.PageInterface

	// experiment
	// have all the key access methods in the interface so that we can keep track of marking the page dirty
	getKey(index int64) K
	getValue(index int64) *Value
	setKey(index int64, key K)
	setValue(index int64, value *Value)
	addKey(index int64, key K)
	addValue(index int64, value *Value)
	deleteKey(index int64)
	deleteValue(index int64)
	updateKey(index int64, key K)
	updateValue(index int64, value *Value)
	appendKey(key K)
	appendValue(value *Value)
	splitKVs(fn func(*btreePage[K]) ([]K, []*Value, K, *Value, int64)) ([]K, []*Value, K, *Value, int64)
	// end experiment

	setKeys(keys []K)
	setValues(values []*Value)
	setSize(int64)
	setAdditionalHeader(string, int64)
}

type btreePageMgr[K string | int64] interface {
	btreeMetaDataAccess[K]
	decode()
	//encode() // ** need to implement this
}

func (bpage *btreePage[K]) setKeys(keys []K) {
	bpage.bpMgr.MarkDirty()
	bpage.keys = keys
}
func (bpage *btreePage[K]) setValues(values []*Value) {
	bpage.bpMgr.MarkDirty()
	bpage.values = values
}

func (bpage *btreePage[K]) setKey(index int64, key K) {
	bpage.bpMgr.MarkDirty()
	bpage.keys[index] = key
}
func (bpage *btreePage[K]) setValue(index int64, value *Value) {
	bpage.bpMgr.MarkDirty()
	bpage.values[index] = value
}

func (bpage *btreePage[K]) addKey(index int64, key K) {
	bpage.bpMgr.MarkDirty()
	if index <= 0 {
		bpage.keys = append([]K{key}, bpage.keys...)
	} else if index >= int64(len(bpage.keys)) {
		bpage.keys = append(bpage.keys, key)
	} else {
		bpage.keys = append(bpage.keys[:index], append([]K{key}, bpage.keys[index:]...)...)
	}
}

func (bpage *btreePage[K]) addValue(index int64, value *Value) {
	bpage.bpMgr.MarkDirty()
	if index <= 0 {
		bpage.values = append([]*Value{value}, bpage.values...)
	} else if index >= int64(len(bpage.values)) {
		bpage.values = append(bpage.values, value)
	} else {
		bpage.values = append(bpage.values[:index], append([]*Value{value}, bpage.values[index:]...)...)
	}
}
func (bpage *btreePage[K]) deleteKey(index int64) {
	bpage.bpMgr.MarkDirty()
	if index < 0 || index >= int64(len(bpage.keys)) {
		return
	}
	bpage.keys = append(bpage.keys[:index], bpage.keys[index+1:]...)
}

func (bpage *btreePage[K]) deleteValue(index int64) {
	bpage.bpMgr.MarkDirty()
	if index < 0 || index >= int64(len(bpage.values)) {
		return
	}
	bpage.values = append(bpage.values[:index], bpage.values[index+1:]...)
}

func (bpage *btreePage[K]) updateKey(index int64, key K) {
	bpage.bpMgr.MarkDirty()
	if index < 0 || index >= int64(len(bpage.keys)) {
		return
	}
	bpage.keys[index] = key
}

func (bpage *btreePage[K]) updateValue(index int64, value *Value) {
	bpage.bpMgr.MarkDirty()
	if index < 0 || index >= int64(len(bpage.values)) {
		return
	}
	bpage.values[index] = value
}

func (bpage *btreePage[K]) appendKey(key K) {
	bpage.bpMgr.MarkDirty()
	bpage.keys = append(bpage.keys, key)
}
func (bpage *btreePage[K]) appendValue(value *Value) {
	bpage.bpMgr.MarkDirty()
	bpage.values = append(bpage.values, value)
}

func (bpage *btreePage[K]) splitKVs(fn func(page *btreePage[K]) ([]K, []*Value, K, *Value, int64)) ([]K, []*Value, K, *Value, int64) {
	bpage.bpMgr.MarkDirty()
	return fn(bpage)
}

func (bpage *btreePage[K]) setSize(newSize int64) {
	bpage.bp.Size = newSize
}
func (bpage *btreePage[K]) setAdditionalHeader(key string, value int64) {
	// ** additional check to make sure the key is present in the additional headers
	bpage.additionalHeaders[key] = value
}

func (bpage *btreePage[K]) getMaxSize() int64 {
	return bpage.bp.MaxSize
}
func (bpage *btreePage[K]) getSize() int64 {
	return bpage.bp.Size
}

func (bpage *btreePage[K]) getPageType() int64 {
	return bpage.bp.PageType
}

func (bpage *btreePage[K]) getPageKeys() []K {
	return bpage.keys
}

func (bpage *btreePage[K]) getPageValues() []*Value {
	return bpage.values
}
func (bpage *btreePage[K]) getKey(index int64) K {
	return bpage.keys[index]
}
func (bpage *btreePage[K]) getValue(index int64) *Value {
	return bpage.values[index]
}

func (bpage *btreePage[K]) getParent() int64 {
	return bpage.bp.ParentPageId
}
func (bpage *btreePage[K]) getPageId() int64 {
	return bpage.bp.PageId
}
func (bpage *btreePage[K]) getPageMetadataInterface() storage.PageInterface {
	return bpage.bpMgr
}

/*
*************************************************
*	B+ tree inner and leaf page logic
*************************************************
 */

type btreeInnerPage[K string | int64] struct {
	btreePage[K] // V is int64 for inner pages
}

type btreeLeafPage[K string | int64] struct {
	btreePage[K] // V is [2]int64 for leaf pages
}

func (bpage *btreeInnerPage[K]) decode() {
	buffer := bytes.NewBuffer(bpage.bp.DataArea)
	for _, v := range btreeInnerPageHeaders {
		bpage.additionalHeaders[v] = bpage.brK.nextInt64(buffer)
	}
	// it is assumed that the all the struct elements are declared and made...
	for i := int64(0); i < bpage.additionalHeaders["TotalKV"]; i++ {
		bpage.keys = append(bpage.keys, bpage.brK.next(buffer))
		bpage.values = append(bpage.values, initInnerPageValue(bpage.brK.nextInt64(buffer)))
	}
}

func (bpage *btreeLeafPage[K]) decode() {
	buffer := bytes.NewBuffer(bpage.bp.DataArea[:])
	for _, v := range btreeLeafPageHeaders {
		bpage.additionalHeaders[v] = bpage.brK.nextInt64(buffer)
	}
	// it is assumed that the all the struct elements are declared and made...
	for i := int64(0); i < bpage.additionalHeaders["TotalKV"]; i++ {
		bpage.keys = append(bpage.keys, bpage.brK.next(buffer))
		bpage.values = append(bpage.values, initLeafPageValue(bpage.brK.nextInt64(buffer), bpage.brK.nextInt16(buffer)))
	}
}

func (bpage *btreeInnerPage[K]) getIterator() innerPageDataNavigator[K] {
	return bpage
}

func (bpage *btreeLeafPage[K]) getIterator() leafPageDataNavigator[K] {
	return bpage
}

/*
*************************************************
*	B+ tree inner and leaf page data navigation
*************************************************
 */
type leafPageDataNavigator[K string | int64] interface {
	search(key K) (values []*Value, indices []int64, foundKey bool, isInNextPage bool)
	lowerBoundSearch(key K) (*Value, int64)
	upperBoundSearch(key K) (*Value, int64)
	nextSib() (pageId int64)
	prevSib() (pageId int64)
	parent() (pageId int64)
}

type innerPageDataNavigator[K string | int64] interface {
	isNext() bool
	search(key K) (values []*Value, indices []int64, foundKey bool)
	lowerBoundSearch(key K) (*Value, int64)
	upperBoundSearch(key K) (*Value, int64)
	parent() (pageId int64)
}

type btreeInnerPageMgr[K string | int64] interface {
	btreePageMgr[K]
	getIterator() innerPageDataNavigator[K]
}
type btreeLeafPageMgr[K string | int64] interface {
	btreePageMgr[K]
	getIterator() leafPageDataNavigator[K]
}

// leaf page data navigation:

func (bpage *btreeLeafPage[K]) search(key K) ([]*Value, []int64, bool, bool) {
	lind := bpage.lowerBinarySearchKey(key)
	if lind == -1 {
		return nil, nil, false, false
	}
	if bpage.keys[lind] != key {
		return []*Value{bpage.values[lind]}, []int64{lind}, false, false
	}
	rind := bpage.higherBinarySearchKey(key)
	values := make([]*Value, 0)
	indices := make([]int64, 0)
	for i := lind; i <= rind; i++ {
		values = append(values, bpage.values[i])
		indices = append(indices, i)
	}
	return values, indices, true, rind == bpage.getSize()-1
}

func (bpage *btreeLeafPage[K]) nextSib() int64 {
	return bpage.additionalHeaders["NextPtr"]
}
func (bpage *btreeLeafPage[K]) prevSib() int64 {
	return bpage.additionalHeaders["PrevPtr"]
}

func (bpage *btreeLeafPage[K]) parent() int64 {
	return bpage.additionalHeaders["ParentId"]
}

func (bpage *btreeLeafPage[K]) lowerBoundSearch(key K) (*Value, int64) {
	lind := bpage.lowerBinarySearchKey(key)
	if lind == -1 {
		return nil, -1
	}
	return bpage.values[lind], lind
}
func (bpage *btreeLeafPage[K]) upperBoundSearch(key K) (*Value, int64) {
	rind := bpage.higherBinarySearchKey(key)
	if rind == -1 {
		return nil, -1
	}
	return bpage.values[rind], rind
}

// inner page data navigation:

func (bpage *btreeInnerPage[K]) search(key K) ([]*Value, []int64, bool) {
	lind := bpage.lowerBinarySearchKey(key)
	if lind == -1 {
		return []*Value{initInnerPageValue(bpage.additionalHeaders["NextPtr"])}, []int64{-1}, false
	}
	if bpage.keys[lind] != key {
		return []*Value{bpage.values[lind]}, []int64{lind}, false
	}
	rind := bpage.higherBinarySearchKey(key)
	values := make([]*Value, 0)
	indices := make([]int64, 0)
	for i := lind; i <= rind; i++ {
		values = append(values, bpage.values[i])
		indices = append(indices, i)
	}
	return values, indices, true
}

func (bpage *btreeInnerPage[K]) isNext() bool {
	return bpage.bp.PageType == constants.InnerPageType
}

func (bpage *btreeInnerPage[K]) parent() int64 {
	return bpage.additionalHeaders["ParentId"]
}

func (bpage *btreeInnerPage[K]) lowerBoundSearch(key K) (*Value, int64) {
	lind := bpage.lowerBinarySearchKey(key)
	if lind == -1 {
		return initInnerPageValue(bpage.additionalHeaders["NextPtr"]), -1
	}
	return bpage.values[lind], lind
}
func (bpage *btreeInnerPage[K]) upperBoundSearch(key K) (*Value, int64) {
	rind := bpage.higherBinarySearchKey(key)
	if rind == -1 {
		return initInnerPageValue(bpage.additionalHeaders["NextPtr"]), -1
	}
	return bpage.values[rind], rind
}

/*
*************************************************
Leaf values iterator
*************************************************
*/

type iterator[K string | int64] interface {
	requestPage(pageId int64) btreeLeafPageMgr[K]
	next() (val *Value)
	prev() (val *Value)
	present() (pageId int64, index int64)
}
type coreIterator[K string | int64] struct {
	bufPool     *storage.BuffPoolMgrStr
	leafPage    btreeLeafPageMgr[K]
	leafPageNav leafPageDataNavigator[K]
}
type iteratorState[K string | int64] struct {
	coreIterator[K]
	pageId int64
	index  int64
}

func (lState *iteratorState[K]) next() *Value {
	if lState.index == lState.leafPage.getSize()-1 {
		lState.pageId = lState.leafPageNav.nextSib()
		if lState == nil || lState.pageId == -1 {
			return nil
		}
		lState.leafPage = lState.requestPage(lState.pageId)
		lState.leafPageNav = lState.leafPage.getIterator()
		lState.index = 0
	} else {
		lState.index++
	}
	return lState.leafPage.getValue(lState.index)
}

func (lState *iteratorState[K]) prev() *Value {
	if lState.index == 0 {
		lState.pageId = lState.leafPageNav.prevSib()
		if lState == nil || lState.pageId == -1 {
			return nil
		}
		lState.leafPage = lState.requestPage(lState.pageId)
		lState.leafPageNav = lState.leafPage.getIterator()
		lState.index = lState.leafPage.getSize() - 1
	} else {
		lState.index--
	}
	return lState.leafPage.getValue(lState.index)
}

func (lState *iteratorState[K]) present() (int64, int64) {
	return lState.pageId, lState.index
}

func (lState *iteratorState[K]) requestPage(pageId int64) btreeLeafPageMgr[K] {
	page, readError := lState.bufPool.FetchPage(pageId)
	if readError != nil {
		return nil
	}
	page.Decode()
	if page.GetDecodedBasePage().PageId == constants.InnerPageType {
		return nil
	}
	leafPage := &btreeLeafPage[K]{
		btreePage: btreePage[K]{
			bp:                page.GetDecodedBasePage(),
			bpMgr:             page,
			additionalHeaders: make(map[string]int64),
			keys:              make([]K, 0),
			values:            make([]*Value, 0),
			brK:               customBufferReader[K]{8}, // Adjust size based on K type
		},
	}
	leafPage.decode()
	return leafPage
}

func getIterator[K string | int64](page btreeLeafPageMgr[K], bufPool *storage.BuffPoolMgrStr, index int64) iterator[K] {
	return &iteratorState[K]{
		coreIterator[K]{bufPool, page, page.getIterator()},
		page.getPageId(),
		index}
}

/*
********************************************************************
	B+ tree index logic
********************************************************************
*/

type btreeIndexMgr[K string | int64] interface {
	requestPage(pageId int64) btreePageMgr[K]
	requestNewPage(pageType int64) btreePageMgr[K]
	insert(key K, presentPageId int64, pageId int64, slotId int16)
	search(key K) []*Value // simple search
	rangeSearch(lKey K, rKey K) []*Value
}

type btreeIndex[K string | int64] struct {
	bufPool   *storage.BuffPoolMgrStr
	indexName string
}

func (bti *btreeIndex[K]) getBtreePage(page *storage.Page) btreePageMgr[K] {
	page.Decode()
	if page.GetDecodedBasePage().PageType == constants.InnerPageType {
		innerPage := &btreeInnerPage[K]{
			btreePage: btreePage[K]{
				bp:                page.GetDecodedBasePage(),
				bpMgr:             page,
				additionalHeaders: make(map[string]int64),
				keys:              make([]K, 0),
				values:            make([]*Value, 0),
				brK:               customBufferReader[K]{8}, // Adjust size based on K type
			},
		}
		innerPage.decode()
		return innerPage
	} else if page.GetDecodedBasePage().PageType == constants.LeafPageType {
		leafPage := &btreeLeafPage[K]{
			btreePage: btreePage[K]{
				bp:                page.GetDecodedBasePage(),
				bpMgr:             page,
				additionalHeaders: make(map[string]int64),
				keys:              make([]K, 0),
				values:            make([]*Value, 0),
				brK:               customBufferReader[K]{8}, // Adjust size based on K type
			},
		}
		leafPage.decode()
		return leafPage
	}
	return nil
}

func (bti *btreeIndex[K]) requestPage(pageId int64) btreePageMgr[K] {
	page, _ := bti.bufPool.FetchPage(pageId)
	return bti.getBtreePage(page)
}
func (bti *btreeIndex[K]) requestNewPage(pageType int64) btreePageMgr[K] {
	//panic("Not Implemented requestNewPage")
	page, _ := bti.bufPool.NewPage()
	return bti.getBtreePage(page)
}

func (bti *btreeIndex[K]) resetPageMetaData(page btreePageMgr[K]) {
	page.setSize(int64(len(page.getPageKeys())))

}

func (bti *btreeIndex[K]) search(key K) []*Value {
	rootPageId := bti.getRootPageId()
	page := bti.requestPage(rootPageId)
	var innerPageNav innerPageDataNavigator[K]
	var leafPageNav leafPageDataNavigator[K]
	if page.getPageType() == constants.InnerPageType {
		var ok = true
		iterator := page.(btreeInnerPageMgr[K]).getIterator()
		innerPageNav, ok = iterator.(innerPageDataNavigator[K])
		for ok && innerPageNav.isNext() {
			values, _, _ := innerPageNav.search(key)
			page = bti.requestPage(values[0].getValuePid())
			innerPageNav = page.(btreeInnerPageMgr[K]).getIterator()
		}
	}
	leafPageNav = page.(btreeLeafPageMgr[K]).getIterator() // ** do we have to use ok and add an additional check for ok?
	values, _, foundKey, isInNextPage := leafPageNav.search(key)
	if !foundKey {
		return nil
	}
	var nextVals []*Value
	for isInNextPage {
		page = bti.requestPage(leafPageNav.nextSib())
		leafPageNav = page.(btreeLeafPageMgr[K]).getIterator()
		nextVals, _, foundKey, isInNextPage = leafPageNav.search(key)
		if !foundKey {
			break
		}
		values = append(values, nextVals...) // do we return a copy of the values
	}
	return values
}

func (bti *btreeIndex[K]) rangeSearch(lKey K, rKey K) []*Value {
	rootPageId := bti.getRootPageId()
	rootPage := bti.requestPage(rootPageId)
	lpage := rootPage
	rpage := rootPage
	var innerPageNav innerPageDataNavigator[K]
	if lpage.getPageType() == constants.InnerPageType {
		var ok = true
		iterator := lpage.(btreeInnerPageMgr[K]).getIterator()
		innerPageNav, ok = iterator.(innerPageDataNavigator[K])
		for ok && innerPageNav.isNext() {
			value, _ := innerPageNav.lowerBoundSearch(lKey)
			lpage = bti.requestPage(value.getValuePid())
			innerPageNav = rpage.(btreeInnerPageMgr[K]).getIterator() // this might give an error at casting....
		}
	}
	if rpage.getPageType() == constants.InnerPageType {
		var ok = true
		iterator := rpage.(btreeInnerPageMgr[K]).getIterator()
		innerPageNav, ok = iterator.(innerPageDataNavigator[K])
		for ok && innerPageNav.isNext() {
			value, _ := innerPageNav.upperBoundSearch(rKey)
			rpage = bti.requestPage(value.getValuePid())
			innerPageNav = rpage.(btreeInnerPageMgr[K]).getIterator() // this might give an error at casting....
		}
	}
	// why is the second range search required?
	// we can just do a search on the left page and then keep on traversing the next pointers until we reach the right page? WIP...
	leftLeafPageNav := lpage.(btreeLeafPageMgr[K]).getIterator()
	rightLeafPageNav := rpage.(btreeLeafPageMgr[K]).getIterator()
	_, lIndex := leftLeafPageNav.lowerBoundSearch(lKey)
	_, rIndex := rightLeafPageNav.upperBoundSearch(rKey)
	values := make([]*Value, 0)
	iterator := getIterator[K](lpage.(btreeLeafPageMgr[K]), bti.bufPool, lIndex)
	presentPageId, presentIndex := iterator.present()
	for presentPageId != rpage.getPageId() || presentIndex != rIndex {
		values = append(values, iterator.next())
		presentPageId, presentIndex = iterator.present()
	}
	return values
}

func (bti *btreeIndex[K]) insert(key K, presentPageId int64, pageId int64, slotId int16) {
	if presentPageId == -1 {
		presentPageId = bti.getRootPageId()
	}
	stack := []*Value{initInnerPageValue(presentPageId)}
	var backupPage btreePageMgr[K] = nil
	for len(stack) > 0 {
		page := bti.requestPage(stack[len(stack)-1].getValuePid())
		stack = stack[:len(stack)-1]
		if page.getPageType() == constants.InnerPageType {
			innerPageNav := page.(btreeInnerPageMgr[K]).getIterator()
			values, _, _ := innerPageNav.search(key)
			stack = append(stack, values...)
		}
		if page.getPageType() == constants.LeafPageType {
			if page.getSize()+1 <= page.getMaxSize() {
				bti.insertTuple(key, initLeafPageValue(pageId, slotId), page)
				return
			} else {
				if backupPage != nil {
					backupPage = page
				}
				stack = stack[:len(stack)-1]
			}
		}
		if backupPage != nil {
			bti.insertTuple(key, initLeafPageValue(pageId, slotId), backupPage)
		}
		// *** add another method call to change the appropriate meta data
		return
	}
}

func (bti *btreeIndex[K]) insertTuple(key K, value *Value, page btreePageMgr[K]) {
	if page.getPageType() == constants.InnerPageType {
		innerPageNav := page.(innerPageDataNavigator[K])
		_, indices, _ := innerPageNav.search(key)
		if indices[0] == -1 {
			page.appendKey(key)
			page.appendValue(value)
		} else {
			insertIndex := indices[0]
			page.addKey(insertIndex, key)
			page.addValue(insertIndex, value)
		}
	} else if page.getPageType() == constants.LeafPageType {
		leafPageNav := page.(leafPageDataNavigator[K])
		_, indices, _, _ := leafPageNav.search(key)
		if indices == nil || indices[0] == -1 {
			page.appendKey(key)
			page.appendValue(value)
		}
		insertIndex := indices[0]
		page.addKey(insertIndex, key)
		page.addValue(insertIndex, value)
	}
	if page.getSize() > page.getMaxSize() {
		bti.iterativeSplit(page)
	}
}

func (bti *btreeIndex[K]) iterativeSplit(page btreePageMgr[K]) {
	var newPageId int64
	var splitKey K
	if page.getPageType() == constants.InnerPageType {
		newPageId, splitKey = bti.splitInnerPage(page.(btreeInnerPageMgr[K]))
		// when the inner page is split we need to take care of the change in the parent pointers of the child pages
	} else if page.getPageType() == constants.LeafPageType {
		// when the leaf page is split, we need to make sure that the search and prev pointers are set correctly
		newPageId, splitKey = bti.splitLeafPage(page.(btreeLeafPageMgr[K]))
	}
	var parentPage btreePageMgr[K]
	if page.getParent() == -1 {
		parentPage = bti.requestNewPage(constants.InnerPageType) // request New page should also take the parentId as an input?
		parentPage.setAdditionalHeader("NextPtr", page.getPageId())
	} else {
		parentPage = bti.requestPage(page.getParent())
	}
	bti.insertTuple(splitKey, initInnerPageValue(newPageId), parentPage)
}

func (bti *btreeIndex[K]) splitInnerPage(page btreeInnerPageMgr[K]) (newPageId int64, splitKey K) {
	newPage := bti.requestNewPage(page.getPageType())
	splitFunc := func(page *btreePage[K]) ([]K, []*Value, K, *Value, int64) {
		midKeyIndex := int64(len(page.keys)) / 2
		midKey := page.keys[midKeyIndex]
		midVal := page.values[midKeyIndex]
		keysSplit1 := page.keys[0 : midKeyIndex-1]
		valSplit1 := page.values[0 : midKeyIndex-1]
		keysSplit2 := page.keys[midKeyIndex+1:]
		valSplit2 := page.values[midKeyIndex+1:]
		page.setKeys(keysSplit2)
		page.setValues(valSplit2)
		page.setSize(int64(len(keysSplit2)))
		return keysSplit1, valSplit1, midKey, midVal, midKeyIndex
	}
	keysSplit1, valSplit1, midKey, midVal, midKeyIndex := page.splitKVs(splitFunc)
	newPage.setKeys(keysSplit1)
	newPage.setValues(valSplit1)
	newPage.setAdditionalHeader("NextPtr", midVal.getValuePid())
	bti.reDistributeChild(page, newPage.getPageId(), midKeyIndex)
	newPage.setSize(int64(len(keysSplit1)))
	return newPage.getPageId(), midKey
}

func (bti *btreeIndex[K]) reDistributeChild(oldPage btreePageMgr[K], newPageId int64, splitIndex int64) {
	for i := int64(0); i < splitIndex; i++ {
		childPage := bti.requestPage(oldPage.getPageValues()[i].getValuePid())
		childPage.setAdditionalHeader("ParentId", newPageId)
	}
}

func (bti *btreeIndex[K]) splitLeafPage(page btreeLeafPageMgr[K]) (newPageId int64, splitKey K) {
	newPage := bti.requestNewPage(page.getPageType()) // check the above comment?
	splitFunc := func(page *btreePage[K]) ([]K, []*Value, K, *Value, int64) {
		keysSplit1 := page.keys[0 : len(page.keys)/2]
		keysSplit2 := page.keys[len(page.keys)/2:]
		valsSplit1 := page.values[0 : len(page.values)/2]
		valsSplit2 := page.values[len(page.values)/2:]
		page.setKeys(keysSplit2)
		page.setValues(valsSplit2)
		page.setSize(int64(len(keysSplit2)))
		return keysSplit1, valsSplit1, keysSplit2[0], valsSplit2[0], 0
	}
	keysSplit1, valsSplit1, splitKey, _, _ := page.splitKVs(splitFunc)
	newPage.setKeys(keysSplit1)
	newPage.setValues(valsSplit1)
	leafPageNav := page.getIterator()
	newPage.setAdditionalHeader("PrevPtr", leafPageNav.prevSib())
	newPage.setAdditionalHeader("NextPtr", page.getPageId())
	page.setAdditionalHeader("PrevPtr", newPage.getPageId())

	newPage.setSize(int64(len(keysSplit1)))
	return newPage.getPageId(), splitKey
}

func (bti *btreeIndex[K]) deleteTuple(index int64, page btreeLeafPageMgr[K]) {
	page.getPageMetadataInterface().MarkDirty()
	keys := page.getPageKeys()
	vals := page.getPageValues()
	keys = append(keys[:index], keys[index+1:]...)
	vals = append(vals[:index], vals[index+1:]...)
	page.setKeys(keys)
	page.setValues(vals)

	if index == int64(len(keys)) {
		parentPage := bti.requestPage(page.getParent())
		parentPage.getPageMetadataInterface().MarkDirty()
		parentKeys := parentPage.getPageKeys()
		parentKeys[len(parentKeys)-1] = keys[len(keys)-1]
	}
	if page.getSize() < page.getMaxSize()/2 {
		if page.getPageType() == constants.LeafPageType {
			bti.Merge(page)
		}
	}
}
func (bti *btreeIndex[K]) Merge(page btreeLeafPageMgr[K]) {
	borrowSuccess := bti.borrowFromLeafSib(page)
	if !borrowSuccess {
		bti.mergeLeaf(page, bti.requestPage(page.getIterator().nextSib()).(btreeLeafPageMgr[K]))
	}
}

func (bti *btreeIndex[K]) borrowFromLeafSib(page btreeLeafPageMgr[K]) bool {
	leftLeafPage := bti.requestPage(page.getIterator().prevSib())
	rightLeafPage := bti.requestPage(page.getIterator().nextSib())
	if leftLeafPage.getSize()+1 > leftLeafPage.getMaxSize()/2 {
		bti.borrowLeaf(leftLeafPage, page)
		return true
	} else if rightLeafPage.getSize()+1 > rightLeafPage.getMaxSize()/2 {
		bti.borrowLeaf(page, rightLeafPage)
		return true
	}
	return false
}

func (bti *btreeIndex[K]) borrowLeaf(leftLeafPage btreePageMgr[K], rightLeafPage btreePageMgr[K]) {
	leftLeafPageKeys := leftLeafPage.getPageKeys()
	leftLeafPageValues := leftLeafPage.getPageValues()
	borrowKey := leftLeafPageKeys[len(leftLeafPageKeys)-1]
	borrowValue := leftLeafPageValues[len(leftLeafPageValues)-1]
	leftLeafPageKeys = leftLeafPageKeys[:len(leftLeafPageKeys)-1]
	leftLeafPageValues = leftLeafPageValues[:len(leftLeafPageValues)-1]
	leftLeafPage.setKeys(leftLeafPageKeys)
	leftLeafPage.setValues(leftLeafPageValues)
	pagesKeys := rightLeafPage.getPageKeys()
	pageValues := rightLeafPage.getPageValues()
	pagesKeys = append([]K{borrowKey}, pagesKeys...)
	pageValues = append([]*Value{borrowValue}, pageValues...)
	rightLeafPage.setKeys(pagesKeys)
	rightLeafPage.setValues(pageValues)
	leftParent := bti.requestPage(leftLeafPage.getParent())
	leftParentKeys := leftParent.getPageKeys()
	leftParentValues := leftParent.getPageValues()
	for i := 0; i < len(leftParentKeys); i++ {
		if leftParentKeys[i] == borrowKey && leftParentValues[i].getValuePid() == leftLeafPage.getPageId() {
			leftParentKeys[i] = leftLeafPageKeys[len(leftLeafPageKeys)-1]
			leftParentValues[i] = leftLeafPageValues[len(leftLeafPageValues)-1]
		}
	}
	leftParent.setKeys(leftParentKeys)
	leftParent.setValues(leftParentValues)
}

func (bti *btreeIndex[K]) mergeLeaf(mainPage btreeLeafPageMgr[K], sibPage btreeLeafPageMgr[K]) {
	mainPageKeys := mainPage.getPageKeys()
	mainPageValues := mainPage.getPageValues()
	sibPageKeys := sibPage.getPageKeys()
	sibPageValues := sibPage.getPageValues()
	mainPageKeys = append(mainPageKeys, sibPageKeys...)
	mainPageValues = append(mainPageValues, sibPageValues...)
	mainPage.setKeys(mainPageKeys)
	mainPage.setValues(mainPageValues)
	mainPage.setAdditionalHeader("NextPtr", sibPage.getIterator().nextSib())
	//bti.deleteTuple(sibPage.getPageId(), bti.requestPage(sibPage.getIterator().parent())) -> this triggers a recursive delete and merge. avoided for now... noted as an issue
	// just a normal delete on the parent is performed
	sibParent := bti.requestPage(sibPage.getParent())
	sibParentKeys := sibParent.getPageKeys()
	sibParentValues := sibParent.getPageValues()
	for i := 0; i < len(sibParentKeys); i++ {
		if sibParentValues[i].getValuePid() == sibPage.getPageId() {
			sibParent.deleteKey(int64(i))
			sibParent.deleteValue(int64(i))
		}
	}
}

// -------------------------------------------------------------------------
// get the index metadata stored somewhere....

func (bti *btreeIndex[K]) getIndexKeyType() string {
	return "int64" //** need to implement this
}

func (bti *btreeIndex[K]) getRootPageId() int64 {
	return 0 //** need to implement this
}

// -----------------------------------------------------------------------

// core binary search logic

func (bpage *btreePage[K]) lowerBinarySearchKey(key K) (resIndex int64) {
	low := int64(0)
	high := int64(len(bpage.keys))
	resIndex = -1
	for low <= high {
		mid := int64(low + (high-low)/2)
		if key <= bpage.keys[mid] {
			resIndex = mid
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return resIndex
}

func (bpage *btreePage[K]) higherBinarySearchKey(key K) (resIndex int64) {
	low := int64(0)
	high := int64(len(bpage.keys))
	resIndex = -1
	for low <= high {
		mid := int64(low + (high-low)/2)
		if key < bpage.keys[mid] {
			high = mid - 1
		} else {
			resIndex = mid
			low = mid + 1
		}
	}
	return resIndex
}

func getBtreeIndexMgr[K string | int64]() btreeIndexMgr[K] {
	return &btreeIndex[K]{
		bufPool: storage.InitBuffPoolMgr(diskmgr.DiskFileInit{
			DbFilePath:  "dbtest.db",
			LogFilePath: "dblogtest.log",
		}),
		indexName: "btree",
	}
}
