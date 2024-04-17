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

type customBufferReader[T string | int] struct {
	numBytesPerCustomRead int
}
type bufferReaderMgr[T string | int] interface {
	next(b *bytes.Buffer) T
	nextInt(b *bytes.Buffer) int
	nextInt16(b *bytes.Buffer) int16
}

func (br customBufferReader[T]) next(buffer *bytes.Buffer) T {
	var n T
	err := binary.Read(buffer, binary.BigEndian, &n)
	if err != nil {
		return n // handles this error...
	}
	return n
}
func (br customBufferReader[T]) nextInt(buffer *bytes.Buffer) int {
	var n int
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
	bpMgr             storage.PageInterface
	additionalHeaders map[string]int
	keys              []K
	values            []*Value
	brK               customBufferReader[K]
}

type btreeInnerPage[K string | int] struct {
	btreePage[K] // V is int for inner pages
}

type btreeLeafPage[K string | int] struct {
	btreePage[K] // V is [2]int for leaf pages
}

type leafPageIterator[K string | int] interface {
	search(key K) (values []*Value, indices []int, foundKey bool, isInNextPage bool)
	//leftSearch(key K) (value *Value, index int, foundKey bool)
	//rightSearch(key K) (value *Value, index int, foundKey bool)
	nextSib() (pageId int)
	prevSib() (pageId int)
	parent() (pageId int)
}

type innerPageIterator[K string | int] interface {
	isNext() bool
	search(key K) (values []*Value, indices []int, foundKey bool)
	parent() (pageId int)
}

type btreeMetaDataAccess[K string | int] interface {
	getPageType() int
	getPageKeys() []K
	getPageValues() []*Value
	getMaxSize() int
	getSize() int
	getParent() int
	getPageId() int
	getPageMetadataInterface() storage.PageInterface

	// experiment
	// have all the key access methods in the interface so that we can keep track of marking the page dirty
	getKey(index int) K
	getValue(index int) *Value
	setKey(index int, key K)
	setValue(index int, value *Value)
	addKey(index int, key K)
	addValue(index int, value *Value)
	deleteKey(index int)
	deleteValue(index int)
	updateKey(index int, key K)
	updateValue(index int, value *Value)
	appendKey(key K)
	appendValue(value *Value)
	splitKVs(fn func(*btreePage[K]) ([]K, []*Value, K, *Value, int)) ([]K, []*Value, K, *Value, int)
	//splitKvsMidExld() ([]K, []*Value, K)
	// end experiment

	setKeys(keys []K)
	setValues(values []*Value)
	setSize(int)
	setAdditionalHeader(string, int)
}

type btreePageMgr[K string | int] interface {
	btreeMetaDataAccess[K]
	decode()
	//encode() // ** need to implement this
}

type btreeInnerPageMgr[K string | int] interface {
	btreePageMgr[K]
	getIterator() innerPageIterator[K]
}
type btreeLeafPageMgr[K string | int] interface {
	btreePageMgr[K]
	getIterator() leafPageIterator[K]
}

func (bpage *btreeLeafPage[K]) search(key K) ([]*Value, []int, bool, bool) {
	lind := bpage.lowerBinarySearchKey(key)
	if lind == -1 {
		return nil, nil, false, false
	}
	if bpage.keys[lind] != key {
		return []*Value{bpage.values[lind]}, []int{lind}, false, false
	}
	rind := bpage.higherBinarySearchKey(key)
	values := make([]*Value, 0)
	indices := make([]int, 0)
	for i := lind; i <= rind; i++ {
		values = append(values, bpage.values[i])
		indices = append(indices, i)
	}
	return values, indices, true, rind == bpage.getSize()-1
}

func (bpage *btreeLeafPage[K]) nextSib() int {
	return bpage.additionalHeaders["NextPtr"]
}
func (bpage *btreeLeafPage[K]) prevSib() int {
	return bpage.additionalHeaders["PrevPtr"]
}

func (bpage *btreeLeafPage[K]) parent() int {
	return bpage.additionalHeaders["ParentId"]
}

func (bpage *btreeInnerPage[K]) search(key K) ([]*Value, []int, bool) {
	lind := bpage.lowerBinarySearchKey(key)
	if lind == -1 {
		return []*Value{initInnerPageValue(bpage.additionalHeaders["NextPtr"])}, []int{-1}, false
	}
	if bpage.keys[lind] != key {
		return []*Value{bpage.values[lind]}, []int{lind}, false
	}
	rind := bpage.higherBinarySearchKey(key)
	values := make([]*Value, 0)
	indices := make([]int, 0)
	for i := lind; i <= rind; i++ {
		values = append(values, bpage.values[i])
		indices = append(indices, i)
	}
	return values, indices, true
}

func (bpage *btreePage[K]) setKeys(keys []K) {
	bpage.bpMgr.MarkDirty()
	bpage.keys = keys
}
func (bpage *btreePage[K]) setValues(values []*Value) {
	bpage.bpMgr.MarkDirty()
	bpage.values = values
}

func (bpage *btreePage[K]) setKey(index int, key K) {
	bpage.bpMgr.MarkDirty()
	bpage.keys[index] = key
}
func (bpage *btreePage[K]) setValue(index int, value *Value) {
	bpage.bpMgr.MarkDirty()
	bpage.values[index] = value
}

func (bpage *btreePage[K]) addKey(index int, key K) {
	bpage.bpMgr.MarkDirty()
	if index <= 0 {
		bpage.keys = append([]K{key}, bpage.keys...)
	} else if index >= len(bpage.keys) {
		bpage.keys = append(bpage.keys, key)
	} else {
		bpage.keys = append(bpage.keys[:index], append([]K{key}, bpage.keys[index:]...)...)
	}
}

func (bpage *btreePage[K]) addValue(index int, value *Value) {
	bpage.bpMgr.MarkDirty()
	if index <= 0 {
		bpage.values = append([]*Value{value}, bpage.values...)
	} else if index >= len(bpage.values) {
		bpage.values = append(bpage.values, value)
	} else {
		bpage.values = append(bpage.values[:index], append([]*Value{value}, bpage.values[index:]...)...)
	}
}
func (bpage *btreePage[K]) deleteKey(index int) {
	bpage.bpMgr.MarkDirty()
	if index < 0 || index >= len(bpage.keys) {
		return
	}
	bpage.keys = append(bpage.keys[:index], bpage.keys[index+1:]...)
}

func (bpage *btreePage[K]) deleteValue(index int) {
	bpage.bpMgr.MarkDirty()
	if index < 0 || index >= len(bpage.values) {
		return
	}
	bpage.values = append(bpage.values[:index], bpage.values[index+1:]...)
}

func (bpage *btreePage[K]) updateKey(index int, key K) {
	bpage.bpMgr.MarkDirty()
	if index < 0 || index >= len(bpage.keys) {
		return
	}
	bpage.keys[index] = key
}

func (bpage *btreePage[K]) updateValue(index int, value *Value) {
	bpage.bpMgr.MarkDirty()
	if index < 0 || index >= len(bpage.values) {
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

func (bpage *btreePage[K]) splitKVs(fn func(page *btreePage[K]) ([]K, []*Value, K, *Value, int)) ([]K, []*Value, K, *Value, int) {
	bpage.bpMgr.MarkDirty()
	return fn(bpage)
}

func (bpage *btreePage[K]) setSize(newSize int) {
	bpage.bp.Size = newSize
}
func (bpage *btreePage[K]) setAdditionalHeader(key string, value int) {
	// ** additional check to make sure the key is present in the additional headers
	bpage.additionalHeaders[key] = value
}

func (bpage *btreePage[K]) getMaxSize() int {
	return bpage.bp.MaxSize
}
func (bpage *btreePage[K]) getSize() int {
	return bpage.bp.Size
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
func (bpage *btreePage[K]) getKey(index int) K {
	return bpage.keys[index]
}
func (bpage *btreePage[K]) getValue(index int) *Value {
	return bpage.values[index]
}

func (bpage *btreePage[K]) getParent() int {
	return bpage.bp.ParentPageId
}
func (bpage *btreePage[K]) getPageId() int {
	return bpage.bp.PageId
}

func (bpage *btreeInnerPage[K]) isNext() bool {
	return bpage.bp.PageType == constants.InnerPageType
}

func (bpage *btreeInnerPage[K]) parent() int {
	return bpage.additionalHeaders["ParentId"]
}

func (bpage *btreePage[K]) getPageMetadataInterface() storage.PageInterface {
	return bpage.bpMgr
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

func (bi *btreeInnerPage[K]) getIterator() innerPageIterator[K] {
	return bi
}

func (bi *btreeLeafPage[K]) getIterator() leafPageIterator[K] {
	return bi
}

// ------------------------------------------------------------------------

//	btree index logic

type btreeIndexMgr[K string | int] interface {
	requestPage(pageId int) btreePageMgr[K]
	requestNewPage(pageType int) btreePageMgr[K]
	insert(key K, presentPageId int, pageId int, slotId int16)
	search(key K) []*Value // simple search
}

type btreeIndex[K string | int] struct {
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
				additionalHeaders: make(map[string]int),
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
				additionalHeaders: make(map[string]int),
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

func (bti *btreeIndex[K]) requestPage(pageId int) btreePageMgr[K] {
	page, _ := bti.bufPool.FetchPage(pageId)
	return bti.getBtreePage(page)
}
func (bti *btreeIndex[K]) requestNewPage(pageType int) btreePageMgr[K] {
	//panic("Not Implemented requestNewPage")
	page, _ := bti.bufPool.NewPage()
	return bti.getBtreePage(page)
}

func (bti *btreeIndex[K]) resetPageMetaData(page btreePageMgr[K]) {
	page.setSize(len(page.getPageKeys()))

}

func (bti *btreeIndex[K]) search(key K) []*Value {
	rootPageId := bti.getRootPageId()
	page := bti.requestPage(rootPageId)
	var innerPageItr innerPageIterator[K]
	var leafPageItr leafPageIterator[K]
	if page.getPageType() == constants.InnerPageType {
		var ok = true
		iterator := page.(btreeInnerPageMgr[K]).getIterator()
		innerPageItr, ok = iterator.(innerPageIterator[K])
		for ok && innerPageItr.isNext() {
			values, _, _ := innerPageItr.search(key)
			page = bti.requestPage(values[0].getValuePid())
			innerPageItr = page.(btreeInnerPageMgr[K]).getIterator()
		}
	}
	leafPageItr = page.(btreeLeafPageMgr[K]).getIterator() // ** do we have to use ok and add an additional check for ok?
	values, _, foundKey, isInNextPage := leafPageItr.search(key)
	if !foundKey {
		return nil
	}
	var nextVals []*Value
	for isInNextPage {
		page = bti.requestPage(leafPageItr.nextSib())
		leafPageItr = page.(btreeLeafPageMgr[K]).getIterator()
		nextVals, _, foundKey, isInNextPage = leafPageItr.search(key)
		if !foundKey {
			break
		}
		values = append(values, nextVals...) // do we return a copy of the values
	}
	return values
}

func (bti *btreeIndex[K]) insert(key K, presentPageId int, pageId int, slotId int16) {
	if presentPageId == -1 {
		presentPageId = bti.getRootPageId()
	}
	stack := []*Value{initInnerPageValue(presentPageId)}
	var backupPage btreePageMgr[K] = nil
	for len(stack) > 0 {
		page := bti.requestPage(stack[len(stack)-1].getValuePid())
		stack = stack[:len(stack)-1]
		if page.getPageType() == constants.InnerPageType {
			innerPageItr := page.(btreeInnerPageMgr[K]).getIterator()
			values, _, _ := innerPageItr.search(key)
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
		innerPageItr := page.(innerPageIterator[K])
		_, indices, _ := innerPageItr.search(key)
		if indices[0] == -1 {
			page.appendKey(key)
			page.appendValue(value)
		} else {
			insertIndex := indices[0]
			page.addKey(insertIndex, key)
			page.addValue(insertIndex, value)
		}
	} else if page.getPageType() == constants.LeafPageType {
		leafPageItr := page.(leafPageIterator[K])
		_, indices, _, _ := leafPageItr.search(key)
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
	var newPageId int
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

func (bti *btreeIndex[K]) splitInnerPage(page btreeInnerPageMgr[K]) (newPageId int, splitKey K) {
	newPage := bti.requestNewPage(page.getPageType())
	splitFunc := func(page *btreePage[K]) ([]K, []*Value, K, *Value, int) {
		midKeyIndex := len(page.keys) / 2
		midKey := page.keys[midKeyIndex]
		midVal := page.values[midKeyIndex]
		keysSplit1 := page.keys[0 : midKeyIndex-1]
		valSplit1 := page.values[0 : midKeyIndex-1]
		keysSplit2 := page.keys[midKeyIndex+1:]
		valSplit2 := page.values[midKeyIndex+1:]
		page.setKeys(keysSplit2)
		page.setValues(valSplit2)
		page.setSize(len(keysSplit2))
		return keysSplit1, valSplit1, midKey, midVal, midKeyIndex
	}
	keysSplit1, valSplit1, midKey, midVal, midKeyIndex := page.splitKVs(splitFunc)
	newPage.setKeys(keysSplit1)
	newPage.setValues(valSplit1)
	newPage.setAdditionalHeader("NextPtr", midVal.getValuePid())
	bti.reDistributeChild(page, newPage.getPageId(), midKeyIndex)
	newPage.setSize(len(keysSplit1))
	return newPage.getPageId(), midKey
}

func (bti *btreeIndex[K]) reDistributeChild(oldPage btreePageMgr[K], newPageId int, splitIndex int) {
	for i := 0; i < splitIndex; i++ {
		childPage := bti.requestPage(oldPage.getPageValues()[i].getValuePid())
		childPage.setAdditionalHeader("ParentId", newPageId)
	}
}

func (bti *btreeIndex[K]) splitLeafPage(page btreeLeafPageMgr[K]) (newPageId int, splitKey K) {
	newPage := bti.requestNewPage(page.getPageType()) // check the above comment?
	splitFunc := func(page *btreePage[K]) ([]K, []*Value, K, *Value, int) {
		keysSplit1 := page.keys[0 : len(page.keys)/2]
		keysSplit2 := page.keys[len(page.keys)/2:]
		valsSplit1 := page.values[0 : len(page.values)/2]
		valsSplit2 := page.values[len(page.values)/2:]
		page.setKeys(keysSplit2)
		page.setValues(valsSplit2)
		page.setSize(len(keysSplit2))
		return keysSplit1, valsSplit1, keysSplit2[0], valsSplit2[0], 0
	}
	keysSplit1, valsSplit1, splitKey, _, _ := page.splitKVs(splitFunc)
	newPage.setKeys(keysSplit1)
	newPage.setValues(valsSplit1)
	leafPageItr := page.getIterator()
	newPage.setAdditionalHeader("PrevPtr", leafPageItr.prevSib())
	newPage.setAdditionalHeader("NextPtr", page.getPageId())
	page.setAdditionalHeader("PrevPtr", newPage.getPageId())

	newPage.setSize(len(keysSplit1))
	return newPage.getPageId(), splitKey
}

func (bti *btreeIndex[K]) deleteTuple(index int, page btreeLeafPageMgr[K]) {
	page.getPageMetadataInterface().MarkDirty()
	keys := page.getPageKeys()
	vals := page.getPageValues()
	keys = append(keys[:index], keys[index+1:]...)
	vals = append(vals[:index], vals[index+1:]...)
	page.setKeys(keys)
	page.setValues(vals)

	if index == len(keys) {
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
		bti.mergeLeaf(page)
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

func (bti *btreeIndex[K]) mergeLeaf(page btreeLeafPageMgr[K]) {
	// W . I . P
	leftLeafPage := bti.requestPage(page.getIterator().prevSib())
	rightLeafPage := bti.requestPage(page.getIterator().nextSib())
	leftLeafPageKeys := leftLeafPage.getPageKeys()
	leftLeafPageValues := leftLeafPage.getPageValues()
	rightLeafPageKeys := rightLeafPage.getPageKeys()
	rightLeafPageValues := rightLeafPage.getPageValues()
	leftLeafPageKeys = append(leftLeafPageKeys, rightLeafPageKeys...)
	leftLeafPageValues = append(leftLeafPageValues, rightLeafPageValues...)
	leftLeafPage.setKeys(leftLeafPageKeys)
	leftLeafPage.setValues(leftLeafPageValues)
	//leftLeafPage.setAdditionalHeader("NextPtr", rightLeafPage.getIterator().nextSib())
	//bti.deleteTuple(page.getPageKeys()[0], page)
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

func (bpage *btreePage[K]) lowerBinarySearchKey(key K) (resIndex int) {
	low := 0
	high := len(bpage.keys)
	resIndex = -1
	for low <= high {
		mid := low + (high-low)/2
		if key <= bpage.keys[mid] {
			resIndex = mid
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return resIndex
}

func (bpage *btreePage[K]) higherBinarySearchKey(key K) (resIndex int) {
	low := 0
	high := len(bpage.keys)
	resIndex = -1
	for low <= high {
		mid := low + (high-low)/2
		if key < bpage.keys[mid] {
			high = mid - 1
		} else {
			resIndex = mid
			low = mid + 1
		}
	}
	return resIndex
}

func getBtreeIndexMgr[K string | int]() btreeIndexMgr[K] {
	return &btreeIndex[K]{
		bufPool: storage.InitBuffPoolMgr(diskmgr.DiskFileInit{
			DbFilePath:  "dbtest.db",
			LogFilePath: "dblogtest.log",
		}),
		indexName: "btree",
	}
}
