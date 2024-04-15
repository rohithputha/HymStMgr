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
	bp                    *storage.BasePage
	pageMetadatainterface storage.PageInterface
	additionalHeaders     map[string]int
	keys                  []K
	values                []*Value
	brK                   bufferReader[K]
}

type btreeInnerPage[K string | int] struct {
	btreePage[K] // V is int for inner pages
}

type btreeLeafPage[K string | int] struct {
	btreePage[K] // V is [2]int for leaf pages
}

type btreePageMgr[K string | int] interface {
	decode()
	//encode() // ** need to implement this
	getPageType() int
	getPageKeys() []K
	getPageValues() []*Value
	getMaxSize() int
	getSize() int
	getParent() int
	getPageId() int
	getPageMetadataInterface() storage.PageInterface

	setKeys(keys []K)
	setValues(values []*Value)
	setSize(int)
	setAdditionalHeader(string, int)
}

type btreeInnerPageMgr[K string | int] interface {
	btreePageMgr[K]
	getIterator() innerPageIterator[K]
}
type btreeLeafPageMgr[K string | int] interface {
	btreePageMgr[K]
	getIterator() leafPageIterator[K]
}

type leafPageIterator[K string | int] interface {
	next(key K) (values []*Value, indices []int, foundKey bool, isInNextPage bool)
	nextSib() (pageId int)
	prevSib() (pageId int)
	parent() (pageId int)
}

type innerPageIterator[K string | int] interface {
	isNext() bool
	next(key K) (values []*Value, indices []int, foundKey bool)
	parent() (pageId int)
}

func (bpage *btreeLeafPage[K]) next(key K) ([]*Value, []int, bool, bool) {
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

func (bpage *btreeInnerPage[K]) next(key K) ([]*Value, []int, bool) {
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
	bpage.pageMetadatainterface.MarkDirty()
	bpage.keys = keys
}
func (bpage *btreePage[K]) setValues(values []*Value) {
	bpage.pageMetadatainterface.MarkDirty()
	bpage.values = values
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
	return bpage.pageMetadatainterface
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
	//replacePage(oldPage btreePageMgr[K], newPage btreePageMgr[K])
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
				bp:                    page.GetDecodedBasePage(),
				pageMetadatainterface: page,
				additionalHeaders:     make(map[string]int),
				keys:                  make([]K, 0),
				values:                make([]*Value, 0),
				brK:                   bufferReader[K]{8}, // Adjust size based on K type
			},
		}
		innerPage.decode()
		return innerPage
	} else if page.GetDecodedBasePage().PageType == constants.LeafPageType {
		leafPage := &btreeLeafPage[K]{
			btreePage: btreePage[K]{
				bp:                    page.GetDecodedBasePage(),
				pageMetadatainterface: page,
				additionalHeaders:     make(map[string]int),
				keys:                  make([]K, 0),
				values:                make([]*Value, 0),
				brK:                   bufferReader[K]{8}, // Adjust size based on K type
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
			values, _, _ := innerPageItr.next(key)
			page = bti.requestPage(values[0].getValuePid())
			innerPageItr = page.(btreeInnerPageMgr[K]).getIterator()
		}
	}
	leafPageItr = page.(btreeLeafPageMgr[K]).getIterator() // ** do we have to use ok and add an additional check for ok?
	values, _, foundKey, isInNextPage := leafPageItr.next(key)
	if !foundKey {
		return nil
	}
	var nextVals []*Value
	for isInNextPage {
		page = bti.requestPage(leafPageItr.nextSib())
		leafPageItr = page.(btreeLeafPageMgr[K]).getIterator()
		nextVals, _, foundKey, isInNextPage = leafPageItr.next(key)
		if !foundKey {
			break
		}
		values = append(values, nextVals...)
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
			values, _, _ := innerPageItr.next(key)
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
	page.getPageMetadataInterface().MarkDirty()
	keys := page.getPageKeys()
	vals := page.getPageValues()
	if page.getPageType() == constants.InnerPageType {
		innerPageItr := page.(innerPageIterator[K])
		_, indices, _ := innerPageItr.next(key)
		if indices[0] == -1 {
			keys = append(keys, key)
			vals = append(vals, value)
		} else {
			insertIndex := indices[0]
			keys = append(keys[:insertIndex], append([]K{key}, keys[insertIndex:]...)...)
			vals = append(vals[:insertIndex], append([]*Value{value}, vals[insertIndex:]...)...)
		}
	} else if page.getPageType() == constants.LeafPageType {
		leafPageItr := page.(leafPageIterator[K])
		_, indices, _, _ := leafPageItr.next(key)
		if indices == nil {
			panic("leaf page should not have a nil index")
		}
		insertIndex := indices[0]
		keys = append(keys[:insertIndex], append([]K{key}, keys[insertIndex:]...)...)
		vals = append(vals[:insertIndex], append([]*Value{value}, vals[insertIndex:]...)...)
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
		// when the leaf page is split, we need to make sure that the next and prev pointers are set correctly
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
	keys := page.getPageKeys()
	values := page.getPageValues()
	midKeyIndex := len(keys) / 2
	midKey := keys[midKeyIndex]
	midVal := values[midKeyIndex]
	keysSplit1 := keys[0 : midKeyIndex-1]
	valSplit1 := values[0 : midKeyIndex-1]
	keysSplit2 := keys[midKeyIndex+1:]
	valSplit2 := values[midKeyIndex+1:]
	page.setKeys(keysSplit2)
	page.setValues(valSplit2)
	newPage.setKeys(keysSplit1)
	newPage.setValues(valSplit1)
	newPage.setAdditionalHeader("NextPtr", midVal.getValuePid())
	bti.reDistributeChild(page, newPage.getPageId(), midKeyIndex)
	page.setSize(len(keysSplit2))
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
	keys := page.getPageKeys()
	vals := page.getPageValues()
	keysSplit1 := keys[0 : len(keys)/2]
	keysSplit2 := keys[len(keys)/2:]
	valsSplit1 := vals[0 : len(vals)/2]
	valsSplit2 := vals[len(vals)/2:]
	page.setKeys(keysSplit2)
	page.setValues(valsSplit2)
	newPage.setKeys(keysSplit1)
	newPage.setValues(valsSplit1)
	leafPageItr := page.getIterator()
	newPage.setAdditionalHeader("PrevPtr", leafPageItr.prevSib())
	newPage.setAdditionalHeader("NextPtr", page.getPageId())
	page.setAdditionalHeader("PrevPtr", newPage.getPageId())
	page.setSize(len(keysSplit2))
	newPage.setSize(len(keysSplit1))
	return newPage.getPageId(), keysSplit1[len(keysSplit1)-1]
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
