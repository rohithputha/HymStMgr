package storage

import (
	"errors"
	"fmt"
	"sync"
	"time"
	"container/list"

	"github.com/rohithputha/hymStMgr/constants"
	"github.com/rohithputha/hymStMgr/diskmgr"
)

type buffPoolStats struct{
	totalReadHit int
	diskReadHit int
	bpReadHit int
}
type lruk struct{
	pageHistMap map[int]list.List
	pageLastTimeMap map[int]int64
}

type ReplPol interface{
	addPageTime(pageIndex int, timestamp int64)
	findReplPage(timestamp int64) (pageIndex int)
	setPageTime(timestamp int64)
}

func (l *lruk) addPageTime(pageIndex int, timestamp int64){
	
	if _, ok := l.pageHistMap[pageIndex]; !ok {
		l.pageHistMap[pageIndex] = (*list.New()).PushFront(timestamp)
		l.pageLastTimeMap[pageIndex] = timestamp
		return
	}

	// this if else covers adding time info when the the page is already in the buffers
	if timestamp - l.pageLastTimeMap[pageIndex]> 5000 {
		t := l.pageHistMap[pageIndex]
		corPerRefPg := l.pageLastTimeMap[pageIndex]- t.Front().Value.(int64)
		for i:= t.Front() ; t != nil ;i.Next() {
			i.Value.(int64) += corPerRefPg
		}
		t.PushFront(timestamp)
		l.pageLastTimeMap[pageIndex] = timestamp
	}else{
		l.pageLastTimeMap[pageIndex] = timestamp
	}
}
func (l *lruk) findReplPage(timestamp int64) (pageIndex int64) {
	min:= timestamp
	for pageIndex, krec := range l.pageHistMap {
		// if timestamp - l.pageLastTimeMap[pageIndex] && 
	}
	return int64(0)
}

type BuffPoolMgrStr struct{
	*buffPoolStats
	*ReplPol
	pagePool []Page
	pageMap map[int]int
	freeSet map[int]bool
	pagesMem int
	bpsMux *sync.Mutex
	diskmgr *diskmgr.DiskFileMetaData
	replacer ReplPol
}

type Config struct{

}

type FetchConfig struct{
	Config
}


type BuffPoolMgr interface{
	
}


func InitBuffPoolMgr()(BuffPoolMgr *BuffPoolMgrStr){
	buffPool := BuffPoolMgrStr{
		pagePool: make([]Page,constants.BufferPoolSize), // Size and capacity both set to BufferPoolSize
        pageMap: make(map[int]int),
		freeSet: make(map[int]bool),
        pagesMem: 0,
        bpsMux: &sync.Mutex{},
	}
	return &buffPool
} 

/*
Fetch page should take a pageId and then return a page
if the page is already in memory, then it should return the pointer to that page
else laod the page from disk to one of the free pages and then return the pointer to it. 
*/

func (bp *BuffPoolMgrStr) FetchPage(pageId int) (page *Page, readErr error){

	bp.bpsMux.Lock()
    bp.totalReadHit++
	if i,ok := bp.pageMap[pageId]; ok{
	// this is where the first part of the logic is to be implemented
	// i.e. if the page is already in the buffer
	// we have to update the Hist and Last values (of K)... should be extensible to any K values >=2
	// maybe have a select page from buffer method that does interactions with the LRU struct (Repl policy)
		bp.ReplPol.addPageTime(i, time.Now().Unix())
		bp.bpReadHit++
		return &bp.pagePool[i], nil
	}
	bp.diskReadHit++
	sPage, sErr := bp.selectPage()
	bp.bpsMux.Unlock()
	sPage.pageMux.Lock()
	if sErr!=nil{
		return nil, sErr
	}

	_,err := bp.diskmgr.ReadPage(pageId,(*sPage).pageData[:])
	if err!=nil{
		sPage.IsCorrupted = true
		return nil, err
	}
	sPage.pageMux.Unlock()
	return sPage, nil
}

/*
FlushPage should take a pageId as input and then flush the page to the disk
if the page is pinned and is corrupted the flush will fail
on successful flush, isDirty should be marked as false 
*/
func (bp *BuffPoolMgrStr) FlushPage(pageId int)(flushErr error){
	bp.bpsMux.Lock()
	defer bp.bpsMux.Unlock()
	if i, ok:= bp.pageMap[pageId]; ok {
		if bp.pagePool[i].IsDirty && bp.pagePool[i].Pin==0 && !bp.pagePool[i].IsCorrupted {
			writerErr := bp.diskmgr.WritePage(pageId, bp.pagePool[i].pageData[:])
			bp.pagePool[i].IsDirty=false
			return writerErr
		}else{
			return errors.New("page does not satisfy flush conditions")
		}
	} else{
		return errors.New("page failed for pageId: "+fmt.Sprintf("%d",pageId))
	}
}

/*
DeletePage should free up the page and mark it as a free page. 
If the page isPinned, the delete should fail
On successful delete, isOccupied is marked as false and the page is added freeset. 
*/
func (bp *BuffPoolMgrStr) DeletePage(pageId int) (deleteErr error){
	bp.bpsMux.Lock()
	defer bp.bpsMux.Unlock()

	if i,ok := bp.pageMap[pageId]; ok && bp.pagePool[i].Pin == 0 && bp.pagePool[i].IsOccupied {
		bp.pagePool[i].IsOccupied = false
		bp.freeSet[i] = true		
	}else{
		return errors.New("delete page failed for pageId: "+fmt.Sprintf("%d",pageId))
	}
	return nil
}

func (bp *BuffPoolMgrStr) allocatePageId()(pageId int){
	totalPages := bp.diskmgr.GetTotalNumPages()
	return totalPages+1
}

func (bp *BuffPoolMgrStr) NewPage() (page *Page, newPageErr error){
	bp.bpsMux.Lock()
	defer bp.bpsMux.Unlock()

	newPageId := bp.allocatePageId()
	sPage, sErr := bp.selectPage()
	if sErr!=nil{
		return nil, sErr
	}
    sPage.NewPage()
	writeErr := bp.diskmgr.WritePage(newPageId, sPage.pageData[:])
	if writeErr!=nil{
		return nil, writeErr
	}
    return sPage, nil
}

func (bp *BuffPoolMgrStr) UnpinPage(pageId int) bool{
	bp.bpsMux.Lock()
	defer bp.bpsMux.Unlock()

	if i, ok:=bp.pageMap[pageId]; ok {
		bp.pagePool[i].Pin--
	}
	return false
}

/*
SelectPage has to return a free page
either select a page from the free set 
or evict a page using the replacement policy and return that as a free page. eviction should involve not pinned pages and flushed pages. If not flushed, it should be flushed first. 
*/
func (bp *BuffPoolMgrStr) selectPage() (page *Page, selectErr error){
	if (len(bp.freeSet))
	// need to implement this using the LRU-K repl policy
	// has to be thread safe
	que
}


// should we have 