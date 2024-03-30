package storage

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rohithputha/HymStMgr/constants"
	"github.com/rohithputha/HymStMgr/diskmgr"
	"github.com/rohithputha/HymStMgr/utils"
)

type buffPoolStats struct {
	totalReadHit int
	diskReadHit  int
	bpReadHit    int
}

type BuffPoolMgrStr struct {
	*buffPoolStats
	replPol  ReplPol
	pagePool []Page
	pageMap  map[int]int //mapping from pageId to pagePool index
	freeSet  utils.ISet[int]
	pinSet   utils.ISet[int]
	pagesMem int
	bpsMux   *sync.Mutex
	diskMgr  diskmgr.DiskFileMgr
}

func InitBuffPoolMgr(dikFileInit diskmgr.DiskFileInit) (BuffPoolMgr *BuffPoolMgrStr) {
	buffPool := BuffPoolMgrStr{
		pagePool: make([]Page, constants.BufferPoolSize), // Size and capacity both set to BufferPoolSize
		pageMap:  make(map[int]int),
		freeSet:  utils.GetNewSet[int](), // seems not required
		pagesMem: 0,
		bpsMux:   &sync.Mutex{},
		replPol:  getLrukReplPol(),
		pinSet:   utils.GetNewSet[int](),
		diskMgr:  diskmgr.GetDiskFileMgr(dikFileInit),
	}

	for i := range constants.BufferPoolSize {
		buffPool.pagePool[i].pageMux = &sync.Mutex{}
		buffPool.freeSet.Add(i)
		buffPool.replPol.initPageLruk(i)
	}

	return &buffPool
}

/*
Fetch page should take a pageId and then return a page
if the page is already in memory, then it should return the pointer to that page
else laod the page from disk to one of the free pages and then return the pointer to it.
*/

func (bp *BuffPoolMgrStr) FetchPage(pageId int) (page *Page, readErr error) {
	bp.bpsMux.Lock()
	// defer bp.bpsMux.Unlock()
	if i, ok := bp.pageMap[pageId]; ok {
		// this is where the first part of the logic is to be implemented
		// i.e. if the page is already in the buffer
		// we have to update the Hist and Last values (of K)... should be extensible to any K values >=2
		// maybe have a select page from buffer method that does interactions with the LRU struct (Repl policy)
		defer bp.bpsMux.Unlock()
		bp.replPol.addPageTime(i, time.Now().UnixNano())
		page := &bp.pagePool[i]
		return page, nil
	}
	// bp.diskReadHit++
	sPage, sPageIndex, sErr := bp.selectPage()
	bp.pageMap[pageId] = sPageIndex
	sPage.PageId = pageId
	bp.bpsMux.Unlock()
	sPage.pageMux.Lock()
	if sErr != nil {
		return nil, sErr
	}

	err := bp.diskMgr.ReadPage(pageId, sPage.pageData[:])
	if err != nil {
		sPage.IsCorrupted = true
		return nil, err
	}
	sPage.pageMux.Unlock()
	return sPage, nil
}

func (bp *BuffPoolMgrStr) flushPageByIndex(pageIndex int) (flushErr error) {
	if bp.pagePool[pageIndex].Pin == 0 && !bp.pagePool[pageIndex].IsCorrupted {
		if bp.pagePool[pageIndex].IsDirty {
			writerErr := bp.diskMgr.WritePage(pageIndex, bp.pagePool[pageIndex].pageData[:])
			bp.pagePool[pageIndex].IsDirty = false
			return writerErr
		} else {
			return nil
		}

	} else {
		return errors.New("page does not satisfy flush conditions")
	}
}

/*
FlushPage should take a pageId as input and then flush the page to the disk
if the page is pinned and is corrupted the flush will fail
on successful flush, isDirty should be marked as false
*/
func (bp *BuffPoolMgrStr) FlushPage(pageId int) (flushErr error) {
	bp.bpsMux.Lock()
	defer bp.bpsMux.Unlock()
	fmt.Println(bp.pageMap[pageId])
	if _, ok := bp.pageMap[pageId]; ok {
		return bp.flushPageByIndex(bp.pageMap[1])
	} else {
		return errors.New("page failed for pageId: " + fmt.Sprintf("%d", pageId))
	}
}

func (bp *BuffPoolMgrStr) allocatePageId() (pageId int) {
	return bp.diskMgr.GetPageCount()
}

func (bp *BuffPoolMgrStr) NewPage() (page *Page, newPageErr error) {
	bp.bpsMux.Lock()
	defer bp.bpsMux.Unlock()

	newPageId := bp.allocatePageId()
	sPage, sPageIndex, sErr := bp.selectPage()
	if sErr != nil {
		return nil, sErr
	}
	bp.replPol.initPageLruk(sPageIndex)
	sPage.NewPage()
	writeErr := bp.diskMgr.WritePage(newPageId, sPage.pageData[:])
	if writeErr != nil {
		return nil, writeErr
	}
	bp.pageMap[newPageId] = sPageIndex
	sPage.PageId = newPageId
	return sPage, nil
}

func (bp *BuffPoolMgrStr) UnpinPage(pageId int) bool {
	bp.bpsMux.Lock()
	defer bp.bpsMux.Unlock()

	if i, ok := bp.pageMap[pageId]; ok {
		bp.pagePool[i].Pin--
		if bp.pagePool[i].Pin == 0 {
			bp.pinSet.Delete(i)
		}
		return true
	}

	return false
}

func (bp *BuffPoolMgrStr) PinPage(pageId int) {
	bp.bpsMux.Lock()
	defer bp.bpsMux.Unlock()

	if i, ok := bp.pageMap[pageId]; ok {
		bp.pagePool[i].Pin++
		bp.pinSet.Add(i)
	}
}

/*
select page is responsible for selecting a page from pagePool and returnign the pointer to the page and pageIndex, err if any
select page is NOT responsible for adding any info the page map and any other changes to the times info in lruk
*/
func (bp *BuffPoolMgrStr) selectPage() (page *Page, freePageIndex int, selectErr error) {

	victimePageIndex := bp.replPol.findReplPage(time.Now().UnixNano()/int64(time.Millisecond), bp.pinSet)
	if victimePageIndex < 0 || victimePageIndex >= constants.BufferPoolSize {
		return nil, -1, errors.New("no victim page found by the repl pol")
	}
	flushErr := bp.flushPageByIndex(victimePageIndex) // flush page method for pageIndex
	if flushErr != nil {
		return nil, -1, errors.New("no page is free on memory")
	}
	delete(bp.pageMap, bp.pagePool[victimePageIndex].PageId)
	// we should have the logic of page map allocation in the and page Id allocation in the page here....?
	return &bp.pagePool[victimePageIndex], victimePageIndex, nil
}
