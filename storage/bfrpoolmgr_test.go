package storage

import (
	"testing"

	"github.com/rohithputha/HymStMgr/constants"
	"github.com/rohithputha/HymStMgr/diskmgr"
)

func TestInitBufferPoolMgr(test *testing.T) {
	bfrPool := InitBuffPoolMgr(diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblog.log",
	})
	if len(bfrPool.pagePool) != 500 {
		test.Errorf("init buffer pool did not work as expected")
	}
}

func TestSelectPageWithFreePage(test *testing.T) {
	bfrPool := InitBuffPoolMgr(diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblog.log",
	})
	_, pageIndex, err := bfrPool.selectPage()
	if err != nil || !bfrPool.freeSet.Contains(pageIndex) {
		test.Errorf("select free page not working as expected")
	}
}

func TestSelectPageWithNoFreePage(test *testing.T) {
	bfrPool := InitBuffPoolMgr(diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblog.log",
	})
	for i := range constants.BufferPoolSize {
		bfrPool.freeSet.Delete(i) //deleting all the pages from free set to simulate not free pages available
	}
	_, pageIndex, err := bfrPool.selectPage()
	if err != nil || pageIndex < 0 && pageIndex >= 500 {
		test.Log(err)
		test.Errorf("select free page not working as expected")
	}
}

func TestPinPage(test *testing.T) {
	bfrPool := InitBuffPoolMgr(diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblog.log",
	})
	bfrPool.pageMap[1] = 1 //setting page mapping => page id 1 -> page index 1
	bfrPool.PinPage(1)
	bfrPool.PinPage(1)
	if bfrPool.pagePool[1].Pin != 2 || !bfrPool.pinSet.Contains(1) {
		test.Errorf("pin page not working as expected")
	}
}

func TestUnpinPage(test *testing.T) {
	bfrPool := InitBuffPoolMgr(diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblog.log",
	})
	bfrPool.pageMap[1] = 1 //setting page mapping => page id 1 -> page index 1
	bfrPool.PinPage(1)
	bfrPool.PinPage(1)
	bfrPool.UnpinPage(1)
	if !bfrPool.pinSet.Contains(1) {
		test.Errorf("unpinpage with multiple pins does not work as expected")
	}

	bfrPool.UnpinPage(1)
	if bfrPool.pinSet.Contains(1) {
		test.Errorf("unpinpage does not work as expected")
	}
}

func TestAllocatePage(test *testing.T) {
	bfrPool := InitBuffPoolMgr(diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblog.log",
	})
	pageId := bfrPool.allocatePageId()
	test.Log(pageId)
	if pageId != 0 {
		test.Errorf("allocate page not working as expected")
	}
}

func TestNewPage(test *testing.T) {
	bfrPool := InitBuffPoolMgr(diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblog.log",
	})

	newPage, err := bfrPool.NewPage()
	if err != nil || newPage.pageData[0] != 1 {
		test.Errorf("new page creation not working as expected")
	}
}

func TestNewPageMultiple(test *testing.T) {
	bfrPool := InitBuffPoolMgr(diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblog.log",
	})

	bfrPool.NewPage()
	newPage, _ := bfrPool.NewPage()
	if newPage.pageData[0] != 1 || bfrPool.diskMgr.GetPageCount() != 2 {
		test.Errorf("new page multiple creation not working as expected")
	}
}

func TestFlushPageByIndex(test *testing.T) {
	bfrPool := InitBuffPoolMgr(diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblog.log",
	})
	// setting up the bfrpool page data to all 1s (4096 bytes)
	bfrPool.pageMap[0] = 0
	for i := range bfrPool.pagePool[0].pageData {
		bfrPool.pagePool[0].pageData[i] = 1
	}
	bfrPool.pagePool[0].IsDirty = true
	flushErr := bfrPool.flushPageByIndex(0)
	if flushErr != nil || bfrPool.diskMgr.GetPageCount() != 1 {
		test.Errorf("flush page by index is not working as expected")
	}

}
func TestFlushPageByIndexPageNotDirty(test *testing.T) {
	bfrPool := InitBuffPoolMgr(diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblog.log",
	})
	// setting up the bfrpool page data to all 1s (4096 bytes)
	bfrPool.pageMap[0] = 0
	for i := range bfrPool.pagePool[0].pageData {
		bfrPool.pagePool[0].pageData[i] = 1
	}
	bfrPool.pagePool[0].IsDirty = false
	flushErr := bfrPool.flushPageByIndex(0)
	if flushErr != nil || bfrPool.diskMgr.GetPageCount() != 0 {
		test.Errorf("flush page by index is not working as expected")
	}

}

func TestFlushPage(test *testing.T) {
	bfrPool := InitBuffPoolMgr(diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblog.log",
	})
	// setting up the bfrpool page data to all 1s (4096 bytes)
	bfrPool.pageMap[1] = 0
	for i := range bfrPool.pagePool[bfrPool.pageMap[1]].pageData {
		bfrPool.pagePool[bfrPool.pageMap[1]].pageData[i] = 1
	}
	bfrPool.pagePool[bfrPool.pageMap[1]].IsDirty = true
	flushErr := bfrPool.FlushPage(1)
	if flushErr != nil || bfrPool.diskMgr.GetPageCount() != 1 {
		test.Errorf("flush page by page id is not working as expected")
	}
}

func TestFetchPage(test *testing.T) {
	bfrPool := InitBuffPoolMgr(diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblog.log",
	})

	newPage, _ := bfrPool.NewPage()
	fetchedPage, fetchErr := bfrPool.FetchPage(0)
	if fetchErr != nil || newPage != fetchedPage {
		test.Errorf("fetch page already in buffer not working as expected")
	}
}

func TestFetchPageNotInBuffer(test *testing.T) {
	bfrPool := InitBuffPoolMgr(diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblog.log",
	})

	bfrPool.NewPage()
	delete(bfrPool.pageMap, 0)
	fetchedPage, fetchErr := bfrPool.FetchPage(0)
	if fetchErr != nil || fetchedPage.pageData[0] != 1 {
		test.Errorf("fetch page already in buffer not working as expected")
	}
}
