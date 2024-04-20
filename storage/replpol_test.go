package storage

import (
	"testing"
	"time"

	"github.com/rohithputha/HymStMgr/utils"
)

func TestLrukReplPolInit(test *testing.T) {
	lruk := lruk{}
	if len(lruk.pageHistMap) != 0 {
		test.Errorf("lruk repl pol struct not init as expected")
	}
}

func TestLrukReplPolPageInit(test *testing.T) {
	lruk := lruk{
		pageHistMap:     make(map[int64]utils.IQueue[int64]),
		pageLastTimeMap: make(map[int64]int64),
	}
	lruk.initPageLruk(0)
	if len(lruk.pageHistMap) != 1 {
		test.Errorf("lruk page init not working as expected")
	}
	if lruk.pageHistMap[0].GetSize() != 0 {
		test.Errorf("lruk page hist queue not working work as expected")
	}
}

func TestLrukAddTime(test *testing.T) {
	lruk := lruk{
		pageHistMap:     make(map[int64]utils.IQueue[int64]),
		pageLastTimeMap: make(map[int64]int64),
	}
	lruk.initPageLruk(0)

	lruk.addPageTime(0, time.Now().Unix())
	if lruk.pageHistMap[0].GetSize() != 1 {
		test.Errorf("lruk add first time not working as expected")
	}
}

func TestLrukAddTimeMulti(test *testing.T) {
	lruk := lruk{
		pageHistMap:     make(map[int64]utils.IQueue[int64]),
		pageLastTimeMap: make(map[int64]int64),
	}
	lruk.initPageLruk(0)
	lruk.addPageTime(0, time.Now().UnixNano()/int64(time.Millisecond))
	time.Sleep(20 * time.Millisecond)
	lruk.addPageTime(0, time.Now().UnixNano()/int64(time.Millisecond))

	if histT, _ := lruk.pageHistMap[0].GetLast(); lruk.pageLastTimeMap[0]-histT < 20 && lruk.pageHistMap[0].GetSize() != 1 {
		test.Errorf("lruk add multiple times to a page with same corr ref range not working")
	}
}

func TestLrukAddTimeMultiCorrPeriod(test *testing.T) {
	lruk := lruk{
		pageHistMap:     make(map[int64]utils.IQueue[int64]),
		pageLastTimeMap: make(map[int64]int64),
	}
	lruk.initPageLruk(0)
	lruk.addPageTime(0, time.Now().UnixNano()/int64(time.Millisecond))
	time.Sleep(20 * time.Millisecond)
	lruk.addPageTime(0, time.Now().UnixNano()/int64(time.Millisecond))
	time.Sleep(501 * time.Millisecond)
	lruk.addPageTime(0, time.Now().UnixNano()/int64(time.Millisecond))

	if histT, _ := lruk.pageHistMap[0].GetLast(); lruk.pageLastTimeMap[0]-histT != 0 && lruk.pageHistMap[0].GetSize() != 2 {
		test.Errorf("lruk add multiple times to a page with different corr ref range not working")
	}
	if histT2, _ := lruk.pageHistMap[0].Get(0); lruk.pageLastTimeMap[0]-histT2 < 501 {
		test.Errorf("lruk add multiple times to a page with different corr ref range not working")
	}
}

func TestLrukFindReplPage(test *testing.T) {
	lruk := lruk{
		pageHistMap:     make(map[int64]utils.IQueue[int64]),
		pageLastTimeMap: make(map[int64]int64),
	}
	lruk.initPageLruk(0)
	lruk.initPageLruk(1)
	lruk.pageHistMap[0].ForcePush(1000)
	lruk.pageHistMap[0].ForcePush(1510)
	lruk.pageHistMap[0].ForcePush(2520)
	lruk.pageHistMap[1].ForcePush(980)
	lruk.pageHistMap[1].ForcePush(1505)
	lruk.pageHistMap[1].ForcePush(2510)
	lruk.pageLastTimeMap[0] = 2520
	lruk.pageLastTimeMap[1] = 2510

	replPageIndex := lruk.findReplPage(3530, utils.GetNewSet[int64]())
	if replPageIndex != 1 {
		test.Errorf("lruk find repl page not working as expected")
	}

}
