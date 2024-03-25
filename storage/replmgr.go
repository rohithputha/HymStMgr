package storage

import (
	"errors"

	"github.com/rohithputha/hymStMgr/dstrutsgo"
)

// LRUK should maintain the times of the pages that are accessed,
// Find a victim page if needed based on the info it has stored till then
// reset the stored data for a page
// should setup
// should it store info based on pageIndex or pageId? maybe go with pageIndex: will be easy on memory but not accurate I feel.

type ReplPol interface {
	initPageLruk(pageIndex int)
	addPageTime(pageIndex int, timestamp int64) (err error)
	findReplPage(timestamp int64, excludedPages dstrutsgo.ISet[int]) (pageIndex int)
}

type lruk struct {
	pageHistMap     map[int](dstrutsgo.IQueue[int64])
	pageLastTimeMap map[int]int64
}

func getLrukReplPol() ReplPol {
	lruk := lruk{
		pageHistMap: make(map[int]dstrutsgo.IQueue[int64]),
		pageLastTimeMap: make(map[int]int64),
	}
	return &lruk
}

func (l *lruk) initPageLruk(pageIndex int) {
	l.pageHistMap[pageIndex] = dstrutsgo.GetNewQueue[int64](4)
	l.pageLastTimeMap[pageIndex] = int64(0)
}

func (l *lruk) addPageTime(pageIndex int, timestamp int64) (err error) {
	// is 5000 correct? 5000 units of the time, what is the unit here?
	// should there be a locking mechanism here?
	if timestamp-l.pageLastTimeMap[pageIndex] > 500 {
		timeQueue, ok := l.pageHistMap[pageIndex]
		if !ok {
			return errors.New("pageIndex does not exist")
		}
		recentTime, recentErr := timeQueue.GetLast() //here last is used to find the latest timestamp
		if recentErr != nil {
			timeQueue.ForcePush(timestamp)
			l.pageLastTimeMap[pageIndex] = timestamp
			return nil
		}
		corPeriod := l.pageLastTimeMap[pageIndex] - recentTime
		for i := 0; i < timeQueue.GetSize(); i++ {   // internal queue has this structure ->  (head,....., tail) (head -> 0 , tail -> len(q)-1) tail is the most recent element to be pushed
			queueEle, queueGetErr := timeQueue.Get(i)
			if queueGetErr != nil {
				return queueGetErr
			}
			l.pageHistMap[pageIndex].Update(i, queueEle+corPeriod)
		}
		l.pageHistMap[pageIndex].ForcePush(timestamp)
	}
	l.pageLastTimeMap[pageIndex] = timestamp
	return nil
}

func (l *lruk) findReplPage(timestamp int64, excludedPages dstrutsgo.ISet[int]) (pageIndex int) {
	minTime := timestamp
	victim := -1
	for pageIndex, timeQueue := range l.pageHistMap {
		if excludedPages.Contains(pageIndex) {
			continue
		}
		kQueueEle, queueGetErr := timeQueue.GetFirst()
		if queueGetErr != nil {
			continue
		}
		if timestamp-l.pageLastTimeMap[pageIndex] > 500 && kQueueEle < minTime {
			victim = pageIndex
			minTime = kQueueEle
		}
	}
	if victim == -1 {
		for pageIndex, _ :=range l.pageHistMap{
			if excludedPages.Contains(pageIndex) {
				continue
			}
			return pageIndex // not a very efficient way to return the pageIndex but increases prob of finding a repl page. need to test the real impact later. 
		}
	}
	return victim
}
