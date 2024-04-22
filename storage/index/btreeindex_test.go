package index

import (
	"bytes"
	"fmt"
	"github.com/rohithputha/HymStMgr/constants"
	"testing"
)

func TestCustomBufferedReaderNext(t *testing.T) {
	customBufferedReader := customBufferReader[int64]{
		numBytesPerCustomRead: 8,
	}

	data := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	res := customBufferedReader.next(bytes.NewBuffer(data))
	t.Log(res)
	if res != 1 {
		t.Errorf("Next byte is not as expected")
	}
}

func TestCustomBufferedReaderNextInt16(t *testing.T) {
	customBufferedReader := customBufferReader[int64]{
		numBytesPerCustomRead: 8,
	}
	data := []byte{0, 1}
	res := customBufferedReader.nextInt16(bytes.NewBuffer(data))
	t.Log(res)
	if res != 1 {
		t.Errorf("Next byte is not as expected")
	}
}
func TestCustomBufferedReaderNextInt64(t *testing.T) {
	customBufferedReader := customBufferReader[int64]{
		numBytesPerCustomRead: 8,
	}
	data := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	res := customBufferedReader.nextInt64(bytes.NewBuffer(data))
	if res != 1 {
		t.Errorf("Next byte is not as expected")
	}
}

//**fix the buffer string read test**
//
//func TestCustomBufferedReaderNextString(t *testing.T) {
//	customBufferedReader := customBufferReader[string]{
//		numBytesPerCustomRead: 10,
//	}
//	data := []byte("hello world")
//	res := customBufferedReader.next(bytes.NewBuffer(data))
//	if res != "hello worl" {
//		t.Errorf("Next byte is not as expected")
//	}
//}

func TestInitInnerPageValue(t *testing.T) {
	innerPageValue := initInnerPageValue(1)
	if innerPageValue.pageId != 1 || innerPageValue.slotId != -1 {
		t.Errorf("Inner page not initialized as expected")
	}
}
func TestInitLeafPageValue(t *testing.T) {
	leafPageValue := initLeafPageValue(1, 3)
	if leafPageValue.pageId != 1 || leafPageValue.slotId != 3 {
		t.Errorf("Leaf page not initialized as expected")
	}
}

func TestGetValuePid(t *testing.T) {
	innerPageValue := initInnerPageValue(1)
	if innerPageValue.getValuePid() != 1 {
		t.Errorf("GetValuePid not working as expected")
	}
}
func TestGetValueSid(t *testing.T) {
	innerPageValue := initInnerPageValue(1)
	if innerPageValue.getValueSid() != -1 {
		t.Errorf("GetValueSid not working as expected")
	}
}

func TestRequestNewPage(t *testing.T) {
	btreeIndexMgr := getBtreeIndexMgr[int64](t.TempDir()+"dbtest.db", t.TempDir()+"dblog.log")
	page := btreeIndexMgr.requestNewPage(constants.InnerPageType)
	if page.getPageId() < 0 {
		t.Errorf("Request new page not working as expected")
	}
}
func TestRequestNewPage2(t *testing.T) {
	btreeIndexMgr := getBtreeIndexMgr[int64](t.TempDir()+"dbtest.db", t.TempDir()+"dblog.log")
	page := btreeIndexMgr.requestNewPage(constants.InnerPageType)
	page2 := btreeIndexMgr.requestNewPage(constants.LeafPageType)
	if page.getPageId() < 0 {
		t.Errorf("Request new page not working as expected")
	}
	if page2.getPageId() < 0 {
		t.Errorf("Request new page not working as expected")
	}
	if page.getPageId() == page2.getPageId() {
		t.Errorf("two new Pages have the same pageId. Expected different pageIds. Request New page not working as expected")
	}
	fmt.Println(page.getPageId(), page2.getPageId())
}

func TestGetPageId(t *testing.T) {
	btreeIndexMgr := getBtreeIndexMgr[int64](t.TempDir()+"dbtest.db", t.TempDir()+"dblog.log")
	page := btreeIndexMgr.requestNewPage(constants.LeafPageType)
	if page.getPageId() < 0 {
		t.Errorf("GetPageId not working as expected")
	}
}
func TestGetPageType(t *testing.T) {
	btreeIndexMgr := getBtreeIndexMgr[int64](t.TempDir()+"dbtest.db", t.TempDir()+"dblog.log")
	page := btreeIndexMgr.requestNewPage(constants.LeafPageType)
	if page.getPageType() != constants.LeafPageType {
		t.Errorf("GetPageType not working as expected")
	}
}

func TestGetPageType2(t *testing.T) {
	btreeIndexMgr := getBtreeIndexMgr[int64](t.TempDir()+"dbtest.db", t.TempDir()+"dblog.log")
	page := btreeIndexMgr.requestNewPage(constants.LeafPageType)
	page2 := btreeIndexMgr.requestNewPage(constants.InnerPageType)
	if page.getPageType() != constants.LeafPageType {
		t.Errorf("GetPageType not working as expected")
	}
	if page2.getPageType() != constants.InnerPageType {
		t.Errorf("GetPageType not working as expected")
	}
}

func TestGetMaxSize(t *testing.T) {
	btreeIndexMgr := getBtreeIndexMgr[int64](t.TempDir()+"dbtest.db", t.TempDir()+"dblog.log")
	page := btreeIndexMgr.requestNewPage(constants.LeafPageType)
	if page.getMaxSize() != 190 {
		t.Errorf("GetMaxSize not working as expected. Expected 190, got %d", page.getMaxSize())
	}
}

func TestGetSize(t *testing.T) {
	btreeIndexMgr := getBtreeIndexMgr[int64](t.TempDir()+"dbtest.db", t.TempDir()+"dblog.log")
	page := btreeIndexMgr.requestNewPage(constants.LeafPageType)
	if page.getSize() != 0 {
		t.Errorf("GetSize not working as expected")
	}
}

func TestSetSize(t *testing.T) {
	btreeIndexMgr := getBtreeIndexMgr[int64](t.TempDir()+"dbtest.db", t.TempDir()+"dblog.log")
	page := btreeIndexMgr.requestNewPage(constants.LeafPageType)
	page.setSize(10)
	if page.getSize() != 10 {
		t.Errorf("SetSize not working as expected")
	}
}

func TestGetParentPageId(t *testing.T) {
	btreeIndexMgr := getBtreeIndexMgr[int64](t.TempDir()+"dbtest.db", t.TempDir()+"dblog.log")
	page := btreeIndexMgr.requestNewPage(constants.LeafPageType)
	if page.getParent() != -1 {
		t.Errorf("GetParentPageId not working as expected. Expected -1, got %d", page.getParent())
	}
}

func TestGetKeyEmptyPage(t *testing.T) {
	btreeIndexMgr := getBtreeIndexMgr[int64](t.TempDir()+"dbtest.db", t.TempDir()+"dblog.log")
	page := btreeIndexMgr.requestNewPage(constants.LeafPageType)
	if k, _, err := page.getKV(0); err == nil {
		t.Errorf("GetKey not working as expected. Expected -1, got %d", k)
	}
}

func TestAddKeyEmptyPage(t *testing.T) {
	btreeIndexMgr := getBtreeIndexMgr[int64](t.TempDir()+"dbtest.db", t.TempDir()+"dblog.log")
	page := btreeIndexMgr.requestNewPage(constants.LeafPageType)
	if page.addKV(0, 1, initLeafPageValue(0, 1)); page.getSize() != 1 {
		t.Errorf("AddKey not working as expected. Expected 1, got %d", page.getSize())
	}
	if page.addKV(1, 2, initLeafPageValue(0, 2)); page.getSize() != 2 {
		t.Errorf("AddKey not working as expected. Expected 2, got %d", page.getSize())
	}
}

func TestGetKeyNonEmptyPage(t *testing.T) {
	btreeIndexMgr := getBtreeIndexMgr[int64](t.TempDir()+"dbtest.db", t.TempDir()+"dblog.log")
	page := btreeIndexMgr.requestNewPage(constants.LeafPageType)
	page.addKV(0, 1, initLeafPageValue(0, 0))
	if k, _, err := page.getKV(0); err != nil || k != 1 {
		t.Errorf("GetKey not working as expected. Expected 1, got %d", k)
	}
}

func TestDeleteKeyEmptyPage(t *testing.T) {
	btreeIndexMgr := getBtreeIndexMgr[int64](t.TempDir()+"dbtest.db", t.TempDir()+"dblog.log")
	page := btreeIndexMgr.requestNewPage(constants.LeafPageType)
	if page.deleteKV(0); page.getSize() != 0 {
		t.Errorf("DeleteKey not working as expected. Expected 0, got %d", page.getSize())
	}
}
func TestDeleteKeyNonEmptyPage(t *testing.T) {
	btreeIndexMgr := getBtreeIndexMgr[int64](t.TempDir()+"dbtest.db", t.TempDir()+"dblog.log")
	page := btreeIndexMgr.requestNewPage(constants.LeafPageType)
	page.addKV(0, 1, initLeafPageValue(0, 1))
	if page.deleteKV(0); page.getSize() != 0 {
		t.Errorf("DeleteKey not working as expected. Expected 0, got %d", page.getSize())
	}
}

func TestUpdateKVEmptyPage(t *testing.T) {
	btreeIndexMgr := getBtreeIndexMgr[int64](t.TempDir()+"dbtest.db", t.TempDir()+"dblog.log")
	leafPage := btreeIndexMgr.requestNewPage(constants.LeafPageType)
	//innerPage := btreeIndexMgr.requestNewPage(constants.InnerPageType)
	leafPage.addKV(0, 2, initLeafPageValue(0, 2))
	if leafPage.updateKV(0, 1, initLeafPageValue(0, 1)); leafPage.getSize() != 1 {
		if k, val, err := leafPage.getKV(0); err != nil || k != 1 || val.pageId != 0 {
			t.Errorf("UpdateKeys not working as expected. Expected 1, got %d", leafPage.getSize())
		}
	}

}
