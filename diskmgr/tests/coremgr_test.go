package diskmgr

import (
	"crypto/rand"
	"errors"
	"os"
	"testing"

	"github.com/rohithputha/HymStMgr/constants"
	"github.com/rohithputha/HymStMgr/diskmgr"
)

func TestGetDiskFileMgr(test *testing.T) {
	d := diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblogtest.log",
	}
	diskFile := diskmgr.GetDiskFileMgr(d)
	if diskFile == nil {
		test.Errorf("disk file mgr not working as expected")
	}
}

func TestWritePage(test *testing.T) {
	d := diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblogtest.log",
	}
	diskFile := diskmgr.GetDiskFileMgr(d)
	testByteArray := make([]byte, 4096)
	writeErr := diskFile.WritePage(0, testByteArray)
	if writeErr != nil {

		test.Errorf("db write page not working as expected")
		return
	}

	fileInfo, err := os.Stat(d.DbFilePath)
	if err != nil {
		test.Errorf("file info error")
		return
	}
	if fileInfo.Size() != int64(constants.PageSize) {

		test.Errorf("db write page not working as expected")
	}
}

func TestWritePageDiffArrayLength(test *testing.T) {
	d := diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblogtest.log",
	}
	diskFile := diskmgr.GetDiskFileMgr(d)
	testByteArray := make([]byte, 4000)
	writeErr := diskFile.WritePage(0, testByteArray)
	if writeErr == nil || writeErr.Error() != errors.New("write page size less than the actual page size defined").Error() {
		test.Errorf("write fault page does not throw error")
	}
}

func TestWritePageAppendChecks(test *testing.T) {
	d := diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblogtest.log",
	}
	diskFile := diskmgr.GetDiskFileMgr(d)
	testByteArray := make([]byte, constants.PageSize)
	writeErr := diskFile.WritePage(2, testByteArray)
	if writeErr == nil || writeErr.Error() != errors.New("page failed to be appended after the EOF").Error() {
		test.Errorf("write page append checks not working as expected")
		return
	}
}

func TestWritePageAppend(test *testing.T) {
	d := diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblogtest.log",
	}
	diskFile := diskmgr.GetDiskFileMgr(d)
	testByteArray := make([]byte, constants.PageSize)
	diskFile.WritePage(0, testByteArray)

	testByteArray1 := make([]byte, constants.PageSize)
	writeErr := diskFile.WritePage(1, testByteArray1)

	if writeErr != nil {
		test.Errorf("write page with append is not working as expected")
	}
}

func TestReadPage(test *testing.T) {
	d := diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblogtest.log",
	}
	diskFile := diskmgr.GetDiskFileMgr(d)
	testByteArray := make([]byte, constants.PageSize)
	rand.Read(testByteArray)

	testByteArrayRead := make([]byte, constants.PageSize)
	test.Log(testByteArrayRead[1])
	diskFile.WritePage(0, testByteArray)

	diskFile.ReadPage(0, testByteArrayRead)
	if testByteArray[100] != testByteArrayRead[100] {
		test.Errorf("read page error ")
	}
}

func TestReadPageNonExists(test *testing.T) {
	d := diskmgr.DiskFileInit{
		DbFilePath:  test.TempDir() + "dbtest.db",
		LogFilePath: test.TempDir() + "dblogtest.log",
	}
	diskFile := diskmgr.GetDiskFileMgr(d)
	testByteArray := make([]byte, constants.PageSize)
	err := diskFile.ReadPage(0, testByteArray)
	if err == nil || err.Error() != errors.New("read page not present").Error() {
		test.Errorf("read page error not thrown when the page does not exist")
	}
}
