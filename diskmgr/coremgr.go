package diskmgr

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/rohithputha/hymStMgr/constants"
)


const dbFileFormat string = ".db"
const logFileFormat string = ".log"
func fileFormatCheck(filePath, fileFormat string) bool{
	return strings.HasSuffix(filePath, fileFormat)
}



type DiskFileMetaData struct{
	DbFilePath string
	LogFilePath string
	dbFile *(os.File)
	logFile *(os.File)
	dbFileSize int64
	mux *sync.Mutex
}

type DiskFileInit struct {
	DbFilePath string 
	LogFilePath string
}

type DiskFile interface{
	Init()
	WritePage(pageId int, writeData []byte)(writeErr error)
	ReadPage(pageId int, readData []byte)(readErr error)
	GetTotalNumPages()int
}

func GetDiskFileMgr(init DiskFileInit) DiskFile{
	diskFileMd := DiskFileMetaData{
		DbFilePath:  init.DbFilePath,
		LogFilePath: init.LogFilePath,
		mux: &sync.Mutex{},
	}
	(&diskFileMd).Init()
	return &diskFileMd
}


func (dm *DiskFileMetaData) Init(){
	var err error
	if !fileFormatCheck(dm.DbFilePath, dbFileFormat){
		panic("database file format incorrect!")
	}
	dm.dbFile, err = os.OpenFile(dm.DbFilePath, os.O_CREATE | os.O_RDWR , 0644)
	if err!= nil{
		panic(err)
	}

	if !fileFormatCheck(dm.LogFilePath, logFileFormat){
		panic("log file format incorrect!")
	}
	dm.logFile, err = os.OpenFile(dm.LogFilePath, os.O_CREATE | os.O_APPEND , 0644)
	if err!=nil {
		panic(err)
	}

	dbFileInfo, err := dm.dbFile.Stat()
	if err!=nil{
		panic("db file stats not available")
	}		
	dm.dbFileSize = dbFileInfo.Size()

}


// WritePage should take byte data for a page id and write at the offset of the pageId.
func (dm *DiskFileMetaData) WritePage(pageId int, writeData []byte)(writeErr error) {
	dm.mux.Lock()
	defer dm.mux.Unlock()

	if(len(writeData)< constants.PageSize){
		return errors.New("write page size less than the actual page size defined")
	}
	appendMode := false
	offset := int64(pageId * constants.PageSize)

	if(offset == dm.dbFileSize){
		appendMode = true
	}
	if offset > dm.dbFileSize {
		return errors.New("page failed to be appended after the EOF")
	}

	_, writeErr = dm.dbFile.WriteAt(writeData,offset)
	dm.dbFile.Sync()

	if writeErr == nil && appendMode {
		dm.dbFileSize += int64(constants.PageSize)
	}
	return writeErr
}


func (dm *DiskFileMetaData) ReadPage(pageId int,read []byte)(readErr error){
	dm.mux.Lock()
	defer dm.mux.Unlock()
	
	offset := int64(pageId * constants.PageSize)
	if dm.dbFileSize == 0 || offset > dm.dbFileSize{
		return errors.New("read page not present")
	}

	numRead, readErr := dm.dbFile.ReadAt(read,offset)
	if readErr!=nil {
		return readErr
	}
	if numRead < constants.PageSize {
		return errors.New("number of bytes read is not equal to the pagesize for pageId:"+fmt.Sprintf("%d",pageId))
	}
	return readErr
} 

func (dm *DiskFileMetaData) GetTotalNumPages()(numPages int){
	return int((dm.dbFileSize)/int64(constants.PageSize))
}

//need to add append log and read log



