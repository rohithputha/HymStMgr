package diskmgr

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/rohithputha/hymStMgr/constants"
)


const dbFileFormat string = ".db"
const logFileFormat string = ".log"
func fileFormatCheck(filePath, fileFormat string) bool{
	return strings.HasSuffix(filePath, fileFormat)
}


func (dm *DiskFileMetaData) Init(){
	var err error
	if !fileFormatCheck(dm.DbFilePath, dbFileFormat){
		panic("database file format incorrect!")
	}
	dm.dbFile, err = os.OpenFile(dm.DbFilePath, os.O_CREATE | os.O_RDWR | os.O_APPEND, 0644)
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

	dm.dbFileStat, err = os.Stat(dm.DbFilePath)
	if err!=nil{
		panic("db file stats not available")
	}		
	dm.TFS = dm.dbFileStat.Size() //here the regular is expected to be int64 on modern machines.
	dm.TPgs = dm.TFS/int64(constants.PageSize)

}


// WritePage should take byte data for a page id and write at the offset of the pageId.
func (dm *DiskFileMetaData) WritePage(pageId int, writeData []byte)(writeErr error) {
	dm.mux.Lock()
	defer dm.mux.Unlock()

	if(len(writeData)< constants.PageSize){
		return errors.New("number of bytes to be written is not equal to the page size for pageId:"+fmt.Sprintf("%d",pageId))
	}

	offset := int64(pageId * constants.PageSize)
	appendMode:= false
	if offset == dm.TFS {
		appendMode = true
	}
	if offset > dm.TFS {
		return errors.New("page failed to be appended after the EOF")
	}

	_, writeErr = dm.dbFile.WriteAt(writeData,offset)
	dm.dbFile.Sync()

	if writeErr!=nil && appendMode {
		dm.reloadFileStats()
	}
	return writeErr
}

func (dm *DiskFileMetaData) reloadFileStats() {
	dm.TFS = dm.dbFileStat.Size()
	dm.TPgs = dm.TFS/int64(constants.PageSize)
}

func (dm *DiskFileMetaData) ReadPage(pageId int,read []byte)(readData []byte, readErr error){
	dm.mux.Lock()
	defer dm.mux.Unlock()
	
	offset := int64(pageId * constants.PageSize)
	numRead, readErr := dm.dbFile.ReadAt(read,offset)
	if readErr!=nil{
		return nil, readErr
	}
	if numRead < constants.PageSize {
		return nil,errors.New("number of bytes read is not equal to the pagesize for pageId:"+fmt.Sprintf("%d",pageId))
	}
	return read, readErr
} 

func (dm *DiskFileMetaData) GetTotalNumPages()(numPages int){
	return int(dm.TPgs)
}

//need to add append log and read log
//at this point log is not very used in the OLTP I feel


