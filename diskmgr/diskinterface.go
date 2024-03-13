package diskmgr

import (
	"os"
	"sync"
)

// const diskFactory DiskFactory = DiskFactory{}

type DiskFileMetaData struct{
	DbFilePath string
	LogFilePath string
	TFS int64
	TPgs int64
	dbFile *(os.File)
	logFile *(os.File)
	dbFileStat (os.FileInfo)
	mux *sync.Mutex
}

type DiskFileFunctions interface{
	Init()
	WritePage(pageId int, writeData []byte)(writeErr error)
	ReadPage(pageId int)(readData []byte, readErr error)
}

// type DiskFactory struct{
// 	diskFileMap map[string]DiskFileMetaData
// 	dfmMux *sync.Mutex
// }

// func GetDiskFactory(){
// 	fileMap := make(map[string]DiskFileMetaData)
// 	diskFactory = DiskFactory{diskFileMap: fileMap}
// 	diskFactory = diskFactory{}
// }

// func (di *DiskInterface) get