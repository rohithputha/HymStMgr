package storage

import (
	"sync"

	"github.com/rohithputha/hymStMgr/constants"
)

type Page struct{
	PageId int
	pageData [constants.PageSize]byte // this will be a copy of page data
	Pin int
	IsDirty bool
	IsFlushed bool
	IsCorrupted bool
	IsOccupied bool	
	pageMux *sync.Mutex
}

func (ps *Page) NewPage(){

}