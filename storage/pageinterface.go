package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"

	"github.com/rohithputha/HymStMgr/constants"
)

var headersOrder []string = []string{"PageType", "Lsn", "Size", "MaxSize", "ParentPageId", "PageId"}

const basePageHeaderByteSize = 8 * 6 //6 const headers (int) with 8 bytes each

type Page struct {
	PageId      int
	pageData    [constants.PageSize]byte // this will be a copy of page data
	Pin         int
	IsDirty     bool
	IsFlushed   bool
	IsCorrupted bool
	IsOccupied  bool
	bp          *BasePage
	pageMux     *sync.RWMutex
}

type BasePage struct {
	PageType     int //8bytes
	Lsn          int //8bytes
	Size         int //8bytes -> variable
	MaxSize      int //8bytes
	ParentPageId int //8bytes -> variable
	PageId       int //8bytes
	DataArea     []byte
}

func bytesIntegerConv(buffer *bytes.Buffer) int {
	var n int
	err := binary.Read(buffer, binary.BigEndian, &n)
	if err != nil {
		panic(err)
	}
	return n
}

func nextInt16(buffer *bytes.Buffer) int16 {
	var n int16
	err := binary.Read(buffer, binary.BigEndian, &n)
	if err != nil {
		panic(err)
	}
	return n
}

func getByteBuffer(b []byte) *bytes.Buffer {
	return bytes.NewBuffer(b)
}

func (p *Page) decodeHeaders() (totalDecodeLength int) {
	p.pageMux.RLock()
	defer p.pageMux.RUnlock()
	headerBuffer := getByteBuffer(p.pageData[:])
	for _, v := range headersOrder {
		switch v {
		case "PageType":
			p.bp.PageType = bytesIntegerConv(headerBuffer)
		case "Lsn":
			p.bp.Lsn = bytesIntegerConv(headerBuffer)
		case "Size":
			p.bp.Size = bytesIntegerConv(headerBuffer)
		case "MaxSize":
			p.bp.MaxSize = bytesIntegerConv(headerBuffer)
		case "ParentPageId":
			p.bp.ParentPageId = bytesIntegerConv(headerBuffer)
		case "PageId":
			p.bp.PageId = bytesIntegerConv(headerBuffer)
		}
	}
	return basePageHeaderByteSize
}

func integerBytesConv(d int) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, d)
	if err != nil {
		panic("encode failed") // handle these errors in a better way
	}
	return buf.Bytes()
}

func (p *Page) encodeHeaders() []byte {
	headersBytes := make([]byte, basePageHeaderByteSize)
	for _, v := range headersOrder {
		switch v {
		case "PageType":
			headersBytes = append(headersBytes, integerBytesConv(p.bp.PageType)...)
		case "Lsn":
			headersBytes = append(headersBytes, integerBytesConv(p.bp.Lsn)...)
		case "Size":
			headersBytes = append(headersBytes, integerBytesConv(p.bp.Size)...)
		case "MaxSize":
			headersBytes = append(headersBytes, integerBytesConv(p.bp.MaxSize)...)
		case "ParentPageId":
			headersBytes = append(headersBytes, integerBytesConv(p.bp.ParentPageId)...)
		case "PageId":
			headersBytes = append(headersBytes, integerBytesConv(p.bp.PageId)...)
		}
	}
	return headersBytes
}

//func (p *Page) decodeTuples(slotArray []byte, tuples []byte) (totalTupleBytesDecoded int16) {
//	//tOffset := int16(constants.PageSize - len(tuples))
//	//slotArrayBuffer := getByteBuffer(slotArray)
//	//p.bp.tuples = make([][]byte, 0)
//	//totalTupleBytesDecoded = int16(0)
//	//for i := 0; i < p.bp.slotArraySize; i++ {
//	//	tPtr := nextInt16(slotArrayBuffer)
//	//	tSize := nextInt16(slotArrayBuffer)
//	//	if tPtr == -1 {
//	//		p.bp.tuples = append(p.bp.tuples, make([]byte, 0))
//	//		continue
//	//	}
//	//	var tCopy []byte
//	//	copy(tuples[tPtr-tOffset:(tPtr-tOffset)+tSize], tCopy)
//	//	p.bp.tuples = append(p.bp.tuples, tCopy)
//	//	totalTupleBytesDecoded += tSize
//	//}
//	//return
//}

func (p *Page) Decode() {
	if p.isDecoded() {
		return
	}
	totalHeadersDecodeLength := p.decodeHeaders()
	p.bp.DataArea = p.pageData[totalHeadersDecodeLength:]
}

func (p *Page) Encode() (encodeErr error) {
	p.pageMux.Lock()
	defer p.pageMux.Unlock()
	headerBytes := p.encodeHeaders()
	if len(p.bp.DataArea) != constants.PageSize-basePageHeaderByteSize {
		return errors.New("DataArea not required Size. encode page failed")
	}
	p.pageData = [4096]byte(append(headerBytes, p.bp.DataArea...)) // should pageData have another allocation or he the data copied to it manually maintaining the same array loc?
	return nil
}

func (ps *Page) NewPage() {
	for i := range ps.pageData {
		ps.pageData[i] = 0
	}
	ps.pageData[0] = 1
}

func (p *Page) GetDecodedBasePage() *BasePage {
	return p.bp
}

func (p *Page) isDecoded() bool {
	return p.PageId == p.bp.PageId
}
