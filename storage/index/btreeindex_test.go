package index

import (
	"bytes"
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
	//customBufferedReader := customBufferReader[int64]{
	//	numBytesPerCustomRead: 8,
	//}

}
