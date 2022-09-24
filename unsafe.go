package flydb

import (
	"reflect"
	"unsafe"
)

func unsafeGetUint32FromBytes(data []byte) uint32 {
	if len(data) != 4 {
		return 0
	}
	point := unsafe.Pointer(&data[0])
	number := (*uint32)(point)
	return *number
}

func unsafeGetBytesFromUint32(number uint32) []byte {
	data := make([]byte, 4)
	data[3] = uint8(number)
	data[2] = uint8(number >> 8)
	data[1] = uint8(number >> 16)
	data[0] = uint8(number >> 24)
	return data
}

func unsafeSlice(slice, data unsafe.Pointer, len int) {
	s := (*reflect.SliceHeader)(slice)
	s.Data = uintptr(data)
	s.Cap = len
	s.Len = len
}

func unsafeAdd(base unsafe.Pointer, offset uintptr) unsafe.Pointer {
	return unsafe.Pointer(uintptr(base) + offset)
}
