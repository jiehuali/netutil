package netutil

import (
	"reflect"
	"unsafe"
)

func getUint(buff []byte, pack int) int {
	var ptr = unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&buff)).Data)

	switch pack {
	case 1:
		return int(buff[0])
	case 2:
		return int(*(*uint16)(ptr))
	case 4:
		return int(*(*uint32)(ptr))
	case 8:
		return int(*(*uint64)(ptr))
	}

	return 0
}

func setUint(buff []byte, pack, value int) {
	var ptr = unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&buff)).Data)

	switch pack {
	case 1:
		buff[0] = byte(value)
	case 2:
		*(*uint16)(ptr) = uint16(value)
	case 4:
		*(*uint32)(ptr) = uint32(value)
	case 8:
		*(*uint64)(ptr) = uint64(value)
	}
}

func getUint16(target []byte) uint16 {
	return *(*uint16)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&target)).Data))
}

func setUint16(target []byte, value uint16) {
	*(*uint16)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&target)).Data)) = value
}

func getUint32(target []byte) uint32 {
	return *(*uint32)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&target)).Data))
}

func setUint32(target []byte, value uint32) {
	*(*uint32)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&target)).Data)) = value
}
