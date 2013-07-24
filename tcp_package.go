package tcputil

import (
	"reflect"
	"unsafe"
)

type Output struct {
	owner *Conn
	buff  []byte
	Data  []byte
}

func (this *Output) Send() error {
	return this.owner.sendRaw(this.buff)
}

func (this *Output) WriteUint(pack int, value uint64) *Output {
	switch pack {
	case 1:
		this.WriteUint8(uint8(value))
	case 2:
		this.WriteUint16(uint16(value))
	case 4:
		this.WriteUint32(uint32(value))
	case 8:
		this.WriteUint64(uint64(value))
	default:
		panic("pack != 1 || pack != 2 || pack != 4 || pack != 8")
	}

	return this
}

func (this *Output) WriteInt8(value int8) *Output {
	if len(this.Data) < 1 {
		panic("index out of range")
	}
	this.Data[0] = byte(value)
	this.Data = this.Data[1:]
	return this
}

func (this *Output) WriteUint8(value uint8) *Output {
	if len(this.Data) < 1 {
		panic("index out of range")
	}
	this.Data[0] = byte(value)
	this.Data = this.Data[1:]
	return this
}

func (this *Output) WriteInt16(value int16) *Output {
	if len(this.Data) < 2 {
		panic("index out of range")
	}
	*(*int16)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&this.Data)).Data)) = value
	this.Data = this.Data[2:]
	return this
}

func (this *Output) WriteUint16(value uint16) *Output {
	if len(this.Data) < 2 {
		panic("index out of range")
	}
	*(*uint16)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&this.Data)).Data)) = value
	this.Data = this.Data[2:]
	return this
}

func (this *Output) WriteInt32(value int32) *Output {
	if len(this.Data) < 4 {
		panic("index out of range")
	}
	*(*int32)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&this.Data)).Data)) = value
	this.Data = this.Data[4:]
	return this
}

func (this *Output) WriteUint32(value uint32) *Output {
	if len(this.Data) < 4 {
		panic("index out of range")
	}
	*(*uint32)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&this.Data)).Data)) = value
	this.Data = this.Data[4:]
	return this
}

func (this *Output) WriteInt64(value int64) *Output {
	if len(this.Data) < 8 {
		panic("index out of range")
	}
	*(*int64)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&this.Data)).Data)) = value
	this.Data = this.Data[8:]
	return this
}

func (this *Output) WriteUint64(value uint64) *Output {
	if len(this.Data) < 8 {
		panic("index out of range")
	}
	*(*uint64)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&this.Data)).Data)) = value
	this.Data = this.Data[8:]
	return this
}

func (this *Output) WriteBytes(data []byte) *Output {
	if len(this.Data) < len(data) {
		panic("index out of range")
	}
	copy(this.Data, data)
	this.Data = this.Data[len(data):]
	return this
}

func (this *Output) WriteBytes8(data []byte) *Output {
	this.WriteUint8(uint8(len(data)))
	this.WriteBytes(data)
	return this
}

func (this *Output) WriteBytes16(data []byte) *Output {
	this.WriteUint16(uint16(len(data)))
	this.WriteBytes(data)
	return this
}

func (this *Output) WriteBytes32(data []byte) *Output {
	this.WriteUint32(uint32(len(data)))
	this.WriteBytes(data)
	return this
}

type Input struct {
	Data []byte
}

func NewInput(data []byte) *Input {
	return &Input{data}
}

func (this *Input) Seek(n int) *Input {
	this.Data = this.Data[n:]
	return this
}

func (this *Input) ReadInt8() int8 {
	var result = int8(this.Data[0])
	this.Data = this.Data[1:]
	return result
}

func (this *Input) ReadUint8() uint8 {
	var result = uint8(this.Data[0])
	this.Data = this.Data[1:]
	return result
}

func (this *Input) ReadInt16() int16 {
	if len(this.Data) < 2 {
		panic("index out of range")
	}
	var result = *(*int16)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&this.Data)).Data))
	this.Data = this.Data[2:]
	return result
}

func (this *Input) ReadUint16() uint16 {
	if len(this.Data) < 2 {
		panic("index out of range")
	}
	var result = *(*uint16)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&this.Data)).Data))
	this.Data = this.Data[2:]
	return result
}

func (this *Input) ReadInt32() int32 {
	if len(this.Data) < 4 {
		panic("index out of range")
	}
	var result = *(*int32)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&this.Data)).Data))
	this.Data = this.Data[4:]
	return result
}

func (this *Input) ReadUint32() uint32 {
	if len(this.Data) < 4 {
		panic("index out of range")
	}
	var result = *(*uint32)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&this.Data)).Data))
	this.Data = this.Data[4:]
	return result
}

func (this *Input) ReadInt64() int64 {
	if len(this.Data) < 8 {
		panic("index out of range")
	}
	var result = *(*int64)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&this.Data)).Data))
	this.Data = this.Data[8:]
	return result
}

func (this *Input) ReadUint64() uint64 {
	if len(this.Data) < 8 {
		panic("index out of range")
	}
	var result = *(*uint64)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&this.Data)).Data))
	this.Data = this.Data[8:]
	return result
}

func (this *Input) ReadBytes(n int) []byte {
	var result = this.Data[:n]
	this.Data = this.Data[n:]
	return result
}

func (this *Input) ReadBytes8() []byte {
	var n = this.ReadUint8()
	return this.ReadBytes(int(n))
}

func (this *Input) ReadBytes16() []byte {
	var n = this.ReadUint16()
	return this.ReadBytes(int(n))
}

func (this *Input) ReadBytes32() []byte {
	var n = this.ReadUint32()
	return this.ReadBytes(int(n))
}
