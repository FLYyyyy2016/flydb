package flydb

type Pair struct {
	keyLength   uint32
	valueLength uint32
	offset      uint32
	key         []byte
	value       []byte
}

func getPair(data []byte) *Pair {
	getUint32 := unsafeGetUint32FromBytes
	return &Pair{
		keyLength:   getUint32(data[0:4]),
		valueLength: getUint32(data[4:8]),
		offset:      getUint32(data[8:12]),
		key:         data[12 : 12+getUint32(data[0:4])],
		value:       data[getUint32(data[8:12])-getUint32(data[4:8]) : getUint32(data[8:12])],
	}
}

func setPair(key, value string) []byte {
	pair := Pair{
		keyLength:   uint32(len(key)),
		valueLength: uint32(len(value)),
		offset:      uint32(len(key)) + uint32(len(value)),
		key:         []byte(key),
		value:       []byte(value),
	}
	data := make([]byte, 0)
	data = append(data, unsafeGetBytesFromUint32(pair.keyLength)...)
	data = append(data, unsafeGetBytesFromUint32(pair.valueLength)...)
	data = append(data, unsafeGetBytesFromUint32(pair.offset)...)
	data = append(data, key...)
	data = append(data, value...)
	return data
}
