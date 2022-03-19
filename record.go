package db

import (
	"encoding/binary"
	"errors"
)

const (
	recordHeaderByteCount = 8
)

type record struct {
	key   []byte
	value []byte

	keyByteCount   uint32
	valueByteCount uint32
}

func newRecord(key, value []byte) record {
	return record{
		key:            key,
		value:          value,
		keyByteCount:   uint32(len(key)),
		valueByteCount: uint32(len(value)),
	}
}

func newRecordByBytes(bytes []byte) (rec record, err error) {
	keyByteCount := binary.BigEndian.Uint32(bytes[:recordHeaderByteCount/2])
	valueByteCount := binary.BigEndian.Uint32(bytes[recordHeaderByteCount/2 : recordHeaderByteCount])

	if recordHeaderByteCount+keyByteCount+valueByteCount != uint32(len(bytes)) {
		return record{}, errors.New("bytes format was error!!!")
	}

	rec = record{
		key:   bytes[recordHeaderByteCount : recordHeaderByteCount+keyByteCount],
		value: bytes[recordHeaderByteCount+keyByteCount:],

		keyByteCount:   keyByteCount,
		valueByteCount: valueByteCount,
	}

	return rec, nil
}

func (rec *record) toBytes() (result []byte) {
	result = make([]byte, recordHeaderByteCount+rec.keyByteCount+rec.valueByteCount)

	binary.BigEndian.PutUint32(
		result[0:recordHeaderByteCount/2], rec.keyByteCount,
	)
	binary.BigEndian.PutUint32(
		result[recordHeaderByteCount/2:recordHeaderByteCount], rec.valueByteCount,
	)

	copy(
		result[recordHeaderByteCount:recordHeaderByteCount+rec.keyByteCount],
		[]byte(rec.key),
	)
	copy(result[recordHeaderByteCount+rec.keyByteCount:], rec.value)

	return
}
