package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	defaultRecordInfoByteCount  = 1024
	defaultRecordQeueBufferSize = 16

	tableNumBytesCount     = 4
	totalCountByteCount    = 8
	preRecordInfoByteCount = 64
)

// TODO: need rename, like mTable ?
type table struct {
	tableNum uint16

	// TODO: need replace to map for optimization query
	records    []record
	recordQeue chan record
	count      uint32

	totalByteCount uint64
}

func newTable(tableNum uint16) (result table) {
	result = table{
		tableNum: tableNum,

		records:    make([]record, defaultRecordInfoByteCount),
		recordQeue: make(chan record, defaultRecordQeueBufferSize),

		count:          0,
		totalByteCount: 0,
	}

	go result.doInsert()

	return result
}

func (t *table) get(key []byte) ([]byte, bool) {
	for _, record := range t.records {
		if bytes.Compare(key, record.key) == 0 {
			return record.value, true
		}
	}

	return []byte{}, false
}

func (t *table) put(key, value []byte) {
	// TODO: stop insert new record when count of record more or byte count more than max count
	t.recordQeue <- newRecord(key, value)
}

func (t *table) doInsert() {
	for {
		record, isOpen := <-t.recordQeue

		t.records[t.count] = record

		t.totalByteCount += uint64(record.keyByteCount) + uint64(record.valueByteCount)
		t.count++

		if !isOpen {
			break
		}
	}
}

func (t *table) toBytes() (result []byte) {
	result = make(
		[]byte,
		tableNumBytesCount+totalCountByteCount+t.totalByteCount+uint64(t.count)*preRecordInfoByteCount,
	)

	binary.BigEndian.PutUint16(result[:tableNumBytesCount], t.tableNum)
	binary.BigEndian.PutUint32(result[tableNumBytesCount:tableNumBytesCount+totalCountByteCount], t.count)

	var startIndex, recordByteCount, endIndex uint64 = tableNumBytesCount + totalCountByteCount, 0, 0
	for _, record := range t.records {
		recordByteCount = uint64(record.keyByteCount + record.valueByteCount)
		endIndex = startIndex + defaultRecordInfoByteCount + recordByteCount

		binary.BigEndian.PutUint64(
			result[startIndex:startIndex+recordHeaderByteCount],
			recordByteCount,
		)
		copy(result[startIndex+recordHeaderByteCount:endIndex], record.toBytes())

		startIndex = endIndex
	}

	return
}

func newTableByBytes(bytes []byte) (result table, err error) {
	tableNum := binary.BigEndian.Uint16(bytes[:tableNumBytesCount])
	var count uint32 = 0

	totalRecordCount := binary.BigEndian.Uint32(
		bytes[tableNumBytesCount : tableNumBytesCount+totalCountByteCount],
	)
	totalByteCount := uint64(len(bytes))

	records := make([]record, totalRecordCount)

	var startIndex, recordByteCount, endIndex uint64 = 0, 0, 0
	for startIndex <= totalByteCount {
		recordByteCount = binary.BigEndian.Uint64(bytes[startIndex:defaultRecordInfoByteCount])
		endIndex = startIndex + defaultRecordInfoByteCount + recordByteCount

		record, err := newRecordByBytes(bytes[startIndex+defaultRecordInfoByteCount : endIndex])
		if err != nil { // TODO: need provide a configuration of skip error record
			return table{}, err
		}

		records[count] = record

		count++
		startIndex = endIndex
	}

	if count != totalRecordCount {
		return table{}, errors.New(fmt.Sprintf("count of record in table binary file is illegal: %d", tableNum))

	}

	result = table{
		tableNum: tableNum,

		records:    records,
		recordQeue: make(chan record, defaultRecordQeueBufferSize),

		count:          totalRecordCount,
		totalByteCount: totalByteCount,
	}

	return
}
