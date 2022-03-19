package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
)

type KeyType string
type ValueType []byte

type LSMDb interface {
	Read(key KeyType) (ValueType, error)
	Write(key KeyType, value ValueType) error
}

type Configuration struct {
	LimitKeyCount int
	StorePath     string
}

type database struct {
	conf         Configuration
	mTableCount  int
	activeMTable map[KeyType]ValueType
	refreshChan  chan map[KeyType]ValueType
}

const defaultSize = 1024

func New(conf Configuration) LSMDb {
	db := &database{
		conf:         conf,
		mTableCount:  0,
		activeMTable: make(map[KeyType]ValueType, defaultSize),
		refreshChan:  make(chan map[KeyType]ValueType),
	}

	go db.refresh()

	return db
}

func (db *database) Read(key KeyType) (ValueType, error) {
	if value, existed := db.activeMTable[key]; existed {
		return value, nil
	}
	return []byte{}, errors.New("key Not Existed")
}

func (db *database) Write(key KeyType, value ValueType) error {
	if db.isNeedRefresh() {
		db.refreshChan <- db.activeMTable
		db.activeMTable = make(map[KeyType]ValueType, defaultSize)
	}

	db.activeMTable[key] = value
	return nil
}

func (db *database) isNeedRefresh() bool {
	return db.conf.LimitKeyCount != 0 && len(db.activeMTable) >= db.conf.LimitKeyCount
}

func (db *database) refresh() {
	fmt.Println("run refresh!!!")

	for true {
		fmt.Println("start refresh mTable")
		mTable := <-db.refreshChan

		data := make([][]byte, len(mTable))
		totalBytesSize := 0

		startIndex := 0
		var record []byte
		for key, value := range mTable {
			fmt.Printf("key: %x, value: %x \n", key, value)

			kBytes := []byte(key)

			record = make([]byte, 4+4+len(kBytes)+len(value))

			binary.BigEndian.PutUint32(record[0:4], uint32(len(kBytes)))
			binary.BigEndian.PutUint32(record[4:8], uint32(len(value)))

			copy(record[8:8+len(kBytes)], key)
			copy(record[8+len(kBytes):], value)

			totalBytesSize += len(record)

			data[startIndex] = record
			startIndex++
		}

		fmt.Printf("record bytes array: %x \n", data)

		result := make([]byte, totalBytesSize+len(data)*16)

		startIndex = 0
		middleIndex := 0
		endndIndex := 0
		for _, record := range data {
			middleIndex = startIndex + 16
			endndIndex = middleIndex + len(record)

			binary.BigEndian.PutUint64(result[startIndex:middleIndex], uint64(len(record)))
			copy(result[middleIndex:endndIndex], record)

			startIndex = endndIndex
		}

		fmt.Printf("final store bytes: %x \n", result)

		os.WriteFile(db.conf.StorePath+"/"+strconv.Itoa(db.mTableCount)+".db-lsm", result, 0544)
		db.mTableCount++
	}
}
