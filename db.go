package db

import (
	"errors"
	"os"
	"strconv"
)

type LSMDb interface {
	Read(key []byte) ([]byte, error)
	Write(key []byte, value []byte) error
}

type Configuration struct {
	TableRecordMaxCountLimit uint32
	StorePath                string
}

type database struct {
	conf         Configuration
	activeMTable table
	refreshChan  chan table
}

const (
	defaultSize  = 1024
	initTableNum = 0
)

func New(conf Configuration) LSMDb {
	db := &database{
		conf:         conf,
		activeMTable: newTable(initTableNum),
		refreshChan:  make(chan table),
	}

	go db.refresh()

	return db
}

func (db *database) Read(key []byte) ([]byte, error) {
	if value, existed := db.activeMTable.get(key); existed {
		return value, nil
	}
	return []byte{}, errors.New("key Not Existed")
}

func (db *database) Write(key []byte, value []byte) (err error) {
	if db.isNeedRefresh() {
		db.refreshChan <- db.activeMTable
		db.activeMTable = newTable(db.activeMTable.tableNum + 1)
	}

	db.activeMTable.put(key, value)
	return
}

func (db *database) isNeedRefresh() bool {
	// TODO: this has a bug, don't get count of active table, so don't refresh data to file
	return db.conf.TableRecordMaxCountLimit != 0 && db.activeMTable.count >= db.conf.TableRecordMaxCountLimit
}

func (db *database) refresh() {
	// TODO: need track log when background thread of refresh start
	for {
		mTable, isOpen := <-db.refreshChan
		os.WriteFile(db.conf.StorePath+"/"+strconv.Itoa(int(mTable.tableNum))+".db-lsm", mTable.toBytes(), 0544)

		if !isOpen {
			break
		}
	}
}
