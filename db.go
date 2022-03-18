package db

import "errors"

type LSMDb interface {
	read(key string) ([]byte, error)
	write(key string, value []byte) error
}

type database struct {
	store map[string][]byte
}

func New() LSMDb {
	return &database{
		store: map[string][]byte{},
	}
}

func (db *database) read(key string) ([]byte, error) {
	if value, existed := db.store[key]; existed {
		return value, nil
	}
	return []byte{}, errors.New("key Not Existed")
}

func (db *database) write(key string, value []byte) error {
	db.store[key] = value
	return nil
}
