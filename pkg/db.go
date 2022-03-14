package pkg

type LSMDb interface {
	read(key string) ([]byte, error)
	write(key string, value []byte) error
}

func New() LSMDb {
	return &Db{}
}

type Db struct {
}

func (db *Db) read(key string) ([]byte, error) {
	return []byte("World"), nil
}

func (db *Db) write(key string, value []byte) error {
	return nil
}
