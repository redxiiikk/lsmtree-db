package pkg

import (
	"bytes"
	"testing"
)

func Test_write(t *testing.T) {
	db := New()

	if err := db.write("Hello", []byte("World")); err != nil {
		t.Fail()
	}
}

func Test_read(t *testing.T) {
	db := New()

	if err := db.write("Hello", []byte("World")); err != nil {
		t.Fail()
	}

	if value, err := db.read("Hello"); err != nil || !bytes.Equal(value, []byte("World")) {
		t.Fail()
	}
}
