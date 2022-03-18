package db

import (
	"bytes"
	"strconv"
	"testing"
)

var db LSMDb

func TestMain(m *testing.M) {
	db = New()
}

func TestWrite(t *testing.T) {
	if err := db.write("Hello", []byte("World")); err != nil {
		t.Fail()
	}
}

func TestRead(t *testing.T) {
	if err := db.write("Hello", []byte("World")); err != nil {
		t.Fail()
	}

	if value, err := db.read("Hello"); err != nil || !bytes.Equal(value, []byte("World")) {
		t.Fail()
	}
}

func FuzzRead(f *testing.F) {
	f.Fuzz(func(t *testing.T, key, value string) {
		t.Logf("%s, %s", key, value)

		if err := db.write(key, []byte(value)); err != nil {
			t.Errorf("write key and value into database error: %s, %s", key, value)
		}

		if queriedValue, err := db.read(key); err != nil || !bytes.Equal(queriedValue, []byte(value)) {
			t.Errorf("read key and value from database error: %s, %s", key, value)
		}
	})
}

func BenchmarkWrite(b *testing.B) {
	for n := 0; n < b.N; n++ {
		for i := 0; i < 1000; i++ {
			key := strconv.Itoa(i)
			value := []byte{byte(i)}

			err := db.write(key, value)
			if err != nil {
				b.Errorf("write test of benchmark error: %s, %s", key, value)
			}
		}
	}
}
