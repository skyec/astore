package astore

import (
	"io"
	"io/ioutil"
	"testing"
)

func TestDirectKey(t *testing.T) {
	testDir := mkTestDir()
	defer rmTestDir(testDir)

	dk, err := newDirectKey(testDir)
	if err != nil {
		t.Fatal(err)
	}

	value := []byte("bar")
	key := "foo"
	err = dk.Append(key, value)
	if err != nil {
		t.Fatal(err)
	}

	k, err := OpenKey(testDir, key)
	if err != nil {
		t.Fatal(err)
	}

	var result string
	err = k.ReadEach(func(r io.Reader) error {
		b, err := ioutil.ReadAll(r)
		if err != nil {
			return err
		}
		result = string(b)
		return nil
	})
	if err != nil {
		t.Error(err)
	}
	if result != string(value) {
		t.Errorf("Expected: %s, got: %s", value, result)
	}
}
