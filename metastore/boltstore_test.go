package metastore

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

func TestBoltStore(t *testing.T) {
	testDir, err := ioutil.TempDir("", "bolt-test")
	if err != nil {
		t.Fatal("Error creating temp dir:", err)
	}
	defer os.RemoveAll(testDir)

	conf := NewConfig()
	conf.Bolt.BasePath = testDir

	_, err = newBoltStore(conf)
	if err != nil {
		t.Fatal("Error creating the bolt store:", err)
	}

}

func TestBoltStorePut(t *testing.T) {

	store, testDir := initTest(t)
	defer os.RemoveAll(testDir)

	err := store.Put([]byte("foo-key"), []byte("the data"))
	if err != nil {
		t.Fatal("Put returned an error:", err)
	}
}

func TestBoltStorePutGet(t *testing.T) {

	store, testDir := initTest(t)
	defer os.RemoveAll(testDir)
	key := []byte("foo-key")
	value := []byte("the data")

	err := store.Put(key, value)
	if err != nil {
		t.Fatal("Put returned an error:", err)
	}

	vget, err := store.Get(key)
	if err != nil {
		t.Fatal("Get returned an error:", err)
	}

	if !bytes.Equal(value, vget) {
		t.Errorf("Expected: %s\nGot: %s", value, vget)
	}

}

func initTest(t *testing.T) (*boltStore, string) {

	testDir, err := ioutil.TempDir("", "bolt-test")
	if err != nil {
		t.Fatal("Error creating temp dir:", err)
	}
	conf := NewConfig()
	conf.Bolt.BasePath = testDir

	store, err := newBoltStore(conf)
	if err != nil {
		os.RemoveAll(testDir)
		t.Fatal("Error creating the bolt store:", err)
	}

	return store, testDir
}
