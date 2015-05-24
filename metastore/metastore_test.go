package metastore

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestNewKVStoreBolt(t *testing.T) {

	var err error

	conf := NewConfig()
	conf.Bolt.BasePath, err = ioutil.TempDir("", "metastore-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(conf.Bolt.BasePath)

	store, err := NewKVStore(KV_TYPE_BOLT, conf)
	if err != nil {
		t.Fatal(err)
	}

	if store == nil {
		t.Fatal("store is nil")
	}
}
