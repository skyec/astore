package metastore

import (
	"fmt"

	"github.com/boltdb/bolt"
)

const (
	boltFile        = "metakvstore.bolt"
	boltPermissions = 0644
	bucketName      = "metakv"
)

// boltStore implements the MetaKVStore interface using boltdb local storage
// https://godoc.org/github.com/boltdb/bolt
type boltStore struct {
	db   *bolt.DB
	conf *Config
}

// newBoltStore constructs a new boltStore instance
func newBoltStore(conf *Config) (*boltStore, error) {
	// TODO: assert that conf.Bolt.BasePath is set
	db, err := bolt.Open(fmt.Sprintf("%s/%s", conf.Bolt.BasePath, boltFile), boltPermissions, nil)
	if err != nil {
		return nil, fmt.Errorf("error opening bold DB: %s", err)
	}

	return &boltStore{
		db: db,
	}, nil
}

// Get returns the data associated with key. Get implements the MetaKVStore Get interface.
func (bs *boltStore) Get(key []byte) ([]byte, error) {

	result := []byte{}
	err := bs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return nil
		}
		result = b.Get(key)
		return nil
	})
	return result, err
}

// Put stores the supplied value at key. Put implements the MetaKVStore Put interface.
func (bs *boltStore) Put(key []byte, value []byte) error {

	err := bs.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return fmt.Errorf("error creating bucket: %s: %s:", bucketName, err)
		}
		err = b.Put(key, value)
		if err != nil {
			return fmt.Errorf("put error: %s: %s: %s", bucketName, key, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("update error: %s", err)
	}
	return nil

}
