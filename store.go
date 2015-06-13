package astore

import (
	"fmt"
	"log"
	"os"

	"github.com/skyec/astore/metastore"
)

type WriteableKey interface {
	WriteToKey(key string, data []byte) error
}

type Store interface {
	Initialize() error
	Purge()
	GetMeta(key []byte) []byte
	PutMeta(key, value []byte)
	Close() error
}

type ReadableKey interface {
	ReadEachFromKey(key string, f ReadFunc) error
	GetCountFromKey(key string) (int, error)
}

type WriteableStore interface {
	WriteableKey
	Store
}

type ReadableStore interface {
	ReadableKey
	Store
}

type ReadWriteableStore interface {
	Store
	ReadableKey
	WriteableKey
}

// Implements the ReadWriteableStore interface
type store struct {
	path        string
	sequence    int64
	kv          metastore.KVStore
	initialized bool
	st          *stats
}

func NewReadWriteableStore(path string) (ReadWriteableStore, error) {
	s := newStore(path)
	return s, s.Initialize()
}

func newStore(path string) *store {
	return &store{
		path: path,
		st:   newStats(),
	}
}

// Initialize prepares the store for use.
// TODO: not sure wy this isn't part of the CTOR?
func (s *store) Initialize() (err error) {
	if s.initialized {
		return nil
	}
	if _, err = os.Stat(s.path); os.IsNotExist(err) {
		return os.MkdirAll(s.path, os.ModeDir)
	}
	if err != nil {
		return
	}

	conf := metastore.NewConfig()
	conf.Bolt.BasePath = s.path
	s.kv, err = metastore.NewKVStore(metastore.KV_TYPE_BOLT, conf)

	s.st.run()
	s.initialized = true
	log.Print("Getting stared at path:", s.path)

	return
}

// Purge destroys all the data in the store (if possible) and makes it unusable.
func (s *store) Purge() {
	os.RemoveAll(s.path)
}

// GetRootPath returns the path where the sore files are kept
// TODO: why is this public? If new storage backends are added this may not have any meaning.
func (s *store) GetRootPath() string {
	return s.path
}

// GetKeyPath returns the path where keys are located
// TODO: why is this public?
func (s *store) GetKeyPath() string {
	return fmt.Sprintf("%s/keys", s.path)
}

// WriteToKey appends data to the conent stored at key.
func (s *store) WriteToKey(key string, data []byte) error {
	k, err := OpenKey(s.GetKeyPath(), key)
	if err != nil {
		return fmt.Errorf("error opening key: %s", err)
	}
	err = k.Append(data)
	if err != nil {
		s.st.countError()
		return err
	}
	s.st.countWrite()
	return nil
}

// ReadEachFromKey reads the content at key and calls the callback, f, for each content block.
func (s *store) ReadEachFromKey(key string, f ReadFunc) error {

	k, err := OpenKey(s.GetKeyPath(), key)
	if err != nil {
		return err
	}
	return k.ReadEach(f)
}

// GetCountFromKey returns the number of items saved at key.
func (s *store) GetCountFromKey(key string) (int, error) {

	k, err := OpenKey(s.GetKeyPath(), key)
	if err != nil {
		return 0, err
	}
	return k.Count()
}

// GetMeta returns the value contained at key from the metastore.
// TODO: this interface needs to be able to return an error.
func (s *store) GetMeta(key []byte) []byte {
	b, err := s.kv.Get(key)
	if err != nil {
		panic(fmt.Sprintf("Unexpected reading from metastore: %s", err))
	}
	return b
}

// putMeta saves the value at key in the store's metastore.
// TODO: this interface needs to be able to return an error.
func (s *store) PutMeta(key, value []byte) {
	err := s.kv.Put(key, value)
	if err != nil {
		panic(fmt.Sprintf("Unexpected error writing to metastore: %s", err))
	}
}

// Close closes any resources associated with the store.
func (s *store) Close() error {
	if s.kv != nil {
		return s.kv.Close()
	}
	return nil
}
