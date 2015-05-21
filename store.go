package astore

import (
	"fmt"
	"os"
)

type WriteableKey interface {
	WriteToKey(key string, data []byte) error
}

type ReadableKey interface {
	ReadEachFromKey(key string, f ReadFunc) error
	GetCountFromKey(key string) (int, error)
}

type Store struct {
	path     string
	sequence int64
}

func NewStore(path string) *Store {
	return &Store{
		path: path,
	}
}

func (s *Store) Initialize() error {
	if _, err := os.Stat(s.path); os.IsNotExist(err) {
		return os.MkdirAll(s.path, os.ModeDir)
	}
	return nil
}

func (s *Store) Purge() {
	os.RemoveAll(s.path)
}

func (s *Store) GetRootPath() string {
	return s.path
}

func (s *Store) GetKeyPath() string {
	return fmt.Sprintf("%s/keys", s.GetRootPath())
}

func (s *Store) WriteToKey(key string, data []byte) error {
	k, err := OpenKey(s.GetKeyPath(), key)
	if err != nil {
		return fmt.Errorf("error opening key: %s", err)
	}
	return k.Append(data)
}

func (s *Store) ReadEachFromKey(key string, f ReadFunc) error {

	k, err := OpenKey(s.GetKeyPath(), key)
	if err != nil {
		return err
	}
	return k.ReadEach(f)
}

func (s *Store) GetCountFromKey(key string) (int, error) {

	k, err := OpenKey(s.GetKeyPath(), key)
	if err != nil {
		return 0, err
	}
	return k.Count()
}
