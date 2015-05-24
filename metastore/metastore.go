package metastore

type KVStore interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	Close() error
}

type KVStoreType int

const (
	KV_TYPE_BOLT KVStoreType = iota
)

type Config struct {
	Bolt struct {
		BasePath string
	}
}

func NewConfig() *Config {
	return &Config{}
}

func NewKVStore(storeType KVStoreType, config *Config) (store KVStore, err error) {
	switch storeType {
	case KV_TYPE_BOLT:
		store, err = newBoltStore(config)
	}
	return
}
