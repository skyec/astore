package astore

import (
	"bufio"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"os"

	"github.com/skyec/astore/fluentio"
)

const (
	defaultDirPermissions = 0755
	defaultFilePermisions = 0644
	MAX_HASH_LOG_SIZE     = 41 * 1024 * 1024
	MAX_CONTENT_FILE_SIZE = 500 * 1024
	MIN_GZ_SIZE           = 160
)

type Key struct {
	keyName            string   // name of the key
	originalKeyName    string   // original name of the key
	baseDir            string   // base directory for the keys
	keyDir             string   // directory where the data for a single key lives
	keyDataDir         string   // directory in the key where the data files live
	keyHashLogFileName string   // file name the hash log
	initialized        bool     // flag indicating if the key directory has been initialized
	maxHlogSz          uint     // maximum size of the hashlog; usually MAX_HASH_LOG_SIZE
	maxContentSz       uint     //maximum size of a content file; usuall MAX_CONTENT_FILE_SIZE
	hashes             []string // array of hashes of the stored parts for this key
	syncEnabled        bool     // calls os.File.Sync for every write if enabled

}

// OpenKey opens the key, keyName and basePath. A *Key or error is returned. If the DISABLE_ASTORE_FSYNC
// environment variable is set (to any value - even zero), then writes are made without calling os.File.Sync.
// This improves performance significantly but increases the risk of data corruption.
func OpenKey(basePath, keyName string) (*Key, error) {

	key := &Key{
		keyName:         sanitizeKeyName(keyName),
		originalKeyName: keyName,
		baseDir:         basePath,
		maxHlogSz:       MAX_HASH_LOG_SIZE,
		maxContentSz:    MAX_CONTENT_FILE_SIZE,
		syncEnabled:     true,
	}

	key.keyDir = fmt.Sprintf("%s/%s/%s/%s/%s",
		key.baseDir,
		key.keyName[:1],
		key.keyName[1:2],
		key.keyName[2:3],
		key.keyName)

	key.keyDataDir = fmt.Sprintf("%s/data", key.keyDir)
	key.keyHashLogFileName = fmt.Sprintf("%s/txlog", key.keyDir)

	if len(os.Getenv("DISABLE_ASTORE_FSYNC")) > 0 {
		key.syncEnabled = false
	}
	return key, nil
}

// TODO: add an interface that takes an io.Reader to stream larger messags

func (k *Key) checkFileSzFn(fi os.FileInfo) error {
	if uint(fi.Size()) >= k.maxHlogSz {
		return fmt.Errorf("reached the max hlog size: %d", k.maxHlogSz)
	}
	return nil
}

func (k *Key) writeHashLog(hash string) error {
	return fluentio.OpenFile(k.keyHashLogFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, defaultFilePermisions).
		Stat(k.checkFileSzFn).
		Write([]byte(hash + "\n")).
		Flush().
		Sync(k.syncEnabled).
		Close()
}

func (k *Key) Append(data []byte) error {

	cSize := uint(len(data))
	if cSize > k.maxContentSz {
		return fmt.Errorf("content size (%d) is greater than the maximum (%d)", len(data), k.maxContentSz)
	}

	if !k.initialized {
		_, err := k.initalizeDirectory()
		if err != nil {
			return err
		}

	}

	if err := k.writeContent(data); err != nil {
		return err
	}
	return k.writeHashLog(fmt.Sprintf("%X", sha1.Sum(data)))
}

const magicNumber uint32 = 0xff00ff00
const headerSize = 20 // magic(4)+ crc64(8) + content length(8)
type contentHeader struct {
	Magic  uint32
	CRC64  uint64
	Length uint64
}

func (k *Key) writeContent(data []byte) error {

	header := &contentHeader{magicNumber, crc64.Checksum(data, crc64.MakeTable(crc64.ISO)), uint64(len(data))}

	file, err := os.OpenFile(fmt.Sprintf("%s/content.dat", k.keyDataDir), os.O_CREATE|os.O_APPEND|os.O_WRONLY, defaultFilePermisions)
	buff := bufio.NewWriter(file)
	err = binary.Write(buff, binary.LittleEndian, header)
	if err != nil {
		return fmt.Errorf("error encoding header: %s", err)
	}
	n, err := buff.Write(data)
	if err != nil {
		return fmt.Errorf("error buffering content: %s", err)
	}
	if n < len(data) {
		return fmt.Errorf("short write buffering content: expected: %d, got: %d", len(data), n)
	}
	err = buff.Flush()
	if err != nil {
		return fmt.Errorf("error committing content: %s", err)
	}
	if k.syncEnabled {
		if err = file.Sync(); err != nil {
			return fmt.Errorf("error syncing content: %s", err)
		}
	}
	err = file.Close()
	if err != nil {
		return fmt.Errorf("error closing content: %s", err)
	}
	return nil

}

func (k *Key) ReadEach(r ReadFunc) error {
	file, err := os.Open(fmt.Sprintf("%s/content.dat", k.keyDataDir))
	for err == nil {
		header := &contentHeader{}
		if err = binary.Read(file, binary.LittleEndian, header); err == nil {
			if header.Magic != magicNumber {
				return fmt.Errorf("invalid content block; magic %X doesn't match magic number: %X", header.Magic, magicNumber)
			}
			err = r(io.LimitReader(file, int64(header.Length)))
		}
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func (k *Key) Count() (int, error) {

	if k.hashes == nil {
		if err := k.loadHashes(); err != nil {
			return 0, err
		}
	}

	return len(k.hashes), nil

}

type ReadFunc func(r io.Reader) error

func (k *Key) GetKeyName() string {
	return k.originalKeyName
}

func (k *Key) initalizeDirectory() (*Key, error) {

	if _, err := os.Stat(k.keyDir); os.IsNotExist(err) {
		err = os.MkdirAll(k.keyDir, defaultDirPermissions)
		if err != nil {
			return nil, fmt.Errorf("error creating key path: %s", err)
		}

		err = os.MkdirAll(k.keyDataDir, defaultDirPermissions)
		if err != nil {
			return nil, fmt.Errorf("error creating data path: %s", err)
		}
	}

	k.initialized = true
	return k, nil
}

func sanitizeKeyName(key string) string {
	return fmt.Sprintf("%X", sha1.Sum([]byte(key)))
}

func (k *Key) loadHashes() error {

	k.hashes = []string{}
	f, err := os.Open(k.keyHashLogFileName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		k.hashes = append(k.hashes, scanner.Text())
	}

	return scanner.Err()
}
