package astore

import (
	"bufio"
	"compress/gzip"
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/skyec/astore/fluentio"
)

const (
	defaultDirPermissions = 0755
	defaultFilePermisions = 0644
	MAX_HASH_LOG_SIZE     = 41 * 1024 * 1024
	MAX_CONTENT_FILE_SIZE = 500 * 1024
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

}

func OpenKey(basePath, keyName string) (*Key, error) {

	key := &Key{
		keyName:         sanitizeKeyName(keyName),
		originalKeyName: keyName,
		baseDir:         basePath,
		maxHlogSz:       MAX_HASH_LOG_SIZE,
		maxContentSz:    MAX_CONTENT_FILE_SIZE,
	}

	key.keyDir = fmt.Sprintf("%s/%s/%s/%s/%s",
		key.baseDir,
		key.keyName[:1],
		key.keyName[1:2],
		key.keyName[2:3],
		key.keyName)

	key.keyDataDir = fmt.Sprintf("%s/data", key.keyDir)
	key.keyHashLogFileName = fmt.Sprintf("%s/txlog", key.keyDir)

	return key, nil
}

// TODO: add an interface that takes an io.Reader to stream larger messags
func (k *Key) Append(data []byte) error {

	if uint(len(data)) > k.maxContentSz {
		return fmt.Errorf("content size (%d) is greater than the maximum (%d)", len(data), k.maxContentSz)
	}

	if !k.initialized {
		_, err := k.initalizeDirectory()
		if err != nil {
			return err
		}

	}

	dataFileName := fmt.Sprintf("%s/%X.gz", k.keyDataDir, sha1.Sum(data))

	if _, err := os.Stat(dataFileName); err == nil {

		// The file already exists so this must be a duplicate.
		// Skip the file (and log - TODO).
		return nil
	}

	dataWriter := fluentio.OpenFile(dataFileName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, defaultFilePermisions)

	// TODO: only gzip larger files
	//
	//       Or combine several files together to improve compression and only compress when the combined size
	//       is over some threashold (e.g. 200 bytes)
	gzipWriter := gzip.NewWriter(dataWriter.GetFile())

	err := dataWriter.SetWriter(gzipWriter).
		Write(data).
		Flush().
		Sync().
		Close()

	if err != nil {
		return fmt.Errorf("Error saving content file: %s", err)
	}

	checkFileSzFn := func(fi os.FileInfo) error {
		if uint(fi.Size()) >= k.maxHlogSz {
			return fmt.Errorf("reached the max hlog size: %d", k.maxHlogSz)
		}
		return nil
	}

	err = fluentio.OpenFile(k.keyHashLogFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, defaultFilePermisions).
		Stat(checkFileSzFn).
		Write([]byte(fmt.Sprintf("%s\n", path.Base(dataFileName)))).
		Flush().
		Sync().
		Close()

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

func (k *Key) ReadEach(r ReadFunc) error {

	if k.hashes == nil {
		if err := k.loadHashes(); err != nil {
			return err
		}
	}

	for _, h := range k.hashes {
		fname := fmt.Sprintf("%s/%s", k.keyDataDir, h)
		err := func() error {
			file, err := os.Open(fname)
			if err != nil {
				return err
			}
			defer file.Close()
			reader, err := gzip.NewReader(file)
			if err != nil {
				return err
			}
			return r(reader)
		}()
		if err != nil {
			return err
		}
	}

	return nil
}

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
