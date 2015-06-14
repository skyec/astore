package astore

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"log"
	"os"
	"time"
)

var errMissingTxLog = errors.New("Tx log file is missing")

type keyTxLog struct {
	txLogRootPath string
	writeLogDir   string
	writeLogName  string
	readLogDir    string
}

type txLogBlockHeader struct {
	Magic uint32
	CRC64 uint64
	Key   [sha1.Size]byte
	Len   uint64
}

func newKeyTxLog(rootPath string) (appendableKey, error) {

	txlogroot := rootPath + "/txlog"
	kt := &keyTxLog{
		txLogRootPath: txlogroot,
		writeLogDir:   txlogroot + "/writing",
		writeLogName:  txlogroot + "/writing/tx.log",
		readLogDir:    txlogroot + "/reading",
	}

	if err := kt.initialize(); err != nil {
		return nil, err
	}

	if err := kt.validateLayout(); err != nil {
		return nil, err
	}
	return kt, nil
}

func (kt *keyTxLog) Append(key hashableKey, value []byte) error {

	if len(value) == 0 {
		return fmt.Errorf("Invalid value. Empty playloads are not allowed.")
	}

	header := &txLogBlockHeader{
		Magic: magicNumber,
		CRC64: crc64.Checksum(value, crc64.MakeTable(crc64.ISO)),
		Len:   uint64(len(value)),
	}
	copy(header.Key[:], key.Get())

	file, err := os.OpenFile(kt.writeLogName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, defaultFilePermisions)
	if err != nil {
		return err
	}
	err = binary.Write(file, binary.LittleEndian, header)
	if err != nil {
		return err
	}
	n, err := file.Write(value)
	if err != nil {
		return err
	}
	if n < len(value) {
		return fmt.Errorf("short write; expected: %d, wrote: %d", len(value), n)
	}
	return file.Close()
}

func (kt *keyTxLog) validateLayout() error {
	for _, path := range []string{kt.txLogRootPath, kt.writeLogDir, kt.readLogDir, kt.writeLogName} {
		if !helpWritablePathExists(path) {
			return fmt.Errorf("missing or not writable: %s", path)
		}
	}
	return nil
}

func (kt *keyTxLog) initialize() error {

	for _, path := range []string{kt.writeLogDir, kt.readLogDir} {
		if helpWritablePathExists(path) {
			continue
		}
		if err := os.MkdirAll(path, defaultDirPermissions); err != nil {
			return fmt.Errorf("error creating directory: %s: %s", path, err)
		}
	}

	if !helpWritablePathExists(kt.writeLogName) {
		file, err := os.OpenFile(kt.writeLogName, os.O_CREATE, defaultFilePermisions)
		if err != nil {
			return fmt.Errorf("error creating writelog: %s: %s", kt.writeLogName, err)
		}
		err = file.Close()
		if err != nil {
			return fmt.Errorf("error closing writelog: %s: %s", kt.writeLogName, err)
		}
	}

	return nil
}

type txLogReaderFn func(hashableKey, io.Reader) error

func (kt *keyTxLog) readLog(logfile string, callback txLogReaderFn) error {

	file, err := os.Open(logfile)

	for err == nil {
		header := &txLogBlockHeader{}
		err = binary.Read(file, binary.LittleEndian, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading header block: %s", err)
		}

		// TODO: what happens if the reader doesn't consume all the bytes?
		//       Look at io.LimitedReader.N and comsume any remaining bytes.
		err = callback(newSha1KeyFromHash(header.Key[:]), io.LimitReader(file, int64(header.Len)))
	}

	// EOF is expected and is not an error to be returned
	if err == io.EOF {
		return nil
	}
	log.Println("ERR:", err)
	return err
}

// rotate renames the active write log to the reading directory, setting the unique timestamp. This
// assumes we are working on a filesystem that supports atomic renames (POSIX) to avoid doing our
// own locking. If the tx log hasn't been created yet, return errMissingTxLog which, similar to
// EOF shouldn't be considered execeptional. IOW callers are expected to handle this case gracefully.
func (kt *keyTxLog) rotate() (string, error) {
	format := "20060102T150405.999999999Z" // compressed ISO3339
	newName := fmt.Sprintf("%s/tx-%s.log", kt.readLogDir, time.Now().UTC().Format(format))

	// TODO: Is Go's Rename implemented using atomic renames on all platforms? Windows?
	//       Looks like Windows is getting this in go 1.5:
	//         https://github.com/golang/go/commit/92c57363e0b4d193c4324e2af6902fe56b7524a0
	err := os.Rename(kt.writeLogName, newName)
	if os.IsNotExist(err) {
		return newName, errMissingTxLog
	}
	return newName, err
}

func helpWritablePathExists(path string) bool {
	i, err := os.Stat(path)
	if err != nil {
		return false
	}

	if i.Mode()&0600 == 0600 {
		return true
	}
	return false
}
