package astore

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"path"
	"sort"
	"strings"
	"testing"
)

func TestKeyTxLogAppendRead(t *testing.T) {

	testDir := mkTestDir()
	defer rmTestDir(testDir)

	var k appendableKey
	k, err := newKeyTxLog(testDir)
	if err != nil {
		t.Fatal(err)
	}

	klog := k.(*keyTxLog)
	if err = klog.validateLayout(); err != nil {
		t.Fatalf("layout validation failed: %s", err)
	}

	type fix struct {
		key          string
		val          []byte
		hasAppendErr bool
	}
	fixtures := []fix{
		// writing an empty payload is an error. The store would be fine, but when is this ever valid?
		fix{"don't save me", []byte{}, true},

		// happy path
		fix{"the key", []byte("this is the first value"), false},
		fix{"the 2nd key", []byte("this is the second value"), false},

		// duplicates are OK at this point. They are dropped later when
		// keys get committed.
		fix{"the 2nd key", []byte("this is the second value"), false},

		// keys from an empty string are OK.
		fix{"", []byte("keys from an empty string are OK - produces a valid sha1"), false},
	}

	writtenFixtures := []fix{}

	for i, f := range fixtures {
		tvalue := f.val
		tkey := f.key
		err = k.Append(newSha1Key(tkey), tvalue)

		if f.hasAppendErr && err != nil {
			continue
		}

		if f.hasAppendErr && err == nil {
			t.Errorf("Expected an error but didn't get one for fixture: %d", i)
			continue
		}

		if err != nil {
			t.Errorf("unexpected append error for fixture: %d: %s", i, err)
			continue
		}
		writtenFixtures = append(writtenFixtures, f)

	}
	if len(writtenFixtures) < 1 {
		t.Error("Must have written at least one fixture!")
	}

	readCount := 0
	err = klog.readLog(klog.writeLogName, func(key hashableKey, r io.Reader) error {

		// make sure to always consume all the bits from the reader
		v, err := ioutil.ReadAll(r)
		if err == io.EOF {
			err = nil
		}
		if err != nil {
			return fmt.Errorf("Error reading content block!!! %s", err)
		}

		if readCount >= len(writtenFixtures) {
			t.Errorf("Reading more records from log than in writtenFixtures. Key: %s. Expected: %d, got: %d",
				key, len(writtenFixtures), readCount)
			return nil
		}

		if key.String() != newSha1Key(writtenFixtures[readCount].key).String() {
			t.Errorf("Expected key: %s, got: %s", writtenFixtures[readCount].key, key)
		}

		if !bytes.Equal(v, writtenFixtures[readCount].val) {
			t.Errorf("Expected value:\n'%s'\nGot:\n'%s'", writtenFixtures[readCount].val, v)
		}
		readCount++
		return nil
	})
	if err != nil {
		t.Error("Error reading log:", err)
	}

	if len(writtenFixtures) != readCount {
		t.Errorf("Written fixtures != read count. Expected: %d, got: %d", len(writtenFixtures), readCount)
	}

}

func TestKeTxLogRotate(t *testing.T) {
	testDir := mkTestDir()
	defer rmTestDir(testDir)

	klog, err := helpMkTxLog(t, testDir)
	if err != nil {
		t.Fatalf("validation failed: %s", err)
	}

	fixtures := map[string]string{
		"one":   "p1",
		"two":   "bar, baz and bing",
		"three": "whatever",
	}

	writtenKeys, err := helpAppendTxLog(klog, fixtures)
	if err != nil {
		t.Fatal(err)
	}

	newLogName, err := klog.rotate()
	if err != nil {
		t.Fatal("Error testing rotate:", err)
	}
	if newLogName == "" {
		t.Fatal("Expected a new log name after rotate, got none")
	}

	helpValidateWrittenKeys(t, newLogName, klog, writtenKeys)
}

func TestKeyTxLogRotateMaintainsWriteSequence(t *testing.T) {

	testDir := mkTestDir()
	defer rmTestDir(testDir)

	klog, err := helpMkTxLog(t, testDir)
	if err != nil {
		t.Fatalf("validation failed: %s", err)
	}

	fixtures := []map[string]string{
		map[string]string{"one": "p1"},
		map[string]string{"two": "p2"},
		map[string]string{"three": "p2"},
	}

	logNames := []string{}
	for i := range fixtures {
		_, err := helpAppendTxLog(klog, fixtures[i])
		if err != nil {
			t.Fatal(err)
		}
		newLogName, err := klog.rotate()
		if err != nil {
			t.Fatal(err)
		}
		logNames = append(logNames, path.Base(newLogName))
	}

	log.Printf("Log names:\n%s", strings.Join(logNames, "\n"))
	if !sort.StringsAreSorted(logNames) {
		t.Errorf("Expected the log names to be sorted. Got:\n%s", strings.Join(logNames, "\n"))
	}

}

func TestKeyTxLogRotateBeforeWrite(t *testing.T) {
	testDir := mkTestDir()
	defer rmTestDir(testDir)

	klog, err := helpMkTxLog(t, testDir)
	if err != nil {
		t.Fatalf("validation failed: %s", err)
	}

	// The first rotate should rotate the empty tx file that is created when the
	// log is initialized. There should be no errors in this case.
	_, err = klog.rotate()
	if err != nil {
		t.Fatal(err)
	}

	// The subsequent rotates (without am append) should fail because the tx log
	// file doesn't exist yet. This should return errMissingTxLog.
	_, err = klog.rotate()
	if err != errMissingTxLog {
		t.Fatal(err)
	}

}

func helpMkTxLog(t *testing.T, dir string) (*keyTxLog, error) {

	k, err := newKeyTxLog(dir)
	if err != nil {
		t.Fatal(err)
	}

	klog := k.(*keyTxLog)
	if err = klog.validateLayout(); err != nil {
		t.Fatalf("layout validation failed: %s", err)
	}
	return klog, nil
}

func helpAppendTxLog(kt *keyTxLog, fixtures map[string]string) ([]string, error) {

	keys := []string{}
	for k, v := range fixtures {
		err := kt.Append(newSha1Key(k), []byte(v))
		if err != nil {
			return keys, err
		}
		keys = append(keys, k)
	}
	return keys, nil
}

func helpValidateWrittenKeys(t *testing.T, logFile string, kt *keyTxLog, writtenKeys []string) {

	readCount := 0
	err := kt.readLog(logFile, func(key hashableKey, r io.Reader) error {
		_, err := ioutil.ReadAll(r)
		if err != nil && err != io.EOF {
			return err
		}

		if newSha1Key(writtenKeys[readCount]).String() != key.String() {
			t.Errorf("Keys don't match for read record: %d. Expected: %s, got: %s",
				readCount, writtenKeys[readCount], key)
		}
		readCount++
		return nil
	})

	if err != nil {
		t.Error("Error reading new transaction log:", err)
	}

	if readCount != len(writtenKeys) {
		t.Errorf("Expected %d reads, got: %d", len(writtenKeys), readCount)
	}

}
