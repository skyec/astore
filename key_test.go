package astore

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestOpenKey(t *testing.T) {

	testDir := mkTestDir()
	defer rmTestDir(testDir)

	_, err := OpenKey(testDir, "test-key")
	if err != nil {
		t.Fatalf("Failed to open key:", err)
	}
}

func TestKeyAppend(t *testing.T) {
	testDir := mkTestDir()
	defer rmTestDir(testDir)

	k, err := OpenKey(testDir, "test-key")
	if err != nil {
		t.Fatalf("Failed to open key:", err)
	}

	err = k.Append([]byte("This is a test"))
	if err != nil {
		t.Fatalf("Failed to append to key: %s", err)
	}
}

func TestKeyReadEach(t *testing.T) {
	testDir := mkTestDir()
	defer rmTestDir(testDir)

	key := "test-key"
	k, err := OpenKey(testDir, key)
	if err != nil {
		t.Fatalf("Failed to open key:", err)
	}

	m1 := []byte("This is a test 1.")
	m2 := []byte("This is a test 2.")

	err = k.Append(m1)
	if err != nil {
		t.Fatalf("Failed to append to key: %s", err)
	}

	// Re-open the key again to test the common case
	k, err = OpenKey(testDir, key)
	if err != nil {
		t.Fatalf("Failed to open key:", err)
	}
	err = k.Append(m2)
	if err != nil {
		t.Fatalf("Failed to append to key: %s", err)
	}

	if n, _ := k.Count(); n != 2 {
		t.Error("wrong count: expected 2: got:", n)
	}

	buffer := &bytes.Buffer{}
	k.ReadEach(func(r io.Reader) error {
		_, err := io.Copy(buffer, r)
		return err
	})

	expected := &bytes.Buffer{}
	expected.Write(m1)
	expected.Write(m2)

	if !bytes.Equal(expected.Bytes(), buffer.Bytes()) {
		t.Errorf("Buffers don't motch. Expected:\n%s\nGot:\n%s", expected, buffer)
	}

}
func TestKeyNewReadEach(t *testing.T) {
	testDir := mkTestDir()
	defer rmTestDir(testDir)

	key := "test-key"
	k, err := OpenKey(testDir, key)
	if err != nil {
		t.Fatalf("Failed to open key:", err)
	}

	m1 := []byte("This is a test 1.")
	m2 := []byte("This is a test 2.")

	err = k.Append(m1)
	if err != nil {
		t.Fatalf("Failed to append to key: %s", err)
	}

	// Re-open the key again to test the common case
	k, err = OpenKey(testDir, "test-key")
	if err != nil {
		t.Fatalf("Failed to open key:", err)
	}
	err = k.Append(m2)
	if err != nil {
		t.Fatalf("Failed to append to key: %s", err)
	}

	if n, _ := k.Count(); n != 2 {
		t.Error("wrong count: expected 2: got:", n)
	}

	buffer := &bytes.Buffer{}
	k.ReadEach(func(r io.Reader) error {
		_, err := io.Copy(buffer, r)
		return err
	})

	expected := &bytes.Buffer{}
	expected.Write(m1)
	expected.Write(m2)

	if !bytes.Equal(expected.Bytes(), buffer.Bytes()) {
		t.Errorf("Buffers don't match. Expected:\n%s\nGot:\n%s", expected, buffer)
	}

}

func TestSkipDuplicates(t *testing.T) {

	testDir := mkTestDir()
	defer rmTestDir(testDir)

	k, err := OpenKey(testDir, "test-key")
	if err != nil {
		t.Fatalf("Failed to open key:", err)
	}

	m1 := []byte("This is a test 1.")

	err = k.Append(m1)
	if err != nil {
		t.Fatalf("Failed to append to key: %s", err)
	}

	// do it again!
	// posting a duplicate isn't an error. Append is idempotent.
	err = k.Append(m1)
	if err != nil {
		t.Fatalf("Failed to append to key: %s", err)
	}

	if n, _ := k.Count(); n != 1 {
		t.Error("Expected count: 1, got:", n)
	}

	i := 0
	err = k.ReadEach(func(r io.Reader) error {
		i++
		return nil
	})

	if i != 1 {
		t.Fatalf("Incorrect number of records. Expected 1, got %d", i)
	}

}
func TestReadBeforeAppend(t *testing.T) {
	testDir := mkTestDir()
	defer rmTestDir(testDir)

	originalKey := "testing key"
	k, err := OpenKey(testDir, originalKey)
	if err != nil {
		t.Fatalf("Failed to open key:", err)
	}

	err = k.ReadEach(func(r io.Reader) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if n, err := k.Count(); n != 0 || err != nil {
		t.Fatalf("unexpected count, expected:\n0 records, got: %d\nnil err, got: %s", n, err)
	}
}

func TestGetOriginalKeyName(t *testing.T) {
	testDir := mkTestDir()
	defer rmTestDir(testDir)

	originalKey := "this is the original key"
	k, err := OpenKey(testDir, originalKey)
	if err != nil {
		t.Fatalf("Failed to open key:", err)
	}

	if k.GetKeyName() != originalKey {
		t.Errorf("original key name didn't match. Expected: %s, got: %s", originalKey, k.GetKeyName())
	}
}

// NOTE: This tests an implementation detail. However, the key value is used to generate files on disk.
// Ensure that it is 'sanitized' so potentially damaging paths are not permitted
func TestKeyIsSanitized(t *testing.T) {

	testDir := mkTestDir()
	defer rmTestDir(testDir)

	originalKey := "this key should be hashed ./"
	k, err := OpenKey(testDir, originalKey)
	if err != nil {
		t.Fatalf("Failed to open key:", err)
	}

	badChars := regexp.MustCompile("[^a-zA-Z0-9]")
	if badChars.MatchString(k.keyName) {
		t.Errorf("Key name'%s' contains bad chars that match the regex: '%s'", k.keyName, badChars.String())
	}

	if k.keyName == originalKey {
		t.Error("key is the same as the original:", k.keyName)
	}

	if strings.Contains(k.keyDir, originalKey) {
		t.Errorf("key path '%s' contains the origial: '%s'", k.keyDir, originalKey)
	}

}

func TestMaxContentSz(t *testing.T) {
	testDir := mkTestDir()
	defer rmTestDir(testDir)

	k, err := OpenKey(testDir, "ignored")
	if err != nil {
		t.Fatal(err)
	}

	k.maxContentSz = 10
	err = k.Append([]byte("12345678901"))

	if err == nil {
		t.Error("Saving too large content didn't generate an error")
	}
}

func TestMaxHashLogSz(t *testing.T) {
	testDir := mkTestDir()
	defer rmTestDir(testDir)

	k, err := OpenKey(testDir, "ignored")
	if err != nil {
		t.Fatal(err)
	}

	err = k.Append([]byte("first record"))
	if err != nil {
		t.Fatal(err)
	}

	k.maxHlogSz = 10
	err = k.Append([]byte("second record"))
	if err == nil {
		t.Error("Saving too many records didn't generate an error")
	}
}

func mkTestDir() string {
	dir, err := ioutil.TempDir("", "key-test-")
	if err != nil {
		log.Fatalf("Failed to create the temp dir: %s", err)
	}
	return dir
}

func rmTestDir(dirName string) {
	if os.Getenv("KEEP_TEST_DIR") != "" {
		log.Println("Preserving tests in:", dirName)
		return
	}
	err := os.RemoveAll(dirName)
	if err != nil {
		log.Fatal("Failed to remove the test dir '%s': %s", dirName, err)
	}
}
