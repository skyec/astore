package astore

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func TestInitializeAndPurgeInterface(t *testing.T) {
	store := NewStore("/tmp/alfs-test")
	defer store.Purge() // Purge is idempotent

	err := store.Initialize()
	if err != nil {
		t.Error("Failed to initialize the store:", err)
	}

	if _, err := os.Stat(store.GetRootPath()); os.IsNotExist(err) {
		t.Error("Failed to find the store dir:", err)
	}

	store.Purge()
	if _, err := os.Stat(store.GetRootPath()); err == nil {
		t.Error("Root dir still exsits after purge:", store.GetRootPath())
	}
}

func TestAppend(t *testing.T) {
	dir, err := ioutil.TempDir("", "al-store-")
	if err != nil {
		t.Fatal("Failed to create temporary directory:", err)
	}

	log.Printf("Temp Dir: %s", dir)
	store := NewStore(dir)
	defer store.Purge()

	err = store.Initialize()
	if err != nil {
		t.Error("Failed to initialize the store:", err)
	}

	data := []byte(`this is a test`)
	err = store.WriteToKey("the key", data)

	if err != nil {
		t.Error("Error saving test data:", err)
	}
}

func TestRead(t *testing.T) {
	dir, err := ioutil.TempDir("", "al-store-")
	if err != nil {
		t.Fatal("Failed to create temporary directory:", err)
	}

	log.Printf("Temp Dir: %s", dir)
	store := NewStore(dir)
	defer store.Purge()

	err = store.Initialize()
	if err != nil {
		t.Error("Failed to initialize the store:", err)
	}

	testKey := "the key"

	test1 := `this is a test 1`
	err = store.WriteToKey(testKey, []byte(test1))
	if err != nil {
		t.Error("Error saving test data:", err)
	}

	test2 := `this is a test 2`
	err = store.WriteToKey(testKey, []byte(test2))
	if err != nil {
		t.Error("Error saving test data:", err)
	}

	buffer := bytes.NewBuffer(nil)
	err = store.ReadEachFromKey(testKey, func(r io.Reader) error {
		_, err := io.Copy(buffer, r)
		return err
	})
	if err != nil {
		t.Fatalf("Failed to read from key '%s': %s", testKey, err)
	}

	expected := bytes.NewBufferString(test1)
	expected.WriteString(test2)
	if !bytes.Equal(buffer.Bytes(), expected.Bytes()) {
		t.Errorf("Buffers do not match.\nExpected:\n%s\nGot:\n%s", expected, buffer)
	}
}

func TestReadFunc(t *testing.T) {

	dir, err := ioutil.TempDir("", "al-store-")
	if err != nil {
		t.Fatal("Failed to create temporary directory:", err)
	}

	log.Printf("Temp Dir: %s", dir)

	testKey := "the key"
	bodies := []string{"this is a test", "this is a test 2"}
	helpInitStore(t, dir, testKey, bodies[0])
	helpInitStore(t, dir, testKey, bodies[1])

	store := NewStore(dir)
	defer store.Purge()

	next := 0
	mustNotBeFalse := false

	err = store.ReadEachFromKey(testKey, func(r io.Reader) error {

		mustNotBeFalse = true

		got, err := ioutil.ReadAll(r)
		if err != nil {
			return err
		}

		expect := bodies[next]
		if !bytes.Equal([]byte(expect), got) {
			t.Errorf("content doesn't match. Expected:\n%s\nGot:\n%s", expect, got)
		}
		next++
		return nil
	})

	if err != nil {
		t.Error("Got an error:", err)
	}

	if !mustNotBeFalse {
		t.Error("Callback was never called")
	}
}

func helpInitStore(t *testing.T, dir, key, payload string) {
	store := NewStore(dir)

	err := store.Initialize()
	if err != nil {
		t.Fatal("Failed to initialize the store:", err)
	}

	err = store.WriteToKey(key, []byte(payload))
	if err != nil {
		t.Fatal("Error saving test data:", err)
	}
}
