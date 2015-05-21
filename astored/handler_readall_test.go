package main

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/skyec/astore"
)

type mockReadableKey struct {
	s   map[string][][]byte
	err error
}

func newMockReadableKey() mockReadableKey {
	m := mockReadableKey{}
	m.s = map[string][][]byte{}
	return m
}

func (rk mockReadableKey) ReadEachFromKey(key string, f astore.ReadFunc) error {

	for _, rec := range rk.s[key] {
		err := f(bytes.NewBuffer(rec))
		if err != nil {
			return err
		}
	}
	return nil
}

func (rk mockReadableKey) GetCountFromKey(key string) (int, error) {

	if rk.err != nil {
		return 0, rk.err
	}
	return len(rk.s), nil
}

func TestHandlerReadAll(t *testing.T) {
	testKey := "test key"
	testData := [][]byte{
		[]byte(`{"test":"me"}`),
		[]byte(`{"second":"record"}`),
	}

	vars := MockRequestVars{}
	vars["key"] = testKey

	store := newMockReadableKey()
	store.s[testKey] = testData

	h := NewReadallHandler(store, vars)

	r, w := helpNewRequestResponse(&bytes.Buffer{}, &bytes.Buffer{})
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("invalid response code. Expected 200, got: %d", w.Code)
	}

	expected := bytes.Buffer{}
	expected.Write([]byte("["))
	expected.Write(bytes.Join(testData, []byte(",")))
	expected.Write([]byte("]"))

	if !bytes.Equal(expected.Bytes(), w.Body.Bytes()) {
		t.Errorf("invalid response. Expected:\n%s\nGot:\n%s", expected.Bytes(), w.Body.Bytes())
	}
}

// This case gets a 400 not a 404 because it's a user error to not include a key
func TestHandlerReadallMissingKey(t *testing.T) {

	h := NewReadallHandler(newMockReadableKey(), MockRequestVars{})
	r, w := helpNewRequestResponse(&bytes.Buffer{}, &bytes.Buffer{})

	h.ServeHTTP(w, r)

	validateErrorResponse(t, ErrorMissingKey, w)
}

// Looking up a key that doesn't exist in the storage does not generate an error.
func TestHandlerReadallEmptyResults(t *testing.T) {

	testKey := "test key"

	vars := MockRequestVars{}
	vars["key"] = testKey

	h := NewReadallHandler(newMockReadableKey(), vars)

	r, w := helpNewRequestResponse(&bytes.Buffer{}, &bytes.Buffer{})
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("invalid response code. Expected 200, got: %d", w.Code)
	}

	expected := []byte("[]")
	if !bytes.Equal(expected, w.Body.Bytes()) {
		t.Errorf("invalid response. Expected:\n%s\nGot:\n%s", expected, w.Body)
	}
}
