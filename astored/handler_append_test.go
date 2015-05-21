package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandlerAppend(t *testing.T) {

	testKey := "the key"
	part := []byte(`{"foo":"bar"}`)

	vars := MockRequestVars{}
	vars["key"] = testKey
	moc := &MockWriteableKey{}
	h := NewAppendHandler(moc, vars)

	r, w := helpNewRequestResponse(bytes.NewBuffer(part), &bytes.Buffer{})
	r.Method = "POST"
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Error("Expected 200, got:", w.Code)
	}

	if moc.key != testKey {
		t.Errorf("expected '%s', got: %s", testKey, moc.key)
	}

	if !bytes.Equal(part, moc.data) {
		t.Errorf("expected:\n%s\nGot:\n%s", part, moc.data)
	}

}

func TestHandlerAppendMissingContentType(t *testing.T) {
	h := NewAppendHandler(&MockWriteableKey{}, MockRequestVars{})

	r, w := helpNewRequestResponse(&bytes.Buffer{}, &bytes.Buffer{})
	r.Header.Del("Content-Type")
	r.Method = "POST"
	h.ServeHTTP(w, r)

	validateErrorResponse(t, ErrorInvalidContentType, w)
}

func TestHandlerAppendEmptyBody(t *testing.T) {

	vars := MockRequestVars{}
	vars["key"] = "asdf"
	h := NewAppendHandler(&MockWriteableKey{}, vars)

	r, w := helpNewRequestResponse(&bytes.Buffer{}, &bytes.Buffer{})
	r.Method = "POST"
	h.ServeHTTP(w, r)

	validateErrorResponse(t, ErrorEmptyBody, w)
}

func TestHandlerAppendInvalidMethod(t *testing.T) {

	h := NewAppendHandler(&MockWriteableKey{}, MockRequestVars{})

	r, w := helpNewRequestResponse(nil, &bytes.Buffer{})
	h.ServeHTTP(w, r)

	validateErrorResponse(t, ErrorInvalidAppendMethod, w)
}

func validateErrorResponse(t *testing.T, code ErrorResponseCode, w *httptest.ResponseRecorder) {
	er := ErrorResponses[code]
	if w.Code != er.StatusCode {
		t.Errorf("expected response '%d', got: %d", er.StatusCode, w.Code)
	}

	if w.Body == nil {
		t.Fatal("empty body, expected a json message")
	}

	expected, _, _ := encodeErrorResponse(code)

	if !bytes.Equal(expected, w.Body.Bytes()) {
		t.Errorf("Expected:\n%s\nGot:\n%s", expected, w.Body)
	}

}

type MockWriteableKey struct {
	key  string
	data []byte
	err  error
}

func (wk *MockWriteableKey) WriteToKey(key string, data []byte) error {
	wk.key = key
	wk.data = data
	return wk.err
}

type MockRequestVars map[string]string

func (rv MockRequestVars) Vars(r *http.Request) map[string]string {
	return rv
}
