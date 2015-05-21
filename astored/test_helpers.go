package main

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func helpNewRequestResponse(rb, wb *bytes.Buffer) (*http.Request, *httptest.ResponseRecorder) {

	r := &http.Request{}
	r.Body = ioutil.NopCloser(rb)

	if r.Header == nil {
		r.Header = map[string][]string{}
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("User-Agent", "test")

	r.URL, _ = url.Parse("/v1/keys")

	w := &httptest.ResponseRecorder{}
	w.Body = wb

	return r, w
}

func helpValidateErrorResponse(t *testing.T, code ErrorResponseCode, w *httptest.ResponseRecorder) {

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
