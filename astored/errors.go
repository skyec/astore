package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type ErrorResponse struct {
	StatusCode   int               `json:"-"`
	ErrorCode    ErrorResponseCode `json:"errorCode"`
	ErrorMessage string            `json:"errorMessage"`
}

type ErrorResponseCode int

var ErrorResponses map[ErrorResponseCode]*ErrorResponse

const (
	ErrorEmptyBody ErrorResponseCode = 1000 + iota
	ErrorInvalidAppendMethod
	ErrorInvalidContentType
	ErrorNotFound
	ErrorMissingKey
	ErrorStoreError
)

func init() {

	ErrorResponses = map[ErrorResponseCode]*ErrorResponse{

		// ErrorEmptyBody: Caller made a request that contains an empty body
		ErrorEmptyBody: &ErrorResponse{
			http.StatusBadRequest,
			ErrorEmptyBody,
			"Request must have a body",
		},

		// ErrorInvalidAppendMethod: Append only accepts POST requests
		ErrorInvalidAppendMethod: &ErrorResponse{
			http.StatusBadRequest,
			ErrorInvalidAppendMethod,
			"Append only accepts POST requests",
		},

		// ErrorInvalidContentType: Requests must be JSON
		ErrorInvalidContentType: &ErrorResponse{
			http.StatusBadRequest,
			ErrorInvalidContentType,
			"Missing or invalid Content-Type. Request content type must be application/json",
		},

		// ErrorNotFound: error message for 404's
		ErrorNotFound: &ErrorResponse{
			http.StatusNotFound,
			ErrorNotFound,
			"404 Resource not found",
		},

		// ErrorMissingKey: caller didn't provide a key
		ErrorMissingKey: &ErrorResponse{
			http.StatusBadRequest,
			ErrorMissingKey,
			"Missing 'key' in the request URL",
		},

		// ErrorStoreError: error interacting with the underlying store
		ErrorStoreError: &ErrorResponse{
			http.StatusInternalServerError,
			ErrorStoreError,
			"Error interacting with the store",
		},
	}
}

func writeErrorResponse(w http.ResponseWriter, r *http.Request, code ErrorResponseCode) {

	rbuf, rcode, err := encodeErrorResponse(code)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(rcode)

	n, err := w.Write(rbuf)
	if err != nil {
		// TODO log bad stuff
		log.Println("Double fault! Error writing error response:", err)
	}
	if n != len(rbuf) {
		log.Println("Double fault! Short write writting error response")
	}
	logRequest(r, rcode)
}

func encodeErrorResponse(code ErrorResponseCode) ([]byte, int, error) {

	errResp := ErrorResponses[code]
	if errResp == nil {
		return nil, 0, fmt.Errorf("invalid error code: %d", code)
	}

	buf, err := json.Marshal(errResp)
	if err != nil {
		return nil, 0, fmt.Errorf("error encoding error %d: %s", code, err)
	}

	return buf, errResp.StatusCode, nil
}
