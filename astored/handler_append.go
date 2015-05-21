package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/skyec/astore"
)

type AppendHandler struct {
	store astore.WriteableKey
	vars  RequestVars
}

func NewAppendHandler(store astore.WriteableKey, vars RequestVars) *AppendHandler {
	return &AppendHandler{
		store: store,
		vars:  vars,
	}
}

func (h *AppendHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		writeErrorResponse(w, r, ErrorInvalidAppendMethod)
		return
	}

	if r.Header.Get("Content-Type") != "application/json" {
		writeErrorResponse(w, r, ErrorInvalidContentType)
		return
	}

	key := h.vars.Vars(r)["key"]
	if key == "" {
		logRequest(r, http.StatusNotImplemented)
		http.Error(w, "error handling for empty key value not implemented", http.StatusNotImplemented)
		return
	}

	// TODO: protect this call from large senders by reading request into
	//       a fixed buffer set to the max file size allowed in the store
	//       OR see the reader passthrough option below
	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logRequest(r, http.StatusInternalServerError)
		http.Error(w, fmt.Sprintf("error reading body: %s", err), http.StatusInternalServerError)
		return
	}

	if len(buf) == 0 {
		writeErrorResponse(w, r, ErrorEmptyBody)
		return
	}

	// TODO: add a reader interface to the store to avoid a buffer copy here
	err = h.store.WriteToKey(key, buf)
	if err != nil {
		log.Println("TODO not implemented: ", err)
		logRequest(r, http.StatusNotImplemented)
		http.Error(w, "not implemented", http.StatusNotImplemented)
	}

	//w.WriteHeader(http.StatusOK)
	//logRequest(r, http.StatusOK)
	writeOKResponse(w, r, map[string]string{"status": "ok"})
}
