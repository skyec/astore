package main

import (
	"io"
	"log"
	"net/http"

	"github.com/skyec/astore"
)

type HandlerReadAll struct {
	store astore.ReadableKey
	vars  RequestVars
}

func NewReadallHandler(st astore.ReadableKey, rv RequestVars) *HandlerReadAll {
	return &HandlerReadAll{
		store: st,
		vars:  rv,
	}
}

func (h *HandlerReadAll) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := h.vars.Vars(r)["key"]
	if key == "" {
		writeErrorResponse(w, r, ErrorMissingKey)
		return
	}

	w.WriteHeader(http.StatusOK)
	wr := &recordWriter{
		w: w,
	}

	wr.Write([]byte("["))
	count := 0
	err := h.store.ReadEachFromKey(key, func(r io.Reader) error {
		_, err := io.Copy(wr, r)
		if err != nil {
			return err
		}

		n, err := h.store.GetCountFromKey(key)
		if err != nil {
			return err
		}

		if count < n {
			wr.Write([]byte(","))
		}
		count++
		return err
	})
	wr.Write([]byte("]"))

	if err != nil {
		log.Println("error writing response:", err)
	}

	if wr.err != nil {
		log.Println("error writing response:", wr.err)
		return
	}
	logRequest(r, http.StatusOK)
}

// recordWriter is a utility to aid in making multiple writes and postponing
// error checking to the end. Implements the io.Writer interface so that functions
// like io.Copy will work.
type recordWriter struct {
	err error
	n   uint64
	w   io.Writer
}

// Implements Writer but never returns an error
func (rw *recordWriter) Write(data []byte) (int, error) {

	if rw.err != nil {
		return int(rw.n), nil
	}

	var n int
	n, rw.err = rw.w.Write(data)
	rw.n += uint64(n)
	return n, nil
}
