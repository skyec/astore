package main

import "net/http"

type Handle404 struct{}

func (h Handle404) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	writeErrorResponse(w, r, ErrorNotFound)
}
