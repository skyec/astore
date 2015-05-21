package main

import (
	"log"
	"net/http"
)

func logRequest(r *http.Request, code int) {
	log.Printf("%s %s %d %s %s", r.RemoteAddr, r.UserAgent(), code, r.Method, r.URL.Path)
}
