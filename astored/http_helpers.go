package main

import (
	"encoding/json"
	"log"
	"net/http"
)

func helpWriteOKResponse(w http.ResponseWriter, r *http.Request, payload interface{}) {
	buf, err := json.Marshal(payload)
	if err != nil {
		// TODO: need a proper error handler for this
		http.Error(w, "json error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Add("Content-Type", "application/json")
	if _, err := w.Write(buf); err != nil {
		log.Println("Socket write error!", err)
	}

	logRequest(r, http.StatusOK)
}
