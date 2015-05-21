package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/skyec/astore"
)

func main() {
	var (
		storeDir   string
		listenAddr string
	)

	flag.StringVar(&storeDir, "s", "/var/astore", "Directory that contains the store data")
	flag.StringVar(&listenAddr, "l", ":9898", "Port the main service listens on")

	flag.Parse()

	store := astore.NewStore(storeDir)

	var vars MuxVars = mux.Vars

	r := mux.NewRouter()
	r.NotFoundHandler = Handle404{}

	r.Handle("/v1/keys/{key}", NewAppendHandler(store, vars)).Methods("POST")
	r.Handle("/v1/keys/{key}", NewReadallHandler(store, vars)).Methods("GET")

	log.Println("Starting ...")
	log.Println("Listening on:", listenAddr)
	log.Println("Store directory:", storeDir)

	http.Handle("/", r)
	http.ListenAndServe(listenAddr, nil)

}

// RequestVars is the interface implemented by objects that know how to parse parameters
// out of the request (URL etc)
type RequestVars interface {
	Vars(r *http.Request) map[string]string
}

type MuxVars func(r *http.Request) map[string]string

func (m MuxVars) Vars(r *http.Request) map[string]string {
	return m(r)
}

func writeOKResponse(w http.ResponseWriter, r *http.Request, payload interface{}) {
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
