package main

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func main() {
	err := initializeTransactionLog()
	if err != nil {
		log.Fatal(err)
	}

	r := mux.NewRouter()

	r.HandleFunc("/v1/{key}", keyPutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", keyGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", keyDeleteHandler).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":8080", r))
}
