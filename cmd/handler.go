package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/gorilla/mux"
)

var logger TransactionLogger

func keyPutHandler(w http.ResponseWriter, r *http.Request) {
	key := keyHelper(r)

	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = Put(key, string(value))

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	logger.WritePut(key, string(value))

	w.WriteHeader(http.StatusCreated)
}

func keyGetHandler(w http.ResponseWriter, r *http.Request) {
	key := keyHelper(r)

	value, err := Get(key)

	if errors.Is(err, ErrNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.Write([]byte(value))
}

func keyDeleteHandler(w http.ResponseWriter, r *http.Request) {
	key := keyHelper(r)

	err := Delete(key)

	if errors.Is(err, ErrNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	logger.WriteDelete(key)

	w.Write([]byte(fmt.Sprintf("%s deleted", key)))
}

func initializeTransactionLog() error {
	var err error
	logger, err = NewFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}
	events, errors := logger.ReadEvents()
	e, ok := Event{}, true
	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case EventDelete:
				err = Delete(e.Key)
			case EventPut:
				err = Put(e.Key, e.Value)
			}
		}
	}
	logger.AppendLogs()

	return err
}

func keyHelper(r *http.Request) string {
	vars := mux.Vars(r)
	key := vars["key"]

	return key
}
