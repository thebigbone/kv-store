package main

import (
	"errors"
	"log"
	"sync"
)

type Store struct {
	mu sync.RWMutex
	m  map[string]string
}

var store = Store{
	m: make(map[string]string),
}

var ErrNoSuchKey = errors.New("no such key")

// TODO: dont insert duplicate kv
func Put(key, value string) error {
	store.mu.Lock()
	store.m[key] = value
	store.mu.Unlock()

	return nil
}

func Get(key string) (string, error) {
	store.mu.RLock()
	value, ok := store.m[key]
	log.Println(value)
	store.mu.RUnlock()

	if !ok {
		return "", ErrNoSuchKey
	}

	return value, nil
}

func Delete(key string) error {
	delete(store.m, key)

	return nil
}
