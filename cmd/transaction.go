package main

import (
	"bufio"
	"fmt"
	"os"
)

// methods for writing to the log file
type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	ReadEvents() (<-chan Event, <-chan error)
	AppendLogs()
}

// types of events like PUT and DELETE along with a unique id
type Event struct {
	Seq       uint64
	EventType EventType
	Key       string
	Value     string
}

type EventType byte

// EventType enum
const (
	_                     = iota
	EventDelete EventType = iota
	EventPut              = iota
)

// implementation of events
type FileTransactionLogger struct {
	events  chan<- Event // send-only channel for Event
	errors  <-chan error // read-only channel for errors
	lastSeq uint64
	file    *os.File
}

func (logger *FileTransactionLogger) WritePut(key, value string) {
	logger.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (logger *FileTransactionLogger) WriteDelete(key string) {
	logger.events <- Event{EventType: EventDelete, Key: key}
}

func NewFileTransactionLogger(filename string) (TransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}
	return &FileTransactionLogger{file: file}, nil
}

func (logger *FileTransactionLogger) AppendLogs() {
	events := make(chan Event, 16)
	logger.events = events

	errors := make(chan error, 1)
	logger.errors = errors

	go func() {
		for e := range events {
			logger.lastSeq++

			_, err := fmt.Fprintf(logger.file, "%d\t%d\t%s\t%s\n", logger.lastSeq, e.EventType, e.Key, e.Value)

			if err != nil {
				errors <- err
				return
			}
		}
	}()
}

func (logger *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(logger.file)
	outEvent := make(chan Event)
	outError := make(chan error, 1)

	go func() {
		var e Event

		defer close(outEvent)
		defer close(outError)

		for scanner.Scan() {
			line := scanner.Text()
			if _, err := fmt.Sscanf(line, "%d\t%d\t%s\t%s",
				&e.Seq, &e.EventType, &e.Key, &e.Value); err != nil {
				outError <- fmt.Errorf("input parse error: %w", err)
				return
			}

			if logger.lastSeq >= e.Seq {
				outError <- fmt.Errorf("txn numbers are out of order")
			}

			logger.lastSeq = e.Seq
			outEvent <- e
		}
		if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
			return
		}
	}()

	return outEvent, outError
}
