a simple KV store with a transaction logger

- writes to a log file of all the events
- reads the log files concurrently incase the program crashes
