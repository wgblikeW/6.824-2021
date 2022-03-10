package raft

type logEntry struct {
	// Index holds the index of the log entry.
	Index uint64

	// Term holds the election term of the log entry.
	Term uint64

	// Command holds the log entry's Command that will be applied to FSM.
	Command interface{}
}

// LogStore is used to provide an interface for storing
// and retrieving logs in a durable fashion.
type LogStore interface {
	// FirstIndex returns the first index written. 0 for no entries.
	FirstIndex() (uint64, error)

	// LastIndex returns the last index written. 0 for no entries.
	LastIndex() (uint64, error)

	// GetLog gets a log entry at a given index.
	GetLog(index uint64, log *logEntry) error

	// StoreLog stores a log entry.
	StoreLog(log *logEntry) error

	// StoreLogs stores multiple log entries.
	StoreLogs(logs []*logEntry) error

	// DeleteRange deletes a range of log entries. The range is inclusive.
	DeleteRange(min, max uint64) error
}
