package raft

import "log"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Greaterthan(numA uint64, numB uint64) bool {
	return numA > numB
}

func Max(numA uint64, numB uint64) uint64 {
	if numA > numB {
		return numA
	} else {
		return numB
	}
}

func Min(numA uint64, numB uint64) uint64 {
	if numA < numB {
		return numA
	} else {
		return numB
	}
}
