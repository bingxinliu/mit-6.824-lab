package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Debugging
const Debug = false
// const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

var debugStart time.Time
var debugVerbosity int

type logTopic string
const (
    dAERPC   logTopic = "APDE"
    dRVRPC   logTopic = "RQVT"
	dHBeat   logTopic = "HTBT"
	dTerm    logTopic = "TERM"
	dVote    logTopic = "VOTE"
	dRole    logTopic = "ROLE"
	dCommit  logTopic = "CMIT"
	dApply   logTopic = "APLY"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTimer   logTopic = "TIME"
	dTrace   logTopic = "TRCE"
	dConsist logTopic = "CNST"
	dState   logTopic = "STAT"
	dInfo    logTopic = "INFO"
	dWarn    logTopic = "WARN"
)

func getVerbosity() int {
    v := os.Getenv("VERBOSE")
    level := 0
    if v != "" {
        var err error
        level, err = strconv.Atoi(v)
        if err != nil {
            log.Fatalf("Invalid verbosity %v", v)
        }
    }
    return level
}

func init() {
    debugVerbosity = getVerbosity()
    debugStart = time.Now()

    log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DbgPrintf(topic logTopic, format string, a ...interface{}) {
    if debugVerbosity > 0 {
        time := time.Since(debugStart).Microseconds()
        time /= 100
        prefix := fmt.Sprintf("%06d %v ", time, string(topic))
        format = prefix + format
        log.Printf(format, a...)
    }
}
