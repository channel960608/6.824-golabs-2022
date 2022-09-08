package logger

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type LogTopic string

const (
	DClient  LogTopic = "CLNT"
	DCommit  LogTopic = "CMIT"
	DDrop    LogTopic = "DROP"
	DError   LogTopic = "ERRO"
	DInfo    LogTopic = "INFO"
	DLeader  LogTopic = "LEAD"
	DLog     LogTopic = "LOG1"
	DLog2    LogTopic = "LOG2"
	DPersist LogTopic = "PERS"
	DSnap    LogTopic = "SNAP"
	DTerm    LogTopic = "TERM"
	DTest    LogTopic = "TEST"
	DTimer   LogTopic = "TIMR"
	DTrace   LogTopic = "TRCE"
	DVote    LogTopic = "VOTE"
	DWarn    LogTopic = "WARN"
)

// Retrieve the verbosity level from an environment variable
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

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic LogTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
