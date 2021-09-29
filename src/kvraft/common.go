package kvraft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
	ErrCommon      = "ErrCommon"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId int64
	ClientId  int64
}

type PutAppendReply struct {
	Err      Err
	LeaderId int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId int64
	ClientId  int64
}

type GetReply struct {
	Err      Err
	Value    string
	LeaderId int
}

type logTopic string

const (
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTest    logTopic = "TEST"
	dWarn    logTopic = "WARN"
	dClient  logTopic = "CLIENT"
	dServer  logTopic = "SERVER"
	dAPPEND  logTopic = "APPEND"
	dPUT     logTopic = "PUT"
	dGET     logTopic = "GET"
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

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DebugPf(topic logTopic, format string, a ...interface{}) {
	if Debug {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
	return
}
