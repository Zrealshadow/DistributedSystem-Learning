package shardkv

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
	ErrNoReady     = "ErrNoReady"
)

const (
	PUT    = "Put"
	APPEND = "Append"
	GET    = "Get"
	UPDATE = "Update"
	ADD    = "Add"
	FINISH = "Finish"
	DELETE = "Delete"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
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
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId int64
	ClientId  int64
}

type GetReply struct {
	Err   Err
	Value string
}

// --------------------- Log ------------------//
var Debug bool = true

type logTopic string

const (
	dError    logTopic = "ERRO"
	dInfo     logTopic = "INFO"
	dLog      logTopic = "LOG1"
	dLog2     logTopic = "LOG2"
	dTest     logTopic = "TEST"
	dWarn     logTopic = "WARN"
	dClient   logTopic = "CLIENT"
	dServer   logTopic = "SERVER"
	dPUT      logTopic = "PUT"
	dAPPEND   logTopic = "Append"
	dGET      logTopic = "GET"
	dConfig   logTopic = "CONFIG"
	dFetch    logTopic = "FETCH"
	dSubmit   logTopic = "SUBMIT"
	dSnapshot logTopic = "SNAPSHOT"
	dUpdate   logTopic = "UPDATE"
	dFinish   logTopic = "FINISH"
	dDelete   logTopic = "DELETE"
	dADD      logTopic = "ADD"
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

func logDebug(topic logTopic, format string, a ...interface{}) {
	if Debug {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
	return
}
