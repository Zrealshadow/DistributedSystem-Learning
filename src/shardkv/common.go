package shardkv

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"6.824/shardctrler"
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
	ErrTimeout     = "ErrTimeout"
	ErrNoReady     = "ErrNoReady"
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
	ClientId  int64
	RequestId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	RequestId int64
}

type GetReply struct {
	Err   Err
	Value string
}

// ------------------------- * Helper Function * ----------------------
const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
	UPDATE = "UPDATE"
	FINISH = "FINISH"
	ADD    = "ADD"
	DELETE = "DELETE"
)

func deepCopyShard(m map[string]string) map[string]string {
	m_ := make(map[string]string)
	for k, v := range m {
		m_[k] = v
	}
	return m_
}

func deepCopyConfig(c *shardctrler.Config) shardctrler.Config {
	config := shardctrler.Config{Num: c.Num, Shards: c.Shards, Groups: make(map[int][]string)}
	for k, v := range c.Groups {
		config.Groups[k] = v
	}
	return config
}

// ------------------------- * Debug Log Part *----------------------
const Debug bool = false

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
	dDELETE  logTopic = "DELETE"
	dADD     logTopic = "ADD"
	dConfig  logTopic = "CONFIG"
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
