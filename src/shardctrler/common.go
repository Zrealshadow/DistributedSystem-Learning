package shardctrler

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK          = "OK"
	WrongLeader = "WrongLeader"
	Timeout     = "Timeout"
)

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings

	RequestId int64
	ClientId  int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int

	RequestId int64
	ClientId  int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int

	RequestId int64
	ClientId  int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number

	RequestId int64
	ClientId  int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

// --------------------- Log ------------------//
const Debug bool = false

type logTopic string

const (
	dError  logTopic = "ERRO"
	dInfo   logTopic = "INFO"
	dLog    logTopic = "LOG1"
	dLog2   logTopic = "LOG2"
	dTest   logTopic = "TEST"
	dWarn   logTopic = "WARN"
	dClient logTopic = "CLIENT"
	dServer logTopic = "SERVER"
	dQury   logTopic = "QUERY"
	dMove   logTopic = "MOVE"
	dLeave  logTopic = "LEAVE"
	dJoin   logTopic = "JOIN"
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
