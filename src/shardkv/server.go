package shardkv

import (
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// For PutAppend And Get
	Key         string
	Value       string
	ClientId    int64
	RequestId   int64
	CommandType string
}

type Result struct {
	Err   Err
	Value string
}

type ShardKV struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	gid      int
	ctrlers  []*labrpc.ClientEnd

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	mck          *shardctrler.ShardCtrler

	// Your definitions here.
	doneCh map[int]chan Result
	config shardctrler.Config

	ack        map[int64]int64
	data       [shardctrler.NShards]map[string]string
	validShard [shardctrler.NShards]bool
	updated    bool
}

// ----------------------- * Append Raft Log API * ------------------------ //
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Key:         args.Key,
		CommandType: GET,
		ClientId:    args.ClientId,
		RequestId:   args.RequestId,
	}

	res := kv.submitLog(op)

	reply.Err = res.Err

	if res.Err == OK {
		reply.Value = res.Value
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	if args.Op == "Put" {
		op.CommandType = PUT
	} else {
		op.CommandType = APPEND
	}

	res := kv.submitLog(op)

	reply.Err = res.Err
}

func (kv *ShardKV) startReconfiguration() {
	// default it is under leader condition

	if !kv.isLeader() {
		return
	}

}

func (kv *ShardKV) endReconfiguration() {
	// default it is under leader condition
	if !kv.isLeader() {
		return
	}
}

func (kv *ShardKV) AddShard() {
	if !kv.isLeader() {
		return
	}
}

func (kv *ShardKV) deleteShard() {
	if !kv.isLeader() {

	}
}

func (kv *ShardKV) submitLog(op Op) Result {
	commandIndex, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		return Result{Err: ErrWrongLeader}
	}

	kv.mu.Lock()
	if _, ok := kv.doneCh[commandIndex]; !ok {
		kv.doneCh[commandIndex] = make(chan Result, 1)
	}
	done := kv.doneCh[commandIndex]
	kv.mu.Unlock()

	var res Result
	select {
	case res = <-done:
	case <-time.After(240 * time.Millisecond):
		{
			res = Result{Err: ErrTimeout}
		}
	}

	return res
}

// --------------------- * Main Execution Routine * -----------------------//

func (kv *ShardKV) applier() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()

		if msg.CommandValid {
			cmd := msg.Command.(Op)
			res := kv.applyCommand(&cmd)

			if _, ok := kv.doneCh[msg.CommandIndex]; !ok {
				kv.doneCh[msg.CommandIndex] = make(chan Result, 1)
			}
			done := kv.doneCh[msg.CommandIndex]

			select {
			case <-done:
			default:
			}
			done <- res

			// update snapshot

		} else {
			// for followers to upload snapshot

		}
		kv.mu.Unlock()

	}
}

func (kv *ShardKV) reconfigurationMonitor() {
	for {

	}
}

// -----------------------* Apply Command Function * -----------------------//

func (kv *ShardKV) applyCommand(cmd *Op) Result {
	res := Result{}
	switch cmd.CommandType {
	case GET:
	case PUT:
	case APPEND:
	}
	return res
}

func (kv *ShardKV) applyGet(cmd *Op) Result {
	shard := key2shard(cmd.Key)
	if !kv.isGroup(shard) {
		return Result{Err: ErrWrongGroup}
	}

	if !kv.isDuplicated(cmd.ClientId, cmd.RequestId) {
		// update ack
		kv.ack[cmd.ClientId] = cmd.RequestId
	}

	sharddata := kv.data[shard]
	if _, ok := sharddata[cmd.Key]; !ok {
		return Result{Err: ErrNoKey}
	} else {
		return Result{Err: OK, Value: sharddata[cmd.Key]}
	}

}

func (kv *ShardKV) applyPut(cmd *Op) Result {
	shard := key2shard(cmd.Key)
	if !kv.isGroup(shard) {
		return Result{Err: ErrWrongGroup}
	}

	if !kv.isDuplicated(cmd.ClientId, cmd.RequestId) {
		kv.data[shard][cmd.Key] = cmd.Value
		kv.ack[cmd.ClientId] = cmd.RequestId
	}
	return Result{Err: OK}
}

func (kv *ShardKV) applyAppend(cmd *Op) Result {
	shard := key2shard(cmd.Key)
	if !kv.isGroup(shard) {
		return Result{Err: ErrWrongGroup}
	}

	if !kv.isDuplicated(cmd.ClientId, cmd.RequestId) {
		sharddata := kv.data[shard]
		if _, ok := sharddata[cmd.Key]; !ok {
			kv.data[shard][cmd.Key] = cmd.Value
		} else {
			kv.data[shard][cmd.Key] = kv.data[shard][cmd.Key] + cmd.Value
		}
		kv.ack[cmd.ClientId] = cmd.RequestId
	}
	return Result{Err: OK}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// ------------------- * Tool Func --------------------

func (kv *ShardKV) isLeader() bool {
	_, isleader := kv.rf.GetState()
	return isleader
}

func (kv *ShardKV) isGroup(shard int) bool {
	if kv.updated {
		return kv.validShard[shard]
	} else {
		return kv.config.Shards[shard] == kv.gid
	}
}

func (kv *ShardKV) isDuplicated(ClientId int64, RequestId int64) bool {
	if _, ok := kv.ack[ClientId]; !ok {
		return false
	}
	return kv.ack[ClientId] >= RequestId
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
