package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	PUT    = "PUT"
	APPEND = "APPEND"
	GET    = "GET"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    string
	ClientId  int64
	RequestId int64
	Key       string
	Value     string
}

type Ans struct {
	OK        bool
	Err       Err
	ClientId  int64
	RequestId int64
	Key       string
	Value     string
}

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()
	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big

	ack map[int64]int64 // clientIndex to latest requestIndex  for duplication

	data map[string]string // k-v store
	// Your definitions here.

	finishCh map[int]chan Ans // Log index To wheather it finished

	timeOutDuration time.Duration
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		OpType:    GET,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
	}

	ans := kv.SubmitToRaft(op)

	if ans.OK {
		reply.LeaderId = kv.me
		reply.Value = ans.Value
		reply.Err = OK
		return
	}

	reply.Err = ans.Err
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
		Value:     args.Value,
	}

	if args.Op == "Put" {
		op.OpType = PUT
	} else {
		op.OpType = APPEND
	}

	ans := kv.SubmitToRaft(op)

	if ans.OK {
		reply.LeaderId = kv.me
		reply.Err = OK
		DebugPf(dLog2, "Retuen OK %+v", reply)
		return
	}
	reply.Err = ans.Err
}

func (kv *KVServer) isRequestDuplicated(clientId int64, requestId int64) bool {
	if requestId_, ok := kv.ack[clientId]; ok {
		return requestId_ >= requestId
	}
	return false
}

func (kv *KVServer) isMatch(ans *Ans, op *Op) bool {
	return ans.ClientId == op.ClientId && ans.RequestId == op.RequestId
}

func (kv *KVServer) SubmitToRaft(op Op) Ans {
	// Weather it is leader
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DebugPf(dServer, "Server %d is not Leader", kv.me)
		return Ans{OK: false, Err: ErrWrongLeader}
	}

	DebugPf(dServer, "Server %d Submit Operation %+v", kv.me, op)

	kv.mu.Lock()
	if _, ok := kv.finishCh[index]; !ok {
		kv.finishCh[index] = make(chan Ans, 1)
	}
	ch := kv.finishCh[index]
	kv.mu.Unlock()
	select {
	// wait until raft reach agreement
	case ans := <-ch:
		{
			DebugPf(dLog, "Get Ans %+v in Submit Channel for op %+v", ans, op)
			if kv.isMatch(&ans, &op) {
				return ans
			}
			return Ans{OK: false, Err: ErrCommon}
		}
		// or timeout return false
	case <-time.After(kv.timeOutDuration):
		{
			return Ans{OK: false, Err: ErrTimeOut}
		}
	}
}

func (kv *KVServer) ExecuteLog() {
	for !kv.killed() {
		msg := <-kv.applyCh
		// DebugPf(dLog, "Add MSG %+v", msg)
		if msg.CommandValid {
			// execute CommanValid
			op := msg.Command.(Op)
			ans := kv.applyOp(&op)
			DebugPf(dServer, "Server %d Execute log %+v and Get ans %+v", kv.me, op, ans)
			kv.mu.Lock()
			if _, ok := kv.finishCh[msg.CommandIndex]; !ok {
				// no way
				kv.finishCh[msg.CommandIndex] = make(chan Ans, 1)
			}

			ch := kv.finishCh[msg.CommandIndex]
			kv.mu.Unlock()

			select {
			case <-ch: // clean stale data , it won't be invoked
			default:
			}

			ch <- ans
			// check the raft size and create snapshot
			if kv.persister.RaftStateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
				// create snapshot
				DebugPf(dSnap, "[%d S] : Create Snapshot in Comman Indes %d, Command %+v", kv.me, msg.CommandIndex, msg, msg.Command)
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.ack)
				e.Encode(kv.data)
				go kv.rf.Snapshot(msg.CommandIndex, w.Bytes())
			}

			DebugPf(dInfo, "Insert Ans %+v to Index Channel", ans)
		} else {
			// create Snapshot
			DebugPf(dSnap, "[%d S] Recover from Snapshot ", kv.me)
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)
			var lastIncludedIndex, lastIncludedTerm int
			var lastCommand Op
			d.Decode(&lastCommand)
			d.Decode(&lastIncludedIndex)
			d.Decode(&lastIncludedTerm)

			d.Decode(&kv.ack)
			d.Decode(&kv.data)

		}
	}
}

func (kv *KVServer) applyOp(op *Op) Ans {
	var ans Ans
	ans.ClientId = op.ClientId
	ans.RequestId = op.RequestId
	switch op.OpType {
	case GET:
		{
			if _, ok := kv.data[op.Key]; ok {
				ans.OK = true
				ans.Err = OK
				ans.Value = kv.data[op.Key]
			} else {
				ans.OK = false
				ans.Err = ErrNoKey
			}
		}
	case PUT:
		{
			if !kv.isRequestDuplicated(op.ClientId, op.RequestId) {
				kv.data[op.Key] = op.Value
			}
			ans.OK = true
			ans.Err = OK
			// update ack
			kv.ack[op.ClientId] = op.RequestId

		}
	case APPEND:
		{
			if !kv.isRequestDuplicated(op.ClientId, op.RequestId) {
				kv.data[op.Key] += op.Value
			}
			ans.OK = true
			ans.Err = OK
			kv.ack[op.ClientId] = op.RequestId // update Ack
			// DebugPf(dAPPEND, "After Append Op %+v check Data %+v", op, kv.data)
		}
	}
	return ans
}

func (kv *KVServer) readPersist() {
	if kv.persister.SnapshotSize() == 0 {
		return
	}
	var lastIncludedIndex, lastIncludedTerm int
	var lastCommand Op
	snapshot := kv.persister.ReadSnapshot()
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&lastCommand)
	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)
	d.Decode(&kv.ack)
	d.Decode(&kv.data)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.

	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	kv.ack = make(map[int64]int64)
	kv.data = make(map[string]string)
	kv.finishCh = make(map[int]chan Ans)
	kv.timeOutDuration = 240 * time.Millisecond
	// DebugPf(dServer, "%d Server Start", kv.me)
	kv.readPersist()
	go kv.ExecuteLog()
	return kv
}
