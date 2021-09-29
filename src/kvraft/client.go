package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	mu      sync.Mutex
	// You will have to modify this struct.
	leader    int
	clientId  int64
	RequestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.RequestId = 0
	ck.leader = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	ck.mu.Lock()
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: ck.RequestId,
	}
	ck.RequestId += 1
	leader := ck.leader
	ck.mu.Unlock()

	for ; ; leader = (leader + 1) % len(ck.servers) {
		reply := GetReply{}
		DebugPf(dClient, "[RequestId : %d ] Client: %d GET Key %s from Server %d", args.RequestId, ck.clientId, key, leader)
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok {
			DebugPf(dInfo, "[RequestId : %d], Reply %+v", args.RequestId, reply)
			switch reply.Err {
			case OK:
				ck.leader = leader
				DebugPf(dInfo, "[RequestId : %d], Key %s get Value %s", args.RequestId, key, reply.Value)
				return reply.Value

			case ErrNoKey:
				DebugPf(dInfo, "[RequestId : %d], Key %s is not existed", args.RequestId, key)
				ck.leader = leader
				return ""

			}
		}
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: ck.RequestId,
	}
	ck.RequestId += 1
	leader := ck.leader
	ck.mu.Unlock()

	for ; ; leader = (leader + 1) % len(ck.servers) {
		reply := PutAppendReply{}
		DebugPf(dClient, "[RequestId : %d ] Client: %d PutAppend Key %s Value %s Op %s from Server %d", args.RequestId, ck.clientId, key, value, op, leader)
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				DebugPf(dWarn, "[RequestId : %d], Key %s put Value %s", args.RequestId, key, value)
				ck.leader = leader
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
