package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	requestId int64
	clientId  int64
	mu        sync.Mutex

	groupsPrefer map[int]int
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.clientId = nrand()
	ck.requestId = 1
	ck.groupsPrefer = make(map[int]int)

	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key

	ck.mu.Lock()
	args.RequestId = ck.requestId
	ck.requestId += 1
	ck.mu.Unlock()
	args.ClientId = ck.clientId

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]

		// if servers, ok := ck.config.Groups[gid]; ok {
		// 	// try each server for the shard.
		// 	for si := 0; si < len(servers); si++ {
		// 		srv := ck.make_end(servers[si])
		// 		var reply GetReply
		// 		ok := srv.Call("ShardKV.Get", &args, &reply)
		// 		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
		// 			return reply.Value
		// 		}
		// 		if ok && (reply.Err == ErrWrongGroup) {
		// 			break
		// 		}
		// 		// ... not ok, or ErrWrongLeader
		// 	}
		// }

		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); {
				idx := (si + ck.groupsPrefer[gid]) % len(servers)
				srv := ck.make_end(servers[idx])
				var reply GetReply

				// logDebug(dClient, "[Client Get Request %d],  Shard %d, GroupId %d, server %s, serverId %d", args.RequestId, shard, gid, servers[idx], idx)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.groupsPrefer[gid] = idx
					logDebug(dClient, "[Client Get Request %d Reply OK Value %s],  Shard %d, GroupId %d, serverId %d [Key: %s \t Value: %s]", args.RequestId, reply.Value, shard, gid, idx, args.Key, reply.Value)
					return reply.Value
				}

				if ok && (reply.Err == ErrWrongGroup) {
					ck.groupsPrefer[gid] = idx
					break
				}

				if ok && (reply.Err == ErrTimeOut) {

				} else {
					si += 1
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op

	ck.mu.Lock()
	args.RequestId = ck.requestId
	ck.requestId += 1
	ck.mu.Unlock()
	args.ClientId = ck.clientId

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); {
				idx := (si + ck.groupsPrefer[gid]) % len(servers)
				srv := ck.make_end(servers[idx])
				var reply PutAppendReply
				// logDebug(dClient, "[Client %s Request %d],  Shard %d, GroupId %d, serverId %d || [Key: %s \tValue: %s]", op, args.RequestId, shard, gid, idx, args.Key, args.Value)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					ck.groupsPrefer[gid] = idx
					logDebug(dClient, "[Client %s Request %d Reply OK],  Shard %d, GroupId %d, serverId %d , [Key: %s \tValue: %s]", op, args.RequestId, shard, gid, idx, args.Key, args.Value)
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader

				if ok && (reply.Err == ErrTimeOut) {

				} else {
					si += 1
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
