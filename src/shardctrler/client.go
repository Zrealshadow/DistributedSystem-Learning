package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu        sync.Mutex
	requestId int64
	clientId  int64
	prefer    int
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
	ck.requestId = 0
	ck.clientId = nrand()
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	ck.mu.Lock()
	args.RequestId = ck.requestId
	ck.requestId += 1
	ck.mu.Unlock()
	args.ClientId = ck.clientId
	args.Num = num
	for {
		// try each known server.
		for i := 0; i < len(ck.servers); {

			idx := (ck.prefer + i) % len(ck.servers)
			logDebug(dClient, "[Query] %d Clerk send Query RequestId %d Num %d to Server %d", ck.clientId, args.RequestId, args.Num, idx)
			srv := ck.servers[idx]
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.Err == OK {
				logDebug(dClient, "[Query Reply] %d Clerk get Qurey Reply %+v from Server %d", ck.clientId, reply, i)
				ck.prefer = (ck.prefer + i) % len(ck.servers)
				return reply.Config
			}

			if ok && reply.Err == Timeout {

			} else {
				i += 1
			}

		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.RequestId = ck.requestId
	ck.requestId += 1
	ck.mu.Unlock()

	for {
		// try each known server.
		for i := 0; i < len(ck.servers); {

			idx := (ck.prefer + i) % len(ck.servers)
			logDebug(dClient, "[Join] %d Clerk send Join Request %d to Server %d [Info] %+v", ck.clientId, args.RequestId, idx, args)
			srv := ck.servers[idx]
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.Err == OK {
				logDebug(dClient, "[Join Reply] %d Clerk get Join Request %+v success from server %d", ck.clientId, args, idx)
				ck.prefer = (ck.prefer + i) % len(ck.servers)
				return
			}
			if ok && reply.Err == Timeout {

			} else {
				i += 1
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.RequestId = ck.requestId
	ck.requestId += 1
	ck.mu.Unlock()

	for {
		// try each known server.
		for i := 0; i < len(ck.servers); {
			idx := (ck.prefer + i) % len(ck.servers)
			logDebug(dClient, "[Leave] %d Clerk send Leave Request %d to Server %d [Info] %+v", ck.clientId, args.RequestId, idx, args)
			srv := ck.servers[idx]
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.Err == OK {
				logDebug(dClient, "[Leave Reply] %d Clerk get Leave Request %+v success  from server %d", ck.clientId, args, idx)
				ck.prefer = (ck.prefer + i) % len(ck.servers)
				return
			}

			if ok && reply.Err == Timeout {

			} else {
				i += 1
			}

		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	ck.mu.Lock()
	args.RequestId = ck.requestId
	ck.requestId += 1
	ck.mu.Unlock()
	args.ClientId = ck.clientId
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for i := 0; i < len(ck.servers); {
			idx := (ck.prefer + i) % len(ck.servers)
			logDebug(dClient, "[Move] %d Clerk send Move Request %d to Server %d [Info] %+v", ck.clientId, args.RequestId, idx, args)
			srv := ck.servers[idx]
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.Err == OK {
				logDebug(dClient, "[Move Reply] %d Clerk get Move Request %+v success  from server %d ", ck.clientId, args, idx)
				ck.prefer = (ck.prefer + i) % len(ck.servers)
				return
			}

			if ok && reply.Err == Timeout {

			} else {
				i += 1
			}

		}
		time.Sleep(100 * time.Millisecond)
	}
}
