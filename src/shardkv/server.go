package shardkv

import (
	"bytes"
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
	Optype    string
	Key       string
	Value     string
	RequestId int64
	ClientId  int64
	// update args
	DropShard []int

	// Add Shard args
	ShardIds  []int
	ShardData []map[string]string
	Ack       map[int64]int64

	// finish args
	NextConfig    shardctrler.Config
	NextConfigNum int

	// delete args
	//ShardIds []int
}

type Result struct {
	Err       Err
	Value     string
	ClientId  int64
	RequestId int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister

	doneCh map[int]chan Result
	ack    map[int64]int64
	data   [shardctrler.NShards]map[string]string

	config     shardctrler.Config
	mck        *shardctrler.Clerk
	updated    bool                      // whether state machine is updating config
	validShard [shardctrler.NShards]bool // when updated is true, it can record whether shard is useful
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) isGroup(key string) bool {
	shard := key2shard(key)
	if !kv.updated {
		return kv.config.Shards[key2shard(key)] == kv.gid
	} else {
		return kv.validShard[shard]
	}

}

func isMatch(op *Op, res *Result) bool {
	return op.ClientId == res.ClientId && op.RequestId == res.RequestId
}

func (kv *ShardKV) isDuplicated(clientId int64, requestId int64) bool {
	return kv.ack[clientId] >= requestId
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// check leader
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Optype:    GET,
		Key:       args.Key,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	}

	res := kv.submitEntryToRaft(op)
	// logDebug(dTest, "Get Reply %+v", reply)
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
		Optype:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	}

	res := kv.submitEntryToRaft(op)

	reply.Err = res.Err
}

func (kv *ShardKV) submitEntryToRaft(op Op) Result {
	commandIndex, _, isLeader := kv.rf.Start(op)
	res := Result{}
	// logDebug(dSubmit, "Leader [%d] Submit CommandIndex: %d Op %+v", kv.me, commandIndex, op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return res
	}

	kv.mu.Lock()

	if _, ok := kv.doneCh[commandIndex]; !ok {
		kv.doneCh[commandIndex] = make(chan Result, 1)
	}
	done := kv.doneCh[commandIndex]
	kv.mu.Unlock()

	select {
	case r := <-done:
		{
			res = r
		}
	case <-time.After(500 * time.Minute):
		{
			res.Err = ErrTimeOut
		}
	}
	return res
}

func (kv *ShardKV) run() {
	for {
		msg := <-kv.applyCh
		var res Result

		if kv.isLeader() {
			logDebug(dServer, "[GID %d Server %d] || [CommandIndex %d] Get MSG  %+v", kv.gid, kv.me, msg.CommandIndex, msg)
		}

		kv.mu.Lock()
		if msg.CommandValid {
			cmd := msg.Command.(Op)
			switch cmd.Optype {
			case GET:
				res = kv.applyGet(&cmd)
			case APPEND:
				res = kv.applyAppend(&cmd)
			case PUT:
				res = kv.applyPut(&cmd)
			case UPDATE:
				res = kv.applyUpdate(&cmd)
			case ADD:
				res = kv.applyAdd(&cmd)
			case FINISH:
				res = kv.applyFinish(&cmd)
			case DELETE:
				res = kv.applyDelete(&cmd)
			}

			// insert res into done
			// update ack

			commandIndex := msg.CommandIndex
			// initialize done for replicated server

			if _, ok := kv.doneCh[commandIndex]; !ok {
				kv.doneCh[commandIndex] = make(chan Result, 1)
			}
			done := kv.doneCh[commandIndex]

			select {
			case <-done:
			default:
			}
			// logDebug(dServer, "[CommandIndex %d] Reply %+v", msg.CommandIndex, res)
			done <- res

			if kv.persister.RaftStateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
				// create snapshot and trim log
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)

				// var data [shardctrler.NShards]map[string]string
				// c := shardctrler.Config{Shards: kv.config.Shards}
				e.Encode(kv.updated)
				e.Encode(kv.validShard)

				err := e.Encode(kv.ack)
				if err != nil {
					logDebug(dError, "Encode ack %s", err.Error())
				}
				err = e.Encode(kv.data)
				if err != nil {
					logDebug(dError, "Encode data %s", err.Error())
				}

				err = e.Encode(kv.config)
				if err != nil {
					logDebug(dError, "Encode config %s", err.Error())
				}
				snapshot_ := w.Bytes()
				// logDebug(dSnapshot, "Create Snapshot in commandIndex %d\n snapshot state ConfigNum:%d Update:%t ValidShard %+v", commandIndex, kv.config.Num, kv.updated, kv.validShard)
				kv.rf.Snapshot(commandIndex, w.Bytes())

				if kv.isLeader() {
					logDebug(dSnapshot, "[GID %d Server %d] [Create Snapshot CommandIndex %d] data %+v, updated %t, validShard %+v \n config %+v", kv.gid, kv.me, commandIndex, kv.data, kv.updated, kv.validShard, kv.config)

					logDebug(dTest, "----------------------group %d leader %d Decode-------------------------\n", kv.gid, kv.me)
					b := new(bytes.Buffer)
					ee := labgob.NewEncoder(b)
					err := ee.Encode(msg.Command)
					if err != nil {
						panic("Encode Command Failed")
					}
					ee.Encode(commandIndex)
					ee.Encode(2)
					logDebug(dLog, "Prefix Buffer %+v", b.Bytes())
					logDebug(dLog, "snapshot_ Buffer %+v", snapshot_)
					snapshot_ = append(b.Bytes(), snapshot_...)

					// logDebug(dTest, "original size : %d", len(snapshot_))

					// snapshot := kv.persister.ReadSnapshot()
					// logDebug(dLog, "snapshot Buffer %v", snapshot)
					var lastIncludedIndex, lastIncludedTerm int
					var lastCommand Op

					var data [shardctrler.NShards]map[string]string
					config := shardctrler.Config{}
					var update bool
					var validShard [shardctrler.NShards]bool
					ack := make(map[int64]int64)

					r := bytes.NewBuffer(snapshot_)
					d := labgob.NewDecoder(r)

					d.Decode(&lastCommand)
					d.Decode(&lastIncludedIndex)
					d.Decode(&lastIncludedTerm)

					d.Decode(&update)
					d.Decode(&validShard)

					err = d.Decode(&ack)
					if err != nil {
						logDebug(dError, "ack %s", err.Error())
					}

					err = d.Decode(&data)
					if err != nil {
						logDebug(dError, "data %s", err.Error())
					}

					err = d.Decode(&config)
					if err != nil {
						logDebug(dError, "config %s", err.Error())
					}
					logDebug(dTest, "LastIncludedTerm and Index : %d %d, lastCOmmand %+v", lastIncludedTerm, lastIncludedIndex, lastCommand)
					logDebug(dTest, "data : %+v \n config %+v", data, config)
					logDebug(dTest, "validShard %+v, update %t", validShard, update)
					logDebug(dTest, "size : %d", kv.persister.SnapshotSize())
					logDebug(dTest, "---------------------- end -------------------------\n\n\n")
				}

			}

		} else {
			// Install snapshot
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)
			var lastIncludedIndex, lastIncludedTerm int
			var op Op
			d.Decode(&op)
			d.Decode(&lastIncludedIndex)
			d.Decode(&lastIncludedTerm)
			d.Decode(&kv.updated)
			d.Decode(&kv.validShard)

			d.Decode(&kv.ack)
			d.Decode(&kv.data)
			d.Decode(&kv.config)

		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) applyGet(op *Op) Result {
	res := Result{}

	if !kv.isGroup(op.Key) {
		res.Err = ErrWrongGroup
		return res
	}

	shard := key2shard(op.Key)
	sharddata := kv.data[shard]

	if _, ok := sharddata[op.Key]; !ok {
		logDebug(dTest, "[Apply GID %d Server %d] || Get ErrNokey shard %d || sharddata:%+v, kvdata %+v", kv.gid, kv.me, shard, sharddata, kv.data)
		res.Err = ErrNoKey
	} else {
		res.Err = OK
		res.Value = kv.data[shard][op.Key]
		if kv.isLeader() {
			logDebug(dGET, "[Apply GID %d Server %d] || Apply Get Key %s Value: %s", kv.gid, kv.me, op.Key, op.Value)
		}
	}
	kv.ack[op.ClientId] = op.RequestId
	return res
}

func (kv *ShardKV) applyAppend(op *Op) Result {

	if !kv.isGroup(op.Key) {
		return Result{Err: ErrWrongGroup}
	}
	shard := key2shard(op.Key)

	if !kv.isDuplicated(op.ClientId, op.RequestId) {
		sharddata := kv.data[shard]

		if _, ok := sharddata[op.Key]; !ok {
			kv.data[shard][op.Key] = ""
		}
		kv.data[shard][op.Key] += op.Value

		if kv.isLeader() {
			logDebug(dAPPEND, "[Apply GID %d Server %d] || Apply Append Key %s Append Value %s now Value:%s", kv.gid, kv.me, op.Key, op.Value, kv.data[shard][op.Key])
		}
		kv.ack[op.ClientId] = op.RequestId
	}
	return Result{Err: OK}
}

func (kv *ShardKV) applyPut(op *Op) Result {

	if !kv.isGroup(op.Key) {
		return Result{Err: ErrWrongGroup}
	}
	shard := key2shard(op.Key)
	// if kv.isLeader() {
	// 	logDebug(dTest, "[Apply GID %d Server %d] isDuplicate %t, op.Cientid %d op.RequestId %d Ack %+v", kv.gid, kv.me, kv.isDuplicated(op.ClientId, op.RequestId), op.ClientId, op.RequestId, kv.ack)
	// }
	if !kv.isDuplicated(op.ClientId, op.RequestId) {
		kv.data[shard][op.Key] = op.Value
		if kv.isLeader() {
			logDebug(dPUT, "[Apply GID %d Server %d] || Apply Put Key %s Value %s", kv.gid, kv.me, op.Key, op.Value)
		}
		kv.ack[op.ClientId] = op.RequestId
	}

	return Result{Err: OK}
}

func (kv *ShardKV) applyUpdate(op *Op) Result {
	kv.updated = true

	for shard, gid := range kv.config.Shards {
		if gid == kv.gid {
			kv.validShard[shard] = true
		} else {
			kv.validShard[shard] = false
		}
	}

	for _, shard := range op.DropShard {
		kv.validShard[shard] = false
	}

	if kv.isLeader() {
		logDebug(dUpdate, "[Apply GID %d Server %d] || Apply Update", kv.gid, kv.me)

	}

	return Result{Err: OK}
}

func (kv *ShardKV) applyAdd(op *Op) Result {

	// Add shard
	for idx, shardId := range op.ShardIds {
		kv.data[shardId] = op.ShardData[idx]

		// update validshard, now it can return msg which request this shard
		kv.validShard[shardId] = true
	}

	// merge ack
	for clientId, requestId := range op.Ack {
		if _, ok := kv.ack[clientId]; !ok {
			kv.ack[clientId] = requestId
		}

		if kv.ack[clientId] < requestId {
			kv.ack[clientId] = requestId
		}
	}

	if kv.isLeader() {
		logDebug(dADD, "[Apply GID %d Server %d] || Apply Add Shards %+v data %+v", kv.gid, kv.me, op.ShardIds, op.ShardData)
	}

	return Result{Err: OK}
}

func (kv *ShardKV) applyFinish(op *Op) Result {
	// change update state and change configuration
	kv.updated = false
	kv.config = op.NextConfig
	if kv.isLeader() {
		logDebug(dFinish, "[Apply GID %d Server %d] || finish reconfiguration from [%d] to [%d]", kv.gid, kv.me, kv.config.Num-1, kv.config.Num)
	}
	return Result{Err: OK}

}

func (kv *ShardKV) applyDelete(op *Op) Result {

	for _, shard := range op.ShardIds {
		kv.data[shard] = make(map[string]string)
	}

	if kv.isLeader() {
		logDebug(dDelete, "[Apply GID %d Server %d] || delete shards %+v", kv.gid, kv.me, op.ShardIds)
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

func (kv *ShardKV) checkNewConfig() {
	for {
		if kv.isLeader() {
			kv.mu.Lock()
			currentConfigNum := kv.config.Num
			kv.mu.Unlock()

			if c := kv.mck.Query(-1); c.Num != currentConfigNum {
				// find new Config

				// change configure in order
				for i := currentConfigNum + 1; i <= c.Num; i++ {

					nextConfig := kv.mck.Query(i)

					logDebug(dTest, "[GID %d, Leader %d] [Reconfigure %d To %d, End:%d] NextConfig:%+v", kv.gid, kv.me, i-1, i, c.Num, nextConfig)
					dropShard := kv.getDropedShard(&nextConfig)
					fetchShardMap := kv.getFetchedShardMap(&nextConfig)
					logDebug(dTest, "[GID %d Leader %d] DropedShard : %+v \t FetchShard %+v", kv.gid, kv.me, dropShard, fetchShardMap)
					// change to update state
					kv.Update(dropShard)
					// Fetch shard from other server using RPC and submit new shard to raft
					kv.getFetchedShards(fetchShardMap)

					// send delete request
					kv.InformGroups(fetchShardMap)

					// finish update, submit command to raft to change state
					kv.Finish(nextConfig)

				}
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) Update(dropShard []int) {
	// get droped car
	kv.mu.Lock()
	f := kv.updated
	kv.mu.Unlock()

	if !f {
		op := Op{
			Optype:    UPDATE,
			DropShard: dropShard,
		}
		// logDebug(dUpdate, "GID %d Leader %d Send UPDate OP", kv.gid, kv.me)
		for {
			res := kv.submitEntryToRaft(op)
			if res.Err == OK {
				logDebug(dUpdate, "[GID %d Leader %d] Update Successful", kv.gid, kv.me)
				return
			}
		}
	}

}

func (kv *ShardKV) Finish(nextConfig shardctrler.Config) {
	op := Op{
		Optype:     FINISH,
		NextConfig: nextConfig,
	}

	for {
		res := kv.submitEntryToRaft(op)
		if res.Err == OK {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// return dropShard
func (kv *ShardKV) getDropedShard(nextConfig *shardctrler.Config) []int {
	dropShard := make([]int, 0)
	currentConfig := kv.config
	for shard, gid := range currentConfig.Shards {
		if gid == kv.gid && nextConfig.Shards[shard] != kv.gid {
			dropShard = append(dropShard, shard)
		}
	}
	return dropShard
}

// return gid -> shards
func (kv *ShardKV) getFetchedShardMap(nextConfig *shardctrler.Config) map[int][]int {
	gid2shards := make(map[int][]int)

	for shard, gid := range nextConfig.Shards {
		if gid == kv.gid && kv.config.Shards[shard] != kv.gid {
			oriGid := kv.config.Shards[shard]
			if _, ok := gid2shards[oriGid]; !ok {
				gid2shards[oriGid] = make([]int, 0)
			}
			gid2shards[oriGid] = append(gid2shards[oriGid], shard)
		}
	}

	if _, ok := gid2shards[0]; ok {
		// 0 gid is initialize config, 0 group is not exist
		delete(gid2shards, 0)
	}
	return gid2shards
}

// ------------------------ Shard Migration RPC -------------------------- //
type FetchShardArg struct {
	Gid       int
	ShardIds  []int
	ConfigNum int
}

type FetchShardReply struct {
	Err       Err
	ShardData []map[string]string
	ShardIds  []int
	Ack       map[int64]int64
}

//RPC
func (kv *ShardKV) FetchShard(args *FetchShardArg, reply *FetchShardReply) {
	// caller is Update state
	// Two condition return Shard
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
	}

	if args.ConfigNum < kv.config.Num || (args.ConfigNum == kv.config.Num && kv.updated) {
		reply.Err = OK
		reply.ShardData = make([]map[string]string, 0)
		reply.Ack = make(map[int64]int64)
		reply.ShardIds = args.ShardIds

		// copy map
		for _, shardId := range args.ShardIds {
			data := kv.data[shardId]
			m := make(map[string]string)
			for key, value := range data {
				m[key] = value
			}
			reply.ShardData = append(reply.ShardData, m)
		}

		for clientId, requestId := range kv.ack {
			reply.Ack[clientId] = requestId
		}

		logDebug(dFetch, "%d group Fetch %d group's %d Server  Shards [%+v]", args.Gid, kv.gid, kv.me, args.ShardIds)

	} else {
		reply.Err = ErrNoReady
	}

}

func (kv *ShardKV) getFetchedShards(fetchShardMap map[int][]int) {
	var wg sync.WaitGroup
	kv.mu.Lock()
	for gid, shards := range fetchShardMap {
		// is Duplicated Fetch
		if !kv.validShard[shards[0]] {
			wg.Add(1)
			// logDebug(dFetch, "[GID %d Leader %d] Fetch Shards %+v from Gid %d", kv.gid, kv.me, shards, gid)
			go kv.sendAndProcessFetchedRPC(gid, shards, &wg)
		}

	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) sendAndProcessFetchedRPC(gid int, shards []int, wg *sync.WaitGroup) {
	defer wg.Done()
	servers := kv.config.Groups[gid]
	args := FetchShardArg{
		Gid:       kv.gid,
		ShardIds:  shards,
		ConfigNum: kv.config.Num,
	}

	reply := FetchShardReply{}
	done := false

	for !done {
		for _, server := range servers {
			srv := kv.make_end(server)
			ok := srv.Call("ShardKV.FetchShard", &args, &reply)
			if ok && reply.Err == OK {
				done = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	logDebug(dFetch, "[GID %d Leader %d] Fetch Shards %+v from Gid %d, Got Reply %+v", kv.gid, kv.me, shards, gid, reply)

	// submit to raft and process reply
	op := Op{
		Optype:    ADD,
		ShardData: reply.ShardData,
		ShardIds:  reply.ShardIds,
		Ack:       reply.Ack,
	}
	kv.submitEntryToRaft(op)
}

// ----------------------------- Garbage Collection RPC -------------------------------//

type CleanUpArgs struct {
	ShardIds  []int
	ConfigNum int
	Gid       int
}

type CleanUpReply struct {
	Err Err
}

//RPC
func (kv *ShardKV) CleanUp(args *CleanUpArgs, reply *CleanUpReply) {
	kv.mu.Lock()
	currentConfigNum := kv.config.Num
	kv.mu.Unlock()

	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	// impossible, before clean up , it is fetched data by requester
	if args.ConfigNum > currentConfigNum {
		reply.Err = ErrNoReady
		return
	}
	op := Op{
		Optype:   DELETE,
		ShardIds: args.ShardIds,
	}

	kv.submitEntryToRaft(op)
	reply.Err = OK
}

func (kv *ShardKV) InformGroups(fetchedShardMap map[int][]int) {

	// inform groups which we fetched data from, tell them to delete these fetched data
	for gid, shardIds := range fetchedShardMap {
		args := CleanUpArgs{
			ShardIds:  shardIds,
			ConfigNum: kv.config.Num,
			Gid:       kv.gid,
		}
		servers := kv.config.Groups[gid]
		logDebug(dDelete, "[GID %d Leader %d], send Delete Cmd to Gid [%d] clean up shards %+v  ", kv.gid, kv.me, gid, shardIds)
		go func(srvs []string) {
			reply := CleanUpReply{}
			for idx := 0; ; {
				server := srvs[idx%len(srvs)]
				srv := kv.make_end(server)
				ok := srv.Call("ShardKV.CleanUp", &args, &reply)
				if ok && reply.Err == OK {
					return
				}

				if ok && reply.Err == ErrNoReady {
					time.Sleep(2 * time.Millisecond)
				} else {
					idx += 1
				}
			}
		}(servers)
	}

	// no need to wait all cleanUp routine finish
}

func (kv *ShardKV) readPersist() {
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

	d.Decode(&kv.updated)
	d.Decode(&kv.validShard)

	d.Decode(&kv.ack)
	d.Decode(&kv.data)
	d.Decode(&kv.config)

	logDebug(dSnapshot, "[GID %d Server %d] Update Snapshot data %+v, updated %t, validShard %+v", kv.gid, kv.me, kv.data, kv.updated, kv.validShard)
}

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
	labgob.Register(shardctrler.Config{})
	// labgob.Register([shardctrler.NShards]map[string]string{})
	// labgob.Register(map[int64]int64{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mck = shardctrler.MakeClerk(ctrlers)
	kv.persister = persister
	kv.config = kv.mck.Query(0)
	kv.doneCh = make(map[int]chan Result)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.data[i] = make(map[string]string)
	}

	kv.doneCh = make(map[int]chan Result)
	kv.ack = make(map[int64]int64)
	kv.updated = false
	kv.readPersist()
	go kv.checkNewConfig()
	go kv.run()

	return kv
}
