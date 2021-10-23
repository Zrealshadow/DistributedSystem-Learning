package shardkv

import (
	"bytes"
	"fmt"
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

	ConfigNum int
	ShardIds  []int
	Ack       map[int64]int64
	//Fetched Shard
	Shards []map[string]string

	//finish
	Config shardctrler.Config
}

type Result struct {
	Err       Err
	Value     string
	ClientId  int64
	RequestId int64
	ConfigNum int
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
	mck          *shardctrler.Clerk

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

func (kv *ShardKV) startReconfiguration(dropedShardId []int, order int) {
	// default it is under leader condition
	// must change state to UPDATE
	for {
		if !kv.isLeader() {
			return
		}

		op := Op{
			CommandType: UPDATE,
			ShardIds:    dropedShardId,
			ConfigNum:   order,
		}

		res := kv.submitLog(op)

		if res.Err == OK {
			break
		}
	}
	kv.plog(dConfig, "Apply [UPDATE] in order:%d \t dropedShardId:%+v", order, dropedShardId)
}

func (kv *ShardKV) endReconfiguration(newConfig *shardctrler.Config) {
	// default it is under leader condition
	for {
		if !kv.isLeader() {
			return
		}

		op := Op{
			CommandType: FINISH,
			ConfigNum:   newConfig.Num - 1,
			Config:      deepCopyConfig(newConfig),
		}

		res := kv.submitLog(op)

		if res.Err == OK {
			break
		}
	}
	kv.plog(dConfig, "Apply Finish in order:%d, Change Config From %d -> %d", newConfig.Num-1, newConfig.Num-1, newConfig.Num)
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

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {

				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)

				e.Encode(kv.updated)
				e.Encode(kv.validShard)
				e.Encode(kv.config)
				e.Encode(kv.ack)
				e.Encode(kv.data)

				go kv.rf.Snapshot(msg.CommandIndex, w.Bytes())
			}

		} else {
			// for followers to upload snapshot
			kv.applySnapshot(msg.Snapshot)
		}
		kv.mu.Unlock()

	}
}

func (kv *ShardKV) reconfigurationMonitor() {
	for {
		if kv.isLeader() {
			newConfigNum := kv.mck.Query(-1).Num
			if kv.config.Num != newConfigNum {
				kv.plog(dConfig, "now Num:%d Find New Config num %d", kv.config.Num, newConfigNum)
			}
			for i := kv.config.Num + 1; i <= newConfigNum; i++ {
				nextConfig := kv.mck.Query(i)
				kv.reconfiguration(&nextConfig)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// -----------------------* Apply Command Function * -----------------------//

func (kv *ShardKV) applyCommand(cmd *Op) Result {
	res := Result{}
	switch cmd.CommandType {
	case GET:
		res = kv.applyGet(cmd)
	case PUT:
		res = kv.applyPut(cmd)
	case APPEND:
		res = kv.applyAppend(cmd)
	case UPDATE:
		res = kv.applyUpdate(cmd)
	case FINISH:
		res = kv.applyFinish(cmd)
	case ADD:
		res = kv.applyAdd(cmd)
	case DELETE:
		res = kv.applyDelete(cmd)
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

func (kv *ShardKV) applyUpdate(cmd *Op) Result {
	if !kv.updated && kv.config.Num == cmd.ConfigNum {
		kv.updated = true

		// forbidden service of shards which are lost in next config
		for _, shard := range cmd.ShardIds {
			kv.validShard[shard] = false
		}
	}

	return Result{Err: OK}
}

func (kv *ShardKV) applyAdd(cmd *Op) Result {
	if kv.config.Num == cmd.ConfigNum {
		for idx, shardId := range cmd.ShardIds {
			if !kv.validShard[shardId] {
				kv.data[shardId] = deepCopyShard(cmd.Shards[idx])
				kv.validShard[shardId] = true
			}
		}

		// merge ack

		for clientId, requestId := range cmd.Ack {
			if _, ok := kv.ack[clientId]; !ok || requestId > kv.ack[clientId] {
				kv.ack[clientId] = requestId
			}
		}
	}
	return Result{Err: OK}
}

func (kv *ShardKV) applyDelete(cmd *Op) Result {
	if cmd.ConfigNum <= kv.config.Num {
		for _, shardId := range cmd.ShardIds {
			if !kv.validShard[shardId] {
				kv.data[shardId] = make(map[string]string)
			}
		}
	}
	return Result{Err: OK}
}

func (kv *ShardKV) applyFinish(cmd *Op) Result {
	if kv.config.Num == cmd.ConfigNum {
		kv.updated = false
		kv.config = deepCopyConfig(&cmd.Config)
		for shard, gid := range kv.config.Shards {
			if gid == kv.gid {
				kv.validShard[shard] = true
			}
		}
	}

	// kv.plog(dTest, "newConfig %+v", kv.config)
	return Result{Err: OK}
}

func (kv *ShardKV) applySnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex, lastIncludedTerm int
	// var lastCommand Op
	// if d.Decode(&lastCommand) != nil && d.Decode(&lastIncludedIndex) !=
	// 	nil && d.Decode(&lastIncludedTerm) != nil {

	// }
	if err := d.Decode(&lastIncludedIndex); err != nil {

	}

	if err := d.Decode(&lastIncludedTerm); err != nil {

	}

	if err := d.Decode(&kv.updated); err != nil {

	}

	if err := d.Decode(&kv.validShard); err != nil {

	}

	if err := d.Decode(&kv.config); err != nil {

	}

	if err := d.Decode(&kv.ack); err != nil {

	}

	if err := d.Decode(&kv.data); err != nil {

	}
}

// ----------------- * Reconfiguration * ----------------

func (kv *ShardKV) reconfiguration(nextConfig *shardctrler.Config) {

	order := nextConfig.Num - 1
	// kv.plog(dConfig, "[Start] %d -> %d, currentConfig:%+v nextConfig:%+v\n", order, order+1, kv.config, nextConfig)
	// Get Forbidden Shard
	ForbiddenShardIds := kv.getDropedShardIds(nextConfig)

	// Get Fetched Shard
	FetchedShardMap := kv.getFetchedShardMaps(nextConfig)
	kv.plog(dConfig, "[%d -> %d] ForbiddenShardId: %+v \tFetchedShardMap %+v", order, order+1, ForbiddenShardIds, FetchedShardMap)
	// update and forbidden droped Shard service
	kv.startReconfiguration(ForbiddenShardIds, order)
	kv.fetchAndAddShards(FetchedShardMap, order)
	kv.sendDeleteRequests(FetchedShardMap, order)
	kv.endReconfiguration(nextConfig)
}

func (kv *ShardKV) getDropedShardIds(nextConf *shardctrler.Config) []int {
	dropedData := make([]int, 0)

	for shardId, f := range kv.validShard {
		if f && nextConf.Shards[shardId] != kv.gid {
			dropedData = append(dropedData, shardId)
		}
	}
	return dropedData
}

// return gid -> shardIds
func (kv *ShardKV) getFetchedShardMaps(nextConf *shardctrler.Config) map[int][]int {
	m := make(map[int][]int, 0)

	for shardId, f := range kv.validShard {
		if !f && nextConf.Shards[shardId] == kv.gid {
			lastOwnerGid := kv.config.Shards[shardId]
			if lastOwnerGid == 0 {
				continue
			}
			if _, ok := m[lastOwnerGid]; !ok {
				m[lastOwnerGid] = make([]int, 0)
			}
			m[lastOwnerGid] = append(m[lastOwnerGid], shardId)
		}
	}
	return m
}

func (kv *ShardKV) fetchAndAddShards(m map[int][]int, order int) {
	var wg sync.WaitGroup
	for gid, shardIds := range m {
		wg.Add(1)
		go func(groupId int, shards []int, configNum int) {
			defer wg.Done()

			args := TransferShardArgs{
				GID:      kv.gid,
				Num:      order,
				ShardIds: shards,
			}

			var reply TransferShardReply

			kv.sendTransferShard(groupId, &args, &reply)

			// process reply
			// submit Log into Raft
			d := make([]map[string]string, 0)
			for idx, _ := range args.ShardIds {
				shardData := reply.ShardData[idx]
				m := deepCopyShard(shardData)
				d = append(d, m)
			}

			for {
				op := Op{
					CommandType: ADD,
					ConfigNum:   args.Num,
					ShardIds:    args.ShardIds,
					Shards:      d,
					Ack:         reply.Ack,
				}

				res := kv.submitLog(op)
				if res.Err == OK {
					break
				}
			}
			kv.plog(dADD, "[order %d] Successfully fetched From GID %d with Shards %+v", configNum, groupId, shards)
		}(gid, shardIds, order)
	}
	wg.Wait()
}

func (kv *ShardKV) sendTransferShard(gid int, args *TransferShardArgs, reply *TransferShardReply) {
	servers := kv.config.Groups[gid]
	for {
		kv.plog(dTest, "Send Transfer to GID %d Get Shards %+v", gid, args.ShardIds)
		for i := 0; i < len(servers); {
			srv := kv.make_end(servers[i])
			ok := srv.Call("ShardKV.TransferShard", args, reply)

			if ok && reply.Err == OK {
				return
			}

			if ok && reply.Err == ErrNoReady {
				time.Sleep(10 * time.Millisecond)
			} else {
				i++
			}

			kv.plog(dTest, "ok %t \t Reply %+v", ok, reply)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Fetch RPC
type TransferShardArgs struct {
	GID      int
	Num      int
	ShardIds []int
}

type TransferShardReply struct {
	Err       Err
	Num       int
	ShardData []map[string]string
	Ack       map[int64]int64
}

func (kv *ShardKV) TransferShard(args *TransferShardArgs, reply *TransferShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isLeader() {
		logDebug(dTest, "[WRONG Leader] Get From GID %d Fetch Shard args:%+v", args.GID, args)
		reply.Err = ErrWrongLeader
		return
	}

	kv.plog(dTest, "Get From GID %d Fetch Shard args:%+v", args.GID, args)

	if kv.config.Num > args.Num || (args.Num == kv.config.Num && kv.updated) {

		reply.ShardData = make([]map[string]string, 0)
		reply.Ack = make(map[int64]int64)

		for _, shardId := range args.ShardIds {
			reply.ShardData = append(reply.ShardData, deepCopyShard(kv.data[shardId]))
		}

		for k, v := range kv.ack {
			reply.Ack[k] = v
		}

		reply.Err = OK
		kv.plog(dTest, "Result to GID %d Reply:%+v", args.GID, reply)
	} else {
		reply.Err = ErrNoReady
	}

}

func (kv *ShardKV) sendDeleteRequests(m map[int][]int, num int) {
	for gid, shardIds := range m {
		srvs := kv.config.Groups[gid]
		args := DeleteShardArgs{
			Num:      kv.config.Num,
			ShardIds: shardIds,
		}
		reply := DeleteShardReply{}
		go kv.sendDeleteShard(srvs, &args, &reply)
	}
}

type DeleteShardArgs struct {
	Num      int
	ShardIds []int
}

type DeleteShardReply struct {
	Err Err
}

func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {

	kv.mu.Lock()
	n := kv.config.Num
	kv.mu.Unlock()

	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	if args.Num > n {
		reply.Err = ErrNoReady
	}

	op := Op{
		CommandType: DELETE,
		ConfigNum:   args.Num,
		ShardIds:    args.ShardIds,
	}

	res := kv.submitLog(op)

	reply.Err = res.Err
}

func (kv *ShardKV) sendDeleteShard(servers []string, args *DeleteShardArgs, reply *DeleteShardReply) {
	for {
		for _, server := range servers {
			srv := kv.make_end(server)
			ok := srv.Call("ShardKV.DeleteShard", args, reply)

			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ------------------- * Tool Func --------------------

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

func (kv *ShardKV) readPersist() {
	if kv.persister.SnapshotSize() == 0 {
		return
	}
	kv.applySnapshot(kv.persister.ReadSnapshot())
}

func (kv *ShardKV) plog(topic logTopic, format string, a ...interface{}) {
	Prefix := fmt.Sprintf("[GID %d Leader %d]\t", kv.gid, kv.me)
	if kv.isLeader() {
		logDebug(topic, Prefix+format, a...)
	}
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

	kv.mck = shardctrler.MakeClerk(ctrlers)
	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	kv.config = kv.mck.Query(0)
	kv.doneCh = make(map[int]chan Result)
	kv.ack = make(map[int64]int64)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.data[i] = make(map[string]string)
	}
	kv.readPersist()
	go kv.reconfigurationMonitor()
	go kv.applier()

	return kv
}
