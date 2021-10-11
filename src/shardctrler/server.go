package shardctrler

import (
	"sort"
	"sync"

	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	ack    map[int64]int64
	doneCh map[int]chan Result //channel for CommandIndex To Result

	timeOutDuration time.Duration
}

type Op struct {
	// Your data here.
	OpType    string
	RequestId int64
	ClientId  int64
	// Args      interface{}
	//Join
	Servers map[int][]string
	//Leave
	GIDs []int
	//Move
	Shard int
	GID   int
	//Query
	Num int
}

type Result struct {
	Err    Err
	Config Config
}

func (sc *ShardCtrler) isDuplicated(clientId int64, requestId int64) bool {
	_, ok := sc.ack[clientId]

	if !ok {
		return false
	}

	return sc.ack[clientId] >= requestId
}

func (sc *ShardCtrler) getCurrentConfig() Config {
	return sc.configs[len(sc.configs)-1]
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	// Check Leader
	if _, ok := sc.rf.GetState(); !ok {
		reply.WrongLeader = true
		reply.Err = WrongLeader
		return
	}

	// Submit to Channel
	op := Op{
		OpType:    Join,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
		Servers:   args.Servers,
	}

	result := sc.SubmitOp(op)

	if result.Err != OK {
		reply.Err = result.Err
		return
	}

	reply.Err = OK
	return
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	// Check Leader
	if _, ok := sc.rf.GetState(); !ok {
		reply.WrongLeader = true
		reply.Err = WrongLeader
		return
	}

	// Submit to Channel
	op := Op{
		OpType:    Leave,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
		GIDs:      args.GIDs,
	}

	result := sc.SubmitOp(op)

	if result.Err != OK {
		reply.Err = result.Err
		return
	}

	reply.Err = OK
	return
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	// Check Leader
	if _, ok := sc.rf.GetState(); !ok {
		reply.WrongLeader = true
		reply.Err = WrongLeader
		return
	}

	// Submit to Channel
	op := Op{
		OpType:    Move,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
		Shard:     args.Shard,
		GID:       args.GID,
	}

	result := sc.SubmitOp(op)

	if result.Err != OK {
		reply.Err = result.Err
		return
	}

	reply.Err = OK
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// Check Leader
	if _, ok := sc.rf.GetState(); !ok {
		reply.WrongLeader = true
		reply.Err = WrongLeader
		return
	}

	// Submit to Channel
	op := Op{
		OpType:    Query,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
		Num:       args.Num,
	}

	result := sc.SubmitOp(op)

	if result.Err != OK {
		reply.Err = result.Err
		return
	}

	reply.Err = OK
	reply.Config = result.Config
	return
}

func (sc *ShardCtrler) SubmitOp(op Op) Result {
	commandIndex, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return Result{Err: WrongLeader}
	}

	sc.mu.Lock()
	if _, ok := sc.doneCh[commandIndex]; !ok {
		sc.doneCh[commandIndex] = make(chan Result, 1)
	}
	done := sc.doneCh[commandIndex]
	sc.mu.Unlock()
	logDebug(dServer, "%d (Leader) Submits one CommandIndex %d from client %d RequestId %d", sc.me, commandIndex, op.ClientId, op.RequestId)
	// logDebug(dInfo, "[Info] Cmd %+v", op)
	select {
	case r := <-done:
		{
			close(done)
			return r
		}
	case <-time.After(sc.timeOutDuration):
		{
			return Result{Err: Timeout}
		}
	}
}

func (sc *ShardCtrler) run() {
	for msg := range sc.applyCh {
		// logDebug(dInfo, "Msg :%+v", msg)
		if msg.CommandValid {
			cmd := msg.Command.(Op)
			result := sc.apply(&cmd)

			CommandIndex := msg.CommandIndex

			sc.mu.Lock()
			_, ok := sc.doneCh[CommandIndex]
			if !ok {
				// for replicated server, we should help initialize an doneCh
				sc.doneCh[CommandIndex] = make(chan Result, 1)
			}
			done := sc.doneCh[CommandIndex]
			sc.mu.Unlock()

			select {
			case <-done: // maybe previous result was applied but not picked for the timeout, will cause block
			default:
			}

			logDebug(dServer, "%d Finish One CMD Index %d from client %d ReuqestId %d", sc.me, CommandIndex, cmd.ClientId, cmd.RequestId)
			// logDebug(dInfo, "[Info] Cmd %+v", cmd)
			// return done in channel
			done <- result

		}
	}
}

func (sc *ShardCtrler) apply(op *Op) Result {
	r := Result{}
	switch op.OpType {
	case Join:
		{
			if !sc.isDuplicated(op.ClientId, op.RequestId) {
				sc.applyJoin(op)
			}
		}
	case Move:
		{
			if !sc.isDuplicated(op.ClientId, op.RequestId) {
				sc.applyMove(op)
			}
		}
	case Query:
		{
			r.Config = sc.applyQuery(op)
		}
	case Leave:
		{
			if !sc.isDuplicated(op.ClientId, op.RequestId) {
				sc.applyLeave(op)
			}
		}
	}
	r.Err = OK
	sc.ack[op.ClientId] = op.RequestId
	return r
}

func (sc *ShardCtrler) applyJoin(op *Op) {

	nextConfig := sc.createNextConfig()
	sortedGIDS := make([]int, 0)

	for key, _ := range op.Servers {
		sortedGIDS = append(sortedGIDS, key)
	}
	sort.Ints(sortedGIDS)
	for _, gid := range sortedGIDS {
		servers := op.Servers[gid]
		nextConfig.Groups[gid] = servers
		// assign shards if there is no initialization
		// Optimization Todo:
		for i := 0; i < NShards; i++ {
			if nextConfig.Shards[i] == 0 {
				nextConfig.Shards[i] = gid
			}
		}

	}
	// rebalance groups
	rebalanceGroups(&nextConfig)
	//debug
	// logDebug(dError, "Server %d in C:%d\tR:%d \n [Config Shards] %+v", sc.me, op.ClientId, op.RequestId, nextConfig.Shards)
	sc.configs = append(sc.configs, nextConfig)
}

func (sc *ShardCtrler) applyMove(op *Op) {
	nextConfig := sc.createNextConfig()
	nextConfig.Shards[op.Shard] = op.GID
	sc.configs = append(sc.configs, nextConfig)
}

func (sc *ShardCtrler) applyQuery(op *Op) Config {

	num := op.Num
	if num < 0 || num > len(sc.configs) {
		return sc.getCurrentConfig()
	} else {
		return sc.configs[num]
	}
}

func (sc *ShardCtrler) applyLeave(op *Op) {

	nextConfig := sc.createNextConfig()

	gid2shards := createGid2Shards(&nextConfig)
	leftShards := make([]int, 0)

	for _, gid := range op.GIDs {
		shards := gid2shards[gid]
		leftShards = append(leftShards, shards...)
		delete(nextConfig.Groups, gid)
		// logDebug(dLeave, "delete gid %d, Groups After delete %+v", gid, nextConfig.Groups)
	}
	// logDebug(dLeave, "Groups After delete %+v", nextConfig.Groups)
	if len(nextConfig.Groups) == 0 {
		for shard := range nextConfig.Shards {
			nextConfig.Shards[shard] = 0
		}
	} else {
		//choose the most biggest gid to hold these left shard
		//can not choose a random one either nor choose the first element using map iterator
		stayGid := -1
		for gid := range nextConfig.Groups {
			if gid > stayGid {
				stayGid = gid
			}
		}

		// move left shard into stayGid
		for _, shard := range leftShards {
			nextConfig.Shards[shard] = stayGid
		}
		// rebalance all shard
		rebalanceGroups(&nextConfig)
		//debug
		// logDebug(dError, "Server %d in C%dR%d \n [Config Shards] %+v", sc.me, op.ClientId, op.RequestId, nextConfig.Shards)
	}
	sc.configs = append(sc.configs, nextConfig)
}

func (sc *ShardCtrler) createNextConfig() Config {
	currentConfig := sc.getCurrentConfig()
	nextConfig := Config{Num: currentConfig.Num + 1}
	nextConfig.Num = currentConfig.Num + 1
	nextConfig.Shards = currentConfig.Shards
	nextConfig.Groups = make(map[int][]string)
	for gid, servers := range currentConfig.Groups {
		nextConfig.Groups[gid] = servers
	}
	return nextConfig
}

func rebalanceGroups(config *Config) {
	gid2shards := createGid2Shards(config)

	if len(config.Groups) == 0 {
		// No replicate group and reset shards
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
	} else {
		// rebalance shards
		mean := NShards / len(config.Groups)
		numToMove := 0

		for _, shards := range gid2shards {
			if len(shards) < mean {
				numToMove += mean - len(shards)
			}
		}

		for i := 0; i < numToMove; i++ {
			srcGid, desGid := createMovePairInRebalance(gid2shards)
			N := len(gid2shards[srcGid]) - 1

			// change in Config Shard
			config.Shards[gid2shards[srcGid][N]] = desGid

			//append in des
			gid2shards[desGid] = append(gid2shards[desGid], gid2shards[srcGid][N])

			//delete in src
			gid2shards[srcGid] = gid2shards[srcGid][:N]

		}
	}
}

// gid 2 []shard
func createGid2Shards(config *Config) map[int][]int {
	gid2Shards := make(map[int][]int, 0)
	for gid := range config.Groups {
		gid2Shards[gid] = make([]int, 0)
	}

	for shard, gid := range config.Shards {
		gid2Shards[gid] = append(gid2Shards[gid], shard)
	}
	return gid2Shards
}

func createMovePairInRebalance(gid2Shards map[int][]int) (int, int) {
	srcGid, desGid := -1, -1
	sortedGids := make([]int, 0)
	for gid, _ := range gid2Shards {
		sortedGids = append(sortedGids, gid)
	}
	sort.Ints(sortedGids)

	for _, gid := range sortedGids {
		shards := gid2Shards[gid]
		if srcGid == -1 || len(shards) > len(gid2Shards[srcGid]) {
			srcGid = gid
		}

		if desGid == -1 || len(shards) < len(gid2Shards[desGid]) {
			desGid = gid
		}
	}
	return srcGid, desGid
}

func getSortedKey(m map[int]interface{}) []int {
	keys := make([]int, 0)
	for key, _ := range m {
		keys = append(keys, key)
	}

	sort.Ints(keys)
	return keys
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.timeOutDuration = 240 * time.Millisecond
	sc.doneCh = make(map[int]chan Result)
	sc.ack = make(map[int64]int64)
	// Your code here.

	go sc.run()

	return sc
}
