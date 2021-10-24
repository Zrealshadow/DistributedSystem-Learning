# Distributed System 6.824 Spring 2021



## Outline

##### Lab 1 Mapreduce

##### Lab 2 Raft Protocal

- [x]  2A Leader Election

- [x]  2B Log Completion

- [x]  2C Persistence

- [x]  2D Log Compaction

##### Lab 3 Key-Value Service

- [x] Part A : Key/Value service without snapshots

- [x] Part B : Key/Value service with snapshots

##### Lab 4 Sharded Key-Value Service

- [x] Part A : The Shard controller

- [x] Part B : Sharded Key/Value Server
  - Challenge 1 : Garbage collection of state
  - Challenge 2 : Client requests during configuration changes





## Notes and Reference About Lab

- [Lab2 Keynotes Gist](https://gist.github.com/Zrealshadow/9e4a8e213bb9eca5b5ce2e985b396f7c)
- [Lab2 Unreliable Unit test Pass solution](https://gist.github.com/Zrealshadow/5ae7da85d00194cc331a0b996b57b90e)
- [Lab3 duplicated request process](https://jaxonwang.github.io/programming/mit-6.824-lab3-%E4%B8%80%E7%A7%8Dclient-request-%E5%8E%BB%E9%87%8D%E6%96%B9%E5%BC%8F/)
- [Lab3 discussion about kv server based on raft](https://www.zhihu.com/question/278551592)
- [Lab4 Shard Migration Protocal Reference](https://jaxonwang.github.io/programming/MIT-6.824-Lab-4-Sharded-KeyValue-Service%E7%9A%84%E4%B8%AA%E4%BA%BA%E5%AE%9E%E7%8E%B0/)





## Solution for challenges in Lab4

**Challenge 1**

I do not arrange an extra goroutine for Garbage Collection. And it turns out this is not necessary. My solution is simple, After Group A fetch Shards data from Group B, append the Add log to raft and apply it successfully, Group A will create an gorountine to send Group B a DeleteShard RPC, which will do a certain degree of fault tolerance and ensure that Group B delete the fetched shards data.

**Challenge 2**

- Aggregating all shards migration in one raft log to apply can not achieve this goal. We should split fetched step into more peices part. For example, If Group A fetch Shard 1 and Shard 2 from Group B, and fetch Shard 3 and Shard 5 from Group C. If A successfully get Shard 1 and Shard 2 from Group B, it can submit an raft log to apply immediately. When the server apply this log, it can offer serivce for client who want tdo get/put data in Shard 1 and Shard 2.
- If we split the reconfiguration step into pieces, we have to record the intermediate state. The `validShard` variable is to record which shard can offer serivce. The `updated` variable is to record whether the server is reconfiguring. And these two varible are included in snapshot, just in case the server shutdown ocassionally.
- The reconfiguration process is not an atomic operation. For example, Group A fetch Shard 1 and Shard 2 from Group B, and fetch Shard 3 and Shard 5 from Group C. Group A fetched Shard 1 and Shard 2 successfully and apply it in raft. but fail in fetching from Group C. At that time, Group A shutDown. For a while it restart. Now it will reconfigure again since last reconfiguration before shuntdown failed. According to the `validShard` Shard 1 and Shard 2 are got successfully, now we should avoid duplicated operation even though we have to reconfigure again. 



## Run 

The solution can pass all unit tests reliably in Lab1- Lab3. Just go into the workspace and run `go test -race`

In Lab4, we have to change the raft code slighty in order to pass the reliable

In file `src/raft/raft.go`, comment line 304, we don't store the last command information in snapshot

```go
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log.getBaseIndex()
	lastIndex := rf.log.getLastIndex()

	if index <= baseIndex || index > lastIndex {
		// can not trim the Log
		return
	}
	DebugPf(dSnap, "%d Server CreateSnap  LastAppliedIndex:%d ", rf.me, index)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	lastEntry := rf.log.getEntry(index)
	// e.Encode(lastEntry.Command)
	e.Encode(lastEntry.Index)
	e.Encode(lastEntry.Term)
	rf.log.trim(index, lastEntry.Term)
	snapshot_ := append(w.Bytes(), snapshot...)
	rf.persister.SaveStateAndSnapshot(rf.getRaftPersistDataL(), snapshot_)
}
```



Actually, there is no need to store last command information in snapshot according to the Raft paper.

However in order to pass the raft unit tests in MIT 6.824, we have to add it. We can check the file `src/raft/config.go`  Method `appliersnap`.

In 13 - 17 line of this code segment, we find that it decode the snapshot and consider the first element in snapshot is the command information. It force you to add command information in snapshot to pass the unit test, which is different from previous code.

```go
// periodically snapshot raft state
func (cfg *config) applierSnap(i int, applyCh chan ApplyMsg) {
	lastApplied := 0
	for m := range applyCh {
		if m.SnapshotValid {
			cfg.mu.Lock()
			if cfg.rafts[i].CondInstallSnapshot(m.SnapshotTerm,
				m.SnapshotIndex, m.Snapshot) {
				cfg.logs[i] = make(map[int]interface{})
				r := bytes.NewBuffer(m.Snapshot)
				d := labgob.NewDecoder(r)

				var v int
				if d.Decode(&v) != nil {
					log.Fatalf("decode error\n")
				}
				cfg.logs[i][m.SnapshotIndex] = v
				lastApplied = m.SnapshotIndex
			}
			cfg.mu.Unlock()
		} else if m.CommandValid && m.CommandIndex > lastApplied {
			cfg.mu.Lock()
			err_msg, prevok := cfg.checkLogs(i, m)
			cfg.mu.Unlock()
			if m.CommandIndex > 1 && prevok == false {
				err_msg = fmt.Sprintf("server %v apply out of order %v", i, m.CommandIndex)
			}
			if err_msg != "" {
				log.Fatalf("apply error: %v\n", err_msg)
				cfg.applyErr[i] = err_msg
				// keep reading after error so that Raft doesn't block
				// holding locks...
			}
			lastApplied = m.CommandIndex
			if (m.CommandIndex+1)%SnapShotInterval == 0 {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				v := m.Command
				e.Encode(v)
				cfg.rafts[i].Snapshot(m.CommandIndex, w.Bytes())
			}
		} else {
			// Ignore other types of ApplyMsg or old
			// commands. Old command may never happen,
			// depending on the Raft implementation, but
			// just in case.
			// DPrintf("Ignore: Index %v lastApplied %v\n", m.CommandIndex, lastApplied)

		}
	}
}
```

Why do I delete the command encoding in Lab4 ?

I met an strange decoding bug. When there is a last command information included in snapshot, it will lead other element decoding error and raise extra data in buffer. If I exclude the last command information in snapshot, it can decode the snapshot successfully. I still haven't figured out the bug. Maybe raft command in Lab4 is a complicated structure which include maps and other structure.







