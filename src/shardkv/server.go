package shardkv

import (
	"bytes"
	"labrpc"
	"log"
	"shardmaster"
	"strconv"
	"time"
)
import "raft"
import "sync"
import "labgob"

type Op struct {
	OpType	string  "operation type(eg. put/append/gc/get)"
	Key 	string  "key for normal, config num for gc"
	Value	string
	Cid		int64	"cid for put/append, operation uid for get/gc"
	SeqNum	int		"seqnum for put/append, shard for gc"
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg

	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	mck				*shardmaster.Clerk
	cfg				shardmaster.Config
	persist 		*raft.Persister
	db 				map[string]string
	chMap   		map[int]chan Op
	cid2Seq			map[int64]int

	toOutShards 	map[int]map[int]map[string]string "cfg num -> (shard -> db)"
	comeInShards	map[int]int     "shard->config number"
	myShards		map[int]bool	"to record which shard i can offer service"
	garbages		map[int]map[int]bool              "cfg number -> shards"

	killCh  	chan bool

	flag       int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	originOp := Op{"Get",args.Key,"",Nrand(),0}
	reply.Err,reply.Value = kv.templateStart(originOp)
}
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	originOp := Op{args.Op,args.Key,args.Value,args.Cid,args.SeqNum}
	reply.Err,_ = kv.templateStart(originOp)
}
func (kv *ShardKV) templateStart(originOp Op) (Err, string) {
	index,_,isLeader := kv.rf.Start(originOp)
	if isLeader {
		ch := kv.put(index, true)
		op := kv.beNotified(ch, index)
		if equalOp(originOp, op) { return OK, op.Value }
		if op.OpType == ErrWrongGroup { return ErrWrongGroup, "" }
	}
	return ErrWrongLeader,""
}
func (kv *ShardKV) GarbageCollection(args *MigrateArgs, reply *MigrateReply) {
	reply.Err = ErrWrongLeader
	if _, isLeader := kv.rf.GetState(); !isLeader {return}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _,ok := kv.toOutShards[args.ConfigNum]; !ok {return}
	if _,ok := kv.toOutShards[args.ConfigNum][args.Shard]; !ok {return}
	originOp := Op{"GC",strconv.Itoa(args.ConfigNum),"",Nrand(),args.Shard}
	kv.mu.Unlock()
	reply.Err,_ = kv.templateStart(originOp)
	kv.mu.Lock()
}

func (kv *ShardKV) ShardMigration(args *MigrateArgs, reply *MigrateReply) {
	reply.Err, reply.Shard, reply.ConfigNum = ErrWrongLeader, args.Shard, args.ConfigNum
	if _,isLeader := kv.rf.GetState(); !isLeader {return}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = ErrWrongGroup
	if args.ConfigNum >= kv.cfg.Num {return}
	reply.Err,reply.ConfigNum, reply.Shard = OK, args.ConfigNum, args.Shard
	reply.DB, reply.Cid2Seq = kv.deepCopyDBAndDedupMap(args.ConfigNum,args.Shard)
}
func (kv *ShardKV) deepCopyDBAndDedupMap(config int,shard int) (map[string]string, map[int64]int) {
	db2 := make(map[string]string)
	cid2Seq2 := make(map[int64]int)
	for k, v := range kv.toOutShards[config][shard] {
		db2[k] = v
	}
	for k, v := range kv.cid2Seq {
		cid2Seq2[k] = v
	}
	return db2, cid2Seq2
}

func (kv *ShardKV) beNotified(ch chan Op,index int) Op{
	select {
	case notifyArg,ok := <- ch :
		if ok {
			close(ch)
		}
		kv.mu.Lock()
		delete(kv.chMap,index)
		kv.mu.Unlock()
		return notifyArg
	case <- time.After(time.Duration(1000)*time.Millisecond):
		return Op{}
	}
}
func (kv *ShardKV) put(idx int,createIfNotExists bool) chan Op{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.chMap[idx]; !ok {
		if !createIfNotExists {return nil}
		kv.chMap[idx] = make(chan Op,1)
	}
	return kv.chMap[idx]
}
func equalOp(a Op, b Op) bool{
	return a.Key == b.Key &&  a.OpType == b.OpType && a.SeqNum == b.SeqNum && a.Cid == b.Cid
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	select{
	case  <-kv.killCh:
	default:
	}
	kv.killCh <- true
}

func (kv *ShardKV) readSnapShot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) < 1 {return}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var cid2Seq map[int64]int
	var toOutShards map[int]map[int]map[string]string
	var comeInShards map[int]int
	var myShards	map[int]bool
	var garbages	map[int]map[int]bool
	var cfg shardmaster.Config
	if d.Decode(&db) != nil || d.Decode(&cid2Seq) != nil || d.Decode(&comeInShards) != nil ||
		d.Decode(&toOutShards) != nil || d.Decode(&myShards) != nil || d.Decode(&cfg) != nil ||
		d.Decode(&garbages) != nil {
		log.Fatal("readSnapShot ERROR for server %v",kv.me)
	} else {
		kv.db, kv.cid2Seq, kv.cfg = db, cid2Seq, cfg
		kv.toOutShards, kv.comeInShards, kv.myShards, kv.garbages = toOutShards,comeInShards,myShards,garbages
	}
}

func (kv *ShardKV) needSnapShot() bool {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	threshold := 10
	return kv.maxraftstate > 0 &&
		kv.maxraftstate - kv.persist.RaftStateSize() < kv.maxraftstate/threshold
}

func (kv *ShardKV) doSnapShot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.db)
	e.Encode(kv.cid2Seq)
	e.Encode(kv.comeInShards)
	e.Encode(kv.toOutShards)
	e.Encode(kv.myShards)
	e.Encode(kv.cfg)
	e.Encode(kv.garbages)
	kv.mu.Unlock()
	kv.rf.DoSnapShot(index,w.Bytes())
}



func (kv *ShardKV) tryPollNewCfg() {
	_, isLeader := kv.rf.GetState();
	kv.mu.Lock()
	if !isLeader || len(kv.comeInShards) > 0{
		kv.mu.Unlock()
		return
	}
	next := kv.cfg.Num + 1
	kv.mu.Unlock()
	cfg := kv.mck.Query(next)
	if cfg.Num == next {
		kv.rf.Start(cfg) //sync follower with new cfg
	}
}
func (kv *ShardKV) tryGC() {
	_, isLeader := kv.rf.GetState();
	kv.mu.Lock()
	if !isLeader || len(kv.garbages) == 0{
		kv.mu.Unlock()
		return
	}
	var wait sync.WaitGroup
	for cfgNum, shards := range kv.garbages {
		for shard := range shards {
			wait.Add(1)
			go func(shard int, cfg shardmaster.Config) {
				defer wait.Done()
				args := MigrateArgs{shard, cfg.Num}
				gid := cfg.Shards[shard]
				for _, server := range cfg.Groups[gid] {
					srv := kv.make_end(server)
					reply := MigrateReply{}
					if ok := srv.Call("ShardKV.GarbageCollection", &args, &reply); ok && reply.Err == OK {
						kv.mu.Lock()
						defer kv.mu.Unlock()
						delete(kv.garbages[cfgNum], shard)
						if len(kv.garbages[cfgNum]) == 0 {
							delete(kv.garbages, cfgNum)
						}
					}
				}
			}(shard, kv.mck.Query(cfgNum))
		}
	}
	kv.mu.Unlock()
	wait.Wait()
}
func (kv *ShardKV) tryPullShard() {
	_, isLeader := kv.rf.GetState();
	kv.mu.Lock()
	if  !isLeader || len(kv.comeInShards) == 0 {
		kv.mu.Unlock()
		return
	}
	var wait sync.WaitGroup
	for shard, idx := range kv.comeInShards {
		wait.Add(1)
		go func(shard int, cfg shardmaster.Config) {
			defer wait.Done()
			args := MigrateArgs{shard, cfg.Num}
			gid := cfg.Shards[shard]
			for _, server := range cfg.Groups[gid] {
				srv := kv.make_end(server)
				reply := MigrateReply{}
				ok := srv.Call("ShardKV.ShardMigration", &args, &reply);
				if  ok && reply.Err == OK {
					kv.rf.Start(reply)
				}
			}
		}(shard, kv.mck.Query(idx))
	}
	kv.mu.Unlock()
	wait.Wait()
}

func (kv *ShardKV) daemon(do func(), sleepMS int) {
	for {
		select {
		case <-kv.killCh:
			return
		default:
			do()
		}
		time.Sleep(time.Duration(sleepMS) * time.Millisecond)
	}
}

func (kv *ShardKV) apply(applyMsg raft.ApplyMsg) {
	if cfg, ok := applyMsg.Command.(shardmaster.Config); ok {
		kv.updateInAndOutDataShard(cfg)
	} else if migrationData, ok := applyMsg.Command.(MigrateReply); ok{
		kv.updateDBWithMigrateData(migrationData)
	}else {
		op := applyMsg.Command.(Op)
		if op.OpType == "GC" {
			cfgNum,_ := strconv.Atoi(op.Key)
			kv.gc(cfgNum,op.SeqNum);
		} else {
			kv.normal(&op)
		}
		if notifyCh := kv.put(applyMsg.CommandIndex,false); notifyCh != nil {
			kv.mu.Lock()
			send(notifyCh,op)
			kv.mu.Unlock()
		}
	}
	if kv.needSnapShot() {
		go kv.doSnapShot(applyMsg.CommandIndex)
	}

}

func (kv *ShardKV) gc(cfgNum int, shard int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.toOutShards[cfgNum]; ok {
		delete(kv.toOutShards[cfgNum], shard)
		if len(kv.toOutShards[cfgNum]) == 0 {
			delete(kv.toOutShards, cfgNum)
		}
	}
}

func (kv *ShardKV) updateInAndOutDataShard(cfg shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if cfg.Num <= kv.cfg.Num { //only consider newer config
		return
	}
	oldCfg, toOutShard := kv.cfg, kv.myShards
	kv.myShards, kv.cfg = make(map[int]bool), cfg
	for shard, gid := range cfg.Shards {
		if gid != kv.gid {continue}
		if _, ok := toOutShard[shard]; ok || oldCfg.Num == 0 {
			kv.myShards[shard] = true
			delete(toOutShard, shard)
		} else {
			kv.comeInShards[shard] = oldCfg.Num
		}
	}
	if len(toOutShard) > 0 { // prepare data that needed migration
		kv.toOutShards[oldCfg.Num] = make(map[int]map[string]string)
		for shard := range toOutShard {
			outDb := make(map[string]string)
			for k, v := range kv.db {
				if key2shard(k) == shard {
					outDb[k] = v
					delete(kv.db, k)
				}
			}
			kv.toOutShards[oldCfg.Num][shard] = outDb
		}
	}
}

func (kv *ShardKV) updateDBWithMigrateData(migrationData MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if migrationData.ConfigNum != kv.cfg.Num-1 {return}
	delete(kv.comeInShards, migrationData.Shard)
	//this check is necessary, to avoid use  kv.cfg.Num-1 to update kv.cfg.Num's shard
	if _, ok := kv.myShards[migrationData.Shard]; !ok {
		kv.myShards[migrationData.Shard] = true
		for k, v := range migrationData.DB {
			kv.db[k] = v
		}
		for k, v := range migrationData.Cid2Seq {
			kv.cid2Seq[k] = Max(v,kv.cid2Seq[k])
		}
		if _, ok := kv.garbages[migrationData.ConfigNum]; !ok {
			kv.garbages[migrationData.ConfigNum] = make(map[int]bool)
		}
		kv.garbages[migrationData.ConfigNum][migrationData.Shard] = true
	}
}

func (kv *ShardKV) normal(op *Op) {
	shard := key2shard(op.Key)
	kv.mu.Lock()
	if _, ok := kv.myShards[shard]; !ok {
		op.OpType = ErrWrongGroup
	} else {
		maxSeq,found := kv.cid2Seq[op.Cid]
		if !found || op.SeqNum > maxSeq {
			if op.OpType == "Put" {
				kv.db[op.Key] = op.Value
			} else if op.OpType == "Append" {
				kv.db[op.Key] += op.Value
			}
			kv.cid2Seq[op.Cid] = op.SeqNum
		}
		if op.OpType == "Get" {
			op.Value = kv.db[op.Key]
		}
	}
	kv.mu.Unlock()
}

func send(notifyCh chan Op,op Op) {
	select{
	case  <-notifyCh:
	default:
	}
	notifyCh <- op
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(MigrateArgs{})
	labgob.Register(MigrateReply{})
	labgob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	// Your initialization code here.
	kv.persist = persister
	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.cfg = shardmaster.Config{}

	kv.db = make(map[string]string)
	kv.chMap = make(map[int]chan Op)
	kv.cid2Seq = make(map[int64]int)

	kv.toOutShards = make(map[int]map[int]map[string]string)
	kv.comeInShards = make(map[int]int)
	kv.myShards = make(map[int]bool)
	kv.garbages = make(map[int]map[int]bool)

	kv.readSnapShot(kv.persist.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killCh = make(chan bool,1)
	go kv.daemon(kv.tryPollNewCfg,50)
	go kv.daemon(kv.tryPullShard,35)
	go kv.daemon(kv.tryGC,100)
	go func() {
		for {
			select {
			case <- kv.killCh:
				return
			case applyMsg := <- kv.applyCh:
				if !applyMsg.CommandValid {
					kv.readSnapShot(applyMsg.SnapShot)
					continue
				}
				kv.apply(applyMsg)
			}
		}
	}()
	return kv
}