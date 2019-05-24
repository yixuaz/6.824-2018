package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"strconv"
	"sync"
	"time"
)

type Op struct {
	OpType	string "operation type(eg. put/append)"
	Key 	string
	Value	string
	Cid		int64
	SeqNum	int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	persist *raft.Persister
	db 		map[string]string
	chMap   map[int]chan Op
	cid2Seq	map[int64]int
	killCh  chan bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//from hint: A simple solution is to enter every Get() (as well as each Put() and Append()) in the Raft log.
	originOp := Op{"Get",args.Key,strconv.FormatInt(Nrand(),10),0,0}
	reply.WrongLeader = true
	index,_,isLeader := kv.rf.Start(originOp)
	if !isLeader {return}
	ch := kv.put(index,true)
	op := kv.beNotified(ch,index)
	if equalOp(op,originOp) {
		reply.WrongLeader = false
		kv.mu.Lock()
		reply.Value = kv.db[op.Key]
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	originOp := Op{args.Op,args.Key,args.Value,args.Cid,args.SeqNum}
	reply.WrongLeader = true
	index,_,isLeader := kv.rf.Start(originOp)
	if !isLeader {return}
	ch := kv.put(index,true)
	op := kv.beNotified(ch,index)
	if equalOp(originOp,op) {
		reply.WrongLeader = false
	}
}
func (kv *KVServer) beNotified(ch chan Op,index int) Op{
	select {
	case notifyArg := <- ch :
		close(ch)
		kv.mu.Lock()
		delete(kv.chMap,index)
		kv.mu.Unlock()
		return notifyArg
	case <- time.After(time.Duration(600)*time.Millisecond):
		return Op{}
	}
}
func (kv *KVServer) put(idx int,createIfNotExists bool) chan Op{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.chMap[idx]; !ok {
		if !createIfNotExists {return nil}
		kv.chMap[idx] = make(chan Op,1)
	}
	return kv.chMap[idx]
}
func equalOp(a Op, b Op) bool{
	return a.Key == b.Key && a.Value == b.Value && a.OpType == b.OpType && a.SeqNum == b.SeqNum && a.Cid == b.Cid
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
	kv.killCh <- true
}

func (kv *KVServer) readSnapShot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) < 1 {return}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var cid2Seq map[int64]int
	if  d.Decode(&db) != nil || d.Decode(&cid2Seq) != nil {
		log.Fatal("readSnapShot ERROR for server %v",kv.me)
	} else {
		kv.db, kv.cid2Seq = db, cid2Seq
	}
}

func (kv *KVServer) needSnapShot() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	threshold := 10
	return kv.maxraftstate > 0 &&
		kv.maxraftstate - kv.persist.RaftStateSize() < kv.maxraftstate/threshold
}

func (kv *KVServer) doSnapShot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.db)
	e.Encode(kv.cid2Seq)
	kv.mu.Unlock()
	kv.rf.DoSnapShot(index,w.Bytes())
}
func send(notifyCh chan Op,op Op) {
	select{
	case  <-notifyCh:
	default:
	}
	notifyCh <- op
}
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persist = persister
	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.chMap = make(map[int]chan Op)
	kv.cid2Seq = make(map[int64]int)
	kv.readSnapShot(kv.persist.ReadSnapshot())
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killCh = make(chan bool,1)
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
				op := applyMsg.Command.(Op)
				kv.mu.Lock()
				maxSeq,found := kv.cid2Seq[op.Cid]
				if !found || op.SeqNum > maxSeq {
					switch op.OpType {
					case "Put":
						kv.db[op.Key] = op.Value
					case "Append":
						kv.db[op.Key] += op.Value
					}
					kv.cid2Seq[op.Cid] = op.SeqNum
				}
				kv.mu.Unlock()
				if kv.needSnapShot() {
					go kv.doSnapShot(applyMsg.CommandIndex)
				}
				if notifyCh := kv.put(applyMsg.CommandIndex,false); notifyCh != nil {
					send(notifyCh,op)
				}

			}
		}
	}()
	return kv
}


