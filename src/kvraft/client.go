package raftkv

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	lastLeader  int
	id          int64
	seqNum      int
}

func Nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = Nrand()//give each client a unique identifier, and then have them
	ck.seqNum = 0// tag each request with a monotonically increasing sequence number.
	ck.lastLeader = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	index := ck.lastLeader
	for {
		args := GetArgs{key}
		reply := GetReply{}
		ok := ck.servers[index].Call("KVServer.Get", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.lastLeader = index
			return reply.Value
		}
		index = (index + 1) % len(ck.servers)
		time.Sleep(time.Duration(50)*time.Millisecond)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	index := ck.lastLeader

	args := PutAppendArgs{key, value, op, ck.id, ck.seqNum}
	ck.seqNum++
	for {
		reply := PutAppendReply{}
		ok := ck.servers[index].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.lastLeader = index
			return
		}
		index = (index + 1) % len(ck.servers)
		time.Sleep(time.Duration(50)*time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
