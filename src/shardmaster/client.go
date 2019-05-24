package shardmaster

//
// Shardmaster clerk.
//

import (
	"labrpc"
	"sync/atomic"
)
import "time"
import "crypto/rand"
import "math/big"

const RetryInterval = time.Duration(30 * time.Millisecond)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	id   int64
	seqNum int
	lastLeader   int32
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
	ck.id = Nrand()
	ck.seqNum = 0
	ck.lastLeader = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{Num: num}
	for {
		var reply QueryReply
		if ck.servers[atomic.LoadInt32(&ck.lastLeader)].Call("ShardMaster.Query", &args, &reply) && !reply.WrongLeader {
			return reply.Config
		}
		atomic.StoreInt32(&ck.lastLeader,(atomic.LoadInt32(&ck.lastLeader) + 1) % int32(len(ck.servers)))
		//ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{Servers: servers, Cid:ck.id, SeqNum:ck.seqNum}
	ck.seqNum++
	for {
		var reply JoinReply
		if ck.servers[atomic.LoadInt32(&ck.lastLeader)].Call("ShardMaster.Join", &args, &reply) && !reply.WrongLeader {
			return
		}
		atomic.StoreInt32(&ck.lastLeader,(atomic.LoadInt32(&ck.lastLeader) + 1) % int32(len(ck.servers)))
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{GIDs: gids, Cid:ck.id, SeqNum:ck.seqNum}
	ck.seqNum++
	for {
		var reply LeaveReply
		if ck.servers[atomic.LoadInt32(&ck.lastLeader)].Call("ShardMaster.Leave", &args, &reply) && !reply.WrongLeader {
			return
		}
		atomic.StoreInt32(&ck.lastLeader,(atomic.LoadInt32(&ck.lastLeader) + 1) % int32(len(ck.servers)))
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{Shard: shard, GID: gid, Cid:ck.id, SeqNum:ck.seqNum}
	ck.seqNum++
	for {
		var reply MoveReply
		if ck.servers[atomic.LoadInt32(&ck.lastLeader)].Call("ShardMaster.Move", &args, &reply) && !reply.WrongLeader {
			return
		}
		atomic.StoreInt32(&ck.lastLeader,(atomic.LoadInt32(&ck.lastLeader) + 1) % int32(len(ck.servers)))
		time.Sleep(RetryInterval)
	}
}