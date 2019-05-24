package raftkv

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Cid    int64 "client unique id"
	SeqNum int   "each request with a monotonically increasing sequence number"
}


type PutAppendReply struct {
	WrongLeader bool
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

