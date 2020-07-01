package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClerkID int64
	OpID    int64
	Key     string
	Value   string
	Op      string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key     string
	ClerkID int64
	OpID    int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
