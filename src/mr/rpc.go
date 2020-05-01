package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// ExampleArgs is ...
type ExampleArgs struct {
	X int
}

// ExampleReply is ...
type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// RequestArgs is ...
type RequestArgs struct {
	State string
	Num   int
}

// ReplyArgs is ...
type ReplyArgs struct {
	Task         string
	TaskNumber   int
	ReduceNumber int
	MapNumber    int
	Files        []string
	NumFiles     int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
