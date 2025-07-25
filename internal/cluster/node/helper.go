package node

import (
	"encoding/gob"

	"github.com/parishadmk/CT_HW4/internal/cluster/replica"
)

func init() {
	gob.Register(Message{})
	gob.Register(Response{})
}

type MessageType string

const (
	Set      MessageType = "SET"
	Get      MessageType = "GET"
	Delete   MessageType = "DELETE"
	Snapshot MessageType = "SNAPSHOT"
)

type Message struct {
	Type        MessageType
	PartitionId int
	Timestamp   int64
	Key         string
	Value       string
	Snapshot    replica.Snapshot
}

type Response struct {
	Error error
	Value string
}
