package antidote

import (
	"potionDB/src/proto"

	pb "github.com/golang/protobuf/proto"
)

//Add here a code for each new protobuf you create
const (
	MinServerMsg = 200 //all server to server msgs must have a code >= 200
	Ping         = 250
	Pong         = 251
)

//Converts a msg code to the respective (empty) structure
func getSSProtoStruct(code byte, msgBuf []byte) (protobuf pb.Message) {
	switch code {
	case Ping:
		protobuf = &proto.Ping{}
	case Pong:
		protobuf = &proto.Pong{}
	}
	return
}
