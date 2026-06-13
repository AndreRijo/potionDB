package components

import (
	"potionDB/crdt/clocksi"
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
)

//This would make more sense to be defined in protoServer.go,
//but we need to send this buffers through the protobufing layer,
//which is both in crdt and antidote package, and it contains references to protobufs.
//so it can't be in the protobufs package either, thus it is here.

// Re-usable buffers per (protoServer) client, for lessening GC pressure.
type ClientBuffers struct {
	CommitRespProto     *proto.ApbCommitResp
	Clk                 clocksi.Timestamp
	Upds                []crdt.UpdateObjectParams
	StaticReadRespProto *proto.ApbStaticReadObjectsResp
	ReadBuf             []crdt.ReadObjectParams
	TMStaticReadChan    chan TMStaticReadReply
	TMStaticUpdateChan  chan TMStaticUpdateReply

	*proto.PbBuffers //Buffers re-used by unmarshal.
}

func (c *ClientBuffers) Init(txnDescSize int) (clientBufs *ClientBuffers) {
	*c = ClientBuffers{
		CommitRespProto: &proto.ApbCommitResp{Success: new(bool), CommitTime: make([]byte, txnDescSize)},
		Clk:             clocksi.NewSliceTimestamp(),
		Upds:            nil,
		StaticReadRespProto: &proto.ApbStaticReadObjectsResp{
			Committime: &proto.ApbCommitResp{Success: new(bool), Errorcode: new(uint32), CommitTime: make([]byte, txnDescSize)},
			Objects:    &proto.ApbReadObjectsResp{Success: new(bool), Errorcode: new(uint32)}},
		TMStaticReadChan:   make(chan TMStaticReadReply),
		TMStaticUpdateChan: make(chan TMStaticUpdateReply),
		ReadBuf:            make([]crdt.ReadObjectParams, 2), //Will get automatically expanded as needed.
		PbBuffers:          &proto.PbBuffers{},
	}
	c.PbBuffers.ReadInit()
	return c
}

// Initializes only a subset of the fields
func (c *ClientBuffers) CPReceiveInit() (clientBufs *ClientBuffers) {
	*c = ClientBuffers{PbBuffers: &proto.PbBuffers{S2SReply: &proto.S2SWrapperReply{ClientID: new(uint64), MsgID: new(proto.WrapperType)}}}
	return c
}

func InitializeClientBuffers(txnDescSize int) (clientBufs *ClientBuffers) {
	return (&ClientBuffers{}).Init(txnDescSize)
}
