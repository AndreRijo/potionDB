package crdt

import (
	"potionDB/src/clocksi"
	"potionDB/src/proto"

	pb "github.com/golang/protobuf/proto"
)

type AvgCrdt struct {
	*genericInversibleCRDT
	sum   int64
	nAdds int64
}

type AvgState struct {
	Value float64
}

type AvgFullState struct {
	Sum   int64
	NAdds int64
}

type AvgGetFullArguments struct{}

type AddValue struct {
	Value int64
}

type AddMultipleValue struct {
	SumValue int64
	NAdds    int64
}

type AddMultipleValueEffect AddMultipleValue

func (crdt *AvgCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_AVG }

func (args AddValue) GetCRDTType() proto.CRDTType { return proto.CRDTType_AVG }

func (args AddMultipleValue) GetCRDTType() proto.CRDTType { return proto.CRDTType_AVG }

func (args AvgState) GetCRDTType() proto.CRDTType { return proto.CRDTType_AVG }

func (args AvgState) GetREADType() proto.READType { return proto.READType_FULL }

func (args AvgFullState) GetCRDTType() proto.CRDTType { return proto.CRDTType_AVG }

func (args AvgFullState) GetREADType() proto.READType { return proto.READType_FULL }

func (args AddMultipleValue) MustReplicate() bool { return true }

func (args AvgGetFullArguments) GetCRDTType() proto.CRDTType { return proto.CRDTType_AVG }

func (args AvgGetFullArguments) GetREADType() proto.READType { return proto.READType_GET_FULL_AVG }

//Note: crdt can (and most often will be) nil
func (crdt *AvgCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return &AvgCrdt{
		genericInversibleCRDT: (&genericInversibleCRDT{}).initialize(startTs),
		sum:                   0,
		nAdds:                 0,
	}
}

//Used to initialize when building a CRDT from a remote snapshot
func (crdt *AvgCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *AvgCrdt) {
	crdt.genericInversibleCRDT = (&genericInversibleCRDT{}).initialize(startTs)
	return crdt
}

func (crdt *AvgCrdt) Read(args ReadArguments, updsNotYetApplied []*UpdateArguments) (state State) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) > 0 {
		return crdt.GetValue()
	}
	//var sum int64 = crdt.sum
	//var nAdds int64 = crdt.nAdds
	sum, nAdds := crdt.sum, crdt.nAdds
	for _, upd := range updsNotYetApplied {
		typedUpd := (*upd).(AddMultipleValue)
		sum += typedUpd.SumValue
		nAdds += typedUpd.NAdds
	}
	return AvgState{Value: float64(sum) / float64(nAdds)}
}

func (crdt *AvgCrdt) GetValue() (state State) {
	return AvgState{Value: float64(crdt.sum) / float64(crdt.nAdds)}
}

func (crdt *AvgCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	switch typedArgs := args.(type) {
	case AddValue:
		downstreamArgs = AddMultipleValue{SumValue: typedArgs.Value, NAdds: 1}
	case AddMultipleValue:
		downstreamArgs = args.(DownstreamArguments)
	}
	return
}

func (crdt *AvgCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	effect := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)

	return nil
}

func (crdt *AvgCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	typedUpd := downstreamArgs.(AddMultipleValue)
	var effectValue Effect = AddMultipleValueEffect(typedUpd)
	crdt.sum += typedUpd.SumValue
	crdt.nAdds += typedUpd.NAdds
	return &effectValue
}

func (crdt *AvgCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *AvgCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := AvgCrdt{
		genericInversibleCRDT: crdt.genericInversibleCRDT.copy(),
		sum:                   crdt.sum,
		nAdds:                 crdt.nAdds,
	}
	return &newCRDT
}

func (crdt *AvgCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	//TODO: Might be worth to check if there's a better way of doing this for avg
	crdt.genericInversibleCRDT.rebuildCRDTToVersion(targetTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
}

func (crdt *AvgCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *AvgCrdt) undoEffect(effect *Effect) {
	typedEffect := (*effect).(AddMultipleValueEffect)
	crdt.sum -= typedEffect.SumValue
	crdt.nAdds -= typedEffect.NAdds
}

func (crdt *AvgCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf functions

func (crdtOp AddMultipleValue) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	avgProto := protobuf.GetAvgop()
	crdtOp.SumValue, crdtOp.NAdds = avgProto.GetValue(), avgProto.GetNValues()
	return crdtOp
}

func (crdtOp AddMultipleValue) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Avgop: &proto.ApbAverageUpdate{
		Value: pb.Int64(crdtOp.SumValue), NValues: pb.Int64(crdtOp.NAdds),
	}}
}

func (crdtOp AddValue) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Value = protobuf.GetAvgop().GetValue()
	return crdtOp
}

func (crdtOp AddValue) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Avgop: &proto.ApbAverageUpdate{Value: pb.Int64(crdtOp.Value), NValues: pb.Int64(1)}}
}

func (crdtState AvgState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.Value = protobuf.GetAvg().GetAvg()
	return crdtState
}

func (crdtState AvgState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Avg: &proto.ApbGetAverageResp{Avg: pb.Float64(crdtState.Value)}}
}

func (crdtState AvgFullState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	avgReadReply := protobuf.GetPartread().GetAvg().GetGetfull()
	crdtState.Sum, crdtState.NAdds = avgReadReply.GetSum(), avgReadReply.GetNAdds()
	return crdtState
}

func (args AvgGetFullArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return args
}

func (args AvgGetFullArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Avg: &proto.ApbAvgPartialRead{Getfull: &proto.ApbAvgFullRead{}}}
}

func (downOp AddMultipleValue) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	avgProto := protobuf.GetAvgOp()
	downOp.SumValue, downOp.NAdds = avgProto.GetSumValue(), avgProto.GetNAdds()
	return downOp
}

func (downOp AddMultipleValue) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{AvgOp: &proto.ProtoAvgDownstream{SumValue: pb.Int64(downOp.SumValue), NAdds: pb.Int64(downOp.NAdds)}}
}

func (crdt *AvgCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	sum, nAdds := crdt.sum, crdt.nAdds
	return &proto.ProtoState{Avg: &proto.ProtoAvgState{Sum: &sum, NAdds: &nAdds}}
}

func (crdt *AvgCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (newCRDT CRDT) {
	protoAvg := proto.GetAvg()
	return (&AvgCrdt{sum: protoAvg.GetSum(), nAdds: protoAvg.GetNAdds()}).initializeFromSnapshot(ts, replicaID)
}
