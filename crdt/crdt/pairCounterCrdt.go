package crdt

import (
	"fmt"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"

	pb "google.golang.org/protobuf/proto"
	//pb "github.com/golang/protobuf/proto"
)

type PairCounterCrdt struct {
	CRDTVM
	first  int32
	second float64
}

type PairCounterState struct {
	First  int32
	Second float64
}

type SingleFirstCounterState int32
type SingleSecondCounterState float64

type ReadFirstArguments struct{}
type ReadSecondArguments struct{}

type IncrementFirst int32
type DecrementFirst float64
type IncrementSecond int32
type DecrementSecond float64
type IncrementBoth struct {
	ChangeFirst  int32
	ChangeSecond float64
}
type DecrementBoth struct {
	ChangeFirst  int32
	ChangeSecond float64
}

type IncrementFirstEffect int32
type IncrementSecondEffect float64
type DecrementFirstEffect int32
type DecrementSecondEffect float64
type IncrementBothEffect IncrementBoth
type DecrementBothEffect DecrementBoth

func (crdt *PairCounterCrdt) GetCRDTType() proto.CRDTType         { return proto.CRDTType_PAIR_COUNTER }
func (args IncrementFirst) GetCRDTType() proto.CRDTType           { return proto.CRDTType_PAIR_COUNTER }
func (args DecrementFirst) GetCRDTType() proto.CRDTType           { return proto.CRDTType_PAIR_COUNTER }
func (args IncrementSecond) GetCRDTType() proto.CRDTType          { return proto.CRDTType_PAIR_COUNTER }
func (args DecrementSecond) GetCRDTType() proto.CRDTType          { return proto.CRDTType_PAIR_COUNTER }
func (args IncrementBoth) GetCRDTType() proto.CRDTType            { return proto.CRDTType_PAIR_COUNTER }
func (args DecrementBoth) GetCRDTType() proto.CRDTType            { return proto.CRDTType_PAIR_COUNTER }
func (state PairCounterState) GetCRDTType() proto.CRDTType        { return proto.CRDTType_PAIR_COUNTER }
func (state PairCounterState) GetREADType() proto.READType        { return proto.READType_FULL }
func (state SingleFirstCounterState) GetCRDTType() proto.CRDTType { return proto.CRDTType_PAIR_COUNTER }
func (state SingleFirstCounterState) GetREADType() proto.READType { return proto.READType_PAIR_FIRST }
func (state SingleSecondCounterState) GetCRDTType() proto.CRDTType {
	return proto.CRDTType_PAIR_COUNTER
}
func (state SingleSecondCounterState) GetREADType() proto.READType { return proto.READType_PAIR_SECOND }
func (args IncrementFirst) MustReplicate() bool                    { return true }
func (args DecrementFirst) MustReplicate() bool                    { return true }
func (args IncrementSecond) MustReplicate() bool                   { return true }
func (args DecrementSecond) MustReplicate() bool                   { return true }
func (args IncrementBoth) MustReplicate() bool                     { return true }
func (args DecrementBoth) MustReplicate() bool                     { return true }
func (args ReadFirstArguments) GetCRDTType() proto.CRDTType        { return proto.CRDTType_PAIR_COUNTER }
func (args ReadSecondArguments) GetCRDTType() proto.CRDTType       { return proto.CRDTType_PAIR_COUNTER }
func (args ReadFirstArguments) GetREADType() proto.READType        { return proto.READType_PAIR_FIRST }
func (args ReadSecondArguments) GetREADType() proto.READType       { return proto.READType_PAIR_SECOND }
func (args ReadFirstArguments) HasInnerReads() bool                { return false }
func (args ReadSecondArguments) HasInnerReads() bool               { return false }
func (args ReadFirstArguments) HasVariables() bool                 { return false }
func (args ReadSecondArguments) HasVariables() bool                { return false }

func (crdt *PairCounterCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return &PairCounterCrdt{
		CRDTVM: (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete),
		first:  0,
		second: 0,
	}
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *PairCounterCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *PairCounterCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
	return crdt
}

func (crdt *PairCounterCrdt) IsBigCRDT() bool { return false }

func (crdt *PairCounterCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	switch args.(type) {
	case StateReadArguments:
		return crdt.getState(updsNotYetApplied)
	case ReadFirstArguments:
		return crdt.getFirst(updsNotYetApplied)
	case ReadSecondArguments:
		return crdt.getSecond(updsNotYetApplied)
	default:
		fmt.Printf("[AVGCrdt]Unknown read type: %+v\n", args)
	}
	return nil
}

func (crdt *PairCounterCrdt) getState(updsNotYetApplied []UpdateArguments) (state PairCounterState) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		return PairCounterState{First: crdt.first, Second: crdt.second}
	}
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case IncrementFirst:
			state.First += int32(typedUpd)
		case DecrementFirst:
			state.First -= int32(typedUpd)
		case IncrementSecond:
			state.Second += float64(typedUpd)
		case DecrementSecond:
			state.Second -= float64(typedUpd)
		case IncrementBoth:
			state.First += typedUpd.ChangeFirst
			state.Second += typedUpd.ChangeSecond
		case DecrementBoth:
			state.First -= typedUpd.ChangeFirst
			state.Second -= typedUpd.ChangeSecond
		}
	}
	return state
}

func (crdt *PairCounterCrdt) getFirst(updsNotYetApplied []UpdateArguments) (state SingleFirstCounterState) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		return SingleFirstCounterState(crdt.first)
	}
	value := int32(0)
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case IncrementFirst:
			value += int32(typedUpd)
		case DecrementFirst:
			value -= int32(typedUpd)
		case IncrementBoth:
			value += typedUpd.ChangeFirst
		case DecrementBoth:
			value -= typedUpd.ChangeFirst
			//Ignore effects on 2nd.
		}
	}
	state = SingleFirstCounterState(value)
	return state
}

func (crdt *PairCounterCrdt) getSecond(updsNotYetApplied []UpdateArguments) (state SingleSecondCounterState) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		return SingleSecondCounterState(crdt.second)
	}
	value := 0.0
	for _, upd := range updsNotYetApplied {
		switch typedUpd := upd.(type) {
		case IncrementSecond:
			value += float64(typedUpd)
		case DecrementSecond:
			value -= float64(typedUpd)
		case IncrementBoth:
			value += typedUpd.ChangeSecond
		case DecrementBoth:
			value -= typedUpd.ChangeSecond
			//Ignore effects on 1st.
		}
	}
	state = SingleSecondCounterState(value)
	return state
}

func (crdt *PairCounterCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	return args.(DownstreamArguments)
}

func (crdt *PairCounterCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	effect := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)

	return nil
}

func (crdt *PairCounterCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	var effectValue Effect
	switch incOrDec := downstreamArgs.(type) {
	case IncrementFirst:
		crdt.first += int32(incOrDec)
		effectValue = IncrementFirstEffect(incOrDec)
	case DecrementFirst:
		crdt.first -= int32(incOrDec)
		effectValue = DecrementFirstEffect(incOrDec)
	case IncrementSecond:
		crdt.second += float64(incOrDec)
		effectValue = IncrementSecondEffect(incOrDec)
	case DecrementSecond:
		crdt.second -= float64(incOrDec)
		effectValue = DecrementSecondEffect(incOrDec)
	case IncrementBoth:
		crdt.first += incOrDec.ChangeFirst
		crdt.second += incOrDec.ChangeSecond
	case DecrementBoth:
		crdt.first -= incOrDec.ChangeFirst
		crdt.second -= incOrDec.ChangeSecond
	}
	return &effectValue
}

func (crdt *PairCounterCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *PairCounterCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := PairCounterCrdt{CRDTVM: crdt.CRDTVM.copy(), first: crdt.first, second: crdt.second}
	return &newCRDT
}

func (crdt *PairCounterCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	//TODO: Most likely can do a small optimization to the one possible for Counters.
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *PairCounterCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *PairCounterCrdt) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case IncrementFirstEffect:
		crdt.first -= int32(typedEffect)
	case DecrementFirstEffect:
		crdt.first += int32(typedEffect)
	case IncrementSecondEffect:
		crdt.second -= float64(typedEffect)
	case DecrementSecondEffect:
		crdt.second += float64(typedEffect)
	case IncrementBothEffect:
		crdt.first -= typedEffect.ChangeFirst
		crdt.second -= typedEffect.ChangeSecond
	case DecrementBothEffect:
		crdt.first += typedEffect.ChangeFirst
		crdt.second += typedEffect.ChangeSecond
	}
}

func (crdt *PairCounterCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf functions

func (crdtOp IncrementFirst) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return IncrementFirst(protobuf.GetPaircounterop().GetIncFirst())
}

func (crdtOp IncrementFirst) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Paircounterop: &proto.ApbPairCounterUpdate{IncFirst: pb.Int32(int32(crdtOp))}}
}

func (crdtOp IncrementSecond) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return IncrementSecond(protobuf.GetPaircounterop().GetIncSecond())
}

func (crdtOp IncrementSecond) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Paircounterop: &proto.ApbPairCounterUpdate{IncSecond: pb.Float64(float64(crdtOp))}}
}

func (crdtOp IncrementBoth) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	proto := protobuf.GetPaircounterop()
	return IncrementBoth{ChangeFirst: proto.GetIncFirst(), ChangeSecond: proto.GetIncSecond()}
}

func (crdtOp IncrementBoth) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Paircounterop: &proto.ApbPairCounterUpdate{
		IncFirst: pb.Int32(int32(crdtOp.ChangeFirst)), IncSecond: pb.Float64(float64(crdtOp.ChangeSecond))}}
}

func (crdtOp DecrementFirst) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return DecrementFirst(-protobuf.GetPaircounterop().GetIncFirst())
}

func (crdtOp DecrementFirst) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Paircounterop: &proto.ApbPairCounterUpdate{IncFirst: pb.Int32(int32(-crdtOp))}}
}

func (crdtOp DecrementSecond) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return DecrementSecond(-protobuf.GetPaircounterop().GetIncSecond())
}

func (crdtOp DecrementSecond) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Paircounterop: &proto.ApbPairCounterUpdate{IncSecond: pb.Float64(float64(-crdtOp))}}
}

func (crdtOp DecrementBoth) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	proto := protobuf.GetPaircounterop()
	return IncrementBoth{ChangeFirst: -proto.GetIncFirst(), ChangeSecond: -proto.GetIncSecond()}
}

func (crdtOp DecrementBoth) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Paircounterop: &proto.ApbPairCounterUpdate{
		IncFirst: pb.Int32(int32(-crdtOp.ChangeFirst)), IncSecond: pb.Float64(float64(-crdtOp.ChangeSecond))}}
}

func (crdtState PairCounterState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	proto := protobuf.GetPaircounter()
	return PairCounterState{First: proto.GetFirst(), Second: proto.GetSecond()}
}

func (crdtState PairCounterState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Paircounter: &proto.ApbGetPairCounterResp{First: pb.Int32(crdtState.First), Second: pb.Float64(crdtState.Second)}}
}

func (crdtState SingleFirstCounterState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return SingleFirstCounterState(protobuf.GetPaircounter().GetFirst())
}

func (crdtState SingleFirstCounterState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Paircounter: &proto.ApbGetPairCounterResp{First: pb.Int32(int32(crdtState))}}
}

func (crdtState SingleSecondCounterState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	return SingleFirstCounterState(protobuf.GetPaircounter().GetSecond())
}

func (crdtState SingleSecondCounterState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Paircounter: &proto.ApbGetPairCounterResp{Second: pb.Float64(float64(crdtState))}}
}

// The caller is the one who has to see what kind of partial read this is.
func (args ReadFirstArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return args
}

func (args ReadFirstArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Paircounter: &proto.ApbPairCounterPartialRead{First: &proto.ApbPairCounterFirstRead{}}}
}

// The caller is the one who has to see what kind of partial read this is.
func (args ReadSecondArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return args
}

func (args ReadSecondArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Paircounter: &proto.ApbPairCounterPartialRead{Second: &proto.ApbPairCounterSecondRead{}}}
}

// The caller is the one who has to see what kind of update this is.
func (downOp IncrementFirst) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return IncrementFirst(protobuf.GetPairCounterOp().GetFirstChange())
}

func (downOp IncrementFirst) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{PairCounterOp: &proto.ProtoPairCounterDownstream{IsInc: pb.Bool(true), FirstChange: pb.Int32(int32(downOp))}}
}

// The caller is the one who has to see what kind of update this is.
func (downOp IncrementSecond) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return IncrementSecond(protobuf.GetPairCounterOp().GetSecondChange())
}

func (downOp IncrementSecond) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{PairCounterOp: &proto.ProtoPairCounterDownstream{IsInc: pb.Bool(true), SecondChange: pb.Float64(float64(downOp))}}
}

// The caller is the one who has to see what kind of update this is.
func (downOp IncrementBoth) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	op := protobuf.GetPairCounterOp()
	return IncrementBoth{ChangeFirst: op.GetFirstChange(), ChangeSecond: op.GetSecondChange()}
}

func (downOp IncrementBoth) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{PairCounterOp: &proto.ProtoPairCounterDownstream{
		IsInc: pb.Bool(true), FirstChange: pb.Int32(int32(downOp.ChangeFirst)), SecondChange: pb.Float64(float64(downOp.ChangeSecond))}}
}

// The caller is the one who has to see what kind of update this is.
func (downOp DecrementFirst) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return DecrementFirst(-protobuf.GetPairCounterOp().GetFirstChange())
}

func (downOp DecrementFirst) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{PairCounterOp: &proto.ProtoPairCounterDownstream{IsInc: pb.Bool(false), FirstChange: pb.Int32(int32(-downOp))}}
}

// The caller is the one who has to see what kind of update this is.
func (downOp DecrementSecond) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return DecrementSecond(-protobuf.GetPairCounterOp().GetSecondChange())
}

func (downOp DecrementSecond) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{PairCounterOp: &proto.ProtoPairCounterDownstream{IsInc: pb.Bool(false), SecondChange: pb.Float64(float64(-downOp))}}
}

// The caller is the one who has to see what kind of update this is.
func (downOp DecrementBoth) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	op := protobuf.GetPairCounterOp()
	return DecrementBoth{ChangeFirst: -op.GetFirstChange(), ChangeSecond: -op.GetSecondChange()}
}

func (downOp DecrementBoth) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{PairCounterOp: &proto.ProtoPairCounterDownstream{
		IsInc: pb.Bool(false), FirstChange: pb.Int32(int32(-downOp.ChangeFirst)), SecondChange: pb.Float64(float64(-downOp.ChangeSecond))}}
}

func (crdt *PairCounterCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	first, second := crdt.first, crdt.second
	return &proto.ProtoState{PairCounter: &proto.ProtoPairCounterState{First: &first, Second: &second}}
}

func (crdt *PairCounterCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (newCRDT CRDT) {
	pairProto := proto.GetPairCounter()
	return (&PairCounterCrdt{first: pairProto.GetFirst(), second: pairProto.GetSecond()}).initializeFromSnapshot(ts, replicaID)
}

func (crdt *PairCounterCrdt) GetCRDT() CRDT { return crdt }

//ApbPairCounterPartialReadResp
//ApbPairCounterFirstRead
//ApbPairCounterSecondRead
//ApbPairCounterPartialRead
//ApbPairCounterUpdate
//type PairCounterState struct {First, Second int32}
//type SingleCounterState int32

//type ReadFirstArguments struct{}
//type ReadSecondArguments struct{}

//type IncrementFirst int32
//type DecrementFirst int32
//type IncrementSecond int32
//type DecrementSecond int32
//type IncrementBoth struct{ ChangeFirst, ChangeSecond int32 }
//type DecrementBoth struct{ ChangeFirst, ChangeSecond int32 }

//func (crdt *PairCounterCrdt) GetCRDT() CRDT { return crdt }
