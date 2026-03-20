package crdt

import (
	"fmt"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
	"potionDB/shared/shared"

	//pb "github.com/golang/protobuf/proto"
	pb "google.golang.org/protobuf/proto"
)

type CounterFloatCrdt struct {
	CRDTVM
	value float64
}

/*
	type CounterFloatState struct {
		Value float64
	}
*/
type CounterFloatState float64

type IncrementFloat struct {
	Change float64
}

type DecrementFloat struct {
	Change float64
}

type IncrementFloatEffect struct {
	Change float64
}

type DecrementFloatEffect struct {
	Change float64
}

func (crdt *CounterFloatCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_COUNTER_FLOAT }
func (crdt *CounterFloatCrdt) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args IncrementFloat) GetCRDTType() proto.CRDTType    { return proto.CRDTType_COUNTER_FLOAT }
func (args IncrementFloat) GetDATAType() proto.DATAType    { return proto.DATAType_DEFAULT }
func (args DecrementFloat) GetCRDTType() proto.CRDTType    { return proto.CRDTType_COUNTER_FLOAT }
func (args DecrementFloat) GetDATAType() proto.DATAType    { return proto.DATAType_DEFAULT }
func (args CounterFloatState) GetCRDTType() proto.CRDTType { return proto.CRDTType_COUNTER_FLOAT }
func (args CounterFloatState) GetREADType() proto.READType { return proto.READType_FULL }
func (args CounterFloatState) GetDATAType() proto.DATAType { return proto.DATAType_DEFAULT }
func (args IncrementFloat) MustReplicate() bool            { return true }
func (args DecrementFloat) MustReplicate() bool            { return true }

func (args CounterFloatState) GetAggregateResult(aggrType AggregateType, aggrKey string) (result float64, count int) {
	return float64(args), 1
}

// Note: crdt can (and most often will be) nil
func (crdt *CounterFloatCrdt) Initialize(startTs *clocksi.Timestamp, replicaID uint16) (newCrdt CRDT) {
	crdt = &CounterFloatCrdt{value: 0}
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(crdt)
	return crdt
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *CounterFloatCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID uint16) (sameCRDT *CounterFloatCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(crdt)
	return crdt
}

func (crdt *CounterFloatCrdt) IsBigCRDT() bool { return false }

func (crdt *CounterFloatCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	if len(updsNotYetApplied) == 0 {
		return CounterFloatState(crdt.value)
	}
	copyValue := crdt.value
	for _, upd := range updsNotYetApplied {
		switch typedUpd := (upd).(type) {
		case IncrementFloat:
			copyValue += typedUpd.Change
		case DecrementFloat:
			copyValue -= typedUpd.Change
		}
	}
	return CounterFloatState(copyValue)
}

/*func (crdt *CounterFloatCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	if len(updsNotYetApplied) == 0 {
		return crdt.GetValue()
	}
	counterState := crdt.GetValue().(CounterFloatState)
	for _, upd := range updsNotYetApplied {
		switch typedUpd := (upd).(type) {
		case IncrementFloat:
			counterState.Value += typedUpd.Change
		case DecrementFloat:
			counterState.Value -= typedUpd.Change
		}
	}
	return counterState
}

func (crdt *CounterFloatCrdt) GetValue() (state State) {
	return CounterFloatState{Value: crdt.value}
}*/

func (crdt *CounterFloatCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	return args.(DownstreamArguments)
}

func (crdt *CounterFloatCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	effect := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)

	return nil
}

func (crdt *CounterFloatCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	var effectValue Effect
	switch incOrDec := downstreamArgs.(type) {
	case IncrementFloat:
		crdt.value += incOrDec.Change
		effectValue = IncrementFloatEffect{Change: incOrDec.Change}
	case DecrementFloat:
		crdt.value -= incOrDec.Change
		effectValue = DecrementFloatEffect{Change: incOrDec.Change}
	case MultiUpd:
		totalDiff := 0.0
		for _, singleOp := range incOrDec {
			switch singleTypedOp := singleOp.(type) {
			case IncrementFloat:
				totalDiff += singleTypedOp.Change
			case DecrementFloat:
				totalDiff -= singleTypedOp.Change
			}
		}
		crdt.value += totalDiff
		effectValue = IncrementFloatEffect{Change: totalDiff}
	default:
		fmt.Printf("[CounterFloat][Downstream]Unsupported downstream type: %v (%T)\n", downstreamArgs, downstreamArgs)
	}
	return &effectValue
}

func (crdt *CounterFloatCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *CounterFloatCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := CounterFloatCrdt{
		CRDTVM: crdt.CRDTVM.copy(),
		value:  crdt.value,
	}
	return &newCRDT
}

func (crdt *CounterFloatCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	//TODO: Optimize and make one specific for counters (no need to redo operations!)
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *CounterFloatCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *CounterFloatCrdt) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case IncrementFloatEffect:
		crdt.value -= typedEffect.Change
	case DecrementFloatEffect:
		crdt.value += typedEffect.Change
	}
}

func (crdt *CounterFloatCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf functions

func (crdtOp IncrementFloat) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Change = protobuf.GetCounterfloatop().GetInc()
	return crdtOp
}

func (crdtOp IncrementFloat) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Counterfloatop{Counterfloatop: &proto.ApbCounterFloatUpdate{Inc: pb.Float64(crdtOp.Change)}}}
}

func (crdtOp DecrementFloat) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Change = -protobuf.GetCounterfloatop().GetInc()
	return crdtOp
}

func (crdtOp DecrementFloat) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Op: &proto.ApbUpdateOperation_Counterfloatop{Counterfloatop: &proto.ApbCounterFloatUpdate{Inc: pb.Float64(-crdtOp.Change)}}}
}

func (crdtState CounterFloatState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	//crdtState.Value = protobuf.GetCounterfloat().GetValue()
	crdtState = CounterFloatState(protobuf.GetCounterfloat().GetValue())
	return crdtState
}

func (crdtState CounterFloatState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Resp: &proto.ApbReadObjectResp_Counterfloat{Counterfloat: &proto.ApbGetCounterFloatResp{Value: pb.Float64(float64(crdtState))}}}
	//return &proto.ApbReadObjectResp{Counterfloat: &proto.ApbGetCounterFloatResp{Value: pb.Float64(crdtState.Value)}}
}

func (downOp IncrementFloat) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downOp.Change = protobuf.GetCounterfloatOp().GetChange()
	return downOp
}

func (downOp DecrementFloat) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downOp.Change = -protobuf.GetCounterfloatOp().GetChange()
	return downOp
}

func (downOp IncrementFloat) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_CounterfloatOp{CounterfloatOp: &proto.ProtoCounterFloatDownstream{IsInc: shared.TRUE_POINTER, Change: pb.Float64(downOp.Change)}}}
}

func (downOp DecrementFloat) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{Op: &proto.ProtoOpDownstream_CounterfloatOp{CounterfloatOp: &proto.ProtoCounterFloatDownstream{IsInc: shared.FALSE_POINTER, Change: pb.Float64(-downOp.Change)}}}
}

func (crdt *CounterFloatCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	value := crdt.value
	return &proto.ProtoState{State: &proto.ProtoState_Counterfloat{Counterfloat: &proto.ProtoCounterFloatState{Value: &value}}}
}

func (crdt *CounterFloatCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID uint16) (newCRDT CRDT) {
	return (&CounterFloatCrdt{value: proto.GetCounterfloat().GetValue()}).initializeFromSnapshot(ts, replicaID)
}

func (crdt *CounterFloatCrdt) GetCRDT() CRDT { return crdt }
