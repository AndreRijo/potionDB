package crdt

import (
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"

	"github.com/AndreRijo/go-tools/src/tools"
	pb "google.golang.org/protobuf/proto"
)

// A MV-Register, also known as Multi-Value Register
// It works like a typical MV-Register, returning all concurrent writes when there is no single "latest" write.
// However, it also features a special "Single" read, that always returns only 1 value.
// This is useful for apps that really want to read only one value and only care that said value is one of the latest.
// This read returns the concurrent write from the replica with the lowest replicaID, so it is consistent across replicas.
type MVRegisterCrdt struct {
	CRDTVM
	clkValuePair PairValueClk //The "current" value, which is the one with the highest clock (or, in case of ties, the clk produced by the replica with lowest replicaID)
	replicaID    int16        //ReplicaID of the replica with the "current" value
	//concValues     map[any]clocksi.Timestamp //Important note: we're at most gonna have as many entries as the number of replicas-1.
	concValues     tools.SliceWithCounter[PairValueClk] //Note: at most we will have as many entries as the number of replicas-1.
	localReplicaID int16                                //ReplicaID of the replica with this CRDT instance
}

type MVRegisterState struct {
	Values []any
}

type MVSetValue struct { //Needed so that EmbMaps can recognize the type of CRDT correctly.
	NewValue any
}

type MVRegisterSingleState RegisterState
type MVRegisterSingleReadArguments struct{}

type DownstreamMVSetValue struct {
	NewValue  any
	ReplicaID int16
	Clk       clocksi.Timestamp
}

//Effects wanted (note: always keep the list of clks removed from concValues.)
//1. The new value went to value, because it was >=.
//2. The new value is concurrent to the current value, but it went still to value.
//3. The new value is concurrent to the current value, and it went to concValues.

type MVRegisterEffect interface {
	GetRemovedConcValues() tools.SliceWithCounter[PairValueClk] //Clks removed from concValues while the operation was being applied.
}

type SetValueHigherEffect struct { //New value is >= current value
	OldPair      PairValueClk //The old value that was replaced by the new value
	OldReplicaID int16
	RemovedConc  tools.SliceWithCounter[PairValueClk]
}

type SetValueConcurrentHigherEffect struct { //New value is concurrent to current value, but it went still to value due to replicaID being lower.
	OldPair      PairValueClk //The old value that was replaced by the new value
	OldReplicaID int16
	RemovedConc  tools.SliceWithCounter[PairValueClk]
}

type SetValueConcurrentEffect struct { //New value is concurrent to current value and it went to concValues.
	NewPair     PairValueClk //The new value that was added to concValues
	RemovedConc tools.SliceWithCounter[PairValueClk]
}

func (crdt *MVRegisterCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_MVREG }

func (args MVSetValue) GetCRDTType() proto.CRDTType           { return proto.CRDTType_MVREG }
func (args DownstreamMVSetValue) GetCRDTType() proto.CRDTType { return proto.CRDTType_MVREG }
func (args DownstreamMVSetValue) MustReplicate() bool         { return true }

// Reads
func (args MVRegisterSingleReadArguments) GetCRDTType() proto.CRDTType { return proto.CRDTType_MVREG }
func (args MVRegisterSingleReadArguments) GetREADType() proto.READType {
	return proto.READType_MULTI_SINGLE
}
func (args MVRegisterSingleReadArguments) HasInnerReads() bool { return false }
func (args MVRegisterSingleReadArguments) HasVariables() bool  { return false }

// States
func (state MVRegisterState) GetCRDTType() proto.CRDTType       { return proto.CRDTType_MVREG }
func (state MVRegisterSingleState) GetCRDTType() proto.CRDTType { return proto.CRDTType_MVREG }
func (state MVRegisterState) GetREADType() proto.READType       { return proto.READType_FULL }
func (state MVRegisterSingleState) GetREADType() proto.READType { return proto.READType_MULTI_SINGLE }

// Effects
func (eff SetValueHigherEffect) GetRemovedConcValues() tools.SliceWithCounter[PairValueClk] {
	return eff.RemovedConc
}
func (eff SetValueConcurrentHigherEffect) GetRemovedConcValues() tools.SliceWithCounter[PairValueClk] {
	return eff.RemovedConc
}
func (eff SetValueConcurrentEffect) GetRemovedConcValues() tools.SliceWithCounter[PairValueClk] {
	return eff.RemovedConc
}

// Note: crdt can (and most often will be) nil
func (crdt *MVRegisterCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return &MVRegisterCrdt{
		CRDTVM: (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete),
		//value:          "",
		//clk:            clocksi.DummyTs,
		clkValuePair: PairValueClk{Value: "", Clk: clocksi.DummyTs},
		//concValues:     make(map[any]clocksi.Timestamp),
		concValues:     tools.NewSliceWithCounter[PairValueClk](len(clocksi.GetKeys()) - 1),
		localReplicaID: replicaID,
	}
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *MVRegisterCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *MVRegisterCrdt) {
	crdt.CRDTVM, crdt.localReplicaID = (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete), replicaID
	return crdt
}

func (crdt *MVRegisterCrdt) IsBigCRDT() bool { return false }

func (crdt *MVRegisterCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		if args.GetREADType() == proto.READType_FULL {
			return crdt.GetValue()
		} //else: single read
		return MVRegisterSingleState{Value: crdt.clkValuePair.Value}
	}
	if args.GetREADType() == proto.READType_FULL { //Correct value is always the one in the last update, as it has seen all values in concValues.
		return MVRegisterState{Values: []any{(updsNotYetApplied[len(updsNotYetApplied)-1]).(SetValue).NewValue}}
	}
	return MVRegisterSingleState{Value: updsNotYetApplied[len(updsNotYetApplied)-1].(SetValue).NewValue}
}

func (crdt *MVRegisterCrdt) GetValue() (state State) {
	/*values, i := make([]any, len(crdt.convValues)+1), 1
	values[0] = crdt.value
	for val := range crdt.concValues {
		values[i] = val
		i++
	}
	return MVRegisterState{Values: values}*/
	values, i := make([]any, crdt.concValues.Len+1), 1
	values[0] = crdt.clkValuePair.Value
	for _, pair := range crdt.concValues.ToSlice() {
		values[i] = pair.Value
		i++
	}
	return MVRegisterState{Values: values}
}

func (crdt *MVRegisterCrdt) Update(args UpdateArguments) (downStreamArgs DownstreamArguments) {
	if upd, ok := args.(MVSetValue); ok {
		newClk := crdt.getMaxClk().NextTimestamp(crdt.localReplicaID)
		return DownstreamMVSetValue{NewValue: upd.NewValue, Clk: newClk, ReplicaID: crdt.localReplicaID}
	}
	return nil
}

// This returns a clock that, given all clks in this crdt, has the highest for each entry.
func (crdt *MVRegisterCrdt) getMaxClk() (maxClk clocksi.Timestamp) {
	//maxClk = crdt.clk.Copy()
	//for _, clk := range crdt.concValues {
	//maxClk.MergeInto(clk) //Safe to use MergeInto as maxClk is already a copy.
	//}
	maxClk = crdt.clkValuePair.Clk.Copy() //We start with the current value's clk.
	for _, pair := range crdt.concValues.ToSlice() {
		maxClk.MergeInto(pair.Clk) //Safe to use MergeInto as maxClk is already a copy.
	}
	return maxClk
}

func (crdt *MVRegisterCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	crdt.addToHistory(&updTs, &downstreamArgs, crdt.applyDownstream(downstreamArgs))
	return nil
}

// TODO: Effects :((((
/*func (crdt *MVRegisterCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	var effectValue Effect
	if downUpd, ok := downstreamArgs.(DownstreamMVSetValue); ok {
		crdt.removeLowerClks(downUpd.Clk)   //Removes any clks that may be < than downUpd.Clk. Nothing happens if concValues is empty.
		if downUpd.Clk.IsHigher(crdt.clk) { //We definitely want this new value to be crdt.value. Afterwards, check if it is >= concurrent ones.
			crdt.value, crdt.clk, crdt.replicaID = downUpd.NewValue, downUpd.Clk, downUpd.replicaID
		} else { //Concurrent to this one. Need to check if it should be crdt.value, and then if it is >= than any concValues.
			if downUpd.replicaID < crdt.replicaID {
				crdt.concValues[crdt.value] = crdt.clk
				crdt.value, crdt.clk, crdt.replicaID = downUpd.NewValue, downUpd.Clk, downUpd.replicaID
			} else {
				crdt.concValues[downUpd.NewValue] = downUpd.Clk
			}
		}
		return &effectValue
	} else {
		effectValue = NoEffect{}
	}
	return &effectValue
}*/

func (crdt *MVRegisterCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	var effectValue Effect
	if downUpd, ok := downstreamArgs.(DownstreamMVSetValue); ok {
		removedClks := crdt.removeLowerClks(downUpd.Clk) //Removes any clks that may be < than downUpd.Clk. Nothing happens if concValues is empty.
		if downUpd.Clk.IsHigher(crdt.clkValuePair.Clk) { //We definitely want this new value to be crdt.value. Afterwards, check if it is >= concurrent ones.
			effectValue = SetValueHigherEffect{OldPair: crdt.clkValuePair, OldReplicaID: crdt.replicaID, RemovedConc: removedClks}
			crdt.clkValuePair.Value, crdt.clkValuePair.Clk, crdt.replicaID = downUpd.NewValue, downUpd.Clk, downUpd.ReplicaID
		} else { //Concurrent to this one. Need to check if it should be crdt.value, and then if it is >= than any concValues.
			if downUpd.ReplicaID < crdt.replicaID {
				effectValue = SetValueConcurrentHigherEffect{OldPair: crdt.clkValuePair, OldReplicaID: crdt.replicaID, RemovedConc: removedClks}
				crdt.concValues.AddToEnd(crdt.clkValuePair)
				crdt.clkValuePair, crdt.replicaID = PairValueClk{Value: downUpd.NewValue, Clk: downUpd.Clk}, downUpd.ReplicaID
			} else {
				effectValue = SetValueConcurrentEffect{NewPair: PairValueClk{Value: downUpd.NewValue, Clk: downUpd.Clk}, RemovedConc: removedClks}
				crdt.concValues.AddToEnd(PairValueClk{Value: downUpd.NewValue, Clk: downUpd.Clk})
			}
		}
		return &effectValue
	} else {
		effectValue = NoEffect{}
	}
	return &effectValue
}

// Iterates through concValues, removing any clk that is < refClk. Returns the removed clks for effects purposes.
/*func (crdt *MVRegisterCrdt) removeLowerClks(refClk clocksi.Timestamp) {
	for _, clk := range crdt.concValues {
		if clk.IsLower(refClk) {
			delete(crdt.concValues, clk)
		}
	}
}*/

// Iterate from end, to minimize shifts to left.
func (crdt *MVRegisterCrdt) removeLowerClks(refClk clocksi.Timestamp) (removedClks tools.SliceWithCounter[PairValueClk]) {
	if crdt.concValues.Len == 0 {
		return
	}
	removedClks = tools.NewSliceWithCounter[PairValueClk](crdt.concValues.Len)
	for i := crdt.concValues.Len - 1; i >= 0; i-- {
		if crdt.concValues.Get(i).Clk.IsLower(refClk) {
			removedClks.AddToEnd(crdt.concValues.RemoveAndGet(i))
		}
	}
	if removedClks.Len == 0 {
		return tools.SliceWithCounter[PairValueClk]{}
	}
	return removedClks
}

func (crdt *MVRegisterCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *MVRegisterCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := MVRegisterCrdt{
		CRDTVM: crdt.CRDTVM.copy(),
		//value:          crdt.value,
		//clk:            crdt.clk,
		//concValues:     make(map[any]clocksi.Timestamp, len(crdt.concValues)),
		clkValuePair:   crdt.clkValuePair,
		concValues:     crdt.concValues.Copy(),
		replicaID:      crdt.replicaID,
		localReplicaID: crdt.localReplicaID,
	}
	/*for val, clk := range crdt.concValues {
		newCRDT.concValues[val] = clk
	}*/
	return &newCRDT
}

func (crdt *MVRegisterCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	//TODO: Might be worth it to make one specific for registers
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *MVRegisterCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *MVRegisterCrdt) undoEffect(effect *Effect) {
	if mvEff, ok := (*effect).(MVRegisterEffect); ok {
		crdt.reAddClks(mvEff.GetRemovedConcValues()) //Re-adds the clks that were removed from concValues.

		switch typedEff := mvEff.(type) {
		case SetValueHigherEffect:
			crdt.clkValuePair, crdt.replicaID = typedEff.OldPair, typedEff.OldReplicaID
		case SetValueConcurrentHigherEffect:
			crdt.clkValuePair, crdt.replicaID = typedEff.OldPair, typedEff.OldReplicaID
		case SetValueConcurrentEffect:
			tools.RemoveByValueSlice(&crdt.concValues, typedEff.NewPair) //Removes the new pair from concValues.
		}
	}
}

func (crdt *MVRegisterCrdt) reAddClks(removedClks tools.SliceWithCounter[PairValueClk]) {
	for _, pair := range removedClks.ToSlice() {
		crdt.concValues.AddToEnd(pair)
	}
}

func (crdt *MVRegisterCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf functions

func (crdtOp MVSetValue) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.NewValue = string(protobuf.GetRegop().GetValue())
	return crdtOp
}

func (crdtOp MVSetValue) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Regop: &proto.ApbRegUpdate{Value: []byte(crdtOp.NewValue.(string))}}
}

func (crdtState MVRegisterState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	byteValues := protobuf.GetMvreg().GetValues()
	crdtState.Values = make([]any, len(byteValues))
	for i, byteValue := range byteValues {
		crdtState.Values[i] = string(byteValue)
	}
	return crdtState
}

func (crdtState MVRegisterState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	byteValues := make([][]byte, len(crdtState.Values))
	for i, value := range crdtState.Values {
		byteValues[i] = []byte(value.(string))
	}
	return &proto.ApbReadObjectResp{Mvreg: &proto.ApbGetMVRegResp{Values: byteValues}}
}

func (crdtState MVRegisterSingleState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.Value = string(protobuf.GetReg().GetValue())
	return crdtState
}

func (crdtState MVRegisterSingleState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Mvreg: &proto.ApbMVRegPartialReadResp{
		Single: &proto.ApbMVRegSingleResp{Value: []byte((crdtState.Value).(string))}}}}
}

func (args MVRegisterSingleReadArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return args
}

func (args MVRegisterSingleReadArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Mvreg: &proto.ApbMVRegPartialRead{Single: &proto.ApbMVRegSingleRead{}}}
}

func (downOp DownstreamMVSetValue) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	regOp := protobuf.GetMvregOp()
	downOp.NewValue, downOp.ReplicaID, downOp.Clk = string(regOp.GetValue()), int16(regOp.GetReplicaID()), clocksi.ClockSiTimestamp{}.FromBytes(regOp.GetClk())
	return downOp
}

func (downOp DownstreamMVSetValue) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{MvregOp: &proto.ProtoMVRegisterDownstream{
		Value: []byte(downOp.NewValue.(string)), Clk: downOp.Clk.ToBytes(), ReplicaID: pb.Int32(int32(downOp.ReplicaID)),
	}}
}

func (crdt *MVRegisterCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	mvState := &proto.ProtoMVRegState{Value: []byte((crdt.clkValuePair.Value).(string)), Clk: crdt.clkValuePair.Clk.ToBytes(), ReplicaID: pb.Int32(int32(crdt.replicaID))}
	mvState.ConcValues, mvState.ConcClks = make([][]byte, crdt.concValues.Len), make([][]byte, crdt.concValues.Len)
	for i, pair := range crdt.concValues.ToSlice() {
		mvState.ConcValues[i], mvState.ConcClks[i] = []byte(pair.Value.(string)), pair.Clk.ToBytes()
	}
	return &proto.ProtoState{Mvreg: mvState}
}

func (crdt *MVRegisterCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (newCRDT CRDT) {
	mvProto := proto.GetMvreg()
	crdt.clkValuePair = PairValueClk{Value: string(mvProto.GetValue()), Clk: clocksi.ClockSiTimestamp{}.FromBytes(mvProto.GetClk())}
	crdt.replicaID = int16(mvProto.GetReplicaID())
	protoV, protoC := mvProto.GetConcValues(), mvProto.GetConcClks()
	crdt.concValues = tools.NewSliceWithCounter[PairValueClk](len(clocksi.GetKeys()))
	for i := 0; i < len(protoV); i++ {
		crdt.concValues.AddToEnd(PairValueClk{Value: string(protoV[i]), Clk: clocksi.ClockSiTimestamp{}.FromBytes(protoC[i])})
	}
	return crdt.initializeFromSnapshot(ts, replicaID)
}

func (crdt *MVRegisterCrdt) GetCRDT() CRDT { return crdt }
