package crdt

import (
	"math"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"

	pb "google.golang.org/protobuf/proto"
)

//Idea that doesn't work:
//State: Incs, Decs, IncsR, DecsR map[int16]int64
//Note that we have causal delivery. So it's okay if it's not grow-only.
//We have two counters for each replica: incs; sets.
//Incs[r] is an absolute value, updateable only by replica r.
//Set value tries to clean all existing Incs[?], by setting Sets[?] for every replica,
//and adjusting its own Incs[r]
//Problem: before I even check if this works properly, this has the same issue as the simpleDateCRDT.
//Setting e.g. day 20 concurrently, when both replicas observe 10, would end up as 30.
//Sigh.

//---------------------

//Only way of really achieving this is to have some LWW mechanism for sets.
//Concurrent LWWs must set. Not act as a counter.

//Idea: keep the last value set, and the clock associated with it.
//When a new set arrives, compare it with the clock. If >=, replace.
//If concurrent but >= (due to replicaID), replace.
//When the arriving set comes, erase all existing incs/decs (as they are all casually before or concurrent)
//When an inc/dec arrives, check its clock: if it is >= than the current set clock, then it is applied.
//Otherwise, it is ignored.
//This works, but at a cost: all operations must carry a full clock.
//Can we somehow reduce the state of incs/decs?
//----Assume each inc/dec only carries the replicaID and ts.
//----How can we detect if this is concurrent?
//----Let's assume inc comes from A with ts 11. The actual clk was [10,15,5]
//----Let's assume last set was from B with vc [9, 20, 5]
//----The operations are concurrent, as the set did not observe inc[A], and inc[A] did not observe the set yet.
//----However, by looking at the ts, it seems ok. Wrong conclusion.
//----Thus, all operations must carry a full vc. :(
//The semantics are very clean.
//If concurrent sets happen, only one wins. The one that wins is the one with highest ts (use replicaID to distinguish similar ts). Just like LWW.
//Incs and decs work as expected among themselves.
//All incs and decs concurrent to a set lose.
//The order does not matter, as causal delivery is assumed: thus all incs/decs concurrent to the winning set will be discarded.
//Even if different replicas applies sets by different order, when the correct set "wins", all existing incs/decs are deleted.
//And an inc/dec after the winning set can only happen if it has seen the winning set - thus the set is already stable and no further resets will happen.

//State is simple:
//int64 setValue: (winning) value set by a set operation.
//clocksi.Timestamp setClk: the clock of the winning set operation.
//int64 setTs: the ts of the winning set operation.
//int16 setReplicaID: the replicaID of the winning set operation.
//int64 incs: all incs + decs applied to this CRDT that have not been "reset" yet by a set.
//Pfew. That was a long struggle to get this right :((((
//Undo of this is... *shrugs*. Probably need to store the clocks of all operations.
//But I guess that's OK as I would already store the Timestamp anyway...

// The final state is obtained by simply adding setValue and inc.
type SetWDateCrdt struct {
	CRDTVM
	setClk         clocksi.Timestamp //the clk of the winning set operation.
	setValue       int64             //(winning) value set by a set operation.
	setTs          int64             //the ts of the winning set operation.
	setReplicaID   int16             //the replicaID of the winning set operation.
	localReplicaID int16
	inc            int64             //all incs + decs applied to this CRDT that have not been "reset" yet by a set.
	currClk        clocksi.Timestamp //Keeps track of the highest clock seen, to know which clock to generate in Update.
}

type DateFullSetWArguments struct{ DateFullArguments }
type DateOnlySetWArguments struct{ DateOnlyArguments }
type TimeSetWArguments struct{ TimeArguments }
type TimestampSetWArguments struct{ TimestampArguments }

//Updates supported:
//SetDateFull, SetDate, SetDateOnly, SetTime, IncDate, IncMS, SetMS

// Note: Keep in mind that Vcs may be the same for two operations in the same txn - this is OK as long as all comparisons take this into consideration.
type DownstreamSetTsSetW struct {
	Value     int64
	Vc        clocksi.Timestamp //Vc of the replica at the time of execution. The position of replicaID is already updated with a new ts.
	ReplicaID int16             //ReplicaID of the replica that issued the set operation. Used to distinguish concurrent sets with the same ts.
}

type DownstreamIncTsSetW struct {
	Inc int64
	Vc  clocksi.Timestamp //Vc of the replica at the time of execution.
}

type SetTsSetWEffect struct {
	OldClk       clocksi.Timestamp
	OldValue     int64
	OldTs        int64
	OldReplicaID int16
}

type IncTsSetWEffect struct {
	Inc int64 //Value that was incremented; to undo, subtract this.
}

// States and ops are the same from SimpleDateCRDT; queries are embedded from SimpleDateCRDT.

func (crdt *SetWDateCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_SETW_DATE }

// Downstreams
func (args DownstreamSetTsSetW) GetCRDTType() proto.CRDTType { return proto.CRDTType_SETW_DATE }
func (args DownstreamIncTsSetW) GetCRDTType() proto.CRDTType { return proto.CRDTType_SETW_DATE }
func (args DownstreamSetTsSetW) MustReplicate() bool         { return true }
func (args DownstreamIncTsSetW) MustReplicate() bool         { return true }

func (crdt *SetWDateCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return &SetWDateCrdt{
		CRDTVM: (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete),
		setClk: clocksi.MinimumTs, setValue: GregorianToTs(1, 1, 1), setTs: math.MinInt64, setReplicaID: math.MaxInt16,
		localReplicaID: replicaID, inc: 0, currClk: (*startTs).Copy(),
	}
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *SetWDateCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *SetWDateCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
	return crdt
}

func (crdt *SetWDateCrdt) IsBigCRDT() bool { return false }

func (crdt *SetWDateCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	ms := crdt.setValue + crdt.inc
	if updsNotYetApplied != nil && len(updsNotYetApplied) > 0 {
		ms = crdt.getTsWithUpdsNotYetApplied(updsNotYetApplied)
	}
	return dateReadHelper(args, ms)
}

func (crdt *SetWDateCrdt) getTsWithUpdsNotYetApplied(updsNotYetApplied []UpdateArguments) (updMs int64) {
	updMs = crdt.setValue + crdt.inc
	//Vc is always >= of what's in here.
	for _, upd := range updsNotYetApplied {
		ms := upd.(DateUpd).ToMS()
		switch upd.(type) {
		case SetDateFull, SetMS:
			updMs = ms
		case SetDate:
			updMs = updMs%1000 + ms //Keep the original ms.
		case SetDateOnly:
			updMs = updMs%msPerDay + ms //Keep the original hour+min+sec+ms.
		case SetTime:
			updMs = (updMs/msPerDay)*msPerDay + ms //Keep the original day+month+year, but set the time to the new one.
		case IncDate, IncMS:
			updMs += ms
		}
	}
	return updMs
}

func (crdt *SetWDateCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	if dateUpd, ok := args.(DateUpd); ok {
		ms, nextVc := dateUpd.ToMS(), crdt.currClk.NextTimestamp(crdt.localReplicaID)
		switch args.(type) {
		case SetDate, SetDateOnly, SetDateFull: //We want the direct difference, and issue an increment representing it
			return DownstreamSetTsSetW{Value: ms, Vc: nextVc, ReplicaID: crdt.localReplicaID}
		case IncDate, IncMS:
			return DownstreamIncTsSetW{Inc: ms, Vc: nextVc}
		case SetTime:
			currMsDay := HourMinSecToMs(ExtractHourMinSec(ms))
			return DownstreamSetTsIncW{Value: ms + currMsDay, Vc: nextVc, ReplicaID: crdt.localReplicaID}
		}
	}
	return
}

func (crdt *SetWDateCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	crdt.addToHistory(&updTs, &downstreamArgs, crdt.applyDownstream(downstreamArgs))
	return nil
}

func (crdt *SetWDateCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	switch typedArgs := downstreamArgs.(type) {
	case DownstreamIncTsSetW:
		effect = crdt.applyIncTs(typedArgs.Inc, typedArgs.Vc)
	case DownstreamSetTsSetW:
		effect = crdt.applySetTs(typedArgs.Value, typedArgs.ReplicaID, typedArgs.Vc)
	}
	return
}

func (crdt *SetWDateCrdt) applyIncTs(opInc int64, opVc clocksi.Timestamp) (effect *Effect) {
	var effectValue Effect
	if opVc.IsHigherOrEqual(crdt.setClk) { //Can apply inc.
		crdt.inc += opInc
		effectValue = IncTsSetWEffect{Inc: opInc}
	} else { //Ignore op due to a concurrent set.
		effectValue = NoEffect{}
	}
	crdt.currClk.MergeInto(opVc) //Always update the highest clock seen
	return &effectValue
}

func (crdt *SetWDateCrdt) applySetTs(opSet int64, opReplicaID int16, opVc clocksi.Timestamp) (effect *Effect) {
	compResult := opVc.Compare(crdt.setClk)
	var effectValue Effect
	if compResult == clocksi.HigherTs || compResult == clocksi.EqualTs || compResult == clocksi.ConcurrentTs && opReplicaID <= crdt.setReplicaID {
		//Can apply set.
		effectValue = SetTsSetWEffect{OldClk: crdt.setClk, OldValue: crdt.setValue, OldTs: crdt.setTs, OldReplicaID: crdt.setReplicaID}
		crdt.setValue, crdt.setTs, crdt.setClk, crdt.setReplicaID = opSet, opVc.GetPos(opReplicaID), opVc, opReplicaID
		crdt.inc = 0 //Reset incs, as they are all before the set.
	} else { //Ignore op due to a winning concurrent set.
		effectValue = NoEffect{}
	}
	crdt.currClk.MergeInto(opVc) //Always update the highest clock seen
	return &effectValue
}

func (crdt *SetWDateCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *SetWDateCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := SetWDateCrdt{
		CRDTVM: crdt.CRDTVM.copy(), setClk: crdt.setClk.Copy(),
		setValue: crdt.setValue, setTs: crdt.setTs, setReplicaID: crdt.setReplicaID,
		localReplicaID: crdt.localReplicaID, inc: crdt.inc, currClk: crdt.currClk.Copy(),
	}
	return &newCRDT
}

func (crdt *SetWDateCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *SetWDateCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *SetWDateCrdt) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case NoEffect:
		return
	case IncTsSetWEffect:
		crdt.inc -= typedEffect.Inc //Undo the inc.
	case SetTsSetWEffect:
		crdt.setClk = typedEffect.OldClk
		crdt.setValue = typedEffect.OldValue
		crdt.setTs = typedEffect.OldTs
		crdt.setReplicaID = typedEffect.OldReplicaID
	}
}

func (crdt *SetWDateCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf functions - most are already defined in simpleDateCrdt. Only need to define downstream and ProtoState.

func (downOp DownstreamIncTsSetW) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downProto := protobuf.GetSetWDateOp().GetSetWInc()
	return DownstreamIncTsSetW{Inc: downProto.GetInc(), Vc: clocksi.ClockSiTimestamp{}.FromBytes(downProto.GetVc())}
}

func (downOp DownstreamIncTsSetW) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{SetWDateOp: &proto.ProtoSetWDateDownstream{SetWInc: &proto.ProtoSetWIncDownstream{Inc: &downOp.Inc, Vc: downOp.Vc.ToBytes()}}}
}

func (downOp DownstreamSetTsSetW) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downProto := protobuf.GetSetWDateOp().GetSetWSet()
	return DownstreamSetTsSetW{Value: downProto.GetValue(), Vc: clocksi.ClockSiTimestamp{}.FromBytes(downProto.GetVc()), ReplicaID: int16(downProto.GetReplicaID())}
}

func (downOp DownstreamSetTsSetW) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{SetWDateOp: &proto.ProtoSetWDateDownstream{SetWSet: &proto.ProtoSetWSetDownstream{Value: &downOp.Value, Vc: downOp.Vc.ToBytes(), ReplicaID: pb.Int32(int32(downOp.ReplicaID))}}}
}

func (crdt *SetWDateCrdt) ToProtoState() (state *proto.ProtoState) {
	return &proto.ProtoState{SetWDate: &proto.ProtoSetWDateState{
		SetClk: crdt.setClk.ToBytes(), SetValue: pb.Int64(crdt.setValue), SetTs: pb.Int64(crdt.setTs),
		SetReplicaID: pb.Int32(int32(crdt.setReplicaID)), Inc: pb.Int64(crdt.inc), CurrClk: crdt.currClk.ToBytes()}}
}

func (crdt *SetWDateCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (sameCRDT *SetWDateCrdt) {
	protoState := proto.GetSetWDate()
	crdt.setClk, crdt.setValue, crdt.setTs = clocksi.ClockSiTimestamp{}.FromBytes(protoState.GetSetClk()), protoState.GetSetValue(), protoState.GetSetTs()
	crdt.setReplicaID, crdt.localReplicaID, crdt.inc = int16(protoState.GetSetReplicaID()), replicaID, protoState.GetInc()
	crdt.currClk = clocksi.ClockSiTimestamp{}.FromBytes(protoState.GetCurrClk())
	return crdt
}

func (crdt *SetWDateCrdt) GetCRDT() CRDT { return crdt }
