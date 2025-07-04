package crdt

import (
	"math"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"

	"github.com/AndreRijo/go-tools/src/tools"
	pb "google.golang.org/protobuf/proto"
)

/*
For future consideration: regarding concurrency on set (applies to incW and setW)
At the moment, we consider sets as setting the "whole date". If the user issues a partial set (e.g., only hh:mm:ss), we take in the current date for the unset fields (dd:mm:yyyy)
This is the simplest semantics to implement, however, questionably it may be undesirable. E.g., if we change only hh:mm:ss, we may be okay with concurrent sets changing the dd:mm:yyyy
Alternative semantics:
Consider collision based on each field: yyyy:mm:dd, hh:mm:ss:ms.
Sets that change different fields (e.g., year and day) commute.
If a user does not want his set to commute with others, he can set the other fields to the current value.
E.g.: a user wanting to change hh:mm:ss, but wants to keep current day and month, must issue a SetDate that sets month, day, hh:mm:ss.
This gives the best of both worlds.
Problems:
How to represent concurrency? Now we may have multiple sets that are relevant.
	- Maybe a bitset and assume replicaIDs are sorted and have a very small range (64? Could extend as needed). Could be very add to update when new replicas are added.
	- (...)
	- How to represent setValue itself? Set only relevant fields? But how to correctly identify that + how to deal with casually previous sets, that now should not lead to conflicts anymore?
		- Well, maybe clk can help with this but... not sure.
How to maintain proper set-wins semantics? E.g., if we set only the day, increasing hours can lead to day increases, thus ruining the intended semantics.
So, probably better to abandon this idea. Or alternatively, have to ensure that a set makes all concurrent increments of his field or lower fields be ignored
(E.g: setting a day makes concurrent increments of any of dd:hh:mm:ss:ms ignored/conflict.)
It would still allow a concurrent set to a smaller field to work through.
So yeah. For now, keep it simple. It's ok.
*/

// To read: Start with setValue, add all incs and then minus all cancels.
type IncWDateCrdt struct {
	CRDTVM
	setClk         clocksi.Timestamp //the clk of the winning set operation.
	setValue       int64             //(winning) value set by a set operation.
	setTs          int64             //the ts of the winning set operation.
	setReplicaID   int16             //the replicaID of the winning set operation.
	localReplicaID int16
	incs           map[int16]int64   //all incs + decs applied to this CRDT that have not been "reset" yet by a set.
	cancels        map[int16]int64   //all incs + decs that have been cancelled by a set operation.
	cancelsTs      map[int16]int64   //the ts of the cancel operation for each replicaID.
	currClk        clocksi.Timestamp //Keeps track of the highest clock seen, to know which clock to generate in Update.
}

type DateFullIncWArguments struct{ DateFullArguments }
type DateOnlyIncWArguments struct{ DateOnlyArguments }
type TimeIncWArguments struct{ TimeArguments }
type TimestampIncWArguments struct{ TimestampArguments }

//Updates supported:
//SetDateFull, SetDate, SetDateOnly, SetTime, IncDate, IncMS, SetMS

type DownstreamSetTsIncW struct {
	Value     int64
	Vc        clocksi.Timestamp //Vc of the replica at the time of execution. Already includes the updated ts for replicaID
	ReplicaID int16             //ReplicaID of the replica that issued the set operation. Used to distinguish concurrent sets with the same ts.
	IncsSeen  map[int16]int64   //Incs that have been seen (and thus will be cancelled) by the replica at the time of the set operation.
}

type DownstreamIncTsIncW struct {
	Inc       int64
	ReplicaID int16             //ReplicaID of the replica that issued the inc operation. Needed to know which map to update.
	Vc        clocksi.Timestamp //Vc of the replica at the time of execution.
}

type IncTsIncWEffect struct {
	Inc       int64 //Value that was incremented; to undo, subtract this.
	ReplicaID int16
}

type SetTsIncWEffect struct {
	OldClk          clocksi.Timestamp
	OldValue        int64
	OldTs           int64
	OldReplicaID    int16
	ChangedCancels  map[int16]int64 //Old values of cancels that were changed by the set operation.
	ChangedCancelTs map[int16]int64 //Old (ts) values of cancelTs that were changed by the set operation.
}

type LoseSetTsIncWEffect struct { //A concurrent set that lost, but that still updates cancels
	ChangedCancels  map[int16]int64 //Old values of cancels that were changed by the set operation.
	ChangedCancelTs map[int16]int64 //Old (ts) values of cancelTs that were changed by the set operation.
}

/*
type DownstreamSetTsIncW struct {
	Value     int64
	Vc        clocksi.Timestamp //Vc of the replica at the time of execution.
	Ts        int64             //Ts associated to realtime clock. For concurrent sets, the set with the highest ts wins.
	ReplicaID int16             //ReplicaID of the replica that issued the set operation. Used to distinguish concurrent sets with the same ts.
	IncsSeen  map[int16]int64   //Incs that have been seen (and thus will be cancelled) by the replica at the time of the set operation.
}

type DownstreamIncTsIncW struct {
	Inc       int64
	ReplicaID int16             //ReplicaID of the replica that issued the inc operation. Needed to know which map to update.
	Vc        clocksi.Timestamp //Vc of the replica at the time of execution.
}*/

// States and ops are the same from SimpleDateCRDT; queries are embedded from SimpleDateCRDT.

func (crdt *IncWDateCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_SETW_DATE }

// Downstreams
func (args DownstreamSetTsIncW) GetCRDTType() proto.CRDTType { return proto.CRDTType_INCW_DATE }
func (args DownstreamIncTsIncW) GetCRDTType() proto.CRDTType { return proto.CRDTType_INCW_DATE }
func (args DownstreamSetTsIncW) MustReplicate() bool         { return true }
func (args DownstreamIncTsIncW) MustReplicate() bool         { return true }

//TODO: Initialization must set a clk that is for sure <= than any possible clk. Use minimumTs

func (crdt *IncWDateCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	nReplicas := len(clocksi.GetKeys())
	return &IncWDateCrdt{
		CRDTVM: (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete),
		setClk: clocksi.MinimumTs, setValue: GregorianToTs(1, 1, 1), setTs: math.MinInt64, setReplicaID: math.MaxInt16,
		localReplicaID: replicaID, incs: make(map[int16]int64, nReplicas), cancels: make(map[int16]int64, nReplicas),
		cancelsTs: make(map[int16]int64, nReplicas), currClk: (*startTs).Copy(),
	}
}

func (crdt *IncWDateCrdt) IsBigCRDT() bool { return false }

func (crdt *IncWDateCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	ms := crdt.setValue
	for _, replMs := range crdt.incs {
		ms += replMs
	}
	if updsNotYetApplied != nil && len(updsNotYetApplied) > 0 {
		ms = crdt.getTsWithUpdsNotYetApplied(ms, updsNotYetApplied)
	}
	return dateReadHelper(args, ms)
}

func (crdt *IncWDateCrdt) getTsWithUpdsNotYetApplied(baseMs int64, updsNotYetApplied []UpdateArguments) (updMs int64) {
	updMs = baseMs //Already includes the sum of incs.
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

func (crdt *IncWDateCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	if dateUpd, ok := args.(DateUpd); ok {
		ms, nextVc := dateUpd.ToMS(), crdt.currClk.NextTimestamp(crdt.localReplicaID)
		switch args.(type) {
		case SetDate, SetDateOnly, SetDateFull, SetMS:
			return DownstreamSetTsIncW{Value: ms, Vc: nextVc, ReplicaID: crdt.localReplicaID, IncsSeen: tools.MapCopy(crdt.incs)}
		case IncDate, IncMS:
			return DownstreamIncTsIncW{Inc: ms, Vc: nextVc, ReplicaID: crdt.localReplicaID}
		case SetTime: //Special case: Find the current year, month, day and maintain those. Change only the hour, min and day.
			currMsDay := HourMinSecToMs(ExtractHourMinSec(ms))
			return DownstreamSetTsIncW{Value: ms + currMsDay, Vc: nextVc, ReplicaID: crdt.localReplicaID}
		}
	}
	return
}

func (crdt *IncWDateCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	crdt.addToHistory(&updTs, &downstreamArgs, crdt.applyDownstream(downstreamArgs))
	return nil
}

func (crdt *IncWDateCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	switch typedArgs := downstreamArgs.(type) {
	case DownstreamIncTsIncW:
		effect = crdt.applyIncTs(typedArgs.Inc, typedArgs.ReplicaID, typedArgs.Vc)
	case DownstreamSetTsIncW:
		effect = crdt.applySetTs(typedArgs.Value, typedArgs.ReplicaID, typedArgs.Vc, typedArgs.IncsSeen)
	}
	return
}

func (crdt *IncWDateCrdt) applyIncTs(opInc int64, opReplicaID int16, opVc clocksi.Timestamp) (effect *Effect) {
	//Here incs always apply. There is casual delivery, so a set that cancels this inc will have to happen after this inc (and thus will be applied after)
	crdt.incs[opReplicaID] += opInc
	var effectValue Effect = IncTsIncWEffect{Inc: opInc, ReplicaID: opReplicaID}
	return &effectValue
}

func (crdt *IncWDateCrdt) applySetTs(opSet int64, opReplicaID int16, opVc clocksi.Timestamp, incsSeen map[int16]int64) (effect *Effect) {
	compResult := opVc.Compare(crdt.setClk)
	var effectValue Effect

	//Always update cancels if the corresponding ts is higher, even if this set will end up losing.
	changedCancels, changedCancelsTs := make(map[int16]int64), make(map[int16]int64)
	for replicaID, inc := range incsSeen {
		if currTs := opVc.GetPos(replicaID); currTs >= crdt.cancelsTs[replicaID] { //This cancel is newer than the one we have, so we update it.
			changedCancels[replicaID] = crdt.cancels[replicaID] //Copying for later to use with undo.
			changedCancelsTs[replicaID] = crdt.cancelsTs[replicaID]
			crdt.cancels[replicaID] = inc
			crdt.cancelsTs[replicaID] = currTs
		} //Nothing to do on else - we ignore.
	}

	if compResult == clocksi.HigherTs || compResult == clocksi.EqualTs || (compResult == clocksi.ConcurrentTs && opReplicaID <= crdt.setReplicaID) {
		//Can apply set.
		effectValue = SetTsIncWEffect{
			OldClk: crdt.setClk, OldValue: crdt.setValue, OldTs: crdt.setTs, OldReplicaID: crdt.setReplicaID,
			ChangedCancels: changedCancels, ChangedCancelTs: changedCancelsTs,
		}
		crdt.setClk, crdt.setValue, crdt.setTs, crdt.setReplicaID = opVc, opSet, opVc.GetPos(opReplicaID), opReplicaID
	} else {
		effectValue = LoseSetTsIncWEffect{ChangedCancels: changedCancels, ChangedCancelTs: changedCancelsTs}
	}
	return &effectValue
}

func (crdt *IncWDateCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *IncWDateCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := IncWDateCrdt{
		CRDTVM: crdt.CRDTVM.copy(), setClk: crdt.setClk.Copy(), setValue: crdt.setValue, setTs: crdt.setTs,
		setReplicaID: crdt.setReplicaID, localReplicaID: crdt.localReplicaID, incs: tools.MapCopy(crdt.incs),
		cancels: tools.MapCopy(crdt.cancels), cancelsTs: tools.MapCopy(crdt.cancelsTs), currClk: crdt.currClk.Copy(),
	}
	return &newCRDT
}

func (crdt *IncWDateCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *IncWDateCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *IncWDateCrdt) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case NoEffect:
		return
	case IncTsIncWEffect:
		crdt.incs[typedEffect.ReplicaID] -= typedEffect.Inc //Undo the inc.
	case SetTsIncWEffect:
		crdt.setClk = typedEffect.OldClk
		crdt.setValue = typedEffect.OldValue
		crdt.setTs = typedEffect.OldTs
		crdt.setReplicaID = typedEffect.OldReplicaID
		for replicaID, cancelV := range typedEffect.ChangedCancels {
			crdt.cancels[replicaID] = cancelV
			crdt.cancelsTs[replicaID] = typedEffect.ChangedCancelTs[replicaID]
		}
	case LoseSetTsIncWEffect:
		for replicaID, cancelV := range typedEffect.ChangedCancels {
			crdt.cancels[replicaID] = cancelV
			crdt.cancelsTs[replicaID] = typedEffect.ChangedCancelTs[replicaID]
		}
	}
}

func (crdt *IncWDateCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf functions - most are already defined in simpleDateCrdt. Only need to define downstream and ProtoState.

func (downOp DownstreamIncTsIncW) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downProto := protobuf.GetIncWDateOp().GetIncWInc()
	return DownstreamIncTsIncW{Inc: downProto.GetInc(), ReplicaID: int16(downProto.GetReplicaID()), Vc: clocksi.ClockSiTimestamp{}.FromBytes(downProto.GetVc())}
}

func (downOp DownstreamIncTsIncW) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{IncWDateOp: &proto.ProtoIncWDateDownstream{IncWInc: &proto.ProtoIncWIncDownstream{Inc: &downOp.Inc, ReplicaID: pb.Int32(int32(downOp.ReplicaID)), Vc: downOp.Vc.ToBytes()}}}
}

func (downOp DownstreamSetTsIncW) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downProto := protobuf.GetIncWDateOp().GetIncWSet()
	incsSeen, protoIncsSeen := make(map[int16]int64, len(downOp.IncsSeen)), downProto.GetSeenIncs()
	for replicaID, inc := range protoIncsSeen {
		incsSeen[int16(replicaID)] = inc
	}
	return DownstreamSetTsIncW{Value: downProto.GetValue(), Vc: clocksi.ClockSiTimestamp{}.FromBytes(downProto.GetVc()), ReplicaID: int16(downProto.GetReplicaID()), IncsSeen: incsSeen}
}

func (downOp DownstreamSetTsIncW) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	protoIncsSeen := make(map[int32]int64, len(downOp.IncsSeen))
	for replicaID, inc := range downOp.IncsSeen {
		protoIncsSeen[int32(replicaID)] = inc
	}
	return &proto.ProtoOpDownstream{IncWDateOp: &proto.ProtoIncWDateDownstream{IncWSet: &proto.ProtoIncWSetDownstream{Value: &downOp.Value, Vc: downOp.Vc.ToBytes(), ReplicaID: pb.Int32(int32(downOp.ReplicaID)), SeenIncs: protoIncsSeen}}}
}

func (crdt *IncWDateCrdt) ToProtoState() (state *proto.ProtoState) {
	protoIncs, protoCancels, protoCancelsTs := make(map[int32]int64, len(crdt.incs)), make(map[int32]int64, len(crdt.cancels)), make(map[int32]int64, len(crdt.cancelsTs))
	for replicaID, inc := range crdt.incs {
		protoIncs[int32(replicaID)] = inc
	}
	for replicaID, cancel := range crdt.cancels {
		protoCancels[int32(replicaID)] = cancel
		protoCancelsTs[int32(replicaID)] = crdt.cancelsTs[replicaID]
	}
	return &proto.ProtoState{IncWDate: &proto.ProtoIncWDateState{
		SetClk: crdt.setClk.ToBytes(), SetValue: pb.Int64(crdt.setValue), SetTs: pb.Int64(crdt.setTs),
		SetReplicaID: pb.Int32(int32(crdt.setReplicaID)), Incs: protoIncs, Cancels: protoCancels,
		CancelsTs: protoCancelsTs, CurrClk: crdt.currClk.ToBytes()}}
}

// TODO
func (crdt *IncWDateCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (sameCRDT *IncWDateCrdt) {
	protoState := proto.GetIncWDate()
	crdt.setClk, crdt.setValue, crdt.setTs = clocksi.ClockSiTimestamp{}.FromBytes(protoState.GetSetClk()), protoState.GetSetValue(), protoState.GetSetTs()
	crdt.setReplicaID, crdt.localReplicaID, crdt.currClk = int16(protoState.GetSetReplicaID()), replicaID, clocksi.ClockSiTimestamp{}.FromBytes(protoState.GetCurrClk())
	incsProto, cancelsProto, cancelsTs := protoState.GetIncs(), protoState.GetCancels(), protoState.GetCancelsTs()
	crdt.incs, crdt.cancels, crdt.cancelsTs = make(map[int16]int64, len(incsProto)), make(map[int16]int64, len(cancelsProto)), make(map[int16]int64, len(cancelsTs))
	for replicaID, inc := range incsProto {
		crdt.incs[int16(replicaID)] = inc
	}
	for replicaID, cancel := range cancelsProto {
		crdt.cancels[int16(replicaID)] = cancel
		crdt.cancelsTs[int16(replicaID)] = cancelsTs[replicaID]
	}
	return crdt
}

func (crdt *IncWDateCrdt) GetCRDT() CRDT { return crdt }
