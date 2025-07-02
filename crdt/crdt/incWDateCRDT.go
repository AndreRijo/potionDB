package crdt

import (
	"math"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"

	"github.com/AndreRijo/go-tools/src/tools"
	pb "google.golang.org/protobuf/proto"
)

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
	switch args.(type) {
	case DateFullIncWArguments:
		return crdt.getFullDate(ms)
	case DateOnlyIncWArguments:
		return crdt.getDateOnly(ms)
	case TimeIncWArguments:
		return crdt.getTimeOnly(ms)
	case TimestampIncWArguments:
		return crdt.getTimestamp(ms)
	}
	return
}

func (crdt *IncWDateCrdt) getFullDate(updatedMs int64) (state State) {
	year, month, day := TsToGregorian(updatedMs)
	hour, min, sec, ms := ExtractHourMinSecMS(updatedMs)
	return DateFullState{Year: year, Month: month, Day: day, Hour: hour, Minute: min, Second: sec, Millisecond: ms}
}

func (crdt *IncWDateCrdt) getDateOnly(updatedMs int64) (state State) {
	year, month, day := TsToGregorian(updatedMs)
	return DateOnlyState{Year: year, Month: month, Day: day}
}

func (crdt *IncWDateCrdt) getTimeOnly(updatedMs int64) (state State) {
	hour, min, sec, ms := ExtractHourMinSecMS(updatedMs)
	return TimeState{Hour: hour, Minute: min, Second: sec, Millisecond: ms}
}

func (crdt *IncWDateCrdt) getTimestamp(updatedMs int64) (state State) {
	return TimestampState(updatedMs)
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
		case SetDate, SetDateOnly, SetDateFull: //We want the direct difference, and issue an increment representing it
			return DownstreamSetTsIncW{Value: ms, Vc: nextVc, ReplicaID: crdt.localReplicaID, IncsSeen: tools.MapCopy(crdt.incs)}
		case IncDate, IncMS:
			return DownstreamIncTsIncW{Inc: ms, Vc: nextVc, ReplicaID: crdt.localReplicaID}
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
	downProto := protobuf.GetSetWDateOp().GetSetWInc()
	return DownstreamIncTsIncW{Inc: downProto.GetInc(), Vc: clocksi.ClockSiTimestamp{}.FromBytes(downProto.GetVc())}
}

func (downOp DownstreamIncTsIncW) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{IncWDateOp: &proto.ProtoIncWDateDownstream{IncWInc: &proto.ProtoIncWIncDownstream{Inc: &downOp.Inc, ReplicaID: pb.Int32(int32(downOp.ReplicaID))}}}
}

func (downOp DownstreamSetTsIncW) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	downProto := protobuf.GetIncWDateOp().GetIncWSet()
	return DownstreamSetTsIncW{Value: downProto.GetValue(), Vc: clocksi.ClockSiTimestamp{}.FromBytes(downProto.GetVc()), ReplicaID: int16(downProto.GetReplicaID())}
}

func (downOp DownstreamSetTsIncW) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{IncWDateOp: &proto.ProtoIncWDateDownstream{IncWSet: &proto.ProtoIncWSetDownstream{Value: &downOp.Value, Vc: downOp.Vc.ToBytes(), ReplicaID: pb.Int32(int32(downOp.ReplicaID))}}}
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
