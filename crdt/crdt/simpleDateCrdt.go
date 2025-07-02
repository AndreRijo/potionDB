package crdt

import (
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"

	pb "google.golang.org/protobuf/proto"
)

const msPerDay = 86400000 // Milliseconds in a day

type SimpleDateCrdt struct {
	CRDTVM
	dateTs int64 //ms. 0 value is Julian day 0, which is 24th November 4714 BC in the Gregorian calendar.
}

type DateState struct {
	Year   int16
	Month  int8
	Day    int8
	Hour   int8
	Minute int8
	Second int8
}

type DateFullState struct { //Note: This does not fit into a 64-bit integer
	Millisecond int16
	Year        int16
	Month       int8
	Day         int8
	Hour        int8
	Minute      int8
	Second      int8
}

type TimeState struct {
	Millisecond int16
	Hour        int8
	Minute      int8
	Second      int8
}

type DateOnlyState struct {
	Year  int16
	Month int8
	Day   int8
}

type TimestampState uint64 // Represents the internal ms. 0 value is Julian day 0, which is 24th November 4714 BC in the Gregorian calendar.

// Note to self: protobufs can share the read between different CRDTs (as the read of embMaps has the CRDT type), but potionDB can't (only key).
type DateFullArguments struct{}
type DateOnlyArguments struct{}
type TimeArguments struct{}
type TimestampArguments struct{} //Returns the internal ms. 0 value is Julian day 0, which is 24th November 4714 BC in the Gregorian calendar.

type SetDate struct {
	Year   int16
	Month  int8
	Day    int8
	Hour   int8
	Minute int8
	Second int8
}

type SetDateFull struct {
	Millisecond int16
	Year        int16
	Month       int8
	Day         int8
	Hour        int8
	Minute      int8
	Second      int8
}

type SetDateOnly struct {
	Year  int16
	Month int8
	Day   int8
}

type SetTime struct {
	Millisecond int16
	Hour        int8
	Minute      int8
	Second      int8
}

type SetMS int64

type IncDate struct { //Supports receiving negative numbers
	Year   int16
	Month  int8
	Day    int8
	Hour   int8
	Minute int8
	Second int8
}

type IncMS int64 //Use negative numbers to decrement

type SetInitialDate struct {
	Year         int16
	Month        int8
	Day          int8
	Hour         int8
	Minute       int8
	Second       int8
	Milliseconds int16
}

type DateUpd interface {
	ToMS() int64
}

// Note: the effects are the downstreams themselves
type IncMSEffect DownstreamIncMS
type SetInitialDateEffect DownstreamSetInitialDate

/*******************IMPORTANT INFO******************/
/*******************IMPORTANT INFO******************/
/*******************IMPORTANT INFO******************/
/*******************IMPORTANT INFO******************/
/*******************IMPORTANT INFO******************/

type DownstreamIncMS int64          //Both sets and incs will use this. (remember: sets convert down to incs)
type DownstreamSetInitialDate int64 //IMPORTANT NOTE: This is like an initializer. We assume it is only generated once, and only by one replica.
//Implementation of the above is straightforward: set well, sets. Increments well, increments. Effects are also straightforward.

/****************END OF IMPORTANT INFO***************/
/****************END OF IMPORTANT INFO***************/
/****************END OF IMPORTANT INFO***************/
/****************END OF IMPORTANT INFO***************/
/****************END OF IMPORTANT INFO***************/

//Note: no point in having downstreams of SetDateOnly, SetTime, etc. They will still conflict with any inc.
//Example: at first, a SetDateOnly seems like it will not collide with an increment of 5 hours.
//However, incrementing 5h when the current hour is 20 increases the day, thus colliding with SetDate.
//And we only know of this collision on downstream.
//So, better just assume every set collides with every increment, and deal with the collision accordingly
//How to deal with the collision? What is the intended semantics?

//Collision: set vs inc:
//Option 1: any increment/decrement concurrent to a set is ignored. (i.e., as if inc/dec serializes before set) (makes sense actually, a set is kinda like a initialize/reset)
//Option 2: increments/decrements must serialize after the incs/decs
//Option 3: a set is actually a offset from the current date (on prepare), thus eliminating all conflicts with incs/decs.
//However, this bring unitended semantics for set vs set. E.g., assume day is 10 and two concurrent sets it to 20. The final result would be 30 :(
//So, can't use this sadly.
//No further options.

//Collision: set vs set:
//Option 1: lww.
//Option 2: ??? (mw is weird for this kind of object, as sets are expected to be rare, or to be executed without concurrent operations.)

//Conclusion:
//For Option1 of set vs set, a timestamp is enough. However, set vs inc requires a full vc.
//It also has undesirable costs, as we must keep information of what incs/decs were applied, so that a set happens correctly.
//I.e., we would need to keep all incs/decs, with their clocks. Then, depending if option 1 or 2, apply/remove concurrent incs/decs.
//Unless... what if we use updTs from Downstream? It doesn't solve the problem as then we still don't know the clk of the incs/decs.
//But we can still find this out if the look into the history... that would mean this CRDT wouldn't work if history is disabled.
//Tempting to not fully support the set.

//So... options:
//Define a special "initializer operation", which serializes before ALL incs/decs. Literally all.
//Even with this, I will still need at the very least a replicaID to deal with concurrent initializers.
//Or, assume that only one replica will issue a initializer, and only one.
//Then, provide set as a "convenience tool", but transforming it to incs: thus, with the weird set vs set semantic.

//[IMPORTANT!!!]Actual solution:
//3 CRDTs!!!:
//1st one:
//----"efficiency date CRDT" (simpleDateCrdt): has an initializer and some convenience sets.
//----Only one initializer can ever be issued (this is not verified though!) during the whole lifespan of the CRDT.
//----Sets are transformed to incs, so set vs set semantic is bad. (e.g: two concurrents sets to day 20, where initially it was day 10, will end up as day 30.)
//----IMPORTANT NOTE: While very efficient, the concurrency semantics of sets may be undesirable. Consider one of the other two Date CRDTs if set concurrency is important.
//2nd one:
//----"set-wins CRDT" (SetWDateCrdt): has sets and incs, sets win over concurrent incs/decs. Sets are LWW between themselves
//----Has no concurrency anomalies. State size is reasonable, but every operation has to carry a vc + inc (or set) value.
//3rd one:
//----"inc-wins CRDT" (IncWDateCrdt): has sets and incs, incs win over concurrent sets. Sets are LWW between themselves
//----Has no concurrency anomalies. State size is big though (size of SetWDateCrdt + 3x maps, one for incs, one for cancels and one for ts of cancels).
//----Set operations are also bigger, as they must carry vc, set and incs seen. But that should be ok as sets should be rare.
//----Incs are also a bit bigger: they carry the replicaID of who issued the operation.

//Idea for 3rd solution:
//--------Having incs/decs wins is non trivial, as we would need a way to keep track of which concurrent and non-concurrent incs were applied.
//--------A basic idea would be for a set to carry what "incs" it has seen, and remove those. But how to do this and still be compatible with concurrent sets?
//--------Idea:
//-------------State: inc per replicaID, "cancelled" per replicaID, winning set value (only 1)
//-------------Each set carries the vc and the set of incs it has seen.
//-------------It updates each cancelled entry with the incs it has seen, for all positions whose entry in vc is >=.
//-------------This correctly handles the situation where a set sees a more recent state for a given replica, but that inc is lower due to a decrement.
//-------------To ensure convergence, even sets that lose should still update the cancelled entries.
//-------------This works correctly.
//-------------Perfect! *Throws party*

/*
So, what do we want?
Our dateTs should have millisecond precision.
We want to ideally use very old years too.
There is however a problem with leap years.
So how to find the right 0 value?
The easiest seems to be to use the Julian day 0 as the 0 day.
Then we convert the timestamp to Julian calendar, and then apply the fix to gregorian.
*/

/*const msPerDay = 86400000 // Milliseconds in a day

// Converts a Gregorian calendar date to milliseconds since Julian epoch (-4713-11-24)
func GregorianToTimestamp(year, month, day int) uint64 {
	// If month is January or February, treat them as 13th and 14th of previous year
	if month <= 2 {
		year -= 1
		month += 12
	}

	a := year / 100
	b := 2 - a + a/4

	jdn := (36525*(year+4716))/100 +
		(306*(month+1))/10 +
		day + b - 1524

	// JD 0 is -4713-11-24, so JDN 0 corresponds to timestamp 0
	return uint64(jdn) * msPerDay
}

// Converts milliseconds since Julian epoch to Gregorian year, month, day
func TimestampToGregorian(timestamp uint64) (year, month, day int) {
	days := int(timestamp / msPerDay)

	// Now convert Julian Day Number (JDN) to Gregorian date
	l := days + 68569 + 1721426 // JD of 0001-01-01 is 1721426
	n := (4 * l) / 146097
	l = l - (146097*n + 3) / 4
	i := (4000 * (l + 1)) / 1461001
	l = l - (1461*i)/4 + 31
	j := (80 * l) / 2447
	day = l - (2447*j)/80
	l = j / 11
	month = j + 2 - 12*l
	year = 100*(n-49) + i + l

	return
}*/

//Alternative idea for date representation:
//With uint vars - sec: 8b, min: 8b, hour: 8b, day: 8b, month: 8b, year: 16b. total: 56b. Can't represent ms.
//With uint64 - sec: 6b, min: 6b. hour: 5b, day: 5b, month: 4b, year: 16b, ms: 10b. Total: 52b.
//Can definitely fit in 64 bits. We could even have year use 28b, giving plenty of space for negative years.
//However, would be annoying to consider for the years with 366 days.

//Note: despite dateTs being an int64, it is only intended to be used with non-negative values
//The reason to use int64 is to support decrements on an inc operation, by using a negative value.
//No guarantees are made regarding dates originated from negative dateTs.

func (crdt *SimpleDateCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_SIMPLE_DATE }

// Ops
func (args SetDate) GetCRDTType() proto.CRDTType        { return proto.CRDTType_SIMPLE_DATE }
func (args SetDateFull) GetCRDTType() proto.CRDTType    { return proto.CRDTType_SIMPLE_DATE }
func (args SetDateOnly) GetCRDTType() proto.CRDTType    { return proto.CRDTType_SIMPLE_DATE }
func (args SetTime) GetCRDTType() proto.CRDTType        { return proto.CRDTType_SIMPLE_DATE }
func (args SetMS) GetCRDTType() proto.CRDTType          { return proto.CRDTType_SIMPLE_DATE }
func (args IncDate) GetCRDTType() proto.CRDTType        { return proto.CRDTType_SIMPLE_DATE }
func (args IncMS) GetCRDTType() proto.CRDTType          { return proto.CRDTType_SIMPLE_DATE }
func (args SetInitialDate) GetCRDTType() proto.CRDTType { return proto.CRDTType_SIMPLE_DATE }

// Downstreams
func (args DownstreamIncMS) GetCRDTType() proto.CRDTType          { return proto.CRDTType_SIMPLE_DATE }
func (args DownstreamSetInitialDate) GetCRDTType() proto.CRDTType { return proto.CRDTType_SIMPLE_DATE }
func (args DownstreamIncMS) MustReplicate() bool                  { return true }
func (args DownstreamSetInitialDate) MustReplicate() bool         { return true }

// States
func (state DateState) GetCRDTType() proto.CRDTType      { return proto.CRDTType_SIMPLE_DATE }
func (state DateState) GetREADType() proto.READType      { return proto.READType_FULL }
func (state DateFullState) GetCRDTType() proto.CRDTType  { return proto.CRDTType_SIMPLE_DATE }
func (state DateFullState) GetREADType() proto.READType  { return proto.READType_FULL }
func (state DateOnlyState) GetCRDTType() proto.CRDTType  { return proto.CRDTType_SIMPLE_DATE }
func (state DateOnlyState) GetREADType() proto.READType  { return proto.READType_FULL }
func (state TimeState) GetCRDTType() proto.CRDTType      { return proto.CRDTType_SIMPLE_DATE }
func (state TimeState) GetREADType() proto.READType      { return proto.READType_FULL }
func (state TimestampState) GetCRDTType() proto.CRDTType { return proto.CRDTType_SIMPLE_DATE }
func (state TimestampState) GetREADType() proto.READType { return proto.READType_DATE_TIMESTAMP }

// Queries
func (args DateFullArguments) GetCRDTType() proto.CRDTType  { return proto.CRDTType_SIMPLE_DATE }
func (args DateFullArguments) GetREADType() proto.READType  { return proto.READType_DATE_FULL }
func (args DateFullArguments) HasInnerReads() bool          { return false }
func (args DateFullArguments) HasVariables() bool           { return false }
func (args DateOnlyArguments) GetCRDTType() proto.CRDTType  { return proto.CRDTType_SIMPLE_DATE }
func (args DateOnlyArguments) GetREADType() proto.READType  { return proto.READType_DATE_ONLY }
func (args DateOnlyArguments) HasInnerReads() bool          { return false }
func (args DateOnlyArguments) HasVariables() bool           { return false }
func (args TimeArguments) GetCRDTType() proto.CRDTType      { return proto.CRDTType_SIMPLE_DATE }
func (args TimeArguments) GetREADType() proto.READType      { return proto.READType_DATE_TIME_ONLY }
func (args TimeArguments) HasInnerReads() bool              { return false }
func (args TimeArguments) HasVariables() bool               { return false }
func (args TimestampArguments) GetCRDTType() proto.CRDTType { return proto.CRDTType_SIMPLE_DATE }
func (args TimestampArguments) GetREADType() proto.READType { return proto.READType_DATE_TIMESTAMP }
func (args TimestampArguments) HasInnerReads() bool         { return false }
func (args TimestampArguments) HasVariables() bool          { return false }

func (args SetDate) ToMS() int64 {
	return HourMinSecToMs(args.Hour, args.Minute, args.Second) + GregorianToTs(int(args.Year), int(args.Month), int(args.Day))
}
func (args SetDateFull) ToMS() int64 {
	return HourMinSecToMs(args.Hour, args.Minute, args.Second) + GregorianToTs(int(args.Year), int(args.Month), int(args.Day)) + int64(args.Millisecond)
}
func (args SetDateOnly) ToMS() int64 {
	return GregorianToTs(int(args.Year), int(args.Month), int(args.Day))
}
func (args SetTime) ToMS() int64 {
	return HourMinSecToMs(args.Hour, args.Minute, args.Second) + int64(args.Millisecond)
}
func (args IncDate) ToMS() int64 {
	return HourMinSecToMs(args.Hour, args.Minute, args.Second) + GregorianToTs(int(args.Year), int(args.Month), int(args.Day))
}
func (args IncMS) ToMS() int64 { return int64(args) }
func (args SetInitialDate) ToMS() int64 {
	return HourMinSecToMs(args.Hour, args.Minute, args.Second) + GregorianToTs(int(args.Year), int(args.Month), int(args.Day)) + int64(args.Milliseconds)
}

func (crdt *SimpleDateCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return &SimpleDateCrdt{
		CRDTVM: (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete),
		dateTs: GregorianToTs(1, 1, 1),
	}
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *SimpleDateCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *SimpleDateCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
	return crdt
}

func (crdt *SimpleDateCrdt) IsBigCRDT() bool { return false }

func (crdt *SimpleDateCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	ms := crdt.dateTs
	if updsNotYetApplied != nil && len(updsNotYetApplied) > 0 {
		ms = crdt.getTsWithUpdsNotYetApplied(updsNotYetApplied)
	}
	switch args.(type) {
	case DateFullArguments:
		return crdt.getFullDate(ms)
	case DateOnlyArguments:
		return crdt.getDateOnly(ms)
	case TimeArguments:
		return crdt.getTimeOnly(ms)
	case TimestampArguments:
		return crdt.getTimestamp(ms)
	}
	return
}

func (crdt *SimpleDateCrdt) getFullDate(updatedMs int64) (state State) {
	year, month, day := TsToGregorian(updatedMs)
	hour, min, sec, ms := ExtractHourMinSecMS(updatedMs)
	return DateFullState{Year: year, Month: month, Day: day, Hour: hour, Minute: min, Second: sec, Millisecond: ms}
}

func (crdt *SimpleDateCrdt) getDateOnly(updatedMs int64) (state State) {
	year, month, day := TsToGregorian(updatedMs)
	return DateOnlyState{Year: year, Month: month, Day: day}
}

func (crdt *SimpleDateCrdt) getTimeOnly(updatedMs int64) (state State) {
	hour, min, sec, ms := ExtractHourMinSecMS(updatedMs)
	return TimeState{Hour: hour, Minute: min, Second: sec, Millisecond: ms}
}

func (crdt *SimpleDateCrdt) getTimestamp(updatedMs int64) (state State) {
	return TimestampState(updatedMs)
}

func (crdt *SimpleDateCrdt) getTsWithUpdsNotYetApplied(updsNotYetApplied []UpdateArguments) (updMs int64) {
	updMs = crdt.dateTs //Start with the current date. Then, add all updates.
	for _, upd := range updsNotYetApplied {
		ms := upd.(DateUpd).ToMS()
		switch upd.(type) {
		case SetDateFull, SetMS, SetInitialDate:
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

func (crdt *SimpleDateCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	if dateUpd, ok := args.(DateUpd); ok {
		ms := dateUpd.ToMS()
		switch args.(type) {
		case SetDate, SetDateOnly, SetDateFull: //We want the direct difference, and issue an increment representing it
			return DownstreamIncMS(ms - crdt.dateTs)
		case IncDate, IncMS:
			return DownstreamIncMS(ms)
		case SetTime: //Special case: Find the current hour, min, ss and ms and then do the difference.
			currH, currM, currS, currMS := (crdt.dateTs/3600000)%24, (crdt.dateTs/60000)%60, (crdt.dateTs/1000)%60, crdt.dateTs%1000
			currTimeMS := HourMinSecToMs(int8(currH), int8(currM), int8(currS)) + int64(currMS)
			return DownstreamIncMS(ms - currTimeMS)
		case SetInitialDate:
			return DownstreamSetInitialDate(ms)
		}
	}
	return
}

func (crdt *SimpleDateCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	crdt.addToHistory(&updTs, &downstreamArgs, crdt.applyDownstream(downstreamArgs))
	return nil
}

func (crdt *SimpleDateCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	switch typedArgs := downstreamArgs.(type) {
	case DownstreamIncMS:
		effect = crdt.applyInc(int64(typedArgs))
	case DownstreamSetInitialDate:
		effect = crdt.applyInitializer(int64(typedArgs))
	}
	return
}

func (crdt *SimpleDateCrdt) applyInc(opInc int64) (effect *Effect) {
	var effectValue Effect = IncMSEffect(opInc)
	crdt.dateTs += opInc
	return &effectValue
}

func (crdt *SimpleDateCrdt) applyInitializer(opSet int64) (effect *Effect) {
	var effectValue Effect = SetInitialDateEffect(opSet)
	crdt.dateTs = opSet //Can't inc due to the crdt's initialization setting the date to a non-zero value.
	return &effectValue
}

func (crdt *SimpleDateCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *SimpleDateCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := SimpleDateCrdt{
		CRDTVM: crdt.CRDTVM.copy(),
		dateTs: crdt.dateTs,
	}
	return &newCRDT
}

func (crdt *SimpleDateCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs) //Maybe could be simpler?
}

func (crdt *SimpleDateCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs) //Maybe could be simpler?
}

func (crdt *SimpleDateCrdt) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case DownstreamIncMS:
		crdt.dateTs -= int64(typedEffect) //Undo the inc.
	case DownstreamSetInitialDate:
		crdt.dateTs = int64(typedEffect)
	}
}

func (crdt *SimpleDateCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

func HourMinSecToMs(hour, minute, second int8) int64 {
	return int64(second)*1000 + int64(minute)*60000 + int64(hour)*3600000
}

func ExtractHourMinSec(totalMs int64) (hour, min, sec int8) {
	return int8(totalMs / 3600000 % 24), int8(totalMs / 60000 % 60), int8(totalMs / 1000 % 60)
}

func ExtractHourMinSecMS(totalMs int64) (hour, min, sec int8, ms int16) {
	return int8(totalMs / 3600000 % 24), int8(totalMs / 60000 % 60), int8(totalMs / 1000 % 60), int16(totalMs % 1000)
}

func GregorianToTs(year, month, day int) int64 {
	// If month is January or February, treat them as 13th and 14th of previous year
	if month <= 2 {
		year -= 1
		month += 12
	}

	a := year / 100
	b := 2 - a + a/4

	jdn := (36525*(year+4716))/100 +
		(306*(month+1))/10 +
		day + b - 1524

	// JD 0 is -4713-11-24, so JDN 0 corresponds to timestamp 0
	return int64(jdn) * msPerDay
}

// Converts milliseconds since Julian epoch to Gregorian year, month, day
func TsToGregorian(msTs int64) (year16 int16, month8, day8 int8) {
	days := int(msTs / msPerDay)

	// Now convert Julian Day Number (JDN) to Gregorian date
	l := days + 68569 + 1721426 // JD of 0001-01-01 is 1721426
	n := (4 * l) / 146097
	l = l - (146097*n+3)/4
	i := (4000 * (l + 1)) / 1461001
	l = l - (1461*i)/4 + 31
	j := (80 * l) / 2447
	day := l - (2447*j)/80
	l = j / 11
	month := j + 2 - 12*l
	year := 100*(n-49) + i + l

	return int16(year), int8(month), int8(day)
}

//Protobuf functions

func (crdtOp SetDate) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	dateProto := protobuf.GetDateop().GetSet()
	crdtOp.Year, crdtOp.Month, crdtOp.Day = int16(dateProto.GetYear()), int8(dateProto.GetMonth()), int8(dateProto.GetDay())
	crdtOp.Hour, crdtOp.Minute, crdtOp.Second = int8(dateProto.GetHour()), int8(dateProto.GetMinute()), int8(dateProto.GetSecond())
	return crdtOp
}

func (crdtOp SetDate) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Dateop: &proto.ApbDateUpdate{Set: &proto.ApbDateSet{
		Year: pb.Int32(int32(crdtOp.Year)), Month: pb.Int32(int32(crdtOp.Month)), Day: pb.Int32(int32(crdtOp.Day)),
		Hour: pb.Int32(int32(crdtOp.Hour)), Minute: pb.Int32(int32(crdtOp.Minute)), Second: pb.Int32(int32(crdtOp.Second)),
	}}}
}

func (crdtOp SetDateFull) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	dateProto := protobuf.GetDateop().GetSet()
	crdtOp.Year, crdtOp.Month, crdtOp.Day = int16(dateProto.GetYear()), int8(dateProto.GetMonth()), int8(dateProto.GetDay())
	crdtOp.Hour, crdtOp.Minute, crdtOp.Second, crdtOp.Millisecond = int8(dateProto.GetHour()), int8(dateProto.GetMinute()), int8(dateProto.GetSecond()), int16(dateProto.GetMillisecond())
	return crdtOp
}

func (crdtOp SetDateFull) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Dateop: &proto.ApbDateUpdate{Set: &proto.ApbDateSet{
		Year: pb.Int32(int32(crdtOp.Year)), Month: pb.Int32(int32(crdtOp.Month)), Day: pb.Int32(int32(crdtOp.Day)),
		Hour: pb.Int32(int32(crdtOp.Hour)), Minute: pb.Int32(int32(crdtOp.Minute)), Second: pb.Int32(int32(crdtOp.Second)), Millisecond: pb.Int32(int32(crdtOp.Millisecond)),
	}}}
}

func (crdtOp SetDateOnly) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	dateProto := protobuf.GetDateop().GetSet()
	crdtOp.Year, crdtOp.Month, crdtOp.Day = int16(dateProto.GetYear()), int8(dateProto.GetMonth()), int8(dateProto.GetDay())
	return crdtOp
}

func (crdtOp SetDateOnly) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Dateop: &proto.ApbDateUpdate{DateSet: &proto.ApbDateOnlySet{
		Year: pb.Int32(int32(crdtOp.Year)), Month: pb.Int32(int32(crdtOp.Month)), Day: pb.Int32(int32(crdtOp.Day)),
	}}}
}

func (crdtOp SetTime) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	dateProto := protobuf.GetDateop().GetTimeSet()
	crdtOp.Hour, crdtOp.Minute, crdtOp.Second = int8(dateProto.GetHour()), int8(dateProto.GetMinute()), int8(dateProto.GetSecond())
	return crdtOp
}

func (crdtOp SetTime) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Dateop: &proto.ApbDateUpdate{TimeSet: &proto.ApbTimeOnlySet{
		Hour: pb.Int32(int32(crdtOp.Hour)), Minute: pb.Int32(int32(crdtOp.Minute)), Second: pb.Int32(int32(crdtOp.Second)),
	}}}
}

func (crdtOp SetMS) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return SetMS(protobuf.GetDateop().GetSetMS().GetMs())
}

func (crdtOp SetMS) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Dateop: &proto.ApbDateUpdate{SetMS: &proto.ApbSetMS{Ms: pb.Int64(int64(crdtOp))}}}
}

func (crdtOp IncDate) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	dateProto := protobuf.GetDateop().GetInc()
	crdtOp.Year, crdtOp.Month, crdtOp.Day = int16(dateProto.GetYear()), int8(dateProto.GetMonth()), int8(dateProto.GetDay())
	crdtOp.Hour, crdtOp.Minute, crdtOp.Second = int8(dateProto.GetHour()), int8(dateProto.GetMinute()), int8(dateProto.GetSecond())
	return crdtOp
}

func (crdtOp IncDate) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Dateop: &proto.ApbDateUpdate{Inc: &proto.ApbDateInc{
		Year: pb.Int32(int32(crdtOp.Year)), Month: pb.Int32(int32(crdtOp.Month)), Day: pb.Int32(int32(crdtOp.Day)),
		Hour: pb.Int32(int32(crdtOp.Hour)), Minute: pb.Int32(int32(crdtOp.Minute)), Second: pb.Int32(int32(crdtOp.Second)),
	}}}
}

func (crdtOp IncMS) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	return IncMS(protobuf.GetDateop().GetIncMS().GetInc())
}

func (crdtOp IncMS) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Dateop: &proto.ApbDateUpdate{IncMS: &proto.ApbIncMS{Inc: pb.Int64(int64(crdtOp))}}}
}

func (crdtOp SetInitialDate) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	dateProto := protobuf.GetDateop().GetInitialize()
	crdtOp.Year, crdtOp.Month, crdtOp.Day = int16(dateProto.GetYear()), int8(dateProto.GetMonth()), int8(dateProto.GetDay())
	crdtOp.Hour, crdtOp.Minute, crdtOp.Second = int8(dateProto.GetHour()), int8(dateProto.GetMinute()), int8(dateProto.GetSecond())
	return crdtOp
}

func (crdtOp SetInitialDate) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	return &proto.ApbUpdateOperation{Dateop: &proto.ApbDateUpdate{Initialize: &proto.ApbDateInitialize{
		Year: pb.Int32(int32(crdtOp.Year)), Month: pb.Int32(int32(crdtOp.Month)), Day: pb.Int32(int32(crdtOp.Day)),
		Hour: pb.Int32(int32(crdtOp.Hour)), Minute: pb.Int32(int32(crdtOp.Minute)), Second: pb.Int32(int32(crdtOp.Second)),
	}}}
}

func (crdtState DateState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	dateProto := protobuf.GetDate()
	return DateState{Year: int16(dateProto.GetYear()), Month: int8(dateProto.GetMonth()), Day: int8(dateProto.GetDay()),
		Hour: int8(dateProto.GetHour()), Minute: int8(dateProto.GetMinute()), Second: int8(dateProto.GetSecond())}
}

func (crdtState DateState) ToReadResp() *proto.ApbReadObjectResp {
	return &proto.ApbReadObjectResp{Date: &proto.ApbGetDateResp{
		Year: pb.Int32(int32(crdtState.Year)), Month: pb.Int32(int32(crdtState.Month)), Day: pb.Int32(int32(crdtState.Day)),
		Hour: pb.Int32(int32(crdtState.Hour)), Minute: pb.Int32(int32(crdtState.Minute)), Second: pb.Int32(int32(crdtState.Second)),
	}}
}

func (crdtState DateFullState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	dateProto := protobuf.GetPartread().GetDate().GetFull()
	return DateFullState{Year: int16(dateProto.GetYear()), Month: int8(dateProto.GetMonth()), Day: int8(dateProto.GetDay()),
		Hour: int8(dateProto.GetHour()), Minute: int8(dateProto.GetMinute()), Second: int8(dateProto.GetSecond()), Millisecond: int16(dateProto.GetMillisecond())}
}

func (crdtState DateFullState) ToReadResp() *proto.ApbReadObjectResp {
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Date: &proto.ApbDatePartialReadResp{Full: &proto.ApbDateFullResp{
		Year: pb.Int32(int32(crdtState.Year)), Month: pb.Int32(int32(crdtState.Month)), Day: pb.Int32(int32(crdtState.Day)),
		Hour: pb.Int32(int32(crdtState.Hour)), Minute: pb.Int32(int32(crdtState.Minute)), Second: pb.Int32(int32(crdtState.Second)), Millisecond: pb.Int32(int32(crdtState.Millisecond)),
	}}}}
}

func (crdtState TimeState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	dateProto := protobuf.GetPartread().GetDate().GetTime()
	return TimeState{Hour: int8(dateProto.GetHour()), Minute: int8(dateProto.GetMinute()), Second: int8(dateProto.GetSecond()), Millisecond: int16(dateProto.GetMillisecond())}
}

func (crdtState TimeState) ToReadResp() *proto.ApbReadObjectResp {
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Date: &proto.ApbDatePartialReadResp{Time: &proto.ApbTimeResp{
		Hour: pb.Int32(int32(crdtState.Hour)), Minute: pb.Int32(int32(crdtState.Minute)), Second: pb.Int32(int32(crdtState.Second)), Millisecond: pb.Int32(int32(crdtState.Millisecond))}}}}
}

func (crdtState DateOnlyState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	dateProto := protobuf.GetPartread().GetDate().GetFull()
	return DateState{Year: int16(dateProto.GetYear()), Month: int8(dateProto.GetMonth()), Day: int8(dateProto.GetDay()),
		Hour: int8(dateProto.GetHour()), Minute: int8(dateProto.GetMinute()), Second: int8(dateProto.GetSecond())}
}

func (crdtState DateOnlyState) ToReadResp() *proto.ApbReadObjectResp {
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Date: &proto.ApbDatePartialReadResp{DateOnly: &proto.ApbDateOnlyResp{
		Year: pb.Int32(int32(crdtState.Year)), Month: pb.Int32(int32(crdtState.Month)), Day: pb.Int32(int32(crdtState.Day))}}}}
}

func (crdtState TimestampState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	dateProto := protobuf.GetPartread().GetDate().GetFull()
	return DateState{Year: int16(dateProto.GetYear()), Month: int8(dateProto.GetMonth()), Day: int8(dateProto.GetDay()),
		Hour: int8(dateProto.GetHour()), Minute: int8(dateProto.GetMinute()), Second: int8(dateProto.GetSecond())}
}

func (crdtState TimestampState) ToReadResp() *proto.ApbReadObjectResp {
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Date: &proto.ApbDatePartialReadResp{Timestamp: &proto.ApbTimestampResp{Timestamp: pb.Int64(int64(crdtState))}}}}
}

func (args DateFullArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return args
}

func (args DateFullArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Date: &proto.ApbDatePartialRead{DataReadType: proto.READType_DATE_FULL.Enum()}}
}

func (args DateOnlyArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return args
}

func (args DateOnlyArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Date: &proto.ApbDatePartialRead{DataReadType: proto.READType_DATE_ONLY.Enum()}}
}

func (args TimeArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return args
}

func (args TimeArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Date: &proto.ApbDatePartialRead{DataReadType: proto.READType_DATE_TIME_ONLY.Enum()}}
}

func (args TimestampArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return args
}

func (args TimestampArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Date: &proto.ApbDatePartialRead{DataReadType: proto.READType_DATE_TIMESTAMP.Enum()}}
}

func (downOp DownstreamIncMS) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return DownstreamIncMS(protobuf.GetSimpleDateOp().GetInc())
}

func (downOp DownstreamIncMS) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{SimpleDateOp: &proto.ProtoSimpleDateDownstream{IsInitialSet: pb.Bool(false), Inc: pb.Int64(int64(downOp))}}
}

func (downOp DownstreamSetInitialDate) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	return DownstreamSetInitialDate(protobuf.GetSimpleDateOp().GetInc())
}

func (downOp DownstreamSetInitialDate) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	return &proto.ProtoOpDownstream{SimpleDateOp: &proto.ProtoSimpleDateDownstream{IsInitialSet: pb.Bool(true), Inc: pb.Int64(int64(downOp))}}
}

func (crdt *SimpleDateCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	return &proto.ProtoState{SimpleDate: &proto.ProtoSimpleDateState{DateTs: pb.Int64(crdt.dateTs)}}
}

func (crdt *SimpleDateCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (newCRDT CRDT) {
	return (&SimpleDateCrdt{dateTs: proto.GetSimpleDate().GetDateTs()}).initializeFromSnapshot(ts, replicaID)
}

func (crdt *SimpleDateCrdt) GetCRDT() CRDT { return crdt }

/*
func GregorianToTs(year, month, day int) uint64 {
	// If month is January or February, treat them as 13th and 14th of previous year
	if month <= 2 {
		year -= 1
		month += 12
	}

	a := year / 100
	b := 2 - a + a/4

	jdn := (36525*(year+4716))/100 +
		(306*(month+1))/10 +
		day + b - 1524

	// JD 0 is -4713-11-24, so JDN 0 corresponds to timestamp 0
	return uint64(jdn) * msPerDay
}

// Converts milliseconds since Julian epoch to Gregorian year, month, day
func TsToGregorian(msTs uint64) (year, month, day int) {
	days := int(msTs / msPerDay)

	// Now convert Julian Day Number (JDN) to Gregorian date
	l := days + 68569 + 1721426 // JD of 0001-01-01 is 1721426
	n := (4 * l) / 146097
	l = l - (146097*n+3)/4
	i := (4000 * (l + 1)) / 1461001
	l = l - (1461*i)/4 + 31
	j := (80 * l) / 2447
	day = l - (2447*j)/80
	l = j / 11
	month = j + 2 - 12*l
	year = 100*(n-49) + i + l

	return
}

*/
