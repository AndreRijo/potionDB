package crdt

import (
	"fmt"
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
	"potionDB/shared/shared"
)

//TODO: Returned Effect in CRDTs should not be a pointer to an interface

type InversibleCRDT interface {
	CRDT

	Copy() (copyCRDT InversibleCRDT)

	RebuildCRDTToVersion(targetTs clocksi.Timestamp)

	undoEffect(effect *Effect)

	reapplyOp(updArgs DownstreamArguments) (effect *Effect)

	//Used to notify that rebuilt is done.
	//Most CRDTs can ignore this, but it's useful for CRDTs with other embedded CRDTs and possibly for other optimizations
	//(e.g: some CRDT that can recalculate some field more efficiently when done on the end instead of multiple times)
	notifyRebuiltComplete(currTs *clocksi.Timestamp)
}

// Extra methods needed for embedded inversible CRDTs, for efficiency.
// Theorically could also be done by using RebuildCRDTToVersion, albeit it would imply
// adding targetTs to both undoEffect and reapplyOp. Also it would be less efficient.
// Or could also possibly use directly UndoEffect and ReapplyOp, but it would require
// storing effects and ops in the embMap CRDT.
type EmbInversibleCRDT interface {
	InversibleCRDT

	RedoLast()

	UndoLast()
}

type genericInversibleCRDT struct {
	//genericCRDT
	history *[]History     //For memory efficiency reasons we use a pointer, as objects get created but not updated will have this empty.
	crdt    InversibleCRDT //Pointer (for memory efficiency) for us to call the undo, reapply and notify funcs.
}

// Represents an update effect
type Effect interface {
}

type EffectType byte

// Used by operations that had no effect at all. Quite useful for non-uniform CRDTs
type NoEffect struct {
}

// Triple of clk, upd, effect
type History struct {
	ts       *clocksi.Timestamp
	updsArgs []*DownstreamArguments
	effects  []*Effect
}

//TODO: Maybe some way of setting these up depending on the CRDT?
//This takes A LOT of space for TPC-H even with SF = 0.01.
//Likelly due to the LWWRegisters, which we have... a lot
//Obviously we can set this to as low as 1. But that likelly will have considerable impact in performance.
//Might be worth testing and thinking on what is an ideal value.
//Original values were 100/5. 10/5 already gave a decent result.
//Effect can likelly be 1 safely, as it only is more than 1 when there's 2 txns with the same clk and both upd the same obj.

const (
	initialHistSize    = 1
	initialEffectsSize = 1
)

var (
	//This is mostly for now that we're testing memory impact.
	//When Version Management is disabled, share a single instance of genericInversibleCRDT
	disabledVMCrdt = &genericInversibleCRDT{}
	NoEffectVar    = NoEffect{}
)

// TODO: This startTs is not even used anymore... delete?
/*func (crdt *genericInversibleCRDT) initialize(startTs *clocksi.Timestamp, undoEffectFunc func(*Effect),
	reapplyOpFunc func(DownstreamArguments) *Effect, notifyFunc func(*clocksi.Timestamp)) (newCrdt CRDTVM) {
	if shared.IsVMDisabled {
		return disabledVMCrdt
	}
	history := make([]History, 0, initialHistSize)
	newCrdt = &genericInversibleCRDT{
		genericCRDT: genericCRDT{}.initialize(),
		//history:     make([]*History, 1, initialHistSize),
		//history:        make([]History, 0, initialHistSize),
		history:        &history,
		undoEffectFunc: undoEffectFunc,
		reapplyOpFunc:  reapplyOpFunc,
		notifyFunc:     notifyFunc,
	}
	//Add a "initial state" entry to history
	//newCrdt.history[0] = &History{ts: startTs, updsArgs: []*UpdateArguments{}, effects: []*Effect{}}
	return
}*/

func (crdt *genericInversibleCRDT) GetLatestClk() clocksi.Timestamp {
	if shared.IsVMDisabled || crdt.history == nil || len(*crdt.history) == 0 {
		return clocksi.DummyTs
	}
	return (*(*crdt.history)[len(*crdt.history)-1].ts)
}

func (crdt *genericInversibleCRDT) initialize(origCRDT InversibleCRDT) (newCrdt CRDTVM) {
	if shared.IsVMDisabled {
		return disabledVMCrdt
	}
	if shared.TmpHistoryDisable {
		crdt.crdt = origCRDT
		return crdt
	}
	history := make([]History, 0, initialHistSize)
	crdt.history, crdt.crdt = &history, origCRDT
	return crdt
}

// Note that this only copies the generic part
func (crdt *genericInversibleCRDT) copy() (copyCrdt CRDTVM) {
	if shared.IsVMDisabled {
		return crdt
	}
	/*
		copyCrdt = genericInversibleCRDT{
			genericCRDT: crdt.genericCRDT.copy(),
			history:     crdt.history,
		}
	*/
	history := *crdt.history
	newHistory := make([]History, len(history), cap(history))
	/*copyInvCrdt := &genericInversibleCRDT{
		genericCRDT: crdt.genericCRDT.copy(),
		//history:        make([]*History, len(crdt.history), cap(crdt.history)),
		history:        &newHistory,
		undoEffectFunc: crdt.undoEffectFunc,
		reapplyOpFunc:  crdt.reapplyOpFunc,
		notifyFunc:     crdt.notifyFunc,
	}*/
	copyInvCrdt := &genericInversibleCRDT{history: &newHistory, crdt: crdt.crdt}
	//We need to make a deep copy of each history entry, as when we go back in history in a CRDT the effects of updates may change. We also need to copy the effects to a new array
	for i, hist := range history {
		newHistory[i] = History{ts: hist.ts, updsArgs: hist.updsArgs, effects: make([]*Effect, len(hist.effects), cap(hist.effects))}
		for j, eff := range hist.effects {
			valueCopy := *eff
			newHistory[i].effects[j] = &valueCopy
		}
	}
	return copyInvCrdt
}

// Adds an operation to the history
func (crdt *genericInversibleCRDT) addToHistory(ts *clocksi.Timestamp, updArgs *DownstreamArguments, effect *Effect) {
	if shared.IsVMDisabled || shared.TmpHistoryDisable {
		return
	}
	var newHist History
	var historySlice []History
	if crdt.history == nil { //May be after a full GC clean (i.e., when object was not updated recently)
		historySlice = make([]History, 0, initialHistSize)
	} else {
		historySlice = *crdt.history
	}
	if len(historySlice) == 0 || !(*historySlice[len(historySlice)-1].ts).IsEqual(*ts) {
		newHist = History{
			ts:       ts,
			updsArgs: make([]*DownstreamArguments, 0, initialEffectsSize),
			effects:  make([]*Effect, 0, initialEffectsSize),
		}
		historySlice = append(historySlice, newHist)
		crdt.history = &historySlice //Append may have created a new slice.
	} else {
		newHist = historySlice[len(historySlice)-1]
	}
	newHist.updsArgs = append(newHist.updsArgs, updArgs)
	newHist.effects = append(newHist.effects, effect)
	historySlice[len(historySlice)-1] = newHist //Ensure struct is updated, as appends may have created new pointers.
}

// Rebuilds the CRDT to match the CRDT's state in the version received as argument.
func (crdt *genericInversibleCRDT) rebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	if shared.IsVMDisabled {
		return
	}
	historySlice := *crdt.history
	//No history, the CRDT is already in the empty/initial state
	if len(historySlice) == 0 {
		return
	}
	currTs := *historySlice[len(historySlice)-1].ts
	var i int
	origCRDT := crdt.crdt
	//Go back in history until we find a version in which every entry is <= than targetTs.
	for i = len(historySlice) - 1; i >= 0 && !currTs.IsLowerOrEqual(targetTs); i-- {
		for _, effect := range historySlice[i].effects {
			origCRDT.undoEffect(effect)
		}
		if i > 0 {
			currTs = *historySlice[i-1].ts
		}
	}
	//We didn't undo the history to which i points atm
	i++
	//This can happen in the case in which there has been commits in this CRDT's partition but without this CRDT being modified by those commits
	//In this case, we just need to return the state as it is.
	if i == len(historySlice) {
		return
	}
	canStop := false
	//Go forward in history and re-apply only the relevant operations...
	//Note that we might have to stop in a timestamp before targetTs. Example: history has [1], [4] (upds with ts [2], [3] was in other CRDTs) and targetTs is [3]
	currTs = currTs.Copy()
	for ; !canStop; i++ {
		//If the version in history[i] is concurrent to the target version, then we should skip this operation.
		//E.g: target is [3, 2] and we're looking at [1, 3]. Skip [1, 3]
		//fmt.Println("Going forward")
		tsCompare := (*historySlice[i].ts).Compare(targetTs)
		if tsCompare == clocksi.HigherTs {
			canStop = true
		} else if tsCompare != clocksi.ConcurrentTs {
			//If it isn't concurrent nor higher, then we can apply the update.
			for j, updArgs := range historySlice[i].updsArgs {
				historySlice[i].effects[j] = origCRDT.reapplyOp(*updArgs)
			}
			currTs.MergeInto(*historySlice[i].ts) //OK to use MergeInto as the first currTs is already a copy.
			//The history we applied might had the exact clock we were looking for
			if tsCompare == clocksi.EqualTs {
				canStop = true
			}
		} else {
			historySlice = append(historySlice[:i], historySlice[i+1:]...)
			i--
		}
	}
	//fmt.Println("")
	//"delete" (in fact, hide) the remaining history
	historySlice = historySlice[:i]
	crdt.history = &historySlice
	origCRDT.notifyRebuiltComplete(&targetTs)
}

// The last position of history contains the latest clock of the CRDT
// As for every downstream, the timestamp is passed to "addToHistory"
func (crdt *genericInversibleCRDT) GC(safeClk clocksi.Timestamp) {
	//fmt.Printf("[InvCRDT]GC start for clock %v.\n", safeClk.ToSortedString())
	//TODO: Detect when can all the history be cleaned.
	if shared.IsVMDisabled {
		return
	}
	if crdt.history == nil {
		if crdt.crdt.GetCRDTType() == proto.CRDTType_RRMAP {
			fmt.Println("[InvCRDT]History is nil - returning early.")
		}
		return //Nothing to clean.
	}
	if len(*crdt.history) == 0 {
		crdt.history = nil
		return //Nothing to clean.
	}
	startIndex, hasFound := 0, false
	history := *crdt.history
	for i := len(history) - 1; i >= 0; i-- {
		if (*history[i].ts).IsHigher(safeClk) {
			//fmt.Printf("[InvCRDT]Pos %d of history, %v is higher than safeClk: %v.\n", i, (*history[i].ts).ToSortedString(), safeClk.ToSortedString())
		} else {
			startIndex, hasFound = i, true
			//fmt.Printf("[InvCRDT]Pos %d of history, %v is lower than safeClk: %v. We will delete from this position and behind.\n", i, (*history[i].ts).ToSortedString(), safeClk.ToSortedString())
			break
		}
	}
	//startIndex is <= than safeClk. Either way, we do not include startIndex
	//fmt.Printf("[InvCRDT]. StartIndex: %d. Len of history: %d.\n", startIndex, len(history))
	if !hasFound { //All entries are higher than safeClk - we also know history is not empty.
		if crdt.crdt.GetCRDTType() == proto.CRDTType_RRMAP {
			fmt.Println("[InvCRDT]All clocks are too early - returning now, no cleaning to do.")
		}
		return //Nothing to clean - must keep all states
	}
	newLen := len(history) - startIndex - 1
	if newLen == 0 { //Set history to nil: optimization for objects that get created but never (or very seldom) updated.
		//This is also safe as there's no previous version to return to (other than an empty state...?)
		/*if crdt.crdt.GetCRDTType() == proto.CRDTType_RRMAP {
			fmt.Println("[InvCRDT]Story cleaned.")
		}*/
		history = nil
	} else if newLen < len(history)/4 {
		//New len is very small; better make new slice.
		//TODO: Maybe add some extra spaces to newHistory?
		newHistory := make([]History, newLen)
		//copy(newHistory, history[:startIndex+1])
		copy(newHistory, history[startIndex+1:]) //Keep positions after startIndex, as they are > safeClk
		history = newHistory
		/*if crdt.crdt.GetCRDTType() == proto.CRDTType_RRMAP {
			fmt.Println("[InvCRDT]History resized. Memory usage should still be reduced.")
		}*/
	} else {
		//Re-use slice. Move entries to the start.
		//copy(history, history[:startIndex+1])
		copy(history, history[startIndex+1:]) //Keep positions after startIndex, as they are > safeClk

		//Since these are not pointers we can't really clean: Best we can do is set all to nil.
		//for i := startIndex + 1; i < len(history); i++ {
		for i := newLen; i < len(history); i++ {
			history[i].ts, history[i].updsArgs, history[i].effects = nil, nil, nil
		}
		history = history[:newLen]
		/*if crdt.crdt.GetCRDTType() == proto.CRDTType_RRMAP {
			fmt.Println("[InvCRDT]History reused - memory usage should stay similar.")
		}*/
	}
	if gcCRDT, ok := (crdt.crdt).(CRDTWithGC); ok {
		/*if gcCRDT == nil || *crdt.crdt == nil {
			if len(*crdt.history) > 0 {
				fmt.Printf("[InvCRDT][ERROR]ERROR! CRDT inside InversibleCRDT is nil!!! CRDT seems to be of type %T.\n", (*(*crdt.history)[0].updsArgs[0]).GetCRDTType())
			} else {
				fmt.Printf("[InvCRDT][ERROR]ERROR! CRDT inside InversibleCRDT is nil!!! History is empty so the CRDT type is unknown.\n")
			}

		}*/
		//fmt.Printf("[InvCRDT]Detected a RWMapCRDT, calling its GC. Type: %v.\n", (crdt.crdt).GetCRDTType())
		gcCRDT.CRDTGC(safeClk)
	} /*else {
		fmt.Printf("[InvCRDT]Not a RWMapCRDT... please confirm: %T\n", (*crdt.crdt).GetCRDTType())
	}*/
	if history == nil {
		if crdt.crdt.GetCRDTType() == proto.CRDTType_RRMAP {
			fmt.Printf("[InvCRDT]Setting history to nil. Old len: %d. Old cap: %d\n", len(*crdt.history), cap(*crdt.history))
		}
		crdt.history = nil
	} else {
		if crdt.crdt.GetCRDTType() == proto.CRDTType_RRMAP {
			fmt.Printf("[InvCRDT]History partially cleaned. New len: %d. Cap: %d.\n", len(history), cap(history))
		}
		crdt.history = &history
	}
}

/*
//Undoes only the last operation. Intended to be used in CRDTs with other embedded CRDTs
func (crdt *genericInversibleCRDT) undoLast(undoEffectFunc func(*Effect)) {
	previousEffect := crdt.history[len(crdt.history)-1].effects
	effectIndex := len(previousEffect) - 1
	undoEffectFunc(previousEffect[effectIndex])
	//If there's multiple effects for the same clock, we only hide that clock from history after all effects are undone
	if effectIndex == 0 {
		crdt.history = crdt.history[:len(crdt.history)-1]
	}
}

//Redoes only the last operation. Intended to be used in CRDTs with other embedded CRDTs
func (crdt *genericInversibleCRDT) redoLast(reapplyOpFunc func(DownstreamArguments)) {

}
*/
