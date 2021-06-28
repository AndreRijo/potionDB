package crdt

import (
	"potionDB/src/clocksi"
	"potionDB/src/shared"
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

//Extra methods needed for embedded inversible CRDTs, for efficiency.
//Theorically could also be done by using RebuildCRDTToVersion, albeit it would imply
//adding targetTs to both undoEffect and reapplyOp. Also it would be less efficient.
//Or could also possibly use directly UndoEffect and ReapplyOp, but it would require
//storing effects and ops in the embMap CRDT.
type EmbInversibleCRDT interface {
	InversibleCRDT

	RedoLast()

	UndoLast()
}

type genericInversibleCRDT struct {
	genericCRDT
	history []*History
}

//Represents an update effect
type Effect interface {
}

type EffectType byte

//Used by operations that had no effect at all. Quite useful for non-uniform CRDTs
type NoEffect struct {
}

//Triple of clk, upd, effect
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
)

//TODO: This startTs is not even used anymore... delete?
func (crdt *genericInversibleCRDT) initialize(startTs *clocksi.Timestamp) (newCrdt *genericInversibleCRDT) {
	if shared.IsVMDisabled {
		return disabledVMCrdt
	}
	newCrdt = &genericInversibleCRDT{
		genericCRDT: genericCRDT{}.initialize(),
		//history:     make([]*History, 1, initialHistSize),
		history: make([]*History, 0, initialHistSize),
	}
	//Add a "initial state" entry to history
	//newCrdt.history[0] = &History{ts: startTs, updsArgs: []*UpdateArguments{}, effects: []*Effect{}}
	return
}

//Note that this only copies the generic part
func (crdt *genericInversibleCRDT) copy() (copyCrdt *genericInversibleCRDT) {
	if shared.IsVMDisabled {
		return crdt
	}
	/*
		copyCrdt = genericInversibleCRDT{
			genericCRDT: crdt.genericCRDT.copy(),
			history:     crdt.history,
		}
	*/
	copyCrdt = &genericInversibleCRDT{
		genericCRDT: crdt.genericCRDT.copy(),
		history:     make([]*History, len(crdt.history), cap(crdt.history)),
	}
	//We need to make a deep copy of each history entry, as when we go back in history in a CRDT the effects of updates may change. We also need to copy the effects to a new array
	for i, hist := range crdt.history {
		copyCrdt.history[i] = &History{ts: hist.ts, updsArgs: hist.updsArgs, effects: make([]*Effect, len(hist.effects), cap(hist.effects))}
		for j, eff := range hist.effects {
			valueCopy := *eff
			copyCrdt.history[i].effects[j] = &valueCopy
		}
	}
	return
}

//Adds an operation to the history
func (crdt *genericInversibleCRDT) addToHistory(ts *clocksi.Timestamp, updArgs *DownstreamArguments, effect *Effect) {
	if shared.IsVMDisabled {
		return
	}
	var hist *History
	if len(crdt.history) == 0 || !(*crdt.history[len(crdt.history)-1].ts).IsEqual(*ts) {
		hist = &History{
			ts:       ts,
			updsArgs: make([]*DownstreamArguments, 0, initialEffectsSize),
			effects:  make([]*Effect, 0, initialEffectsSize),
		}
		crdt.history = append(crdt.history, hist)
	} else {
		hist = crdt.history[len(crdt.history)-1]
	}
	hist.updsArgs = append(hist.updsArgs, updArgs)
	hist.effects = append(hist.effects, effect)
}

//Rebuilds the CRDT to match the CRDT's state in the version received as argument.
//Note that undoEffectFunc, downstreamFunc and notifyFunc should be provided by each wrapping CRDT.
func (crdt *genericInversibleCRDT) rebuildCRDTToVersion(targetTs clocksi.Timestamp,
	undoEffectFunc func(*Effect), reapplyOpFunc func(DownstreamArguments) *Effect,
	notifyFunc func(*clocksi.Timestamp)) {
	if shared.IsVMDisabled {
		return
	}
	//No history, the CRDT is already in the empty/initial state
	if len(crdt.history) == 0 {
		return
	}
	currTs := *crdt.history[len(crdt.history)-1].ts
	var i int
	//Go back in history until we find a version in which every entry is <= than targetTs.
	for i = len(crdt.history) - 1; i >= 0 && !currTs.IsLowerOrEqual(targetTs); i-- {
		for _, effect := range crdt.history[i].effects {
			undoEffectFunc(effect)
		}
		if i > 0 {
			currTs = *crdt.history[i-1].ts
		}
	}
	//We didn't undo the history to which i points atm
	i++
	//This can happen in the case in which there has been commits in this CRDT's partition but without this CRDT being modified by those commits
	//In this case, we just need to return the state as it is.
	if i == len(crdt.history) {
		return
	}
	canStop := false
	//Go forward in history and re-apply only the relevant operations...
	//Note that we might have to stop in a timestamp before targetTs. Example: history has [1], [4] (upds with ts [2], [3] was in other CRDTs) and targetTs is [3]
	for ; !canStop; i++ {
		//If the version in history[i] is concurrent to the target version, then we should skip this operation.
		//E.g: target is [3, 2] and we're looking at [1, 3]. Skip [1, 3]
		//fmt.Println("Going forward")
		tsCompare := (*crdt.history[i].ts).Compare(targetTs)
		if tsCompare == clocksi.HigherTs {
			canStop = true
		} else if tsCompare != clocksi.ConcurrentTs {
			//If it isn't concurrent nor higher, then we can apply the update.
			for j, updArgs := range crdt.history[i].updsArgs {
				crdt.history[i].effects[j] = reapplyOpFunc(*updArgs)
			}
			currTs = currTs.Merge(*crdt.history[i].ts)
			//The history we applied might had the exact clock we were looking for
			if tsCompare == clocksi.EqualTs {
				canStop = true
			}
		} else {
			crdt.history = append(crdt.history[:i], crdt.history[i+1:]...)
			i--
		}
	}
	//fmt.Println("")
	//"delete" (in fact, hide) the remaining history
	crdt.history = crdt.history[:i]
	notifyFunc(&targetTs)
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
