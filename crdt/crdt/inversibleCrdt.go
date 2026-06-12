package crdt

import (
	"potionDB/crdt/clocksi"
	"potionDB/shared/shared"
)

//TODO: Returned Effect in CRDTs should not be a pointer to an interface

type InversibleCRDT interface {
	CRDT

	Copy() (copyCRDT InversibleCRDT)

	RebuildCRDTToVersion(targetTs clocksi.Timestamp)

	undoEffect(effect Effect)

	reapplyOp(updArgs DownstreamArguments) (effect Effect)

	//Used to notify that rebuilt is done.
	//Most CRDTs can ignore this, but it's useful for CRDTs with other embedded CRDTs and possibly for other optimizations
	//(e.g: some CRDT that can recalculate some field more efficiently when done on the end instead of multiple times)
	notifyRebuiltComplete(currTs clocksi.Timestamp)
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

// Holds a list of histories. Used when the history list is very big, to avoid successive copies of data.
/*type HistoryEffect struct {
	OtherHistory []History
}*/

type History interface {
	GetNUpds() int
	GetTS() clocksi.Timestamp
}

// Most common.
type SingleHistory struct {
	ts      clocksi.Timestamp
	updArgs DownstreamArguments
	effect  Effect
}

// For when we have multiple updates to the same CRDT with a single timestamp (remote txns that got merged; non-static txns.)
type MultiHistory struct {
	ts       clocksi.Timestamp
	updsArgs []DownstreamArguments
	effects  []Effect
}

// Holds a list of histories. Used when the history list is very big, to avoid successive copies of data.
type FrontConnectHistory struct {
	otherHistory []History //Connects to a history that is before.
}

// Similar to FrontConnectHistory, but to a history older than the current one.
type BackConnectHistory struct {
	otherHistory []History //Connects to a history that is after.
}

// Triple of clk, upd, effect
/*type History struct {
	ts       clocksi.Timestamp
	updsArgs []DownstreamArguments
	effects  []Effect
}*/

func (h SingleHistory) GetNUpds() int {
	return 1
}

func (h MultiHistory) GetNUpds() int {
	return len(h.updsArgs)
}

func (h BackConnectHistory) GetNUpds() int {
	return 0
}

func (h FrontConnectHistory) GetNUpds() int {
	return 0
}

func (h SingleHistory) GetTS() clocksi.Timestamp {
	return h.ts
}

func (h MultiHistory) GetTS() clocksi.Timestamp {
	return h.ts
}

func (h BackConnectHistory) GetTS() clocksi.Timestamp {
	//Note: if we have a BackConnectHistory, then the last position of otherHistory is for sure a FrontConnectHistory!!!
	return h.otherHistory[len(h.otherHistory)-2].GetTS() //Thus, we have to look at the 2nd last.
	//return h.otherHistory[len(h.otherHistory)-1].GetTS()
}

func (h FrontConnectHistory) GetTS() clocksi.Timestamp {
	return h.otherHistory[1].GetTS()
	//return h.otherHistory[0].GetTS()
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
/*func (crdt *genericInversibleCRDT) Initialize(startTs clocksi.Timestamp, undoEffectFunc func(*Effect),
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
	return ((*crdt.history)[len(*crdt.history)-1].GetTS())
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

// Note that this only copies the generic part.
// Current caviat: MultiHistory's slices are shared between the copy and the new version. But for the current purposes that is fine and avoids a lot more allocations.
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
	//newHistory := make([]History, len(history), cap(history))
	/*copyInvCrdt := &genericInversibleCRDT{
		genericCRDT: crdt.genericCRDT.copy(),
		//history:        make([]*History, len(crdt.history), cap(crdt.history)),
		history:        &newHistory,
		undoEffectFunc: crdt.undoEffectFunc,
		reapplyOpFunc:  crdt.reapplyOpFunc,
		notifyFunc:     crdt.notifyFunc,
	}*/
	//copyInvCrdt := &genericInversibleCRDT{history: &newHistory, crdt: crdt.crdt}
	//We need to make a deep copy of each history entry, as when we go back in history in a CRDT the effects of updates may change. We also need to copy the effects to a new array
	//TODO: Deal with connecting histories (start/end) (also, could probably merge them into a single history slice here)
	//(Yep, merge into a single history, since we need to deep copy the entries anyway).
	//(No need to copy histories to the front though, as those are empty and are only intended for re-usage.)
	totalLen, nJumps := len(history), 0
	firstEntry, ok := history[0].(BackConnectHistory)
	var tmpEntry BackConnectHistory
	for ok {
		nJumps++
		totalLen += len(firstEntry.otherHistory)
		tmpEntry, ok = firstEntry.otherHistory[0].(BackConnectHistory)
		if ok { //This way, we keep firstEntry always pointing to the oldest valid BackConnectHistory.
			firstEntry = tmpEntry
		}
	}
	if nJumps == 0 {
		newHistory := make([]History, len(history), cap(history))
		copy(newHistory, history)
	}
	newLen := totalLen - 2 - (nJumps-1)*2 //First and last histories only have 1 connector, all others have 2. This works as nJumps >= 1 (i.e., at least 2 histories)
	//firstEntry's history corresponds to the oldest history.
	currStart, newHistory := len(firstEntry.otherHistory)-1, make([]History, newLen)
	copy(newHistory, firstEntry.otherHistory[:len(firstEntry.otherHistory)-1]) //First entry only has connector at end. Don't copy it.
	nextStory, ok := firstEntry.otherHistory[len(firstEntry.otherHistory)-1].(FrontConnectHistory)
	nJumps--
	for ; nJumps > 1; nJumps-- {
		nextSlice := nextStory.otherHistory
		copy(newHistory[currStart:], nextSlice[1:len(nextSlice)-1]) //Don't copy start and end connectors
		currStart += len(nextSlice) - 2
		nextStory, ok = nextSlice[len(nextSlice)-1].(FrontConnectHistory)
	}
	copy(newHistory[currStart:], nextStory.otherHistory[1:]) //Last entry only has connector at start. Don't copy it.
	/*for i, hist := range history {
		newHistory[i] = History{ts: hist.ts, updsArgs: hist.updsArgs, effects: make([]Effect, len(hist.effects), cap(hist.effects))}
		for j, eff := range hist.effects {
			newHistory[i].effects[j] = eff
		}
	}*/
	return &genericInversibleCRDT{history: &newHistory, crdt: crdt.crdt}
	//return copyInvCrdt
}

// Adds an operation to the history
func (crdt *genericInversibleCRDT) addToHistory(ts clocksi.Timestamp, updArgs DownstreamArguments, effect Effect) {
	if shared.IsVMDisabled || shared.TmpHistoryDisable {
		return
	}
	var newHist History
	var historySlice []History
	if crdt.history == nil { //May be after a full GC clean (i.e., when object was not updated recently)
		historySlice = make([]History, 0, 10) //In this case, likely to receive further updates, so we add some capacity.
	} else {
		historySlice = *crdt.history
	}
	if len(historySlice) == 0 || historySlice[len(historySlice)-1].GetTS().IsDifferent(ts) { //Most common case, except for remote txns or non-static txns that update the same CRDT multiple times (which is unusual)
		newHist = SingleHistory{ts: ts, updArgs: updArgs, effect: effect}
		//Let's assume cap(historySlice) == 100. So we can write until 99.
		//lenHistory+1 = cap(historySlice). So, lenHistory is 99.
		//So... 98 is already written. 99 is the front connector. So where to write? We can't!
		//So that's one problem. But easy enough to solve: write in the nextH.
		//The real problem is... how to set the slice of the current history, for the FrontConnectHistory?
		//Maybe we need to use a pointer to the history slice. But how do we ensure when we reslice the slice, it doesn't generate a new pointer?
		//We can't reslice to "cap" when going forward, as we may not have yet used all of the cap.
		//The only solution is if the BackConnectHistory contains the number of entries in the current slice, but then we always have to update it...
		//Re-usage is annoying! :(
		//(But maybe I can fix this when iterating backwards in the history...)

		lenHistory := len(historySlice)
		if lenHistory+1 == cap(historySlice) && lenHistory > 10 { //Test if there's already a connector to next history that we can re-use. If history small, there won't be a connector for sure.
			tmpH := historySlice[:lenHistory+1]                          //Unlocking full slice in order to be able to check.
			if nextH, ok := tmpH[lenHistory].(FrontConnectHistory); ok { //There's a next history slice already allocated (due to previous fastGC). Let's use that.
				/*nextH.otherHistory = nextH.otherHistory[:1] //Resetting the slice, but keeping the allocated space.
				nextH.otherHistory[0] = BackConnectHistory{otherHistory: tmpH}
				tmpH[lenHistory-1], tmpH[lenHistory] = newHist, nextH //Adding the new history and updating the pointer.
				*crdt.history = nextH.otherHistory                    //Updating history to point to the next slice.*/
				//We can't write in this slice as it's already full (due to FrontConnectHistory). Write on the next one.
				nextH.otherHistory[0] = BackConnectHistory{otherHistory: tmpH}
				nextH.otherHistory[1] = newHist
				*crdt.history = nextH.otherHistory[:2] //Hide the remaining positions (which are trash) but keep capacity.
				//Nothing else to do, as there's already the FrontConnectHistory, pointing to the full slice.
			} else { //Write normally. Next iteration will create the new connecting history if needed, or grow.
				tmpH[lenHistory] = newHist
				*crdt.history = tmpH
			}
			return //We already added the new history, so we can return.
		} else {
			if lenHistory == cap(historySlice) {
				if lenHistory > 10 && lenHistory < 30 { //Heuristic: this CRDT seems to be updated often, force a big allocation to slightly ease GC pressure.
					newHistSlice := make([]History, lenHistory, 600) //Was 300 before.
					copy(newHistSlice, historySlice)
					historySlice = newHistSlice
				} else if lenHistory >= 1000 {
					//Copying now is costly. To make usage more efficient, we allocate a bigger slice and don't copy the old one.
					//Instead, on the old slice we add at the end a pointer to the new slice, and vice-versa (but at the end instead).
					//This basically makes history a pseudo linked-list.
					newHistSlice := make([]History, 2, lenHistory*2)
					prevHistory, nextHistory := BackConnectHistory{otherHistory: historySlice}, FrontConnectHistory{otherHistory: newHistSlice}
					//On the new history, first put the pointer to the previous history, and then put the last element of the previous history
					newHistSlice[0], newHistSlice[1] = prevHistory, historySlice[lenHistory-1]
					//Now, on the last element of the previous history, but a pointer to the "next history" (i.e., the one we're creating now.)
					historySlice[lenHistory-1] = nextHistory
					historySlice = newHistSlice
				}
			}
			historySlice = append(historySlice, newHist)
			crdt.history = &historySlice //Append may have created a new slice.
		}
	} else { //Clk already exists, so we add them together.
		newHist = historySlice[len(historySlice)-1]
		if multiHist, ok := newHist.(MultiHistory); ok {
			lenUpds := len(multiHist.effects)
			if lenUpds == cap(multiHist.effects) && lenUpds == 10 { //Heuristic: seems like this CRDT was updated multiple times within a txn. Likely remote txn. Allocate a bigger slice.
				updsArgs, effects := make([]DownstreamArguments, lenUpds, 100), make([]Effect, lenUpds, 100)
				copy(updsArgs, multiHist.updsArgs)
				copy(effects, multiHist.effects)
				multiHist.updsArgs, multiHist.effects = append(updsArgs, updArgs), append(effects, effect)
			} else {
				multiHist.updsArgs, multiHist.effects = append(multiHist.updsArgs, updArgs), append(multiHist.effects, effect)
			}
		} else { //OK, there's at least 2 updates for the same clk. Let's be optimistic that it's a remote txn (or some kind of batch non-static update) and allocate 10 spaces.
			updsArgs, effects := make([]DownstreamArguments, 2, 10), make([]Effect, 2, 10)
			singleHist := newHist.(SingleHistory)
			updsArgs[0], effects[0] = singleHist.updArgs, singleHist.effect
			updsArgs[1], effects[1] = updArgs, effect
			newHist = MultiHistory{ts: singleHist.ts, updsArgs: updsArgs, effects: effects}
		}
		historySlice[len(historySlice)-1] = newHist
	} /*
		if len(historySlice) == 0 || !(historySlice[len(historySlice)-1].ts).IsEqual(ts) {
			newHist = History{
				ts:       ts,
				updsArgs: make([]DownstreamArguments, 0, initialEffectsSize),
				effects:  make([]Effect, 0, initialEffectsSize),
			}
			lenHistory := len(historySlice)
			if lenHistory == cap(historySlice) {
				if lenHistory > 10 && lenHistory < 20 { //Heuristic: this CRDT seems to be updated often, force a big allocation to ease slightly GC pressure.
					newHistory := make([]History, lenHistory, 200)
					copy(newHistory, historySlice)
					historySlice = newHistory
				} else if lenHistory >= 1000 {
					//Copying now is costly. To make usage more efficient, we allocate a bigger slice and don't copy the old one.
					//Instead, on the old slice we add at the end a pointer to the new slice, and vice-versa (but at the end instead).
					//This basically makes history a pseudo linked-list.
					newHistory := make([]History, 2, cap(historySlice)*2)
					var nextHistoryEff Effect = HistoryEffect{OtherHistory: *crdt.history}
					var prevHistoryEff Effect = HistoryEffect{OtherHistory: newHistory}
					//On the new history, first put the pointer to the previous history, and then put the last element of the previous history
					newHistory[0] = History{ts: historySlice[len(historySlice)-2].ts, updsArgs: nil, effects: []Effect{&nextHistoryEff}}
					newHistory[1] = historySlice[len(historySlice)-1]
					//Now, on the last element of the previous history, but a pointer to the "next history" (i.e., the one we're creating now.)
					historySlice[len(historySlice)-1] = History{ts: newHistory[1].ts, updsArgs: nil, effects: []Effect{&prevHistoryEff}}
					historySlice = newHistory
				}
			}
			historySlice = append(historySlice, newHist)
			crdt.history = &historySlice //Append may have created a new slice.
		} else {
			newHist = historySlice[len(historySlice)-1]
		}
		lenUpds := len(newHist.updsArgs)
		if lenUpds == cap(newHist.updsArgs) && lenUpds >= 5 && lenUpds <= 15 { //Heuristic: seems like this CRDT was updated multiple times within a txn. Likely remote txn. Allocate a bigger slice.
			updsArgs, effects := make([]DownstreamArguments, lenUpds, 50), make([]Effect, lenUpds, 50)
			copy(updsArgs, newHist.updsArgs)
			copy(effects, newHist.effects)
			newHist.updsArgs = updsArgs
			newHist.effects = effects
		}
		newHist.updsArgs = append(newHist.updsArgs, updArgs)
		newHist.effects = append(newHist.effects, effect)
		historySlice[len(historySlice)-1] = newHist //Ensure struct is updated, as appends may have created new pointers.*/
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
	currTs := historySlice[len(historySlice)-1].GetTS()
	i := len(historySlice) - 1
	origCRDT := crdt.crdt
	//Go back in history until we find a version in which every entry is <= than targetTs.
	done, connectH := false, false
	origLastLen, nHistJumps := len(historySlice), 0 //If we have multiple slices of history, after we go back and then start heading forward, the len of the slice may be incorrect. So we store it here.
	for !done {
		for ; i >= 1 && !currTs.IsLowerOrEqual(targetTs); i-- {
			switch typedHist := (historySlice[i]).(type) {
			case SingleHistory:
				origCRDT.undoEffect(typedHist.effect)
			case MultiHistory:
				for _, effect := range typedHist.effects {
					origCRDT.undoEffect(effect)
				}
			}
			currTs = historySlice[i-1].GetTS()
			if !currTs.IsLowerOrEqual(targetTs) { //On the first position of history, there may be a pointer to older history.
				i-- //Decrementing to set i to -1, which so that outside the cycle, when we increment, we'll end in the right position.
				switch typedHist := historySlice[0].(type) {
				case BackConnectHistory:
					historySlice = typedHist.otherHistory
					i, connectH = len(historySlice), true //Skip the pointer to the next history. No -1 as later the cycle will do i--.
					nHistJumps++
				case SingleHistory:
					origCRDT.undoEffect(typedHist.effect)
					done = true
				case MultiHistory:
					for _, effect := range typedHist.effects {
						origCRDT.undoEffect(effect)
					}
					done = true
				}
			} else {
				done = true //Current clk is lower or equal, so we stop here.
			}
			/*
				for _, effect := range historySlice[i].effects {
					origCRDT.undoEffect(effect)
				}
				currTs = *historySlice[i-1].ts*/
		}
		/*if !currTs.IsLowerOrEqual(targetTs) { //On the first position of history, there may be a pointer to older history.
			historyEffect, ok := (*historySlice[i].effects[0]).(HistoryEffect)
			i-- //Decrementing to set i to -1, which so that outside the cycle, when we increment, we'll end in the right position.
			if ok {
				historySlice = historyEffect.OtherHistory
				currTs = (*historySlice[len(historySlice)-1].ts)
				i = len(historySlice) //Skip the pointer to the next history. No -1 as later the cycle will do i--.
			} else {
				origCRDT.undoEffect(historySlice[i].effects[0])
				done = true
			}
		} else {
			done = true //Current clk is lower or equal, so we stop here.
		}*/
	}

	/* Old code before we could have old histories as an effect.
	for i = len(historySlice) - 1; i >= 0 && !currTs.IsLowerOrEqual(targetTs); i-- {
		for _, effect := range historySlice[i].effects {
			origCRDT.undoEffect(effect)
		}
		if i > 0 {
			currTs = *historySlice[i-1].ts
		}
	}*/
	//We didn't undo the history to which i points atm
	i++
	//This can happen in the case in which there has been commits in this CRDT's partition but without this CRDT being modified by those commits
	//In this case, we just need to return the state as it is.
	if i == len(historySlice) && !connectH { //If connectH and i == len(historySlice), it means it went back a full history slice, and by sheer "luck", the previous slice had the first clk that was <= the target.
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
		if i == len(historySlice)-1 {
			/*if histEff, ok := (*historySlice[i].effects[0]).(HistoryEffect); ok {
				historySlice = histEff.OtherHistory
				i = 0 //It'll get incremented, so we'll land at i = 1, with the first effect of the next history.
				continue
			}*/
			if histEffect, ok := historySlice[i].(FrontConnectHistory); ok {
				historySlice, i = histEffect.otherHistory, 0 //i will get incremented, so we'll land at i = 1, with the first effect of the next history.
				nHistJumps--
				if nHistJumps == 0 { //Last history. Reset len
					historySlice = historySlice[:origLastLen]
				}
				continue
			} //else: ignore, process as normal, as it's not a connecting history.
		}
		tsCompare := historySlice[i].GetTS().Compare(targetTs)
		/*if tsCompare == clocksi.HigherTs{
			canStop = true
		} else if tsCompare != clocksi.ConcurrentTs {*/
		if tsCompare == clocksi.LowerTs || tsCompare == clocksi.EqualTs {
			//If it isn't concurrent nor higher, then we can apply the update.
			switch typedH := historySlice[i].(type) {
			case SingleHistory:
				typedH.effect = origCRDT.reapplyOp(typedH.updArgs)
				historySlice[i] = typedH
			case MultiHistory:
				for j, updArgs := range typedH.updsArgs {
					typedH.effects[j] = origCRDT.reapplyOp(updArgs)
				}
				historySlice[i] = typedH
			}
			/*for j, updArgs := range historySlice[i].updsArgs {
				historySlice[i].effects[j] = origCRDT.reapplyOp(*updArgs)
			}*/
			currTs.MergeInto(historySlice[i].GetTS()) //OK to use MergeInto as the first currTs is already a copy.
			//The history we applied might had the exact clock we were looking for
			if tsCompare == clocksi.EqualTs {
				canStop = true
			}
		} else { //Higher, or concurrent. Note that if it's concurrent, then at least one entry is already higher than targetTs, so all entries ahead will either be concurrent or HigherTs: so it's safe to stop now.
			canStop = true
		}
		/*else { //Concurrent. We hide the current entry from the slice.
		//Note: If an entry is concurrent, then all following ones are either concurrent or higher (as concurrent already implies at LEAST one position is higher, so we'll never have anything that is fully <.)
		//historySlice = append(historySlice[:i], historySlice[i+1:]...)
		//Optimization: Check ahead if the upcoming clks are also concurrent: high chance they are (or that otherwise we'll stop the iteration after anyway.)
		j := i + 1
		for ; j < len(historySlice) && historySlice[j].GetTS().Compare(targetTs) == clocksi.ConcurrentTs; j++ {
		} //Skip all concurrent.
		if j == len(historySlice) { //Everything else is concurrent. So we stop here.
			canStop = true
			i++ //Compensating for the i-- later. (We want i to be the actual one, to later hide everything on this position onwards)
		} else {
			copy(historySlice[i:], historySlice[j:])            //Hide concurrent entries.
			historySlice = historySlice[:i+len(historySlice)-j] //Shrink slice to new size.
		}
		i-- //Decrementing to recheck the current position*/
	}
	//fmt.Println("")
	//"delete" (in fact, hide) the remaining history
	historySlice = historySlice[:i]
	crdt.history = &historySlice
	origCRDT.notifyRebuiltComplete(targetTs)
}

// The last position of history contains the latest clock of the CRDT
// As for every downstream, the timestamp is passed to "addToHistory"
func (crdt *genericInversibleCRDT) GC(safeClk clocksi.Timestamp, fastGC bool) {
	//fmt.Printf("[InvCRDT]GC start for clock %v.\n", safeClk.ToSortedString())
	if shared.IsVMDisabled {
		return
	}
	if crdt.history == nil {
		/*if crdt.crdt.GetCRDTType() == proto.CRDTType_RRMAP {
			fmt.Println("[InvCRDT]History is nil - returning early.")
		}*/
		return //Nothing to clean.
	}
	if len(*crdt.history) == 0 {
		crdt.history = nil
		return //Nothing to clean.
	}
	startIndex, hasFound, nHistoryJumps := 0, false, 0
	history := *crdt.history
	for !hasFound {
		if (history[0].GetTS()).IsHigher(safeClk) { //Optimization: if the oldest entry is too new, then skip directly to the previous history.
			//Issue: If we're on BackConnectHistory, we'll jump to the last position of the previous history... which is always a FrontConnectHistory. Infinite loop!
			if backHist, ok := (history[0]).(BackConnectHistory); ok {
				history = backHist.otherHistory
				nHistoryJumps++
				continue //We do continue, as maybe we can jump again a whole history.
			} else { //Oldest history is too new... nothing to clean (we'll return later in !hasFound)
				break
			}
		}
		for i := len(history) - 1; i >= 0; i-- { //TODO: Apply binary search for large slices.
			if (history[i].GetTS()).IsHigher(safeClk) {
				//fmt.Printf("[InvCRDT]Pos %d of history, %v is higher than safeClk: %v.\n", i, (history[i].GetTS()).ToSortedString(), safeClk.ToSortedString())
			} else {
				startIndex, hasFound = i, true
				//fmt.Printf("[InvCRDT]Pos %d of history, %v is lower than safeClk: %v. We will delete from this position and behind.\n", i, (history[i].GetTS()).ToSortedString(), safeClk.ToSortedString())
				break
			}
		}
	}
	//startIndex is <= than safeClk. Either way, we do not include startIndex
	//fmt.Printf("[InvCRDT]. StartIndex: %d. Len of history: %d.\n", startIndex, len(history))
	if !hasFound { //All entries are higher than safeClk - we also know history is not empty.
		/*if crdt.crdt.GetCRDTType() == proto.CRDTType_RRMAP {
			fmt.Println("[InvCRDT]All clocks are too early - returning now, no cleaning to do.")
		}*/
		return //Nothing to clean - must keep all states
	}
	//TODO: There could be more back history slices... we want to re-use them (if it's fast GC), if they're not too small.
	//Need to think about this.
	newLen := len(history) - startIndex - 1
	if newLen == 0 { //Set history to nil: optimization for objects that get created but never (or very seldom) updated. Note that even with backwards/forwards histories, this will only happen if all entries are <= safeClk, so it's fine.
		//This is also safe as there's no previous version to return to (other than an empty state...?)
		/*if crdt.crdt.GetCRDTType() == proto.CRDTType_RRMAP {
			fmt.Println("[InvCRDT]Story cleaned.")
		}*/
		history = nil
	} else if newLen < len(history)/4 && !fastGC { //If it's fastGC, updates are still happening - we maintain the same slice.
		//New len is very small; better make new slice.
		//TODO: Maybe add some extra spaces to newHistory?
		newHistory := make([]History, newLen)
		//copy(newHistory, history[:startIndex+1])
		copy(newHistory, history[startIndex+1:]) //Keep positions after startIndex, as they are > safeClk
		if nHistoryJumps == 0 {                  //We didn't go to a backward history.
			history = newHistory
		} else { //Have to update the pointer to this backward history (i.e., access next history, then position 0 (pointer to this history)).
			nextHist := newHistory[newLen-1].(FrontConnectHistory)
			prevPointer := nextHist.otherHistory[0].(BackConnectHistory)
			prevPointer.otherHistory = newHistory
			history = *crdt.history
		}
		/*if crdt.crdt.GetCRDTType() == proto.CRDTType_RRMAP {
			fmt.Println("[InvCRDT]History resized. Memory usage should still be reduced.")
		}*/
	} else {
		firstEntry := history[0] //If doing fastGC, we may need this reference to enable re-usage of old slices.
		//Re-use slice. Move entries to the start.
		//copy(history, history[:startIndex+1])
		copy(history, history[startIndex+1:]) //Keep positions after startIndex, as they are > safeClk

		if !fastGC { //Later a proper GC can clean these up.
			history = history[:cap(history)] //We may have hidden elements before, during fastGC.
			for i := newLen; i < cap(history); i++ {
				history[i] = nil
				//history[i].ts, history[i].updsArgs, history[i].effects = nil, nil, nil
			}
		}
		if nHistoryJumps > 0 { //Have to update the pointer to this backward history (i.e., access next history, then position 0 (pointer to this history))
			nextHist := history[newLen-1].(FrontConnectHistory)
			prevPointer := nextHist.otherHistory[0].(BackConnectHistory)
			prevPointer.otherHistory = history[:newLen]
			history = *crdt.history //Restoring original pointer, as we modified a backward history, but not the latest history.
		} //else: nothing to do, there's no backward history, so there's nothing we can re-use. There's also no pointers to update.

		if fastGC { //We can re-use the previously allocated slices (if any exists), as they'll likely be useful again.
			if oldestH, ok := firstEntry.(BackConnectHistory); ok {
				done := false
				for done {
					if backH, ok := oldestH.otherHistory[0].(BackConnectHistory); ok {
						if len(backH.otherHistory) < 300 { //We won't re-use very small slices.
							done = true
						} else {
							oldestH = backH
						}
					}
				}
				//Connect the new history to the oldest one, in order to enable full re-usage. For this we'll write ahead the connector to the next history.
				origLen := len(history)
				history = history[:cap(history)] //Unlock temporarely the full buffer, so that we can add the connection.
				history[len(history)-1] = FrontConnectHistory{otherHistory: oldestH.otherHistory}
				history = history[:origLen]
				//oldestH's first entry will be written later when appropriate.
			} //else: no further backward history, so nothing to do.
		}
		if nHistoryJumps == 0 { //If nHistoryJumps > 0, we already updated history.
			history = history[:newLen]
		}
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
		gcCRDT.CRDTGC(safeClk, fastGC)
	} /*else {
		fmt.Printf("[InvCRDT]Not a RWMapCRDT... please confirm: %T\n", (*crdt.crdt).GetCRDTType())
	}*/
	if history == nil {
		/*if crdt.crdt.GetCRDTType() == proto.CRDTType_RRMAP {
			fmt.Printf("[InvCRDT]Setting history to nil. Old len: %d. Old cap: %d\n", len(*crdt.history), cap(*crdt.history))
		}*/
		crdt.history = nil
	} else {
		/*if crdt.crdt.GetCRDTType() == proto.CRDTType_RRMAP {
			fmt.Printf("[InvCRDT]History partially cleaned. New len: %d. Cap: %d.\n", len(history), cap(history))
		}*/
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
