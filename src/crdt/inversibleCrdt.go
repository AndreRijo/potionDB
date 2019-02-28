package crdt

import "clocksi"

/*
So, what do I need?
In genericInversibleCRDT, I need access to the CRDT's methods (mainly downstream)
and to UndoEffect()
In each CRDT, I'll need access to the genericInversibleCRDT's methods.
Each CRDT will also need to implement UndoEffect()
The external interface doesn't really need access to UndoEffect - that can be internal.
*/

type InversibleCRDT interface {
	CRDT

	Copy() (copyCRDT InversibleCRDT)

	RebuildCRDTToVersion(targetTs clocksi.Timestamp)

	undoEffect(effect *Effect)

	reapplyOp(updArgs UpdateArguments) (effect *Effect)
}

type genericInversibleCRDT struct {
	genericCRDT
	history []*History
}

//Represents an update effect
type Effect interface {
}

type EffectType byte

//Triple of clk, upd, effect
type History struct {
	ts      *clocksi.Timestamp
	updArgs *UpdateArguments
	effect  *Effect
}

const (
	initialHistSize = 100
)

func (crdt genericInversibleCRDT) initialize() (newCrdt genericInversibleCRDT) {
	return genericInversibleCRDT{
		genericCRDT: genericCRDT{}.initialize(),
		history:     make([]*History, 0, initialHistSize),
	}
}

//Note that this only copies the generic part
func (crdt genericInversibleCRDT) copy() (copyCrdt genericInversibleCRDT) {
	copyCrdt = genericInversibleCRDT{
		genericCRDT: crdt.genericCRDT.copy(),
		history:     crdt.history,
	}
	return
}

//Adds an operation to the history
func (crdt genericInversibleCRDT) addToHistory(ts *clocksi.Timestamp, updArgs *UpdateArguments, effect *Effect) {
	crdt.history = append(crdt.history, &History{
		ts:      ts,
		updArgs: updArgs,
		effect:  effect,
	})
}

//Rebuilds the CRDT to match the CRDT's state in the version received as argument.
//Note that both undoEffectFunc and downstreamFunc should be provided by each wrapping CRDT.
func (crdt genericInversibleCRDT) rebuildCRDTToVersion(targetTs clocksi.Timestamp,
	undoEffectFunc func(*Effect), reapplyOpFunc func(UpdateArguments) *Effect) {
	currTs := *crdt.history[len(crdt.history)-1].ts
	var i int
	//Go back in history until we find a version in which every entry is <= than targetTs.
	for i = len(crdt.history) - 1; !currTs.IsLowerOrEqual(targetTs); i-- {
		undoEffectFunc(crdt.history[i].effect)
		currTs = *crdt.history[i-1].ts
	}
	//Go forward in history and re-apply only the relevant operations...
	for ; !currTs.IsEqual(targetTs); i++ {
		//If the version in history[i] is concurrent to the target version, then we should skip this operation.
		//E.g: target is [3, 2] and we're looking at [1, 3]. Skip [1, 3]
		//If it isn't concurrent, then we can apply the update.
		if !targetTs.IsConcurrent(*crdt.history[i].ts) {
			crdt.history[i].effect = reapplyOpFunc(*crdt.history[i].updArgs)
			currTs = currTs.Merge(*crdt.history[i].ts)
		} else {
			crdt.history = append(crdt.history[:i], crdt.history[i+1:]...)
			i--
		}
	}
}

/*
As for how far back we need to go... we need to find a point in which every entry is <= than the one we want
or basically a common point.

E.g: latestVersion is [4, 4]; read is on [3, 2].
			[2, 1].		[3, 1].
[1, 1].											[4, 4].
			[1, 2].		[1, 3].		[1, 4].
Note that possibly the user could also ask for the version [2, 3], which we never had...
	From the common point, go though each ramification until we reach the version we want. Order doesn't matter.

Versions should be stored by the order they were applied. No need for maps or other optimizations for now.
E.g: [1, 1], [1, 2], [1, 3], [1, 4], [2, 4], [3, 4], [4, 4]
But I do need to know more than versions... At first iteration, I need to know versions + effects. At second, versions + ops.
	- Struct with the three? Should be better than having array + 2 maps

When going forward, we need to update the history - remove operations that were ignored and update the effects of operations that were re-applied

"Pseudo-code" for GetEffects
//Sorry, I didn’t had the time (and patience) to write this yet :(
			//However, the “basic idea is”:
			//1st - Search for all effects whose versions are higher than “clk” in “updEffects”. Store those effects in “toUndo”, following the order specified by the clocks from highest to smallest.
			//2nd – Search for all effects whose clock is concurrent to “clk”. Store those effects, their clocks and their operations (stored in “opHistory”) in “concurrent” by any order. Note: it might be a good idea to group “updEffects” and “opHistory” in one variable only, to avoid having to search in two structures. That variable could be a map of version -> list of pairs (effect, op)
			//3rd – Return “toUndo” and “concurrent”

*/
