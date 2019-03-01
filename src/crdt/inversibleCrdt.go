package crdt

import "clocksi"

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
	ts       *clocksi.Timestamp
	updsArgs []*UpdateArguments
	effects  []*Effect
}

const (
	initialHistSize    = 100
	initialEffectsSize = 5
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
	var hist *History
	if len(crdt.history) == 0 || !(*crdt.history[len(crdt.history)-1].ts).IsEqual(*ts) {
		hist = &History{
			ts:       ts,
			updsArgs: make([]*UpdateArguments, 0, initialEffectsSize),
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
//Note that both undoEffectFunc and downstreamFunc should be provided by each wrapping CRDT.
func (crdt genericInversibleCRDT) rebuildCRDTToVersion(targetTs clocksi.Timestamp,
	undoEffectFunc func(*Effect), reapplyOpFunc func(UpdateArguments) *Effect) {
	currTs := *crdt.history[len(crdt.history)-1].ts
	var i int
	//Go back in history until we find a version in which every entry is <= than targetTs.
	for i = len(crdt.history) - 1; !currTs.IsLowerOrEqual(targetTs); i-- {
		for _, effect := range crdt.history[i].effects {
			undoEffectFunc(effect)
		}
		currTs = *crdt.history[i-1].ts
	}
	//Go forward in history and re-apply only the relevant operations...
	for ; !currTs.IsEqual(targetTs); i++ {
		//If the version in history[i] is concurrent to the target version, then we should skip this operation.
		//E.g: target is [3, 2] and we're looking at [1, 3]. Skip [1, 3]
		//If it isn't concurrent, then we can apply the update.
		if !targetTs.IsConcurrent(*crdt.history[i].ts) {
			for j, updArgs := range crdt.history[i].updsArgs {
				crdt.history[i].effects[j] = reapplyOpFunc(*updArgs)
			}
			currTs = currTs.Merge(*crdt.history[i].ts)
		} else {
			crdt.history = append(crdt.history[:i], crdt.history[i+1:]...)
			i--
		}
	}
}
