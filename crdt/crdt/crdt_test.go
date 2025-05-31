package crdt

import (
	"fmt"
	"potionDB/crdt/clocksi"
	"sort"
	"strings"
	"testing"
)

func TestTopKRmv1(t *testing.T) {
	crdtR1 := (&TopKRmvCrdt{}).Initialize(nil, 666).(*TopKRmvCrdt)
	crdtR2 := (&TopKRmvCrdt{}).Initialize(nil, 777).(*TopKRmvCrdt)
	newDownstreamR1 := make([]DownstreamArguments, 0, 10)
	newDownstreamR2 := make([]DownstreamArguments, 0, 10)
	dummyTs := clocksi.NewClockSiTimestampFromId(0)

	addR1_1 := crdtR1.getTopKAddDownstreamArgs(&TopKAdd{TopKScore: TopKScore{Id: 0, Score: 100}})
	newDownstreamR1 = addDownIfNotEmpty(newDownstreamR1, crdtR1.Downstream(dummyTs, addR1_1))
	addR1_2 := crdtR1.getTopKAddDownstreamArgs(&TopKAdd{TopKScore: TopKScore{Id: 0, Score: 20}})
	newDownstreamR1 = addDownIfNotEmpty(newDownstreamR1, crdtR1.Downstream(dummyTs, addR1_2))

	addR2_1 := crdtR2.getTopKAddDownstreamArgs(&TopKAdd{TopKScore: TopKScore{Id: 0, Score: 50}})
	newDownstreamR2 = addDownIfNotEmpty(newDownstreamR2, crdtR2.Downstream(dummyTs, addR2_1))
	newDownstreamR1 = addDownIfNotEmpty(newDownstreamR1, crdtR2.Downstream(dummyTs, addR1_1))
	remR2_1 := crdtR2.getTopKRemoveDownstreamArgs(&TopKRemove{Id: 0})
	newDownstreamR2 = addDownIfNotEmpty(newDownstreamR2, crdtR2.Downstream(dummyTs, remR2_1))

	newDownstreamR2 = addDownIfNotEmpty(newDownstreamR2, crdtR1.Downstream(dummyTs, addR1_2))
	newDownstreamR1 = addDownIfNotEmpty(newDownstreamR1, crdtR1.Downstream(dummyTs, addR2_1))
	newDownstreamR1 = addDownIfNotEmpty(newDownstreamR1, crdtR1.Downstream(dummyTs, remR2_1))

	for _, down := range newDownstreamR1 {
		crdtR2.Downstream(nil, down)
	}
	for _, down := range newDownstreamR2 {
		crdtR1.Downstream(nil, down)
	}

	stateR1 := crdtR1.Read(StateReadArguments{}, nil).(TopKValueState)
	stateR2 := crdtR2.Read(StateReadArguments{}, nil).(TopKValueState)
	sort.Slice(stateR1.Scores, func(i, j int) bool { return stateR1.Scores[i].Id < stateR1.Scores[j].Id })
	sort.Slice(stateR2.Scores, func(i, j int) bool { return stateR2.Scores[i].Id < stateR2.Scores[j].Id })
	fmt.Println(stateR1.Scores)
	fmt.Println(stateR2.Scores)
}

func TestTopKRmv2(t *testing.T) {
	//R1: add(1, 100, R1) -> add(1, 110, R2) -> rem(1, R2)
	//R2: add(1, 110, R2) -> rem(1, R2) -> add(1, 101, R1)
	//Idea: no matter the order, 101 should survive.

	crdtR1 := (&TopKRmvCrdt{}).Initialize(nil, 666).(*TopKRmvCrdt)
	crdtR2 := (&TopKRmvCrdt{}).Initialize(nil, 777).(*TopKRmvCrdt)
	newDownstreamR1 := make([]DownstreamArguments, 0, 10)
	newDownstreamR2 := make([]DownstreamArguments, 0, 10)
	dummyTs := clocksi.NewClockSiTimestampFromId(0)

	addR1_1 := crdtR1.getTopKAddDownstreamArgs(&TopKAdd{TopKScore: TopKScore{Id: 0, Score: 100}})
	newDownstreamR1 = addDownIfNotEmpty(newDownstreamR1, crdtR1.Downstream(dummyTs, addR1_1))

	addR2_1 := crdtR2.getTopKAddDownstreamArgs(&TopKAdd{TopKScore: TopKScore{Id: 0, Score: 110}})
	newDownstreamR2 = addDownIfNotEmpty(newDownstreamR2, crdtR2.Downstream(dummyTs, addR2_1))
	remR2_1 := crdtR2.getTopKRemoveDownstreamArgs(&TopKRemove{Id: 0})
	newDownstreamR2 = addDownIfNotEmpty(newDownstreamR2, crdtR2.Downstream(dummyTs, remR2_1))

	newDownstreamR1 = addDownIfNotEmpty(newDownstreamR1, crdtR1.Downstream(dummyTs, addR2_1))
	newDownstreamR1 = addDownIfNotEmpty(newDownstreamR1, crdtR1.Downstream(dummyTs, remR2_1))
	newDownstreamR2 = addDownIfNotEmpty(newDownstreamR2, crdtR1.Downstream(dummyTs, addR1_1))

	for _, down := range newDownstreamR1 {
		crdtR2.Downstream(nil, down)
	}
	for _, down := range newDownstreamR2 {
		crdtR1.Downstream(nil, down)
	}

	stateR1 := crdtR1.Read(StateReadArguments{}, nil).(TopKValueState)
	stateR2 := crdtR2.Read(StateReadArguments{}, nil).(TopKValueState)
	sort.Slice(stateR1.Scores, func(i, j int) bool { return stateR1.Scores[i].Id < stateR1.Scores[j].Id })
	sort.Slice(stateR2.Scores, func(i, j int) bool { return stateR2.Scores[i].Id < stateR2.Scores[j].Id })
	fmt.Println(stateR1.Scores)
	fmt.Println(stateR2.Scores)
}

func addDownIfNotEmpty(list []DownstreamArguments, downArgs DownstreamArguments) []DownstreamArguments {
	if downArgs != nil {
		return append(list, downArgs)
	}
	return list
}

// Copy paste from tools, since we can't have import cycles
func StateToString(state State) (stateString string) {
	var sb strings.Builder
	switch typedState := state.(type) {
	//Set
	case SetAWValueState:
		sb.WriteRune('[')
		//TODO: As of now slice might be called multiple times. This shouldn't be here.
		sort.Slice(typedState.Elems, func(i, j int) bool { return typedState.Elems[i] < typedState.Elems[j] })
		for _, elem := range typedState.Elems {
			sb.WriteString(string(elem) + ", ")
		}
		sb.WriteRune(']')
	case SetAWLookupState:
		if typedState.HasElem {
			sb.WriteString("Element found.")
		} else {
			sb.WriteString("Element not found.")
		}
	//Counter
	case CounterState:
		sb.WriteString("Value: ")
		sb.WriteRune(int32(typedState))
	//TopK
	case TopKValueState:
		sb.WriteRune('[')
		//TODO: As of now slice might be called multiple times. This shouldn't be here.
		sort.Slice(typedState.Scores, func(i, j int) bool { return typedState.Scores[i].Id < typedState.Scores[j].Id })
		for i, score := range typedState.Scores {
			sb.WriteString(fmt.Sprintf("%d: (%d, %d), ", i+1, score.Id, score.Score))
		}
		sb.WriteRune(']')
		/*
			type SetAWValueState struct {
				Elems []Element
			}

			type SetAWLookupState struct {
				hasElem bool
			}
			type CounterState struct {
				Value int32
			}
			type TopKValueState struct {
				Scores []TopKScore
		*/
	}
	return sb.String()
}
