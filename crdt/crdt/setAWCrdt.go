package crdt

import (
	"fmt"
	rand "math/rand"
	"time"

	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"

	//pb "github.com/golang/protobuf/proto"
	pb "google.golang.org/protobuf/proto"
)

type Element string

// Note: Implements both CRDT and InversibleCRDT
type SetAWCrdt struct {
	CRDTVM
	elems map[Element]UniqueSet
	//elems map[Element]uniqueSet
	//Used to generate unique identifiers. This should not be included in a serialization to transfer the state.
	random rand.Source
}

type SetAWValueState struct {
	Elems []Element
}

type SetAWLookupState struct {
	HasElem bool
}

type LookupReadArguments struct {
	Elem Element
}

type GetNElementsArguments struct{}

type SetAWNElementsState struct {
	Count int
}

//Operations

type Add struct {
	Element Element
}

type Remove struct {
	Element Element
}

type AddAll struct {
	Elems []Element
}

type RemoveAll struct {
	Elems []Element
}

//Downstream operations

type DownstreamAddAll struct {
	Elems map[Element]Unique
}

type DownstreamRemoveAll struct {
	Elems map[Element]UniqueSet
}

//Operation effects for inversibleCRDT (the first two are probably not needed)

type AddEffect struct {
	Elem   Element
	Unique Unique
}

type RemoveEffect struct {
	Elem    Element
	Uniques UniqueSet
}

type AddAllEffect struct {
	AddedMap map[Element]Unique
}

type RemoveAllEffect struct {
	RemovedMap map[Element]UniqueSet
}

func (crdt *SetAWCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_ORSET }

// Ops
func (args Add) GetCRDTType() proto.CRDTType { return proto.CRDTType_ORSET }

func (args Remove) GetCRDTType() proto.CRDTType { return proto.CRDTType_ORSET }

func (args AddAll) GetCRDTType() proto.CRDTType { return proto.CRDTType_ORSET }

func (args RemoveAll) GetCRDTType() proto.CRDTType { return proto.CRDTType_ORSET }

// Downstreams
func (args DownstreamAddAll) GetCRDTType() proto.CRDTType { return proto.CRDTType_ORSET }

func (args DownstreamRemoveAll) GetCRDTType() proto.CRDTType { return proto.CRDTType_ORSET }

func (args DownstreamAddAll) MustReplicate() bool { return true }

func (args DownstreamRemoveAll) MustReplicate() bool { return true }

// States
func (args SetAWValueState) GetCRDTType() proto.CRDTType { return proto.CRDTType_ORSET }

func (args SetAWValueState) GetREADType() proto.READType { return proto.READType_FULL }

func (args SetAWLookupState) GetCRDTType() proto.CRDTType { return proto.CRDTType_ORSET }

func (args SetAWLookupState) GetREADType() proto.READType { return proto.READType_LOOKUP }

func (args SetAWNElementsState) GetCRDTType() proto.CRDTType { return proto.CRDTType_ORSET }

func (args SetAWNElementsState) GetREADType() proto.READType { return proto.READType_N_ELEMS }

// Queries
func (args LookupReadArguments) GetCRDTType() proto.CRDTType   { return proto.CRDTType_ORSET }
func (args LookupReadArguments) GetREADType() proto.READType   { return proto.READType_LOOKUP }
func (args LookupReadArguments) HasInnerReads() bool           { return false }
func (args LookupReadArguments) HasVariables() bool            { return false }
func (args GetNElementsArguments) GetCRDTType() proto.CRDTType { return proto.CRDTType_ORSET }
func (args GetNElementsArguments) GetREADType() proto.READType { return proto.READType_N_ELEMS }
func (args GetNElementsArguments) HasInnerReads() bool         { return false }
func (args GetNElementsArguments) HasVariables() bool          { return false }

// Note: crdt can (and most often will be) nil
func (crdt *SetAWCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return &SetAWCrdt{
		CRDTVM: (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete),
		elems:  make(map[Element]UniqueSet),
		random: rand.NewSource(time.Now().Unix()),
	}
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *SetAWCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *SetAWCrdt) {
	crdt.CRDTVM, crdt.random = (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete), rand.NewSource(time.Now().Unix())
	return crdt
}

func (crdt *SetAWCrdt) IsBigCRDT() bool { return len(crdt.elems) > 100 }

func (crdt *SetAWCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	switch typedArg := args.(type) {
	case StateReadArguments:
		return crdt.getState(updsNotYetApplied)
	case LookupReadArguments:
		return crdt.lookup(typedArg.Elem, updsNotYetApplied)
	case GetNElementsArguments:
		return crdt.getNElements(updsNotYetApplied)
	default:
		fmt.Printf("[AWSetCrdt]Unknown read type: %+v\n", args)
	}
	return nil
}

func (crdt *SetAWCrdt) getNElements(updsNotYetApplied []UpdateArguments) (state State) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		return SetAWNElementsState{Count: len(crdt.elems)}
	}
	//TODO: Maybe could optimize this
	return SetAWNElementsState{Count: len(crdt.getState(updsNotYetApplied).(SetAWValueState).Elems)}
}

func (crdt *SetAWCrdt) getState(updsNotYetApplied []UpdateArguments) (state State) {
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		//go doesn't have a set structure nor a way to get keys from map.
		//Using an auxiliary array in the state with the elements isn't a good option either - remove would have to search for the element
		//So, unfortunatelly, we need to built it here.
		elems := make([]Element, len(crdt.elems))
		i := 0
		for key := range crdt.elems {
			elems[i] = key
			i++
		}
		return SetAWValueState{Elems: elems}
	}

	//Need to build temporary state
	adds := make(map[Element]struct{})
	rems := make(map[Element]struct{})

	//Idea: for each element, the latest entry is the one that matters
	for i := len(updsNotYetApplied) - 1; i >= 0; i-- {
		switch typedUpd := (updsNotYetApplied[i]).(type) {
		case Add:
			if _, has := rems[typedUpd.Element]; !has {
				adds[typedUpd.Element] = struct{}{}
			}
		case AddAll:
			for _, elem := range typedUpd.Elems {
				if _, has := rems[elem]; !has {
					adds[elem] = struct{}{}
				}
			}
		case Remove:
			if _, has := adds[typedUpd.Element]; !has {
				rems[typedUpd.Element] = struct{}{}
			}
		case RemoveAll:
			for _, elem := range typedUpd.Elems {
				if _, has := adds[elem]; !has {
					rems[elem] = struct{}{}
				}
			}
		}
	}

	//Can't subtract rems, as operations in updsNotYetApplied aren't processed and may refer elems never added
	//TODO: This might be optimized if we call Update() as soon as an operation is received
	elems := make([]Element, len(crdt.elems)+len(adds))
	i := 0
	for elem := range crdt.elems {
		if _, hasRem := rems[elem]; !hasRem {
			elems[i] = elem
			i++
		}
	}
	for elem := range adds {
		if _, has := crdt.elems[elem]; !has {
			elems[i] = elem
			i++
		}
	}
	return SetAWValueState{Elems: elems[0:i]}
}

func (crdt *SetAWCrdt) lookup(elem Element, updsNotYetApplied []UpdateArguments) (state State) {
	_, hasElem := crdt.elems[elem]
	if updsNotYetApplied == nil || len(updsNotYetApplied) == 0 {
		return SetAWLookupState{HasElem: hasElem}
	}

	//Idea: only need to check the latest update of elem
	for i := len(updsNotYetApplied) - 1; i >= 0; i-- {
		switch typedUpd := (updsNotYetApplied[i]).(type) {
		case Add:
			if typedUpd.Element == elem {
				return SetAWLookupState{HasElem: true}
			}
		case AddAll:
			for _, updElem := range typedUpd.Elems {
				if updElem == elem {
					return SetAWLookupState{HasElem: true}
				}
			}
		case Remove:
			if typedUpd.Element == elem {
				return SetAWLookupState{HasElem: false}
			}
		case RemoveAll:
			for _, updElem := range typedUpd.Elems {
				if updElem == elem {
					return SetAWLookupState{HasElem: false}
				}
			}
		}
	}
	//Element wasn't referred in upsNotYetApplied
	return SetAWLookupState{HasElem: hasElem}
}

// TODO: Maybe one day implement add and remove with their own methods (i.e., avoid the overhead of creating/handling arrays and maps?)
func (crdt *SetAWCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	switch opType := args.(type) {
	case Add:
		elemArray := make([]Element, 1)
		elemArray[0] = opType.Element
		downstreamArgs = crdt.getAddAllDownstreamArgs(elemArray)
	case Remove:
		elemArray := make([]Element, 1)
		elemArray[0] = opType.Element
		downstreamArgs = crdt.getRemoveAllDownstreamArgs(elemArray)
	case AddAll:
		downstreamArgs = crdt.getAddAllDownstreamArgs(opType.Elems)
	case RemoveAll:
		downstreamArgs = crdt.getRemoveAllDownstreamArgs(opType.Elems)
	}
	return
}

func (crdt *SetAWCrdt) getAddAllDownstreamArgs(elems []Element) (downstreamArgs DownstreamArguments) {
	uniqueMap := make(map[Element]Unique)
	for _, key := range elems {
		uniqueMap[key] = Unique(crdt.random.Int63())
	}
	downstreamArgs = DownstreamAddAll{Elems: uniqueMap}
	return
}

func (crdt *SetAWCrdt) getRemoveAllDownstreamArgs(elems []Element) (downstreamArgs DownstreamArguments) {
	uniqueMap := make(map[Element]UniqueSet)
	for _, key := range elems {
		//Maps are in fact references, so doing "uniqueMap[key] = crdt.elems[key]" wouldn't actually copy the uniqueSet's (which is a map) contents to a new uniqueSet.
		if elem, has := crdt.elems[key]; has {
			uniqueMap[key] = elem.copy()
		}
	}
	if len(uniqueMap) == 0 {
		//Nothing to effectivelly remove, saving an operation
		downstreamArgs = NoOp{}
	} else {
		downstreamArgs = DownstreamRemoveAll{Elems: uniqueMap}
	}
	//fmt.Println("[SETAWCRDT]Remove all downstream generated:", downstreamArgs)
	return
}

func (crdt *SetAWCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	effect := crdt.applyDownstream(downstreamArgs)
	//Necessary for inversibleCrdt
	crdt.addToHistory(&updTs, &downstreamArgs, effect)

	return nil
}

func (crdt *SetAWCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	switch opType := downstreamArgs.(type) {
	case DownstreamAddAll:
		effect = crdt.applyAddAll(opType.Elems)
	case DownstreamRemoveAll:
		effect = crdt.applyRemoveAll(opType.Elems)
	}
	//fmt.Println("[SETAWCRDT]State after downstream: ", crdt.getState(nil))
	return
}

func (crdt *SetAWCrdt) applyAddAll(toAdd map[Element]Unique) (effect *Effect) {
	for key, newUnique := range toAdd {
		//Checks if the key is already in the map. If it is, adds a unique
		if existingUniques, ok := crdt.elems[key]; ok {
			existingUniques.add(newUnique)
		} else {
			newSet := makeUniqueSet()
			newSet.add(newUnique)
			crdt.elems[key] = newSet
		}
	}
	var effectValue Effect = AddAllEffect{AddedMap: toAdd}
	return &effectValue
}

func (crdt *SetAWCrdt) applyRemoveAll(toRem map[Element]UniqueSet) (effect *Effect) {
	removedMap := make(map[Element]UniqueSet)
	var effectValue Effect = RemoveAllEffect{RemovedMap: removedMap}

	for key, uniquesToRem := range toRem {
		//Checks if the key is already in the map. If it is, removes the uniques in the intersection
		if existingUniques, ok := crdt.elems[key]; ok {
			removedMap[key] = existingUniques.getAndRemoveIntersection(uniquesToRem)
			if len(existingUniques) == 0 {
				delete(crdt.elems, key)
			}
		}
		//The element wasn't in the set already, so nothing to do
	}
	return &effectValue
}

func (crdt *SetAWCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

//METHODS FOR INVERSIBLE_CRDT

func (crdt *SetAWCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCrdt := SetAWCrdt{
		CRDTVM: crdt.CRDTVM.copy(),
		elems:  make(map[Element]UniqueSet),
		random: rand.NewSource(time.Now().Unix()),
	}
	for element, uniques := range crdt.elems {
		newCrdt.elems[element] = uniques.copy()
	}

	return &newCrdt
}

func (crdt *SetAWCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *SetAWCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *SetAWCrdt) undoEffect(effect *Effect) {
	switch typedEffect := (*effect).(type) {
	case AddAllEffect:
		crdt.undoAddAllEffect(&typedEffect)
	case RemoveAllEffect:
		crdt.undoRemoveAllEffect(&typedEffect)
	}
}

func (crdt *SetAWCrdt) undoAddAllEffect(effect *AddAllEffect) {
	for key, uniqueToRem := range effect.AddedMap {
		if existingUniques, ok := crdt.elems[key]; ok {
			delete(existingUniques, uniqueToRem)
			if len(existingUniques) == 0 {
				delete(crdt.elems, key)
			}
		}
	}
}

func (crdt *SetAWCrdt) undoRemoveAllEffect(effect *RemoveAllEffect) {
	for key, uniquesToAdd := range effect.RemovedMap {
		if existingUniques, ok := crdt.elems[key]; ok {
			existingUniques.addAll(uniquesToAdd)
		} else {
			newSet := makeUniqueSet()
			newSet.addAll(uniquesToAdd)
			crdt.elems[key] = newSet
		}
	}
}

func (crdt *SetAWCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

//Protobuf functions

func (crdtOp AddAll) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Elems = ByteMatrixToElementArray(protobuf.GetSetop().GetAdds())
	return crdtOp
}

func (crdtOp AddAll) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	opType := proto.ApbSetUpdate_ADD
	elements := ElementArrayToByteMatrix(crdtOp.Elems)
	return &proto.ApbUpdateOperation{Setop: &proto.ApbSetUpdate{Optype: &opType, Adds: elements}}
}

func (crdtOp RemoveAll) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Elems = ByteMatrixToElementArray(protobuf.GetSetop().GetRems())
	return crdtOp
}

func (crdtOp RemoveAll) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	opType := proto.ApbSetUpdate_REMOVE
	elements := ElementArrayToByteMatrix(crdtOp.Elems)
	return &proto.ApbUpdateOperation{Setop: &proto.ApbSetUpdate{Optype: &opType, Rems: elements}}
}

func (crdtOp Add) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Element = Element(protobuf.GetSetop().GetAdds()[0])
	return crdtOp
}

func (crdtOp Add) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	opType := proto.ApbSetUpdate_ADD
	element := [][]byte{[]byte(crdtOp.Element)}
	return &proto.ApbUpdateOperation{Setop: &proto.ApbSetUpdate{Optype: &opType, Adds: element}}
}

func (crdtOp Remove) FromUpdateObject(protobuf *proto.ApbUpdateOperation) (op UpdateArguments) {
	crdtOp.Element = Element(protobuf.GetSetop().GetRems()[0])
	return crdtOp
}

func (crdtOp Remove) ToUpdateObject() (protobuf *proto.ApbUpdateOperation) {
	opType := proto.ApbSetUpdate_REMOVE
	element := [][]byte{[]byte(crdtOp.Element)}
	return &proto.ApbUpdateOperation{Setop: &proto.ApbSetUpdate{Optype: &opType, Rems: element}}
}

func (crdtState SetAWValueState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.Elems = ByteMatrixToElementArray(protobuf.GetSet().GetValue())
	return crdtState
}

func (crdtState SetAWValueState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Set: &proto.ApbGetSetResp{Value: ElementArrayToByteMatrix(crdtState.Elems)}}
}

func (crdtState SetAWLookupState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.HasElem = protobuf.GetPartread().GetSet().GetLookup().GetHas()
	return crdtState
}

func (crdtState SetAWLookupState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Set: &proto.ApbSetPartialReadResp{
		Lookup: &proto.ApbSetLookupReadResp{Has: pb.Bool(crdtState.HasElem)},
	}}}
}

func (crdtState SetAWNElementsState) FromReadResp(protobuf *proto.ApbReadObjectResp) (state State) {
	crdtState.Count = int(protobuf.GetPartread().GetSet().GetNelems().GetCount())
	return crdtState
}

func (crdtState SetAWNElementsState) ToReadResp() (protobuf *proto.ApbReadObjectResp) {
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Set: &proto.ApbSetPartialReadResp{
		Nelems: &proto.ApbSetNElemsReadResp{Count: pb.Int32(int32(crdtState.Count))},
	}}}
}

func (args LookupReadArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	args.Elem = Element(protobuf.GetSet().GetLookup().GetElement())
	return args
}

func (args LookupReadArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Set: &proto.ApbSetPartialRead{Lookup: &proto.ApbSetLookupRead{Element: []byte(args.Elem)}}}
}

func (args GetNElementsArguments) FromPartialRead(protobuf *proto.ApbPartialReadArgs) (readArgs ReadArguments) {
	return args
}

func (args GetNElementsArguments) ToPartialRead() (protobuf *proto.ApbPartialReadArgs) {
	return &proto.ApbPartialReadArgs{Set: &proto.ApbSetPartialRead{Nelems: &proto.ApbSetNElemsRead{}}}
}

func (downOp DownstreamAddAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	adds := protobuf.GetSetOp().GetAdds()
	downOp.Elems = make(map[Element]Unique)
	for _, pairProto := range adds {
		downOp.Elems[Element(pairProto.GetValue())] = Unique(pairProto.GetUnique())
	}
	return downOp
}

func (downOp DownstreamRemoveAll) FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments) {
	rems := protobuf.GetSetOp().GetRems()
	downOp.Elems = make(map[Element]UniqueSet)
	for _, pairProto := range rems {
		downOp.Elems[Element(pairProto.GetValue())] = UInt64ArrayToUniqueSet(pairProto.GetUniques())
	}
	return downOp
}

func (downOp DownstreamAddAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	adds := make([]*proto.ProtoValueUnique, len(downOp.Elems))
	i := 0
	for value, unique := range downOp.Elems {
		adds[i] = &proto.ProtoValueUnique{Value: []byte(value), Unique: pb.Uint64(uint64(unique))}
		i++
	}
	return &proto.ProtoOpDownstream{SetOp: &proto.ProtoSetDownstream{Adds: adds}}
}

func (downOp DownstreamRemoveAll) ToReplicatorObj() (protobuf *proto.ProtoOpDownstream) {
	rems := make([]*proto.ProtoValueUniques, len(downOp.Elems))
	i := 0
	for value, uniques := range downOp.Elems {
		rems[i] = &proto.ProtoValueUniques{Value: []byte(value), Uniques: UniqueSetToUInt64Array(uniques)}
		i++
	}
	return &proto.ProtoOpDownstream{SetOp: &proto.ProtoSetDownstream{Rems: rems}}
}

func (crdt *SetAWCrdt) ToProtoState() (protobuf *proto.ProtoState) {
	protoElems := make([]*proto.ProtoValueUniques, len(crdt.elems))
	i := 0
	for value, uniques := range crdt.elems {
		protoElems[i] = &proto.ProtoValueUniques{Value: []byte(value), Uniques: UniqueSetToUInt64Array(uniques)}
		i++
	}
	return &proto.ProtoState{Awset: &proto.ProtoAWSetState{Elems: protoElems}}
}

func (crdt *SetAWCrdt) FromProtoState(proto *proto.ProtoState, ts *clocksi.Timestamp, replicaID int16) (newCRDT CRDT) {
	protoElems := proto.GetAwset().GetElems()
	elems := make(map[Element]UniqueSet)
	for _, protoElem := range protoElems {
		elems[Element(protoElem.GetValue())] = UInt64ArrayToUniqueSet(protoElem.GetUniques())
	}
	return (&SetAWCrdt{elems: elems}).initializeFromSnapshot(ts, replicaID)
}

func (crdt *SetAWCrdt) GetCRDT() CRDT { return crdt }
