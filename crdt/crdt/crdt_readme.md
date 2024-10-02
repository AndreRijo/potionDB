# Creating a CRDT

## Overview

Specifying a new CRDT and fully supporting it in PotionDB implies modifications/additions to some existing files, as well as creating the file with the code for the CRDT itself. For ease of reference, assume the new CRDT file is named "myCRDT.go".
The following files are involved in defining a new CRDT:

- crdt/myCRDT.go (the new file);
- crdt/crdtProtoLib.go
- proto/antidote.proto
- proto/replicator.proto

In general, for a fully fledged CRDT, the following tasks need to be done:
- Implementing the CRDT itself, namely the CRDT interface defined in crdt.go. This covers the basic funcionalities of creating, reading and updating the CRDT. This is done in crdt/myCRDT.go;
- Adding support for PotionDB's clients to modify and read the CRDT. In short this implies defining protobufs for reading/updating the CRDT (proto/antidote.proto), how to convert between the protobuf representation and the internal one (crdt/myCRDT.go), as well as associate the protobufs reads/updates with the internal reads/updates (crdt/crdtProtoLib.go);
- Adding support for replication of the new CRDT. This implies creating the protobufs that contain enough information to apply the operation in other replicas (proto/replicator.proto), converting between protobuf and the downstream (crdt/myCRDT.go), as well as associating the protobufs to the correct downstreams (crdt/crdtProtoLib.go);
- Supporting reads in the past in the new CRDT. This consists in implementing the InversibleCRDT interface (crdt/inversibleCRDT.go), namely by definning "effects" for each downstream update that represent the exact change in state that occoured. This is done in crdt/myCRDT.go.

Depending on how the CRDT will be used, some steps may not be necessary. For example, if the new CRDT is for internal usage only (e.g., a CRDT to hold some kind of metadata), no modifications are needed in the proto/antidote.proto file.
It is reccomended to read all the information before attempting to implement a CRDT. The steps do not need to be done in order - for example, some developers may find it easier to handle the CRDT implementation at the same time as the support for reads in the past.
Note: After modifying the .proto files, it is necessary to recompile them again in the terminal. This can be done with the following command:

```
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest //To install the protobuf compiler, only needs to be executed once per machine.
protoc -I=. --go_out=. ./nameOfTheProtoFile.proto
```

## CRDT implementation

To implement a new CRDT it is necessary to define a new file (e.g., crdt/myCRDT.go) and implement the CRDT interface defined in crdt/crdt.go, which is as follows:

```
Initialize(startTs clocksi.Timestamp, replicaID int16) (newCrdt CRDT)
Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State)
Update(args UpdateArguments) (downstreamArgs DownstreamArguments)
Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments)
IsOperationWellTyped(args UpdateArguments) (ok bool, err error)
GetCRDTType() proto.CRDTType
```

*Initialize(startTs clocksi.Timestamp, replicaID int16) (newCrdt CRDT)*
Initialize any internal state or structure of the CRDT (e.g., initialize maps used to store data). Returns an instance of the CRDT.

*Read(args ReadArguments, updsNotYetApplied []\*UpdateArguments) (state State)*
Reads the current state of the CRDT and returns it. Each CRDT must support to, at least, read its full state (e.g., in a set, return the elements in the set). This corresponds to when args is a StateReadArguments{}. Other read operations can be defined in the CRDT by implementing the ReadArguments interface (e.g., in a set, one may define a read that verifies if a given object is in the set). It is necessary to define structs implementing the interface State to hold the result of each type of read (e.g., one state for the normal read, one state for a specific type of read). Finally, updsNotYetApplied contains update operations previously executed in the context of the ongoing transaction for this CRDT - their effects must be considered when executing the read.
Usually, it is easier for this method to only identify the type of read being executed (e.g., using a switch) and then define sub-methods for each type of read.

*Update(args UpdateArguments) (downstreamArgs DownstreamArguments)*
Executes the update phase of an update operation (i.e., the part that only occours in the local replica). The goal is to prepare the arguments for downstream that will then be used by every replica to modify the state. It is necessary to define the update operations supported by the CRDT, as well as the downstream operations. In this phase, the CRDT's state is not modified.
Usually, it is easier for this method to only identify the type of update being executed (e.g., using a switch) and then defining sub-methods for each type of update.

*Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments)*
Applies the operation, modifying the state of the CRDT. DownstreamArgs contains the necessary information to apply the operation. UpdTs contains the clock on which this update is happening on (this is relevant mostly for version management). OtherDownstreamArgs is used only in NuCRDTs (Non-Uniform CRDTs), which may need to generate a new downstream operation when applying this operation.
Usually, it is easier for this method to only identify the type of downstream being executed (e.g., using a switch) and then defining sub-methods for each type of downstream.

*IsOperationWellTyped(args UpdateArguments) (ok bool, err error)*
Unused. Implement with a "return true, nil".

*GetCRDTType() proto.CRDTType*
Returns the type of the CRDT. CRDT types are defined in an enum on proto/antidote.proto.

To ease the process, check how a simple CRDT (e.g. counter, register) is implemented and use it as a guideline. If a more complex example is needed, check the setAW or orMap.

## Client interaction

To add support for PotionDB's clients to read or update the object, it is necessary to add support for them in the public interface. In PotionDB's case, this is done in three parts:
- Defining the protobufs that will be used by the client to read/update the object (proto/antidote.proto);
- Converting between protobufs and the internal CRDT operations, both ways (crdt/myCRDT.go);
- Associating the protobufs with their internal representation (crdt/crdtProtoLib.go).

#### Defining protobufs (proto/antidote.proto)

In proto/antidote.proto, the following steps need to be done:
- Add a new CRDT type representing the new CRDT to the enum "CRDT_type";
- Define one or more protobufs to represent the updates supported by the CRDT, including the necessary arguments that the client needs to supply for the update(s). Usually protobuf updates are named Apb*MyCrdt*Update;
- Define the reply to the default read operation (the one that reads the full state). Usually the reply protobuf is named ApbGet*MyCrdt*Resp;
- Associate the new update(s) and read(s) to, respectively, ApbUpdateOperation and ApbReadObjectResp. Add a new line "linking" to the protobufs created in the steps above. For example, on ApbReadObjectResp, add the line "optional ApbGet*MyCrdt*Resp mycrdt = 10". The number can be any number not yet used, but numbers below 16 are more efficiently handled by protobufs.

Adding reads other than the generic one, while relatively simple, require a few different steps. For ease of reference, we call these reads of "partial reads", since often they do not read/return the whole state. All the protobufs related with partial reads are at the end of proto/antidote.proto file, after the comment "//Partial reading".
The structure assumes that for each CRDT, more than one partial read may be defined. The following steps are necessary independently of the number of partial reads defined:
- Define a protobuf that will hold all the read types of the CRDT and add it to ApbPartialReadArgs. The name convention of this protobuf is Apb*MyCrdt*PartialRead;
- Define a protobuf that will hold all the read **reply** types of the CRDT and add it to ApbPartialReadResp. The name convention of this protobuf is Apb*MyCrdt*PartialReadResp.

Then, for each partial read, do the following steps:
- Add the new read type to the enum "READ_Type";
- Define a protobuf representing the read operation and its arguments. The name convention is Apb*MyCrdtReadName*Read. Add this new protobuf to Apb*MyCrdt*PartialRead;
- Define a protobuf representing the result of the reply of the read operation. The name convention is Apb*MyCrdtReadName*ReadResp. Add this new protobuf to Apb*MyCrdt*PartialReadResp.

Checking an example and using it as a reference can make the process smoother. A good example is the set. 

#### Conversion between protobuf and internal (crdt/myCRDT.go)

Here we implement the interfaces ProtoUpd, ProtoRead and ProtoState. ProtoRead is only necessary to define for partial reads.

The methods of the interfaces convert between the protobuf representation and the CRDT representation of, e.g., an update. The conversion is needed both ways. For example, in a counter CRDT, Increment must implement the ProtoUpd interface, thus it needs to implement two methods: FromUpdateObject() and ToUpdateObject(). The former converts from a protobuf to Increment, while the latter converts from Increment to protobuf. The same must be done for any partial read's arguments (ProtoRead) and for the states (ProtoState).

For examples, please check the counter and the set.

#### Associating protobufs and internal (crdt/crdtProtoLib.go)

Here we need to do the "connection" between the protobufs and the CRDT operations. In practice, this implies that, for a given CRDT type and operation/read type, to know which CRDT's method must be called to do the conversion.

In practice, the following methods need to be modified:
- UpdateProtoToAntidoteUpdate: given the CRDT type and an update protobuf received, identify which CRDT update struct should FromUpdateObject be called upon. E.g., for a counter, do Increment{}.FromUpdateObject(protobuf);
- PartialReadOpToAntidoteRead (this one is only needed if supporting partial reads): given a CRDT type and a read type, identify the right CRDT read arguments struct and call FromPartialRead(protobuf);
- ReadRespProtoToAntidoteState (for full reads): given a CRDT type, identify the right CRDT state struct and call FromReadResp(protobuf);
- partialReadRespProtoToAntidoteState (for partial reads): given a CRDT type and a read type, identify the right CRDT state struct and call FromReadResp(protobuf).

This part is easier done than explained, please check the code and it should be relatively intuitive :)

## Replication

Similarly to what is done for Client interaction, to support replication we can also group the tasks in three groups:
- Defining the protobufs that will be used to contain the data necessary for the downstream operations (proto/replicator.proto);
- Converting between protobufs and the downstream operations, both ways (crdt/myCRDT.go);
- Associating the protobufs with the downstream operations (crdt/crdtProtoLib.go).

#### Defining protobufs (proto/replicator.proto)

In proto/replicator.proto, the following steps need to be done:
- Create a protobuf defining a downstream operation for the new CRDT. The name convention is Proto*MyCrdt*Downstream. This protobuf will "group" all the downstream operations of the new CRDT;
- Add the protobuf defined above to ProtoOpDownstream;
- For each type of downstream operation, define a protobuf that holds the necessary data to correctly execute the downstream operation. If there's only one type, then the Proto*MyCrdt*Downstream defined in the first step can be used for this;
- Assuming a separate protobuf was created for each downstream, add each protobuf defined above to Proto*MyCrdt*Downstream;

The idea is to, for each CRDT, have one protobuf that groups all types of downstream operations, and then have one protobuf for each type. 

#### Conversion between protobuf and internal (crdt/myCRDT.go)

In this file we need to implement the ProtoDownUpd interface for each downstream operation. The interface contains two methods:
- ToReplicatorObj() (protobuf *proto.ProtoOpDownstream): converts from a downstream operation to its protobuf;
- FromReplicatorObj(protobuf *proto.ProtoOpDownstream) (downArgs DownstreamArguments): converts a protobuf to the correct downstream operation.
 
As usual, the counter and set are good examples.

#### Associating protobufs and internal (crdt/crdtProtoLib.go)

In this file we need to do the "connection" between the protobufs and the downstream operations, in order to call the right methods defined in crdt/myCRDT.go.

In practice, only one method needs to be modified, DownstreamProtoToAntidoteDownstream. This method, for a given protobuf and CRDT type, identifies the right downstream struct to call FromReplicatorObj upon. E.g., for a register, DownstreamSetValue{}.FromReplicatorObj(protobuf).

## Reads in the past/Version Management

In PotionDB, due to the possibility of multiple transactions executing concurrently, as well as transactions commiting while others are ongoing, it is necessary to support reads in the past. In PotionDB, this is done by reconstructing any older version on demand, by keeping track of the effects of each update applied in a given CRDT and then reverting them.

The first change that is necessary in the CRDT implementation is to create an instance of "*genericInversibleCRDT" and store it in the CRDT's struct. In the Initialize() method of the CRDT, the genericInversibleCRDT must be initialized too. For example:

```
func (crdt *MyCRDT) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
    return &MyCRDT{
        genericInversibleCRDT: (&genericInversibleCRDT{}).initialize(startTs),
        ... //other fields of MyCRDT
    }
}
```

The second step consists in defining the effects. An effect represents a change to the state. The idea is that, by looking at an effect, it should be immediate how to "undo" the change in the state caused by the operation. A possible approach is to, for each operation, look at the different ways the state can change. For example, in a counter, when an increment is executed, the state increases by a given value - thus, an effect representing the increase in value is enough. However, for the put operation in a map, the state can change in two ways: either the key is new and thus the map was empty before (new value "effect") or the key already existed and the existing value is replacted (replace value "effect"). In the map it is necessary to distinguish both effects, as when we want to undo a put, we need to know if the key should be removed or if we should put back the old value.

In short, to specify effects, do the following steps:
- Pick a downstream operation, analyze it's code. Try to figure out the different ways the state change, and which information is needed to "undo" the change;
- Define a new struct for each different "type" of change - this makes it easier to implement the undo of the change;
- In the method of your downstream operation, initialize the correct effect in the place where the state change occours. If no state change occours, the struct NoEffect{} can be used;
- At the end of the Downstream() method, call crdt.addToHistory(&updTs, &downstreamArgs, effect).

The third step consists in implementing the interface InversibleCRDT defined in crdt/inversibleCrdt, which is as follows:

```
Copy() (copyCRDT InversibleCRDT)

RebuildCRDTToVersion(targetTs clocksi.Timestamp)

undoEffect(effect *Effect)

reapplyOp(updArgs DownstreamArguments) (effect *Effect)

notifyRebuiltComplete(currTs *clocksi.Timestamp)
```

*Copy() (copyCRDT InversibleCRDT)*
Creates a new CRDT whose contents are a **deep** copy of the original CRDT. It is essential to ensure that anything that is a pointer (including maps and slices) must point to different addresses in the copy relative to the original (i.e., create a new map/slice/pointer and copy contents manually).

*RebuildCRDTToVersion(targetTs clocksi.Timestamp)*
Calls genericInversibleCRDT.rebuildCRDTToVersion(targetTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete). In practice, it associates the implementation of MyCRDT's methods to the code of genericInversibleCRDT.

*undoEffect(effect \*Effect)*
For a given effect, undoes the effect by changing the CRDT's state (e.g., in a counter, undoing an increment does a decrement of the same value). This method will need to handle the effects defined for this type of CRDT.

*reapplyOp(updArgs DownstreamArguments) (effect \*Effect)*
For a given downstream operation, applies it. The algorithm that rebuilds the version may need to call this method at times. To implement, call the right downstream method and return its effect.

*notifyRebuiltComplete(currTs \*clocksi.Timestamp)*
Utility method that notifies the CRDT that the intended version has finished being built. Most implementations do not need to do anything with this method. The purpose of this is for optimizations, in case some cleaning or operations are more efficient to be done at the end of the process instead of on each undoEffect.

The counter is a relatively simple example to follow, while the topK is a more complex one which has multiple effects per downstream operation.
