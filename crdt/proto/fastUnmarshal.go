package proto

import (
	binary "encoding/binary"
	fmt "fmt"
	io "io"
	math "math"
	unsafe "unsafe"

	"github.com/AndreRijo/go-tools/src/tools"
	protohelpers "github.com/planetscale/vtprotobuf/protohelpers"
)

//Index:
//S2S
//Reading functions
//Update functions

// ApbTxnProperties is ignored by PotionDB, thus we re-use this protobuf.
var txnP *ApbTxnProperties = &ApbTxnProperties{ReadWrite: new(uint32), RedBlue: new(uint32)}

const updBufStartSize = 5
const updBufMapStartSize = 20      //RWEmbMaps tend to be very frequently used, specially due to their embeddable nature.
const updBufMultiUpdStartSize = 10 //MultiUpds are also somewhat frequent as they can be used by any CRDT.
const updBufInnerUpdsStartSize = 5 //For things like MultiArray, MapCounter, etc, which have several possible of updates.

type PbBuffers struct {
	//Hold by the client directly.
	StaticRead        *ApbStaticRead
	StaticReadObjects *ApbStaticReadObjects
	S2SReq            *S2SWrapper
	S2SReply          *S2SWrapperReply
	StaticUpd         *ApbStaticUpdateObjects

	//fastUnmarshal.go managed.
	//ApbPartialReadArgs
	/*PReadSet          *ApbSetPartialRead
	PReadMap          *ApbMapPartialRead
	PReadTopk         *ApbTopkPartialRead
	PReadAvg          *ApbAvgPartialRead
	PReadProcess      *ApbProcessRead
	PReadPairCounter  *ApbPairCounterPartialRead
	PReadArrayCounter *ApbArrayCounterPartialRead
	PReadArrayFloat   *ApbArrayFloatPartialRead
	PReadMultiArray   *ApbMultiArrayPartialRead
	PReadMVReg        *ApbMVRegPartialRead
	PReadDate         *ApbDatePartialRead
	PReadCompactArray *ApbCompactArrayPartialRead
	PReadStringArray  *ApbStringArrayPartialRead
	PReadByteArray    *ApbByteArrayPartialRead
	PReadMapCounter   *ApbMapCounterPartialRead*/
	PReadSet          *ApbPartialReadArgs_Set
	PReadMap          *ApbPartialReadArgs_Map
	PReadTopk         *ApbPartialReadArgs_Topk
	PReadAvg          *ApbPartialReadArgs_Avg
	PReadProcess      *ApbPartialReadArgs_Process
	PReadPairCounter  *ApbPartialReadArgs_Paircounter
	PReadArrayCounter *ApbPartialReadArgs_Arraycounter
	PReadArrayFloat   *ApbPartialReadArgs_Arrayfloat
	PReadMultiArray   *ApbPartialReadArgs_Multiarray
	PReadMVReg        *ApbPartialReadArgs_Mvreg
	PReadDate         *ApbPartialReadArgs_Date
	PReadCompactArray *ApbPartialReadArgs_Compactarray
	PReadStringArray  *ApbPartialReadArgs_Stringarray
	PReadByteArray    *ApbPartialReadArgs_Bytearray
	PReadMapCounter   *ApbPartialReadArgs_Mapcounter

	//ApbMapPartialRead
	PMapGetValue         *ApbMapPartialRead_Getvalue
	PMapHasKey           *ApbMapPartialRead_Haskey
	PMapGetKeys          *ApbMapPartialRead_Getkeys
	PMapGetValues        *ApbMapPartialRead_Getvalues
	PMapGetAllValues     *ApbMapPartialRead_Getallvalues
	PMapCond             *ApbMapPartialRead_Condread
	PMapExcept           *ApbMapPartialRead_Exceptread
	PMapAllCond          *ApbMapPartialRead_Condallread
	PMapExceptCond       *ApbMapPartialRead_Exceptcondread
	PMapAggregateRead    *ApbMapPartialRead_Aggregateread
	PMapExceptSingle     *ApbMapPartialRead_Exceptsingleread
	PMapExceptCondSingle *ApbMapPartialRead_Exceptcondsingleread

	ReturnPReadSet          *ApbPartialReadArgs_Set
	ReturnPReadMap          *ApbPartialReadArgs_Map
	ReturnPReadTopk         *ApbPartialReadArgs_Topk
	ReturnPReadAvg          *ApbPartialReadArgs_Avg
	ReturnPReadProcess      *ApbPartialReadArgs_Process
	ReturnPReadPairCounter  *ApbPartialReadArgs_Paircounter
	ReturnPReadArrayCounter *ApbPartialReadArgs_Arraycounter
	ReturnPReadArrayFloat   *ApbPartialReadArgs_Arrayfloat
	ReturnPReadMultiArray   *ApbPartialReadArgs_Multiarray
	ReturnPReadMVReg        *ApbPartialReadArgs_Mvreg
	ReturnPReadDate         *ApbPartialReadArgs_Date
	ReturnPReadCompactArray *ApbPartialReadArgs_Compactarray
	ReturnPReadStringArray  *ApbPartialReadArgs_Stringarray
	ReturnPReadByteArray    *ApbPartialReadArgs_Bytearray
	ReturnPReadMapCounter   *ApbPartialReadArgs_Mapcounter

	//ApbMapPartialRead
	ReturnPMapGetValue         *ApbMapPartialRead_Getvalue
	ReturnPMapHasKey           *ApbMapPartialRead_Haskey
	ReturnPMapGetKeys          *ApbMapPartialRead_Getkeys
	ReturnPMapGetValues        *ApbMapPartialRead_Getvalues
	ReturnPMapGetAllValues     *ApbMapPartialRead_Getallvalues
	ReturnPMapCond             *ApbMapPartialRead_Condread
	ReturnPMapExcept           *ApbMapPartialRead_Exceptread
	ReturnPMapAllCond          *ApbMapPartialRead_Condallread
	ReturnPMapExceptCond       *ApbMapPartialRead_Exceptcondread
	ReturnPMapAggregateRead    *ApbMapPartialRead_Aggregateread
	ReturnPMapExceptSingle     *ApbMapPartialRead_Exceptsingleread
	ReturnPMapExceptCondSingle *ApbMapPartialRead_Exceptcondsingleread

	//Update operations
	UpdCounters      tools.SliceWithHideable[*ApbUpdateOperation_Counterop]
	UpdSets          tools.SliceWithHideable[*ApbUpdateOperation_Setop]
	UpdRegs          tools.SliceWithHideable[*ApbUpdateOperation_Regop]
	UpdBCounters     tools.SliceWithHideable[*ApbUpdateOperation_Bcounterop]
	UpdMaps          tools.SliceWithHideable[*ApbUpdateOperation_Mapop]
	UpdFlags         tools.SliceWithHideable[*ApbUpdateOperation_Flagop]
	UpdMaxMins       tools.SliceWithHideable[*ApbUpdateOperation_Maxminop]
	UpdTopKRmvs      tools.SliceWithHideable[*ApbUpdateOperation_Topkrmvop]
	UpdLeaderboards  tools.SliceWithHideable[*ApbUpdateOperation_Leaderboardop]
	UpdAvgs          tools.SliceWithHideable[*ApbUpdateOperation_Avgop]
	UpdCounterFloats tools.SliceWithHideable[*ApbUpdateOperation_Counterfloatop]
	UpdPairCounters  tools.SliceWithHideable[*ApbUpdateOperation_Paircounterop]
	UpdArrayCounters tools.SliceWithHideable[*ApbUpdateOperation_Arraycounterop]
	UpdArrayFloats   tools.SliceWithHideable[*ApbUpdateOperation_Arrayfloatop]
	UpdMultiArrays   tools.SliceWithHideable[*ApbUpdateOperation_Multiarrayop]
	UpdDates         tools.SliceWithHideable[*ApbUpdateOperation_Dateop]
	UpdCompactArrays tools.SliceWithHideable[*ApbUpdateOperation_Compactarrayop]
	UpdStringArrays  tools.SliceWithHideable[*ApbUpdateOperation_Stringarrayop]
	UpdByteArrays    tools.SliceWithHideable[*ApbUpdateOperation_Bytearrayop]
	UpdMapCounters   tools.SliceWithHideable[*ApbUpdateOperation_Mapcounterop]
	UpdMultiOps      tools.SliceWithHideable[*ApbUpdateOperation_Multiupdop]
}

func (pbBuf *PbBuffers) FullInit() {
	pbBuf.ReadInit()
	pbBuf.S2SInit()
	pbBuf.UpdateInit()
}

func (pbBuf *PbBuffers) ReadInit() {
	pbBuf.StaticRead = &ApbStaticRead{}
	pbBuf.StaticReadObjects = &ApbStaticReadObjects{}

	pbBuf.PReadSet = &ApbPartialReadArgs_Set{Set: &ApbSetPartialRead{}}
	pbBuf.PReadMap = &ApbPartialReadArgs_Map{Map: &ApbMapPartialRead{}}
	pbBuf.PReadTopk = &ApbPartialReadArgs_Topk{Topk: &ApbTopkPartialRead{}}
	pbBuf.PReadAvg = &ApbPartialReadArgs_Avg{Avg: &ApbAvgPartialRead{}}
	pbBuf.PReadProcess = &ApbPartialReadArgs_Process{Process: &ApbProcessRead{}}
	pbBuf.PReadPairCounter = &ApbPartialReadArgs_Paircounter{Paircounter: &ApbPairCounterPartialRead{}}
	pbBuf.PReadArrayCounter = &ApbPartialReadArgs_Arraycounter{Arraycounter: &ApbArrayCounterPartialRead{}}
	pbBuf.PReadArrayFloat = &ApbPartialReadArgs_Arrayfloat{Arrayfloat: &ApbArrayFloatPartialRead{}}
	pbBuf.PReadMultiArray = &ApbPartialReadArgs_Multiarray{Multiarray: &ApbMultiArrayPartialRead{}}
	pbBuf.PReadMVReg = &ApbPartialReadArgs_Mvreg{Mvreg: &ApbMVRegPartialRead{}}
	pbBuf.PReadCompactArray = &ApbPartialReadArgs_Compactarray{Compactarray: &ApbCompactArrayPartialRead{}}
	pbBuf.PReadStringArray = &ApbPartialReadArgs_Stringarray{Stringarray: &ApbStringArrayPartialRead{}}
	pbBuf.PReadByteArray = &ApbPartialReadArgs_Bytearray{Bytearray: &ApbByteArrayPartialRead{}}
	pbBuf.PReadMapCounter = &ApbPartialReadArgs_Mapcounter{Mapcounter: &ApbMapCounterPartialRead{}}

	pbBuf.PMapGetValue = &ApbMapPartialRead_Getvalue{Getvalue: &ApbMapGetValueRead{}}
	pbBuf.PMapHasKey = &ApbMapPartialRead_Haskey{Haskey: &ApbMapHasKeyRead{}}
	pbBuf.PMapGetKeys = &ApbMapPartialRead_Getkeys{Getkeys: &ApbMapGetKeysRead{}}
	pbBuf.PMapGetValues = &ApbMapPartialRead_Getvalues{Getvalues: &ApbMapGetValuesRead{}}
	pbBuf.PMapGetAllValues = &ApbMapPartialRead_Getallvalues{Getallvalues: &ApbMapGetAllValuesRead{}}
	pbBuf.PMapCond = &ApbMapPartialRead_Condread{Condread: &ApbMapCondRead{}}
	pbBuf.PMapExcept = &ApbMapPartialRead_Exceptread{Exceptread: &ApbMapExceptRead{}}
	pbBuf.PMapAllCond = &ApbMapPartialRead_Condallread{Condallread: &ApbMapAllCondRead{}}
	pbBuf.PMapExceptCond = &ApbMapPartialRead_Exceptcondread{Exceptcondread: &ApbMapExceptCondRead{}}
	pbBuf.PMapAggregateRead = &ApbMapPartialRead_Aggregateread{Aggregateread: &ApbMapAggregateRead{}}
	pbBuf.PMapExceptSingle = &ApbMapPartialRead_Exceptsingleread{Exceptsingleread: &ApbMapExceptSingleRead{}}
	pbBuf.PMapExceptCondSingle = &ApbMapPartialRead_Exceptcondsingleread{Exceptcondsingleread: &ApbMapExceptCondSingleRead{}}
}

func (pbBuf *PbBuffers) S2SInit() {
	pbBuf.S2SReq = &S2SWrapper{ClientID: new(uint64), MsgID: new(WrapperType)}
	pbBuf.S2SReply = &S2SWrapperReply{ClientID: new(uint64), MsgID: new(WrapperType)}
}

func (pbBuf *PbBuffers) UpdateInit() {
	pbBuf.StaticUpd = &ApbStaticUpdateObjects{}

	pbBuf.UpdCounters = tools.NewSliceWithHideable[*ApbUpdateOperation_Counterop](updBufStartSize)
	pbBuf.UpdSets = tools.NewSliceWithHideable[*ApbUpdateOperation_Setop](updBufStartSize)
	pbBuf.UpdRegs = tools.NewSliceWithHideable[*ApbUpdateOperation_Regop](updBufStartSize)
	pbBuf.UpdBCounters = tools.NewSliceWithHideable[*ApbUpdateOperation_Bcounterop](updBufStartSize)
	pbBuf.UpdMaps = tools.NewSliceWithHideable[*ApbUpdateOperation_Mapop](updBufMapStartSize)
	pbBuf.UpdFlags = tools.NewSliceWithHideable[*ApbUpdateOperation_Flagop](updBufStartSize)
	pbBuf.UpdMaxMins = tools.NewSliceWithHideable[*ApbUpdateOperation_Maxminop](updBufStartSize)
	pbBuf.UpdTopKRmvs = tools.NewSliceWithHideable[*ApbUpdateOperation_Topkrmvop](updBufStartSize)
	pbBuf.UpdLeaderboards = tools.NewSliceWithHideable[*ApbUpdateOperation_Leaderboardop](updBufStartSize)
	pbBuf.UpdAvgs = tools.NewSliceWithHideable[*ApbUpdateOperation_Avgop](updBufStartSize)
	pbBuf.UpdCounterFloats = tools.NewSliceWithHideable[*ApbUpdateOperation_Counterfloatop](updBufStartSize)
	pbBuf.UpdPairCounters = tools.NewSliceWithHideable[*ApbUpdateOperation_Paircounterop](updBufStartSize)
	pbBuf.UpdArrayCounters = tools.NewSliceWithHideable[*ApbUpdateOperation_Arraycounterop](updBufStartSize)
	pbBuf.UpdArrayFloats = tools.NewSliceWithHideable[*ApbUpdateOperation_Arrayfloatop](updBufStartSize)
	pbBuf.UpdMultiArrays = tools.NewSliceWithHideable[*ApbUpdateOperation_Multiarrayop](updBufStartSize)
	pbBuf.UpdDates = tools.NewSliceWithHideable[*ApbUpdateOperation_Dateop](updBufStartSize)
	pbBuf.UpdCompactArrays = tools.NewSliceWithHideable[*ApbUpdateOperation_Compactarrayop](updBufStartSize)
	pbBuf.UpdStringArrays = tools.NewSliceWithHideable[*ApbUpdateOperation_Stringarrayop](updBufMapStartSize)
	pbBuf.UpdByteArrays = tools.NewSliceWithHideable[*ApbUpdateOperation_Bytearrayop](updBufStartSize)
	pbBuf.UpdMapCounters = tools.NewSliceWithHideable[*ApbUpdateOperation_Mapcounterop](updBufStartSize)
	pbBuf.UpdMultiOps = tools.NewSliceWithHideable[*ApbUpdateOperation_Multiupdop](updBufMultiUpdStartSize)
}

func (pbBuf *PbBuffers) ReuseReadProtos() {
	if pbBuf.ReturnPReadSet != nil {
		*pbBuf.ReturnPReadSet.Set = ApbSetPartialRead{}
		pbBuf.PReadSet, pbBuf.ReturnPReadSet = pbBuf.ReturnPReadSet, nil
	}
	if pbBuf.ReturnPReadMap != nil {
		//*pbBuf.ReturnPReadMap.Map = ApbMapPartialRead{}
		pbBuf.PReadMap, pbBuf.ReturnPReadMap = pbBuf.ReturnPReadMap, nil
	}
	if pbBuf.ReturnPReadTopk != nil {
		*pbBuf.ReturnPReadTopk.Topk = ApbTopkPartialRead{}
		pbBuf.PReadTopk, pbBuf.ReturnPReadTopk = pbBuf.ReturnPReadTopk, nil
	}
	if pbBuf.ReturnPReadAvg != nil { //Can fully re-use as it is only one type of read.
		pbBuf.PReadAvg, pbBuf.ReturnPReadAvg = pbBuf.ReturnPReadAvg, nil
	}
	if pbBuf.ReturnPReadProcess != nil {
		*pbBuf.ReturnPReadProcess.Process = ApbProcessRead{}
		pbBuf.PReadProcess, pbBuf.ReturnPReadProcess = pbBuf.ReturnPReadProcess, nil
	}
	if pbBuf.ReturnPReadPairCounter != nil {
		*pbBuf.ReturnPReadPairCounter.Paircounter = ApbPairCounterPartialRead{}
		pbBuf.PReadPairCounter, pbBuf.ReturnPReadPairCounter = pbBuf.ReturnPReadPairCounter, nil
	}
	if pbBuf.ReturnPReadArrayCounter != nil {
		*pbBuf.ReturnPReadArrayCounter.Arraycounter = ApbArrayCounterPartialRead{}
		pbBuf.PReadArrayCounter, pbBuf.ReturnPReadArrayCounter = pbBuf.ReturnPReadArrayCounter, nil
	}
	if pbBuf.ReturnPReadArrayFloat != nil {
		*pbBuf.ReturnPReadArrayFloat.Arrayfloat = ApbArrayFloatPartialRead{}
		pbBuf.PReadArrayFloat, pbBuf.ReturnPReadArrayFloat = pbBuf.ReturnPReadArrayFloat, nil
	}
	if pbBuf.ReturnPReadMultiArray != nil {
		*pbBuf.ReturnPReadMultiArray.Multiarray = ApbMultiArrayPartialRead{}
		pbBuf.PReadMultiArray, pbBuf.ReturnPReadMultiArray = pbBuf.ReturnPReadMultiArray, nil
	}
	if pbBuf.ReturnPReadMVReg != nil { //Can fully re-use as it's only one option.
		pbBuf.PReadMVReg, pbBuf.ReturnPReadMVReg = pbBuf.ReturnPReadMVReg, nil
	}
	if pbBuf.ReturnPReadDate != nil {
		*pbBuf.ReturnPReadDate.Date = ApbDatePartialRead{}
		pbBuf.PReadDate, pbBuf.ReturnPReadDate = pbBuf.ReturnPReadDate, nil
	}
	if pbBuf.ReturnPReadCompactArray != nil {
		*pbBuf.ReturnPReadCompactArray.Compactarray = ApbCompactArrayPartialRead{}
		pbBuf.PReadCompactArray, pbBuf.ReturnPReadCompactArray = pbBuf.ReturnPReadCompactArray, nil
	}
	if pbBuf.ReturnPReadStringArray != nil {
		*pbBuf.ReturnPReadStringArray.Stringarray = ApbStringArrayPartialRead{}
		pbBuf.PReadStringArray, pbBuf.ReturnPReadStringArray = pbBuf.ReturnPReadStringArray, nil
	}
	if pbBuf.ReturnPReadByteArray != nil {
		*pbBuf.ReturnPReadByteArray.Bytearray = ApbByteArrayPartialRead{}
		pbBuf.PReadByteArray, pbBuf.ReturnPReadByteArray = pbBuf.ReturnPReadByteArray, nil
	}
	if pbBuf.ReturnPReadMapCounter != nil {
		*pbBuf.ReturnPReadMapCounter.Mapcounter = ApbMapCounterPartialRead{}
		pbBuf.PReadMapCounter, pbBuf.ReturnPReadMapCounter = pbBuf.ReturnPReadMapCounter, nil
	}

	if pbBuf.ReturnPMapGetValue != nil {
		pbBuf.PMapGetValue, pbBuf.ReturnPMapGetValue = pbBuf.ReturnPMapGetValue, nil
	}
	if pbBuf.ReturnPMapGetKeys != nil {
		pbBuf.PMapGetKeys, pbBuf.ReturnPMapGetKeys = pbBuf.ReturnPMapGetKeys, nil
	}
	if pbBuf.ReturnPMapGetValues != nil {
		pbBuf.PMapGetValues, pbBuf.ReturnPMapGetValues = pbBuf.ReturnPMapGetValues, nil
	}
	if pbBuf.ReturnPMapGetAllValues != nil {
		pbBuf.PMapGetAllValues, pbBuf.ReturnPMapGetAllValues = pbBuf.ReturnPMapGetAllValues, nil
	}
	if pbBuf.ReturnPMapCond != nil {
		pbBuf.PMapCond, pbBuf.ReturnPMapCond = pbBuf.ReturnPMapCond, nil
	}
	if pbBuf.ReturnPMapExcept != nil {
		pbBuf.PMapExcept, pbBuf.ReturnPMapExcept = pbBuf.ReturnPMapExcept, nil
	}
	if pbBuf.ReturnPMapAllCond != nil {
		pbBuf.PMapAllCond, pbBuf.ReturnPMapAllCond = pbBuf.ReturnPMapAllCond, nil
	}
	if pbBuf.ReturnPMapExceptCond != nil {
		pbBuf.PMapExceptCond, pbBuf.ReturnPMapExceptCond = pbBuf.ReturnPMapExceptCond, nil
	}
	if pbBuf.ReturnPMapAggregateRead != nil {
		pbBuf.PMapAggregateRead, pbBuf.ReturnPMapAggregateRead = pbBuf.ReturnPMapAggregateRead, nil
	}
	if pbBuf.ReturnPMapExceptSingle != nil {
		pbBuf.PMapExceptSingle, pbBuf.ReturnPMapExceptSingle = pbBuf.ReturnPMapExceptSingle, nil
	}
	if pbBuf.ReturnPMapExceptCondSingle != nil {
		pbBuf.PMapExceptCondSingle, pbBuf.ReturnPMapExceptCondSingle = pbBuf.ReturnPMapExceptCondSingle, nil
	}
}

func (pbBuf *PbBuffers) ReuseUpdateProtos() {
	pbBuf.UpdCounters.UnhideAll()
	pbBuf.UpdSets.UnhideAll()
	pbBuf.UpdRegs.UnhideAll()
	pbBuf.UpdBCounters.UnhideAll()
	pbBuf.UpdMaps.UnhideAll()
	pbBuf.UpdFlags.UnhideAll()
	pbBuf.UpdMaxMins.UnhideAll()
	pbBuf.UpdTopKRmvs.UnhideAll()
	pbBuf.UpdLeaderboards.UnhideAll()
	pbBuf.UpdAvgs.UnhideAll()
	pbBuf.UpdCounterFloats.UnhideAll()
	pbBuf.UpdPairCounters.UnhideAll()
	pbBuf.UpdArrayCounters.UnhideAll()
	pbBuf.UpdArrayFloats.UnhideAll()
	pbBuf.UpdMultiArrays.UnhideAll()
	pbBuf.UpdDates.UnhideAll()
	pbBuf.UpdCompactArrays.UnhideAll()
	pbBuf.UpdStringArrays.UnhideAll()
	pbBuf.UpdByteArrays.UnhideAll()
	pbBuf.UpdMapCounters.UnhideAll()
	pbBuf.UpdMultiOps.UnhideAll()
}

//S2S

//Implements unmarshalling for top-level protobuf messages, allowing some re-usage of pointers and/or buffers.
//Note that this means any change to protobufs will lead to compilation errors
//(But hey - regardless that already happens inside the CRDT package, with the functions to convert to protobufs :)))

//All the code here is copied from the code generated by VTProtobuf, but with some error checks removed + attempting to re-use buffers whenever possible.

//Note: I should continue this after I change the messages to proto3.

//Fast marshalling is currently supported by all the top level reading protobufs, as well as partial map reads.
//The first layer of inside map reads also support this, albeit conditional reads only have partial support (i.e., it'll mostly resort to non-reuse.)

// Partially optimized. We re-use the S2SWrapper fully.
// All protobufs internally are unmarshalled without resorting to unsafe, as dAtA may be re-used before the message is fully processed.
// Pre-condition: m.ClientID and m.MsgID must already be alloced.
func (m *S2SWrapper) UnmarshalVTOptSafeReuse(dAtA []byte) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.ClientID = v
			hasFields[0] |= uint64(0x00000001)
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.StaticReadObjs == nil {
				m.StaticReadObjs = &ApbStaticReadObjects{}
			}
			if err := m.StaticReadObjs.UnmarshalVTSafeReuse(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.StaticRead == nil {
				m.StaticRead = &ApbStaticRead{}
			}
			if err := m.StaticRead.UnmarshalVTSafeReuse(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.StaticUpd == nil {
				m.StaticUpd = &ApbStaticUpdateObjects{}
			}
			//Cannot use unsafe with updates.
			//fmt.Printf("[FastUnmarshall][S2SWrapper]Calling UnmarshallVT (safe) for static updates.\n")
			if err := m.StaticUpd.UnmarshalVTSafeReuse(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 5:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.StartTxn == nil {
				m.StartTxn = &ApbStartTransaction{}
			} else {
				*m.StartTxn = ApbStartTransaction{}
			}
			if err := m.StartTxn.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 6:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.ReadObjs == nil {
				m.ReadObjs = &ApbReadObjects{}
			} else {
				*m.ReadObjs = ApbReadObjects{}
			}
			if err := m.ReadObjs.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 7:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Read == nil {
				m.Read = &ApbRead{}
			} else {
				*m.Read = ApbRead{}
			}
			if err := m.Read.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 8:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Upd == nil {
				m.Upd = &ApbUpdateObjects{}
			} else {
				*m.Upd = ApbUpdateObjects{}
			}
			//Cannot use unsafe with updates.
			//fmt.Printf("[FastUnmarshall][S2SWrapper]Calling UnmarshallVT (safe) for updates.\n")
			if err := m.Upd.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 9:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.CommitTxn == nil {
				m.CommitTxn = &ApbCommitTransaction{}
			} else {
				*m.CommitTxn = ApbCommitTransaction{}
			}
			if err := m.CommitTxn.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 10:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.AbortTxn == nil {
				m.AbortTxn = &ApbAbortTransaction{}
			} else {
				*m.AbortTxn = ApbAbortTransaction{}
			}
			if err := m.AbortTxn.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 11:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.BcPermsReq == nil {
				m.BcPermsReq = &ProtoBCPermissionsReq{}
			} else {
				*m.BcPermsReq = ProtoBCPermissionsReq{}
			}
			if err := m.BcPermsReq.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 12:
			var v WrapperType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= WrapperType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.MsgID = v
			hasFields[0] |= uint64(0x00000002)
		case 13:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.SingleRead == nil {
				m.SingleRead = &S2SSingleRead{}
			}
			if err := m.SingleRead.UnmarshalVTSafeReuse(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return fmt.Errorf("proto: required field clientID not set")
	}
	if hasFields[0]&uint64(0x00000002) == 0 {
		return fmt.Errorf("proto: required field msgID not set")
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *S2SWrapperReply) UnmarshalVTOptSafeReuse(dAtA []byte) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.ClientID = v
			hasFields[0] |= uint64(0x00000001)
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.StaticReadObjs == nil {
				m.StaticReadObjs = &ApbStaticReadObjectsResp{}
			} else {
				*m.StaticReadObjs = ApbStaticReadObjectsResp{}
			}
			if err := m.StaticReadObjs.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.StartTxn == nil {
				m.StartTxn = &ApbStartTransactionResp{}
			} else {
				*m.StartTxn = ApbStartTransactionResp{}
			}
			if err := m.StartTxn.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.ReadObjs == nil {
				m.ReadObjs = &ApbReadObjectsResp{}
			} else {
				*m.ReadObjs = ApbReadObjectsResp{}
			}
			if err := m.ReadObjs.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 5:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Upd == nil {
				m.Upd = &ApbOperationResp{}
			} else {
				*m.Upd = ApbOperationResp{}
			}
			if err := m.Upd.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 6:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.CommitTxn == nil {
				m.CommitTxn = &ApbCommitResp{}
			} else {
				*m.CommitTxn = ApbCommitResp{}
			}
			if err := m.CommitTxn.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 7:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.SingleRead == nil {
				m.SingleRead = &S2SSingleReadResp{}
			} else {
				*m.SingleRead = S2SSingleReadResp{}
			}
			if err := m.SingleRead.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 12:
			var v WrapperType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= WrapperType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.MsgID = v
			hasFields[0] |= uint64(0x00000002)
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return fmt.Errorf("proto: required field clientID not set")
	}
	if hasFields[0]&uint64(0x00000002) == 0 {
		return fmt.Errorf("proto: required field msgID not set")
	}
	return nil
}

func (m *ApbStaticUpdateObjects) UnmarshalVTSafeReuse(dAtA []byte) error {
	m.Updates = m.Updates[:0] //Reset slice, but keep capacity for re-use.
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Transaction == nil {
				m.Transaction = &ApbStartTransaction{}
				if err := m.Transaction.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				if err := m.Transaction.UnmarshalVTSafeReuse(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if len(m.Updates) < cap(m.Updates) { //Attempt re-use.
				m.Updates = m.Updates[:len(m.Updates)+1]
				if m.Updates[len(m.Updates)-1] == nil {
					m.Updates[len(m.Updates)-1] = &ApbUpdateOp{}
				}
			} else {
				m.Updates = append(m.Updates, &ApbUpdateOp{})
			}
			if err := m.Updates[len(m.Updates)-1].UnmarshalVTSafeReuse(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbStartTransaction) UnmarshalVTSafeReuse(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Timestamp = append(m.Timestamp[:0], dAtA[iNdEx:postIndex]...)
			if m.Timestamp == nil {
				m.Timestamp = []byte{}
			}
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Properties == nil {
				m.Properties = &ApbTxnProperties{}
				if err := m.Properties.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				*m.Properties.ReadWrite, *m.Properties.RedBlue = 0, 0
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *ApbUpdateOp) UnmarshalVTSafeReuse(dAtA []byte) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Boundobject == nil {
				m.Boundobject = &ApbBoundObject{Type: new(CRDTType)}
			}
			if err := m.Boundobject.UnmarshalVTSafeReuse(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Operation == nil {
				m.Operation = &ApbUpdateOperation{}
			} else {
				m.Operation.Op = nil
			}
			if err := m.Operation.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000002)
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbBoundObject) UnmarshalVTSafeReuse(dAtA []byte) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Key = append(m.Key[:0], dAtA[iNdEx:postIndex]...)
			if m.Key == nil {
				m.Key = []byte{}
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		case 2:
			var v CRDTType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= CRDTType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.Type = v
			hasFields[0] |= uint64(0x00000002)
		case 3:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Bucket = append(m.Bucket[:0], dAtA[iNdEx:postIndex]...)
			if m.Bucket == nil {
				m.Bucket = []byte{}
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000004)
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbStaticReadObjects) UnmarshalVTSafeReuse(dAtA []byte) error {
	if m.Objects == nil {
		m.Objects = make([]*ApbBoundObject, 0, 2)
	}
	m.Objects = m.Objects[:0] //Reset slice, but keep capacity for re-use.
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Transaction == nil {
				m.Transaction = &ApbStartTransaction{}
				if err := m.Transaction.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				if err := m.Transaction.UnmarshalVTSafeReuse(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if len(m.Objects) < cap(m.Objects) { //Attempt re-use.
				m.Objects = m.Objects[:len(m.Objects)+1]
				if m.Objects[len(m.Objects)-1] == nil {
					m.Objects[len(m.Objects)-1] = &ApbBoundObject{Type: new(CRDTType)}
				}
			} else {
				m.Objects = append(m.Objects, &ApbBoundObject{Type: new(CRDTType)})
			}
			if err := m.Objects[len(m.Objects)-1].UnmarshalVTSafeReuse(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return fmt.Errorf("proto: required field transaction not set")
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *ApbStaticRead) UnmarshalVTSafeReuse(dAtA []byte) error {
	if cap(m.Fullreads) > 0 {
		m.Fullreads = m.Fullreads[:0]
	}
	if cap(m.Partialreads) > 0 {
		m.Partialreads = m.Partialreads[:0]
	}
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if len(m.Fullreads) < cap(m.Fullreads) { //Attempt re-use.
				m.Fullreads = m.Fullreads[:len(m.Fullreads)+1]
				if m.Fullreads[len(m.Fullreads)-1] == nil {
					m.Fullreads[len(m.Fullreads)-1] = &ApbBoundObject{Type: new(CRDTType)}
				}
			} else {
				m.Fullreads = append(m.Fullreads, &ApbBoundObject{Type: new(CRDTType)})
			}
			if err := m.Fullreads[len(m.Fullreads)-1].UnmarshalVTSafeReuse(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if len(m.Partialreads) < cap(m.Partialreads) { //Attempt re-use.
				m.Partialreads = m.Partialreads[:len(m.Partialreads)+1]
				if m.Partialreads[len(m.Partialreads)-1] == nil {
					m.Partialreads[len(m.Partialreads)-1] = &ApbPartialRead{Readtype: new(READType)}
				}
			} else {
				m.Partialreads = append(m.Partialreads, &ApbPartialRead{Readtype: new(READType)})
			}
			if err := m.Partialreads[len(m.Partialreads)-1].UnmarshalVTSafeReuse(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Transaction == nil {
				m.Transaction = &ApbStartTransaction{}
				if err := m.Transaction.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				if err := m.Transaction.UnmarshalVTSafeReuse(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return fmt.Errorf("proto: required field transaction not set")
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *ApbPartialRead) UnmarshalVTSafeReuse(dAtA []byte) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Object == nil {
				m.Object = &ApbBoundObject{Type: new(CRDTType)}
			}
			if err := m.Object.UnmarshalVTSafeReuse(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		case 2:
			var v READType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= READType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Readtype = &v
			hasFields[0] |= uint64(0x00000002)
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Args == nil {
				m.Args = &ApbPartialReadArgs{}
			} else {
				m.Args.Args = nil
			}
			if err := m.Args.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000004)
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return fmt.Errorf("proto: required field object not set")
	}
	if hasFields[0]&uint64(0x00000002) == 0 {
		return fmt.Errorf("proto: required field readtype not set")
	}
	if hasFields[0]&uint64(0x00000004) == 0 {
		return fmt.Errorf("proto: required field args not set")
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *ApbStaticReadObjects) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	m.Objects = m.Objects[:0] //Reset slice, but keep capacity for re-use.
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Transaction == nil {
				m.Transaction = &ApbStartTransaction{Properties: txnP}
			}
			if err := m.Transaction.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			var obj *ApbBoundObject
			if len(m.Objects) < cap(m.Objects) { //Attempt re-use.
				m.Objects = m.Objects[:len(m.Objects)+1]
				obj = m.Objects[len(m.Objects)-1]
				if obj == nil {
					obj = &ApbBoundObject{Type: new(CRDTType)}
					m.Objects[len(m.Objects)-1] = obj
				}
			} else {
				obj = &ApbBoundObject{Type: new(CRDTType)}
				m.Objects = append(m.Objects, obj)
			}
			if err := obj.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return fmt.Errorf("proto: required field transaction not set")
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *ApbStaticRead) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	m.Fullreads, m.Partialreads = m.Fullreads[:0], m.Partialreads[:0] //Reset slices, but keep capacity for re-use.
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			var read *ApbBoundObject
			if len(m.Fullreads) < cap(m.Fullreads) {
				m.Fullreads = m.Fullreads[:len(m.Fullreads)+1]
				read = m.Fullreads[len(m.Fullreads)-1]
				if read == nil {
					read = &ApbBoundObject{Type: new(CRDTType)}
					m.Fullreads[len(m.Fullreads)-1] = read
				}
			} else {
				read = &ApbBoundObject{Type: new(CRDTType)}
				m.Fullreads = append(m.Fullreads, read)
			}
			if err := read.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			var read *ApbPartialRead
			if len(m.Partialreads) < cap(m.Partialreads) {
				m.Partialreads = m.Partialreads[:len(m.Partialreads)+1]
				read = m.Partialreads[len(m.Partialreads)-1]
				if read == nil {
					m.Partialreads[len(m.Partialreads)-1] = &ApbPartialRead{Readtype: new(READType), Object: &ApbBoundObject{Type: new(CRDTType)}, Args: &ApbPartialReadArgs{}}
					read = m.Partialreads[len(m.Partialreads)-1]
				}
			} else {
				read = &ApbPartialRead{Readtype: new(READType), Object: &ApbBoundObject{Type: new(CRDTType)}, Args: &ApbPartialReadArgs{}}
				m.Partialreads = append(m.Partialreads, read)
			}
			if err := read.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Transaction == nil {
				m.Transaction = &ApbStartTransaction{}
			}
			if err := m.Transaction.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return fmt.Errorf("proto: required field transaction not set")
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *S2SSingleRead) UnmarshalVTSafeReuse(dAtA []byte) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.KeyParams == nil {
				m.KeyParams = &ApbBoundObject{Type: new(CRDTType)}
			}
			if err := m.KeyParams.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		case 2:
			var v READType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= READType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if m.Readtype == nil {
				m.Readtype = &v
			} else {
				*m.Readtype = v
			}
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.PartRead == nil {
				m.PartRead = &ApbPartialReadArgs{}
			} else {
				m.PartRead.Args = nil
			}
			if err := m.PartRead.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return fmt.Errorf("proto: required field keyParams not set")
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *ApbPartialRead) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1: //OK.
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if err := m.Object.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		case 2: //OK
			var v READType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= READType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.Readtype = v
			hasFields[0] |= uint64(0x00000002)
		case 3: //OK
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if err := m.Args.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000004)
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return fmt.Errorf("proto: required field object not set")
	}
	if hasFields[0]&uint64(0x00000002) == 0 {
		return fmt.Errorf("proto: required field readtype not set")
	}
	if hasFields[0]&uint64(0x00000004) == 0 {
		return fmt.Errorf("proto: required field args not set")
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *ApbBoundObject) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Key = dAtA[iNdEx:postIndex]
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		case 2:
			var v CRDTType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= CRDTType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.Type = v
			hasFields[0] |= uint64(0x00000002)
		case 3:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Bucket = dAtA[iNdEx:postIndex]
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000004)
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return fmt.Errorf("proto: required field key not set")
	}
	if hasFields[0]&uint64(0x00000002) == 0 {
		return fmt.Errorf("proto: required field type not set")
	}
	if hasFields[0]&uint64(0x00000004) == 0 {
		return fmt.Errorf("proto: required field bucket not set")
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

//Reading functions.

func (m *ApbPartialReadArgs) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Args.(*ApbPartialReadArgs_Set); ok {
				*oneof.Set = ApbSetPartialRead{} //Reset struct, but keep pointer for re-use.
				if err := oneof.Set.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := pbBuf.PReadSet
				if v == nil {
					v = &ApbPartialReadArgs_Set{Set: &ApbSetPartialRead{}}
				} else {
					pbBuf.ReturnPReadSet = v
					pbBuf.PReadSet = nil
				}
				if err := v.Set.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Args = v
			}
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Args.(*ApbPartialReadArgs_Map); ok {
				//Now it's dangerous to use the one in pbBuf.
				pbBuf.ReturnPReadMap = pbBuf.PReadMap
				pbBuf.PReadMap = nil
				if err := oneof.Map.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
			} else {
				v := pbBuf.PReadMap
				if v == nil {
					v = &ApbPartialReadArgs_Map{Map: &ApbMapPartialRead{}}
				} else {
					pbBuf.ReturnPReadMap = v
					pbBuf.PReadMap = nil
				}
				if err := v.Map.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
				m.Args = v
			}
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Args.(*ApbPartialReadArgs_Topk); ok {
				*oneof.Topk = ApbTopkPartialRead{} //Reset struct, but keep pointer for re-use.
				if err := oneof.Topk.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := pbBuf.PReadTopk
				if v == nil {
					v = &ApbPartialReadArgs_Topk{Topk: &ApbTopkPartialRead{}}
				} else {
					pbBuf.ReturnPReadTopk = v
					pbBuf.PReadTopk = nil
				}
				if err := v.Topk.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Args = v
			}
			iNdEx = postIndex
		case 4:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Args.(*ApbPartialReadArgs_Avg); ok { //AVG OK to fully reuse.
				if err := oneof.Avg.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := pbBuf.PReadAvg
				if v == nil {
					v = &ApbPartialReadArgs_Avg{Avg: &ApbAvgPartialRead{}}
				} else {
					pbBuf.ReturnPReadAvg = v
					pbBuf.PReadAvg = nil
				}
				if err := v.Avg.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Args = v
			}
			iNdEx = postIndex
		case 5:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Args.(*ApbPartialReadArgs_Process); ok {
				*oneof.Process = ApbProcessRead{} //Reset struct, but keep pointer for re-use.
				if err := oneof.Process.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbProcessRead{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Args = &ApbPartialReadArgs_Process{Process: v}
			}
			iNdEx = postIndex
		case 6:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}

			postIndex := iNdEx + msglen
			if oneof, ok := m.Args.(*ApbPartialReadArgs_Paircounter); ok {
				*oneof.Paircounter = ApbPairCounterPartialRead{} //Reset struct, but keep pointer for re-use.
				if err := oneof.Paircounter.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := pbBuf.PReadPairCounter
				if v == nil {
					v = &ApbPartialReadArgs_Paircounter{Paircounter: &ApbPairCounterPartialRead{}}
				} else {
					pbBuf.ReturnPReadPairCounter = v
					pbBuf.PReadPairCounter = nil
				}
				if err := v.Paircounter.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Args = v
			}
			iNdEx = postIndex
		case 7:

			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}

			postIndex := iNdEx + msglen
			if oneof, ok := m.Args.(*ApbPartialReadArgs_Arraycounter); ok {
				*oneof.Arraycounter = ApbArrayCounterPartialRead{} //Reset struct, but keep pointer for re-use.
				if err := oneof.Arraycounter.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := pbBuf.PReadArrayCounter
				if v == nil {
					v = &ApbPartialReadArgs_Arraycounter{Arraycounter: &ApbArrayCounterPartialRead{}}
				} else {
					pbBuf.ReturnPReadArrayCounter = v
					pbBuf.PReadArrayCounter = nil
				}
				if err := v.Arraycounter.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Args = v
			}
			iNdEx = postIndex
		case 8:
			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}

			postIndex := iNdEx + msglen
			if oneof, ok := m.Args.(*ApbPartialReadArgs_Arrayfloat); ok {
				*oneof.Arrayfloat = ApbArrayFloatPartialRead{} //Reset struct, but keep pointer for re-use.
				if err := oneof.Arrayfloat.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := pbBuf.PReadArrayFloat
				if v == nil {
					v = &ApbPartialReadArgs_Arrayfloat{Arrayfloat: &ApbArrayFloatPartialRead{}}
				} else {
					pbBuf.ReturnPReadArrayFloat = v
					pbBuf.PReadArrayFloat = nil
				}
				if err := v.Arrayfloat.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Args = v
			}
			iNdEx = postIndex
		case 9:
			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}

			postIndex := iNdEx + msglen
			if oneof, ok := m.Args.(*ApbPartialReadArgs_Multiarray); ok {
				*oneof.Multiarray = ApbMultiArrayPartialRead{} //Reset struct, but keep pointer for re-use.
				if err := oneof.Multiarray.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := pbBuf.PReadMultiArray
				if v == nil {
					v = &ApbPartialReadArgs_Multiarray{Multiarray: &ApbMultiArrayPartialRead{}}
				} else {
					pbBuf.ReturnPReadMultiArray = v
					pbBuf.PReadMultiArray = nil
				}
				if err := v.Multiarray.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Args = v
			}
			iNdEx = postIndex
		case 10:
			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}

			postIndex := iNdEx + msglen
			if oneof, ok := m.Args.(*ApbPartialReadArgs_Mvreg); ok { //MVReg OK to fully re-use.
				if err := oneof.Mvreg.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := pbBuf.PReadMVReg
				if v == nil {
					v = &ApbPartialReadArgs_Mvreg{Mvreg: &ApbMVRegPartialRead{}}
				} else {
					pbBuf.ReturnPReadMVReg = v
					pbBuf.PReadMVReg = nil
				}
				if err := v.Mvreg.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Args = v
			}
			iNdEx = postIndex
		case 11:
			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}

			postIndex := iNdEx + msglen
			if oneof, ok := m.Args.(*ApbPartialReadArgs_Date); ok {
				*oneof.Date = ApbDatePartialRead{} //Reset struct, but keep pointer for re-use.
				if err := oneof.Date.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := pbBuf.PReadDate
				if v == nil {
					v = &ApbPartialReadArgs_Date{Date: &ApbDatePartialRead{}}
				} else {
					pbBuf.ReturnPReadDate = v
					pbBuf.PReadDate = nil
				}
				if err := v.Date.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Args = v
			}
			iNdEx = postIndex
		case 12:
			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}

			postIndex := iNdEx + msglen
			if oneof, ok := m.Args.(*ApbPartialReadArgs_Compactarray); ok {
				*oneof.Compactarray = ApbCompactArrayPartialRead{} //Reset struct, but keep pointer for re-use.
				if err := oneof.Compactarray.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := pbBuf.PReadCompactArray
				if v == nil {
					v = &ApbPartialReadArgs_Compactarray{Compactarray: &ApbCompactArrayPartialRead{}}
				} else {
					pbBuf.ReturnPReadCompactArray = v
					pbBuf.PReadCompactArray = nil
				}
				if err := v.Compactarray.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Args = v
			}
			iNdEx = postIndex
		case 13:
			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}

			postIndex := iNdEx + msglen
			if oneof, ok := m.Args.(*ApbPartialReadArgs_Stringarray); ok {
				*oneof.Stringarray = ApbStringArrayPartialRead{} //Reset struct, but keep pointer for re-use.
				if err := oneof.Stringarray.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := pbBuf.PReadStringArray
				if v == nil {
					v = &ApbPartialReadArgs_Stringarray{Stringarray: &ApbStringArrayPartialRead{}}
				} else {
					pbBuf.ReturnPReadStringArray = v
					pbBuf.PReadStringArray = nil
				}
				if err := v.Stringarray.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Args = v
			}
			iNdEx = postIndex
		case 14:
			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}

			postIndex := iNdEx + msglen
			if oneof, ok := m.Args.(*ApbPartialReadArgs_Bytearray); ok {
				*oneof.Bytearray = ApbByteArrayPartialRead{} //Reset struct, but keep pointer for re-use.
				if err := oneof.Bytearray.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := pbBuf.PReadByteArray
				if v == nil {
					v = &ApbPartialReadArgs_Bytearray{Bytearray: &ApbByteArrayPartialRead{}}
				} else {
					pbBuf.ReturnPReadByteArray = v
					pbBuf.PReadByteArray = nil
				}
				if err := v.Bytearray.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Args = v
			}
			iNdEx = postIndex
		case 15:
			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}

			postIndex := iNdEx + msglen
			if oneof, ok := m.Args.(*ApbPartialReadArgs_Mapcounter); ok {
				*oneof.Mapcounter = ApbMapCounterPartialRead{} //Reset struct, but keep pointer for re-use.
				if err := oneof.Mapcounter.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := pbBuf.PReadMapCounter
				if v == nil {
					v = &ApbPartialReadArgs_Mapcounter{Mapcounter: &ApbMapCounterPartialRead{}}
				} else {
					pbBuf.ReturnPReadMapCounter = v
					pbBuf.PReadMapCounter = nil
				}
				if err := v.Mapcounter.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Args = v
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *ApbStartTransaction) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Timestamp = dAtA[iNdEx:postIndex]
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Properties == nil {
				m.Properties = txnP
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *ApbTxnProperties) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v uint32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.ReadWrite = v
		case 2:
			var v uint32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.RedBlue = v
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *ApbMapPartialRead) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Read.(*ApbMapPartialRead_Getvalue); ok {
				if err := oneof.Getvalue.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
			} else {
				v := pbBuf.PMapGetValue
				if v == nil {
					v = &ApbMapPartialRead_Getvalue{Getvalue: &ApbMapGetValueRead{}}
				} else {
					pbBuf.ReturnPMapGetValue = v
					pbBuf.PMapGetValue = nil
				}
				if err := v.Getvalue.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
				m.Read = v
			}
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Read.(*ApbMapPartialRead_Haskey); ok {
				if err := oneof.Haskey.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := pbBuf.PMapHasKey
				if v == nil {
					v = &ApbMapPartialRead_Haskey{Haskey: &ApbMapHasKeyRead{}}
				} else {
					pbBuf.ReturnPMapHasKey = v
					pbBuf.PMapHasKey = nil
				}
				if err := v.Haskey.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Read = v
			}
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Read.(*ApbMapPartialRead_Getkeys); ok {
				if err := oneof.Getkeys.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := pbBuf.PMapGetKeys
				if v == nil {
					v = &ApbMapPartialRead_Getkeys{Getkeys: &ApbMapGetKeysRead{}}
				} else {
					pbBuf.ReturnPMapGetKeys = v
					pbBuf.PMapGetKeys = nil
				}
				if err := v.Getkeys.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Read = v
			}
			iNdEx = postIndex
		case 4:
			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Read.(*ApbMapPartialRead_Getvalues); ok {
				if err := oneof.Getvalues.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
			} else {
				v := pbBuf.PMapGetValues
				if v == nil {
					v = &ApbMapPartialRead_Getvalues{Getvalues: &ApbMapGetValuesRead{}}
				} else {
					pbBuf.ReturnPMapGetValues = v
					pbBuf.PMapGetValues = nil
				}
				if err := v.Getvalues.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
				m.Read = v
			}
			iNdEx = postIndex
		case 5:
			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Read.(*ApbMapPartialRead_Getallvalues); ok {
				if err := oneof.Getallvalues.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
			} else {
				v := pbBuf.PMapGetAllValues
				if v == nil {
					v = &ApbMapPartialRead_Getallvalues{Getallvalues: &ApbMapGetAllValuesRead{}}
				} else {
					pbBuf.ReturnPMapGetAllValues = v
					pbBuf.PMapGetAllValues = nil
				}
				if err := v.Getallvalues.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
				m.Read = v
			}
			iNdEx = postIndex
		case 6:
			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Read.(*ApbMapPartialRead_Condread); ok {
				if err := oneof.Condread.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
			} else {
				v := pbBuf.PMapCond
				if v == nil {
					v = &ApbMapPartialRead_Condread{Condread: &ApbMapCondRead{}}
				} else {
					pbBuf.ReturnPMapCond = v
					pbBuf.PMapCond = nil
				}
				if err := v.Condread.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
				m.Read = v
			}
			iNdEx = postIndex
		case 7:
			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Read.(*ApbMapPartialRead_Condallread); ok {
				if err := oneof.Condallread.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
			} else {
				v := pbBuf.PMapAllCond
				if v == nil {
					v = &ApbMapPartialRead_Condallread{Condallread: &ApbMapAllCondRead{}}
				} else {
					pbBuf.ReturnPMapAllCond = v
					pbBuf.PMapAllCond = nil
				}
				if err := v.Condallread.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
				m.Read = v
			}
			iNdEx = postIndex
		case 8:
			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Read.(*ApbMapPartialRead_Exceptread); ok {
				if err := oneof.Exceptread.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
			} else {
				v := pbBuf.PMapExcept
				if v == nil {
					v = &ApbMapPartialRead_Exceptread{Exceptread: &ApbMapExceptRead{}}
				} else {
					pbBuf.ReturnPMapExcept = v
					pbBuf.PMapExcept = nil
				}
				if err := v.Exceptread.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
				m.Read = v
			}
			iNdEx = postIndex
		case 9:
			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Read.(*ApbMapPartialRead_Exceptcondread); ok {
				if err := oneof.Exceptcondread.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
			} else {
				v := pbBuf.PMapExceptCond
				if v == nil {
					v = &ApbMapPartialRead_Exceptcondread{Exceptcondread: &ApbMapExceptCondRead{}}
				} else {
					pbBuf.ReturnPMapExceptCond = v
					pbBuf.PMapExceptCond = nil
				}
				if err := v.Exceptcondread.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
				m.Read = v
			}
			iNdEx = postIndex
		case 10:
			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Read.(*ApbMapPartialRead_Aggregateread); ok {
				if err := oneof.Aggregateread.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
			} else {
				v := pbBuf.PMapAggregateRead
				if v == nil {
					v = &ApbMapPartialRead_Aggregateread{Aggregateread: &ApbMapAggregateRead{}}
				} else {
					pbBuf.ReturnPMapAggregateRead = v
					pbBuf.PMapAggregateRead = nil
				}
				if err := v.Aggregateread.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
				m.Read = v
			}
			iNdEx = postIndex
		case 11:
			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Read.(*ApbMapPartialRead_Exceptsingleread); ok {
				if err := oneof.Exceptsingleread.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
			} else {
				v := pbBuf.PMapExceptSingle
				if v == nil {
					v = &ApbMapPartialRead_Exceptsingleread{Exceptsingleread: &ApbMapExceptSingleRead{}}
				} else {
					pbBuf.ReturnPMapExceptSingle = v
					pbBuf.PMapExceptSingle = nil
				}
				if err := v.Exceptsingleread.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
				m.Read = v
			}
			iNdEx = postIndex
		case 12:
			var msglen int
			for shift := uint(0); ; shift += 7 {

				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Read.(*ApbMapPartialRead_Exceptcondsingleread); ok {
				if err := oneof.Exceptcondsingleread.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
			} else {
				v := pbBuf.PMapExceptCondSingle
				if v == nil {
					v = &ApbMapPartialRead_Exceptcondsingleread{Exceptcondsingleread: &ApbMapExceptCondSingleRead{}}
				} else {
					pbBuf.ReturnPMapExceptCondSingle = v
					pbBuf.PMapExceptCondSingle = nil
				}
				if err := v.Exceptcondsingleread.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
					return err
				}
				m.Read = v
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *ApbMapGetValueRead) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	hasArgs := false
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Key = dAtA[iNdEx:postIndex]
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		case 2:
			hasArgs = true
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Args == nil {
				m.Args = &ApbMapEmbPartialArgs{}
			}
			if err := m.Args.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return fmt.Errorf("proto: required field key not set")
	}
	if !hasArgs {
		m.Args = nil
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// TODO: Probably OK. Doesn't crash but I need to go back to find out what's the problem of Q22.
func (m *ApbMapGetValuesRead) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	args := m.Args[:0]
	m.Keys = m.Keys[:0]
	m.Args = nil
	//m.Keys, m.Args = m.Keys[:0], m.Args[:0] // Hiding existing keys and args.
	//m.Keys, m.Args = make([][]byte, 0), make([]*ApbMapEmbPartialArgs, 0)
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Keys = append(m.Keys, dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			var lArgs *ApbMapEmbPartialArgs
			if len(args) < cap(args) { //Attempt re-use
				args = args[:len(args)+1]
				lArgs = args[len(args)-1]
				if lArgs == nil {
					lArgs = &ApbMapEmbPartialArgs{}
					args[len(args)-1] = lArgs
				}
			} else {
				lArgs = &ApbMapEmbPartialArgs{}
				args = append(args, lArgs)
			}
			if err := lArgs.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if len(args) > 0 {
		m.Args = args
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *ApbMapAggregateRead) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	backupKeys, backupCondArg, backupArgs, backupAggrKey := m.Keys, m.Condarg, m.Args, m.AggrKey
	m.Keys, m.Condarg, m.Args, m.AggrKey = nil, nil, nil, nil // Hiding existing condarg, args and aggrkey. Could potentially re-use but not worth it for now.
	backupKeys = m.Keys[:0]                                   // Hiding existing keys.
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v AGGRType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= AGGRType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Aggregationtype = &v
			hasFields[0] |= uint64(0x00000001)
		case 2:
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + intStringLen
			var stringValue string
			if intStringLen > 0 {
				stringValue = unsafe.String(&dAtA[iNdEx], intStringLen)
			}
			backupKeys = append(backupKeys, stringValue)
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if backupCondArg == nil {
				backupCondArg = &ApbMapCondArgs{}
			}
			m.Condarg = backupCondArg
			m.Condarg.Key = nil
			if err := m.Condarg.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if backupArgs == nil {
				backupArgs = &ApbMapEmbPartialArgs{}
			}
			m.Args = backupArgs
			if err := m.Args.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 5:
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + intStringLen
			var stringValue string
			if intStringLen > 0 {
				stringValue = unsafe.String(&dAtA[iNdEx], intStringLen)
			}
			if backupAggrKey == nil {
				backupAggrKey = new(string)
			}
			*backupAggrKey = stringValue
			m.AggrKey = backupAggrKey
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return fmt.Errorf("proto: required field aggregationtype not set")
	}
	if len(backupKeys) > 0 {
		m.Keys = backupKeys
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ApbMapGetAllValuesRead) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	backupArgs := m.Args
	m.Args = nil // Hiding existing args. Will re-use if this read has args set up.
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if backupArgs == nil {
				backupArgs = &ApbMapEmbPartialArgs{}
			}
			m.Args = backupArgs
			if err := m.Args.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ApbMapCondRead) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	m.Condargs = m.Condargs[:0] // Hiding existing condargs.
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			var read *ApbMapCondArgs
			if len(m.Condargs) < cap(m.Condargs) { //Attempt re-use.
				m.Condargs = m.Condargs[:len(m.Condargs)+1]
				read = m.Condargs[len(m.Condargs)-1]
				if read == nil {
					read = &ApbMapCondArgs{}
					m.Condargs[len(m.Condargs)-1] = read
				}
			} else {
				read = &ApbMapCondArgs{}
				m.Condargs = append(m.Condargs, read)
			}
			if err := read.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ApbMapAllCondRead) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	backupArgs := m.Args
	m.Args = nil
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Condarg == nil {
				m.Condarg = &ApbMapCondArgs{}
			}
			if err := m.Condarg.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if backupArgs == nil {
				backupArgs = &ApbMapEmbPartialArgs{}
			}
			m.Args = backupArgs
			if err := m.Args.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return fmt.Errorf("proto: required field condarg not set")
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ApbMapExceptCondRead) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	m.Keys = m.Keys[:0] // Hiding existing keys.
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Keys = append(m.Keys, dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Condarg == nil {
				m.Condarg = &ApbMapCondArgs{}
			}
			if err := m.Condarg.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return fmt.Errorf("proto: required field condarg not set")
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ApbMapExceptRead) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	m.Keys = m.Keys[:0] // Hiding existing keys.
	backupArgs := m.Args
	m.Args = nil
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Keys = append(m.Keys, dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if backupArgs == nil {
				backupArgs = &ApbMapEmbPartialArgs{}
			}
			m.Args = backupArgs
			if err := m.Args.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ApbMapExceptSingleRead) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	backupArgs := m.Args
	m.Args = nil // Hiding existing args. Will re-use if this read has args set up.
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Key = dAtA[iNdEx:postIndex]
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if backupArgs == nil {
				backupArgs = &ApbMapEmbPartialArgs{}
			}
			m.Args = backupArgs
			if err := m.Args.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return fmt.Errorf("proto: required field key not set")
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ApbMapExceptCondSingleRead) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Key = dAtA[iNdEx:postIndex]
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Condarg == nil {
				m.Condarg = &ApbMapCondArgs{}
			}
			if err := m.Condarg.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000002)
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return fmt.Errorf("proto: required field key not set")
	}
	if hasFields[0]&uint64(0x00000002) == 0 {
		return fmt.Errorf("proto: required field condarg not set")
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// Note: Not optimized. All I did was set the key to nil (for correctness), as it is an optional argument.
func (m *ApbMapCondArgs) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	origString := m.Key
	m.Key = nil
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + intStringLen
			var stringValue string
			if intStringLen > 0 {
				stringValue = unsafe.String(&dAtA[iNdEx], intStringLen)
			}
			if origString == nil {
				origString = new(string)
			}
			*origString = stringValue
			m.Key = origString
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Comp.(*ApbMapCondArgs_Intcomp); ok {
				if err := oneof.Intcomp.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbCondIntCompare{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Comp = &ApbMapCondArgs_Intcomp{Intcomp: v}
			}
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Comp.(*ApbMapCondArgs_Floatcomp); ok {
				if err := oneof.Floatcomp.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbCondFloatCompare{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Comp = &ApbMapCondArgs_Floatcomp{Floatcomp: v}
			}
			iNdEx = postIndex
		case 4:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Comp.(*ApbMapCondArgs_Boolcomp); ok {
				if err := oneof.Boolcomp.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbCondBoolCompare{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Comp = &ApbMapCondArgs_Boolcomp{Boolcomp: v}
			}
			iNdEx = postIndex
		case 5:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Comp.(*ApbMapCondArgs_Stringcomp); ok {
				if err := oneof.Stringcomp.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbCondStringCompare{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Comp = &ApbMapCondArgs_Stringcomp{Stringcomp: v}
			}
			iNdEx = postIndex
		case 6:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Comp.(*ApbMapCondArgs_Bytescomp); ok {
				if err := oneof.Bytescomp.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbCondBytesCompare{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Comp = &ApbMapCondArgs_Bytescomp{Bytescomp: v}
			}
			iNdEx = postIndex
		case 7:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Comp.(*ApbMapCondArgs_Mapcomp); ok {
				if err := oneof.Mapcomp.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbCondMapCompare{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Comp = &ApbMapCondArgs_Mapcomp{Mapcomp: v}
			}
			iNdEx = postIndex
		case 8:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Comp.(*ApbMapCondArgs_Nocomp); ok {
				if err := oneof.Nocomp.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbCondGetNoCompare{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Comp = &ApbMapCondArgs_Nocomp{Nocomp: v}
			}
			iNdEx = postIndex
		case 9:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Comp.(*ApbMapCondArgs_Intvarcomp); ok {
				if err := oneof.Intvarcomp.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbCondIntVarCompare{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Comp = &ApbMapCondArgs_Intvarcomp{Intvarcomp: v}
			}
			iNdEx = postIndex
		case 10:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if oneof, ok := m.Comp.(*ApbMapCondArgs_Floatvarcomp); ok {
				if err := oneof.Floatvarcomp.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbCondFloatVarCompare{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Comp = &ApbMapCondArgs_Floatvarcomp{Floatvarcomp: v}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *ApbMapEmbPartialArgs) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	backupArgs, backupCrdtType := m.Args, m.Type
	m.Args = nil // Hiding existing args. Will re-use if this read has args set up.
	m.Type = nil
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v CRDTType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= CRDTType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if backupCrdtType == nil {
				backupCrdtType = new(CRDTType)
			}
			*backupCrdtType = v
			m.Type = backupCrdtType
		case 2:
			var v READType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= READType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if m.Readtype == nil {
				m.Readtype = new(READType)
			}
			*m.Readtype = v
			hasFields[0] |= uint64(0x00000001)
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if backupArgs == nil {
				backupArgs = &ApbPartialReadArgs{}
			}
			m.Args = backupArgs
			if err := m.Args.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if hasFields[0]&uint64(0x00000001) == 0 {
		return fmt.Errorf("proto: required field readtype not set")
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

//Update functions.

// Only uses unsafe when it is "safe" to do so - i.e., for transaction information and similar.
// All updates are unmarshalled using either a hand-optimized unmarshal (safe) or UnmarshalVT.
// Thus, it is guaranteed all strings and byte slices will be new.
// However, do care that protobufs themselves will be re-used whenever possible.
func (m *ApbStaticUpdateObjects) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Updates = m.Updates[:0] //Reset slice, but keep capacity for re-use.
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Transaction == nil {
				m.Transaction = &ApbStartTransaction{Properties: txnP} //Re-usable ApbTxnProperties
			}
			if err := m.Transaction.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if len(m.Updates) < cap(m.Updates) { //Attempt re-use.
				m.Updates = m.Updates[:len(m.Updates)+1]
				if m.Updates[len(m.Updates)-1] == nil {
					m.Updates[len(m.Updates)-1] = &ApbUpdateOp{}
				}
			} else {
				m.Updates = append(m.Updates, &ApbUpdateOp{})
			}
			if err := m.Updates[len(m.Updates)-1].UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbUpdateOp) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	var hasFields [1]uint64
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Boundobject == nil {
				m.Boundobject = &ApbBoundObject{Type: new(CRDTType)}
			} else {
				//We must ensure that fresh slices will be used, as ProtoServer uses unsafe on these.
				m.Boundobject.Key, m.Boundobject.Bucket = nil, nil
			}
			//Must use a safe version as protoserver will re-use the byte slices.
			if err := m.Boundobject.UnmarshalVTSafeReuse(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000001)
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Operation == nil {
				m.Operation = &ApbUpdateOperation{Specialop: new(SPECIAL_UPD)}
			} else {
				m.Operation.Op = nil
			}
			/*if err := m.Operation.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}*/
			if err := m.Operation.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
			hasFields[0] |= uint64(0x00000002)
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbUpdateOperation) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	//fmt.Printf("[FastUnmarshal][ApbUpdateOperation]Got into UnmarshalVTUnsafeReuse.\n")
	*m.Specialop = SPECIAL_UPD_NORMAL
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			var cOp *ApbUpdateOperation_Counterop
			if !pbBuf.UpdCounters.IsEmpty() {
				cOp = pbBuf.UpdCounters.GetAndHideHead()
			} else {
				cOp = &ApbUpdateOperation_Counterop{Counterop: &ApbCounterUpdate{Inc: new(int64)}}
				pbBuf.UpdCounters.AppendAndHide(cOp)
			}
			m.Op = cOp
			if err := cOp.Counterop.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			var setOp *ApbUpdateOperation_Setop
			if !pbBuf.UpdSets.IsEmpty() {
				setOp = pbBuf.UpdSets.GetAndHideHead()
				*setOp.Setop = ApbSetUpdate{}
			} else {
				setOp = &ApbUpdateOperation_Setop{Setop: &ApbSetUpdate{Optype: new(ApbSetUpdate_SetOpType)}}
				pbBuf.UpdSets.AppendAndHide(setOp)
			}
			m.Op = setOp
			if err := setOp.Setop.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			var regOp *ApbUpdateOperation_Regop
			if !pbBuf.UpdRegs.IsEmpty() {
				regOp = pbBuf.UpdRegs.GetAndHideHead()
				*regOp.Regop = ApbRegUpdate{}
			} else {
				regOp = &ApbUpdateOperation_Regop{Regop: &ApbRegUpdate{}}
				pbBuf.UpdRegs.AppendAndHide(regOp)
			}
			m.Op = regOp
			if err := regOp.Regop.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			var bcOp *ApbUpdateOperation_Bcounterop
			if !pbBuf.UpdBCounters.IsEmpty() {
				bcOp = pbBuf.UpdBCounters.GetAndHideHead()
				//No need to clean as all fields are required and no string/byte slice.
			} else {
				bcOp = &ApbUpdateOperation_Bcounterop{Bcounterop: &ApbBoundCounterUpdate{Limit: new(int64), InitialValue: new(int64), CompEq: new(bool)}}
				pbBuf.UpdBCounters.AppendAndHide(bcOp)
			}
			m.Op = bcOp
			if err := bcOp.Bcounterop.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 5:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			var mapOp *ApbUpdateOperation_Mapop
			if !pbBuf.UpdMaps.IsEmpty() {
				mapOp = pbBuf.UpdMaps.GetAndHideHead()
				//We don't reset mapOp as our custom implementation will try to re-use the slices inside.
			} else {
				mapOp = &ApbUpdateOperation_Mapop{Mapop: &ApbMapUpdate{IsAddsArray: new(bool)}}
				pbBuf.UpdMaps.AppendAndHide(mapOp)
			}
			m.Op = mapOp
			if err := mapOp.Mapop.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 6:
			var v SPECIAL_UPD
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= SPECIAL_UPD(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.Specialop = v
		case 7:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			var flagOp *ApbUpdateOperation_Flagop
			if !pbBuf.UpdFlags.IsEmpty() {
				flagOp = pbBuf.UpdFlags.GetAndHideHead()
				//Doesn't need reset as its only field is required.
			} else {
				flagOp = &ApbUpdateOperation_Flagop{Flagop: &ApbFlagUpdate{Value: new(bool)}}
				pbBuf.UpdFlags.AppendAndHide(flagOp)
			}
			m.Op = flagOp
			if err := flagOp.Flagop.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 8:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			//ApbTopKInit won't be used often, so we don't need to cache them.
			initOp := &ApbUpdateOperation_Topkinitop{Topkinitop: &ApbTopKInit{}}
			if err := initOp.Topkinitop.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Op = initOp
			iNdEx = postIndex
		case 9:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			var maxminOp *ApbUpdateOperation_Maxminop
			if !pbBuf.UpdMaxMins.IsEmpty() {
				maxminOp = pbBuf.UpdMaxMins.GetAndHideHead()
				//Doesn't need reset as all fields are required and simple.
			} else {
				maxminOp = &ApbUpdateOperation_Maxminop{Maxminop: &ApbMaxMinUpdate{Value: new(int64), IsMax: new(bool)}}
				pbBuf.UpdMaxMins.AppendAndHide(maxminOp)
			}
			m.Op = maxminOp
			if err := maxminOp.Maxminop.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 10:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			var topkRmvOp *ApbUpdateOperation_Topkrmvop
			if !pbBuf.UpdTopKRmvs.IsEmpty() {
				topkRmvOp = pbBuf.UpdTopKRmvs.GetAndHideHead()
			} else {
				topkRmvOp = &ApbUpdateOperation_Topkrmvop{Topkrmvop: &ApbTopkRmvUpdate{}}
				pbBuf.UpdTopKRmvs.AppendAndHide(topkRmvOp)
			}
			m.Op = topkRmvOp
			if err := topkRmvOp.Topkrmvop.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 11:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			//This is currently unused.
			topOp := &ApbUpdateOperation_Topkop{Topkop: &ApbTopkUpdate{}}
			if err := topOp.Topkop.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Op = topOp
			iNdEx = postIndex
		case 12:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			//This is currently unused.
			leaderboardOp := &ApbUpdateOperation_Leaderboardop{Leaderboardop: &ApbLeaderboardUpdate{}}
			if err := leaderboardOp.Leaderboardop.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Op = leaderboardOp
			iNdEx = postIndex
		case 13:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			var avgOp *ApbUpdateOperation_Avgop
			if !pbBuf.UpdAvgs.IsEmpty() {
				avgOp = pbBuf.UpdAvgs.GetAndHideHead()
				avgOp.Avgop.NValues = nil //Reset this field as it is optional.
			} else {
				avgOp = &ApbUpdateOperation_Avgop{Avgop: &ApbAverageUpdate{Value: new(int64)}}
				pbBuf.UpdAvgs.AppendAndHide(avgOp)
			}
			m.Op = avgOp
			if err := avgOp.Avgop.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 14:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			var counterOp *ApbUpdateOperation_Counterfloatop
			if !pbBuf.UpdCounterFloats.IsEmpty() {
				counterOp = pbBuf.UpdCounterFloats.GetAndHideHead()
				//No need to reset as its only field is required.
			} else {
				counterOp = &ApbUpdateOperation_Counterfloatop{Counterfloatop: &ApbCounterFloatUpdate{Inc: new(float64)}}
				pbBuf.UpdCounterFloats.AppendAndHide(counterOp)
			}
			m.Op = counterOp
			if err := counterOp.Counterfloatop.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 15:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			var pairOp *ApbUpdateOperation_Paircounterop
			if !pbBuf.UpdPairCounters.IsEmpty() {
				pairOp = pbBuf.UpdPairCounters.GetAndHideHead()
				*pairOp.Paircounterop = ApbPairCounterUpdate{} //Need to reset as both fields are optional.
			} else {
				pairOp = &ApbUpdateOperation_Paircounterop{Paircounterop: &ApbPairCounterUpdate{}}
				pbBuf.UpdPairCounters.AppendAndHide(pairOp)
			}
			m.Op = pairOp
			if err := pairOp.Paircounterop.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 16:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen

			//TODO: This should be optimized to allow proper re-usage of the fields (i.e., ArrayCounterop)
			var counterOp *ApbUpdateOperation_Arraycounterop
			if !pbBuf.UpdArrayCounters.IsEmpty() {
				counterOp = pbBuf.UpdArrayCounters.GetAndHideHead()
			} else {
				counterOp = &ApbUpdateOperation_Arraycounterop{Arraycounterop: &ApbArrayCounterUpdate{UpdType: new(NumberArrayUpdType)}}
				pbBuf.UpdArrayCounters.AppendAndHide(counterOp)
			}
			m.Op = counterOp
			if err := counterOp.Arraycounterop.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 17:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen

			//TODO: This should be optimized to allow proper re-usage of the fields (i.e., Arrayfloatop)
			var arrayFloatOp *ApbUpdateOperation_Arrayfloatop
			if !pbBuf.UpdArrayFloats.IsEmpty() {
				arrayFloatOp = pbBuf.UpdArrayFloats.GetAndHideHead()
			} else {
				arrayFloatOp = &ApbUpdateOperation_Arrayfloatop{Arrayfloatop: &ApbArrayFloatUpdate{UpdType: new(NumberArrayUpdType)}}
				pbBuf.UpdArrayFloats.AppendAndHide(arrayFloatOp)
			}
			m.Op = arrayFloatOp
			if err := arrayFloatOp.Arrayfloatop.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 18:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen

			var arrayMultiOp *ApbUpdateOperation_Multiarrayop
			if !pbBuf.UpdMultiArrays.IsEmpty() {
				arrayMultiOp = pbBuf.UpdMultiArrays.GetAndHideHead()
			} else {
				arrayMultiOp = &ApbUpdateOperation_Multiarrayop{Multiarrayop: &ApbMultiArrayUpdate{Type: new(MultiArrayType), UpdType: new(NumberArrayUpdType)}}
				pbBuf.UpdMultiArrays.AppendAndHide(arrayMultiOp)
			}
			m.Op = arrayMultiOp
			if err := arrayMultiOp.Multiarrayop.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 19:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen

			//TODO: This should be optimized to allow proper re-usage of the fields (i.e., Dateop)
			var dateOp *ApbUpdateOperation_Dateop
			if !pbBuf.UpdDates.IsEmpty() {
				dateOp = pbBuf.UpdDates.GetAndHideHead()
				*dateOp.Dateop = ApbDateUpdate{}
			} else {
				dateOp = &ApbUpdateOperation_Dateop{Dateop: &ApbDateUpdate{}}
				pbBuf.UpdDates.AppendAndHide(dateOp)
			}
			m.Op = dateOp
			if err := dateOp.Dateop.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 20:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen

			//TODO: This should be optimized to allow proper re-usage of the fields (i.e., Compactarrayop)
			var arrayCompactOp *ApbUpdateOperation_Compactarrayop
			if !pbBuf.UpdCompactArrays.IsEmpty() {
				arrayCompactOp = pbBuf.UpdCompactArrays.GetAndHideHead()
				*arrayCompactOp.Compactarrayop = ApbCompactArrayUpdate{}
			} else {
				arrayCompactOp = &ApbUpdateOperation_Compactarrayop{Compactarrayop: &ApbCompactArrayUpdate{}}
				pbBuf.UpdCompactArrays.AppendAndHide(arrayCompactOp)
			}
			m.Op = arrayCompactOp
			if err := arrayCompactOp.Compactarrayop.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 21:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen

			//TODO: This should be optimized to allow proper re-usage of the fields. (i.e, Stringarrayop)
			var arrayStringOp *ApbUpdateOperation_Stringarrayop
			if !pbBuf.UpdStringArrays.IsEmpty() {
				arrayStringOp = pbBuf.UpdStringArrays.GetAndHideHead()
				*arrayStringOp.Stringarrayop = ApbStringArrayUpdate{}
			} else {
				arrayStringOp = &ApbUpdateOperation_Stringarrayop{Stringarrayop: &ApbStringArrayUpdate{}}
				pbBuf.UpdStringArrays.AppendAndHide(arrayStringOp)
			}
			m.Op = arrayStringOp
			if err := arrayStringOp.Stringarrayop.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 22:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen

			//TODO: This should be optimized to allow proper re-usage of the fields (i.e., Bytearrayop)
			var arrayByteOp *ApbUpdateOperation_Bytearrayop
			if !pbBuf.UpdByteArrays.IsEmpty() {
				arrayByteOp = pbBuf.UpdByteArrays.GetAndHideHead()
				*arrayByteOp.Bytearrayop = ApbByteArrayUpdate{}
			} else {
				arrayByteOp = &ApbUpdateOperation_Bytearrayop{Bytearrayop: &ApbByteArrayUpdate{}}
				pbBuf.UpdByteArrays.AppendAndHide(arrayByteOp)
			}
			m.Op = arrayByteOp
			if err := arrayByteOp.Bytearrayop.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 23:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen

			var mapCounterOp *ApbUpdateOperation_Mapcounterop
			if !pbBuf.UpdMapCounters.IsEmpty() {
				mapCounterOp = pbBuf.UpdMapCounters.GetAndHideHead()
			} else {
				mapCounterOp = &ApbUpdateOperation_Mapcounterop{Mapcounterop: &ApbMapCounterUpdate{DataType: new(DATAType), UpdType: new(NumberArrayUpdType), IsDec: new(bool)}}
				pbBuf.UpdMapCounters.AppendAndHide(mapCounterOp)
			}
			m.Op = mapCounterOp
			if err := mapCounterOp.Mapcounterop.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 30:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			var multiOp *ApbUpdateOperation_Multiupdop
			if !pbBuf.UpdMultiOps.IsEmpty() {
				multiOp = pbBuf.UpdMultiOps.GetAndHideHead()
				//No need to reset as our custom handler will deal with the repeated field accordingly.
			} else {
				multiOp = &ApbUpdateOperation_Multiupdop{Multiupdop: &ApbMultiUpdate{Type: new(CRDTType)}}
				pbBuf.UpdMultiOps.AppendAndHide(multiOp)
			}
			m.Op = multiOp
			if err := multiOp.Multiupdop.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbCounterUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	hasInc := false
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
			v2 := int64(v)
			*m.Inc = v2
			hasInc = true
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if !hasInc {
		*m.Inc = 0
	}
	return nil
}

func (m *ApbBoundCounterUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
			v2 := int64(v)
			*m.Limit = v2
		case 2:
			var v uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
			v2 := int64(v)
			*m.InitialValue = v2
		case 3:
			var v int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			b := bool(v != 0)
			*m.CompEq = b
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbFlagUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			b := bool(v != 0)
			*m.Value = b
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbMaxMinUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
			v2 := int64(v)
			*m.Value = v2
		case 2:
			var v int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			b := bool(v != 0)
			*m.IsMax = b
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbTopkRmvUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	m.Rems = nil //Reset rems, as it may be used directly by PotionDB.
	hasLen := false
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Adds == nil {
				m.Adds = &ApbTopKRmvAdd{}
			}
			if err := m.Adds.UnmarshalVTSafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType == 0 {
				var v int32
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= int32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
				m.Rems = append(m.Rems, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Rems) == 0 {
					m.Rems = make([]int32, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v int32
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= int32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
					m.Rems[i] = v
					i++
				}
				m.Rems = m.Rems[:i]
			}
		case 3:
			hasLen = true
			var v uint32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if m.PositiveLen == nil {
				m.PositiveLen = &v
			} else {
				*m.PositiveLen = v
			}
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if !hasLen {
		m.PositiveLen = nil
	}
	return nil
}

func (m *ApbTopKRmvAdd) UnmarshalVTSafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.PlayerIds, m.Scores, m.Data = nil, nil, nil //Resetting as PotionDB may use the slices.
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v int32
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= int32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
				m.PlayerIds = append(m.PlayerIds, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.PlayerIds) == 0 {
					m.PlayerIds = make([]int32, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v int32
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= int32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
					m.PlayerIds[i] = v
					i++
				}
				m.PlayerIds = m.PlayerIds[:i]
			}
		case 2:
			if wireType == 0 {
				var v int32
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= int32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
				m.Scores = append(m.Scores, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Scores) == 0 {
					m.Scores = make([]int32, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v int32
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= int32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
					m.Scores[i] = v
					i++
				}
				m.Scores = m.Scores[:i]
			}
		case 3:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			data := make([]byte, postIndex-iNdEx)
			copy(data, dAtA[iNdEx:postIndex])
			m.Data = append(m.Data, data)
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbAverageUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	hasNValues := false
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
			v2 := int64(v)
			*m.Value = v2
		case 2:
			hasNValues = true
			var v uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
			v2 := int64(v)
			if m.NValues == nil {
				m.NValues = new(int64)
			}
			*m.NValues = v2
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if !hasNValues {
		m.NValues = nil
	}
	return nil
}

func (m *ApbCounterFloatUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v uint64
			v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			v2 := float64(math.Float64frombits(v))
			*m.Inc = v2
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbPairCounterUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	hasFirst, hasSecond := false, false

	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			hasFirst = true
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
			if m.IncFirst == nil {
				m.IncFirst = new(int32)
			}
			*m.IncFirst = v
		case 2:
			hasSecond = true
			var v uint64
			if (iNdEx + 8) > l {
				return io.ErrUnexpectedEOF
			}
			v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			v2 := float64(math.Float64frombits(v))
			if m.IncSecond == nil {
				m.IncSecond = new(float64)
			}
			*m.IncSecond = v2
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if !hasFirst {
		m.IncFirst = nil
	}
	if !hasSecond {
		m.IncSecond = nil
	}
	return nil
}

func (m *ApbMultiUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Updates = m.Updates[:0]
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if len(m.Updates) < cap(m.Updates) { //Attempt re-use.
				m.Updates = m.Updates[:len(m.Updates)+1]
				if m.Updates[len(m.Updates)-1] == nil {
					m.Updates[len(m.Updates)-1] = &ApbUpdateOperation{Specialop: new(SPECIAL_UPD)}
				}
			} else {
				m.Updates = append(m.Updates, &ApbUpdateOperation{Specialop: new(SPECIAL_UPD)})
			}
			if err := m.Updates[len(m.Updates)-1].UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			var v CRDTType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= CRDTType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.Type = v
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbMapUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	*m.IsAddsArray = false //Default value.
	m.Init = nil           //Init is rarely used.
	m.Updates, m.RemovedKeys = m.Updates[:0], m.RemovedKeys[:0]
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if len(m.Updates) < cap(m.Updates) { //Attempt re-use.
				m.Updates = m.Updates[:len(m.Updates)+1]
				if m.Updates[len(m.Updates)-1] == nil {
					m.Updates[len(m.Updates)-1] = &ApbMapNestedUpdate{Key: &ApbMapKey{Type: new(CRDTType)}, Update: &ApbUpdateOperation{Specialop: new(SPECIAL_UPD)}}
				}
			} else {
				m.Updates = append(m.Updates, &ApbMapNestedUpdate{Key: &ApbMapKey{Type: new(CRDTType)}, Update: &ApbUpdateOperation{Specialop: new(SPECIAL_UPD)}})
			}
			if err := m.Updates[len(m.Updates)-1].UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if len(m.RemovedKeys) < cap(m.RemovedKeys) { //Attempt re-use
				m.RemovedKeys = m.RemovedKeys[:len(m.RemovedKeys)+1]
				if m.RemovedKeys[len(m.RemovedKeys)-1] == nil {
					m.RemovedKeys[len(m.RemovedKeys)-1] = &ApbMapKey{Type: new(CRDTType)}
				}
			} else {
				m.RemovedKeys = append(m.RemovedKeys, &ApbMapKey{Type: new(CRDTType)})
			}
			if err := m.RemovedKeys[len(m.RemovedKeys)-1].UnmarshalVTSafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			var v int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			b := bool(v != 0)
			*m.IsAddsArray = b
		case 4:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			m.Init = &ApbMapInit{} //We always set it to nil at the start, as it is rarely used (i.e., only for initialization.)
			if err := m.Init.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbMapNestedUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			//Key is always already initialized.
			if err := m.Key.UnmarshalVTSafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			//Update is always already initialized.
			if err := m.Update.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbMapKey) UnmarshalVTSafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Key = make([]byte, byteLen)
			copy(m.Key, dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			var v CRDTType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= CRDTType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.Type = v
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbMultiArrayUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v MultiArrayType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= MultiArrayType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.Type = v
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IntUpd == nil {
				m.IntUpd = &ApbMultiArrayIntUpdate{}
			} /*else {
				*m.IntUpd = ApbMultiArrayIntUpdate{}
			}*/
			//if err := m.IntUpd.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
			if err := m.IntUpd.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.FloatUpd == nil {
				m.FloatUpd = &ApbMultiArrayFloatUpdate{}
			} /*else {
				*m.FloatUpd = ApbMultiArrayFloatUpdate{}
			}
			if err := m.FloatUpd.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {*/
			if err := m.FloatUpd.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.DataUpd == nil {
				m.DataUpd = &ApbMultiArrayDataUpdate{}
			} /* else {
				*m.DataUpd = ApbMultiArrayDataUpdate{}
			}
			if err := m.DataUpd.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {*/
			if err := m.DataUpd.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 5:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.AvgUpd == nil {
				m.AvgUpd = &ApbMultiArrayAvgUpdate{}
			} /* else {
				*m.AvgUpd = ApbMultiArrayAvgUpdate{}
			}
			if err := m.AvgUpd.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {*/
			if err := m.AvgUpd.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 6:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.MultiUpd == nil {
				m.MultiUpd = &ApbMultiArrayMultiUpdate{}
			} /* else {
				*m.MultiUpd = ApbMultiArrayMultiUpdate{}
			}
			if err := m.MultiUpd.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {*/
			if err := m.MultiUpd.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 7:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.SizeUpd == nil {
				m.SizeUpd = &ApbMultiArraySetSizeUpdate{}
			} /* else {
				*m.SizeUpd = ApbMultiArraySetSizeUpdate{}
			}
			if err := m.SizeUpd.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {*/
			if err := m.SizeUpd.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 8:
			var v NumberArrayUpdType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= NumberArrayUpdType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.UpdType = v
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbMultiArrayIntUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Inc == nil {
				m.Inc = &ApbMultiArrayIntInc{}
			}
			if err := m.Inc.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncSingle == nil {
				m.IncSingle = &ApbMultiArrayIntIncSingle{Pos: new(int32), Change: new(int64)}
			}
			if err := m.IncSingle.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncPos == nil {
				m.IncPos = &ApbMultiArrayIntIncPositions{}
			}
			if err := m.IncPos.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncRange == nil {
				m.IncRange = &ApbMultiArrayIntIncRange{From: new(int32), To: new(int32), Change: new(int64)}
			}
			if err := m.IncRange.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ApbMultiArrayFloatUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Inc == nil {
				m.Inc = &ApbMultiArrayFloatInc{}
			}
			if err := m.Inc.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncSingle == nil {
				m.IncSingle = &ApbMultiArrayFloatIncSingle{Pos: new(int32), Change: new(float64)}
			}
			if err := m.IncSingle.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncPos == nil {
				m.IncPos = &ApbMultiArrayFloatIncPositions{}
			}
			if err := m.IncPos.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncRange == nil {
				m.IncRange = &ApbMultiArrayFloatIncRange{From: new(int32), To: new(int32), Change: new(float64)}
			}
			if err := m.IncRange.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ApbMultiArrayDataUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Set == nil {
				m.Set = &ApbMultiArrayDataSet{}
			}
			if err := m.Set.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.SetSingle == nil {
				m.SetSingle = &ApbMultiArrayDataSetSingle{Pos: new(int32)}
			}
			if err := m.SetSingle.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.SetPos == nil {
				m.SetPos = &ApbMultiArrayDataSetPositions{}
			}
			if err := m.SetPos.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.SetRange == nil {
				m.SetRange = &ApbMultiArrayDataSetRange{From: new(int32), To: new(int32)}
			}
			if err := m.SetRange.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ApbMultiArrayAvgUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Inc == nil {
				m.Inc = &ApbMultiArrayAvgInc{}
			}
			if err := m.Inc.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncSingle == nil {
				m.IncSingle = &ApbMultiArrayAvgIncSingle{Pos: new(int32), Count: new(int32), Value: new(int64)}
			}
			if err := m.IncSingle.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncPos == nil {
				m.IncPos = &ApbMultiArrayAvgIncPositions{}
			}
			if err := m.IncPos.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncRange == nil {
				m.IncRange = &ApbMultiArrayAvgIncRange{From: new(int32), To: new(int32), Count: new(int32), Value: new(int64)}
			}
			if err := m.IncRange.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ApbMultiArraySetSizeUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	*m = ApbMultiArraySetSizeUpdate{} //This update is rarely used, so it's safer and easier to just reset fully.
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.IntSize = &v
		case 2:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.FloatSize = &v
		case 3:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.DataSize = &v
		case 4:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.AvgSize = &v
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ApbMultiArrayIntInc) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Changes = nil //Don't re-use the slice, as PotionDB will use this slice directly.
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v uint64
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= uint64(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
				m.Changes = append(m.Changes, int64(v))
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Changes) == 0 {
					m.Changes = make([]int64, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v uint64
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= uint64(b&0x7F) << shift
						if b < 0x80 {
							break
						}

					}
					v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
					m.Changes[i] = int64(v)
					i++
				}
				m.Changes = m.Changes[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Changes", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ApbMultiArrayIntIncSingle) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.Pos = v
		case 2:
			var v uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
			v2 := int64(v)
			*m.Change = v2
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMultiArrayIntIncPositions) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Pos, m.Change = nil, nil //Don't re-use the slices, as PotionDB will use these slices directly.
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v int32
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= int32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.Pos = append(m.Pos, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Pos) == 0 {
					m.Pos = make([]int32, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v int32
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= int32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.Pos[i] = v
					i++
				}
				m.Pos = m.Pos[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Pos", wireType)
			}
		case 2:
			if wireType == 0 {
				var v uint64
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= uint64(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
				m.Change = append(m.Change, int64(v))
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Change) == 0 {
					m.Change = make([]int64, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v uint64
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= uint64(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
					m.Change[i] = int64(v)
					i++
				}
				m.Change = m.Change[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Change", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ApbMultiArrayIntIncRange) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.From = v
		case 2:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.To = v
		case 3:
			var v uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
			v2 := int64(v)
			*m.Change = v2
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMultiArrayFloatInc) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Changes = nil //Don't re-use the slice, as PotionDB will use this slice directly.
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType == 1 {
				var v uint64
				v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
				iNdEx += 8
				v2 := float64(math.Float64frombits(v))
				m.Changes = append(m.Changes, v2)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				elementCount = packedLen / 8
				if elementCount != 0 && len(m.Changes) == 0 {
					m.Changes = make([]float64, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v uint64
					v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
					iNdEx += 8
					v2 := float64(math.Float64frombits(v))
					m.Changes[i] = v2
					i++
				}
				m.Changes = m.Changes[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Changes", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMultiArrayFloatIncSingle) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.Pos = v
		case 2:
			var v uint64
			if (iNdEx + 8) > l {
				return io.ErrUnexpectedEOF
			}
			v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			v2 := float64(math.Float64frombits(v))
			*m.Change = v2
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMultiArrayFloatIncPositions) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Pos, m.Change = nil, nil //Don't re-use the slices, as PotionDB will use these slices directly.
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v int32
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= int32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.Pos = append(m.Pos, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Pos) == 0 {
					m.Pos = make([]int32, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v int32
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= int32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.Pos[i] = v
					i++
				}
				m.Pos = m.Pos[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Pos", wireType)
			}
		case 2:
			if wireType == 1 {
				var v uint64
				v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
				iNdEx += 8
				v2 := float64(math.Float64frombits(v))
				m.Change = append(m.Change, v2)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				elementCount = packedLen / 8
				if elementCount != 0 && len(m.Change) == 0 {
					m.Change = make([]float64, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v uint64
					v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
					iNdEx += 8
					v2 := float64(math.Float64frombits(v))
					m.Change[i] = v2
					i++
				}
				m.Change = m.Change[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Change", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMultiArrayFloatIncRange) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.From = v
		case 2:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.To = v
		case 3:
			var v uint64
			if (iNdEx + 8) > l {
				return io.ErrUnexpectedEOF
			}
			v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			v2 := float64(math.Float64frombits(v))
			*m.Change = v2
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMultiArrayDataSet) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Data = nil //Don't re-use the slice, as PotionDB will use this slice directly.
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			data := make([]byte, postIndex-iNdEx)
			copy(data, dAtA[iNdEx:postIndex])
			m.Data = append(m.Data, data)
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMultiArrayDataSetSingle) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Data = nil
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.Pos = v
		case 2:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Data = append(m.Data, dAtA[iNdEx:postIndex]...)
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMultiArrayDataSetPositions) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Pos, m.Data = nil, nil //Don't re-use the slice, as PotionDB will use this slice directly.
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v int32
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= int32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.Pos = append(m.Pos, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Pos) == 0 {
					m.Pos = make([]int32, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v int32
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= int32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.Pos[i] = v
					i++
				}
				m.Pos = m.Pos[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Pos", wireType)
			}
		case 2:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + byteLen
			data := make([]byte, postIndex-iNdEx)
			copy(data, dAtA[iNdEx:postIndex])
			m.Data = append(m.Data, data)
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMultiArrayDataSetRange) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Data = nil //Don't re-use the slice, as PotionDB will use this slice directly.
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.From = v
		case 2:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.To = v
		case 3:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Data = append(m.Data, dAtA[iNdEx:postIndex]...)
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMultiArrayAvgInc) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Values, m.Counts = nil, nil //Don't re-use the slice, as PotionDB will use this slice directly.
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v uint64
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= uint64(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
				m.Values = append(m.Values, int64(v))
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Values) == 0 {
					m.Values = make([]int64, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v uint64
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= uint64(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
					m.Values[i] = int64(v)
					i++
				}
				m.Values = m.Values[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Values", wireType)
			}
		case 2:
			if wireType == 0 {
				var v int32
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= int32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
				m.Counts = append(m.Counts, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Counts) == 0 {
					m.Counts = make([]int32, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v int32
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= int32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
					m.Counts[i] = v
					i++
				}
				m.Counts = m.Counts[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Counts", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMultiArrayAvgIncSingle) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.Pos = v
		case 2:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
			*m.Count = v
		case 3:
			var v uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
			v2 := int64(v)
			*m.Value = v2
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMultiArrayAvgIncPositions) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Pos, m.Values, m.Counts = nil, nil, nil //Don't re-use the slices, as PotionDB will use these slices directly.
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v int32
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= int32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.Pos = append(m.Pos, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Pos) == 0 {
					m.Pos = make([]int32, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v int32
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= int32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.Pos[i] = v
					i++
				}
				m.Pos = m.Pos[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Pos", wireType)
			}
		case 2:
			if wireType == 0 {
				var v uint64
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= uint64(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
				m.Values = append(m.Values, int64(v))
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Values) == 0 {
					m.Values = make([]int64, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v uint64
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= uint64(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
					m.Values[i] = int64(v)
					i++
				}
				m.Values = m.Values[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Values", wireType)
			}
		case 3:
			if wireType == 0 {
				var v int32
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= int32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
				m.Counts = append(m.Counts, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Counts) == 0 {
					m.Counts = make([]int32, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v int32
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= int32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
					m.Counts[i] = v
					i++
				}
				m.Counts = m.Counts[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Counts", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMultiArrayAvgIncRange) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.From = v
		case 2:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.To = v
		case 3:
			var v uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
			v2 := int64(v)
			*m.Value = v2
		case 4:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
			*m.Count = v
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMultiArrayMultiUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	*m = ApbMultiArrayMultiUpdate{} //reset to avoid re-usage of the slices, as PotionDB directly uses them.
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v uint64
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= uint64(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
				m.Ints = append(m.Ints, int64(v))
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Ints) == 0 {
					m.Ints = make([]int64, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v uint64
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= uint64(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
					m.Ints[i] = int64(v)
					i++
				}
				m.Ints = m.Ints[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Ints", wireType)
			}
		case 2:
			if wireType == 1 {
				var v uint64
				v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
				iNdEx += 8
				v2 := float64(math.Float64frombits(v))
				m.Floats = append(m.Floats, v2)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				elementCount = packedLen / 8
				if elementCount != 0 && len(m.Floats) == 0 {
					m.Floats = make([]float64, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v uint64
					v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
					iNdEx += 8
					v2 := float64(math.Float64frombits(v))
					m.Floats[i] = v2
					i++
				}
				m.Floats = m.Floats[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Floats", wireType)
			}
		case 3:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			data := make([]byte, postIndex-iNdEx)
			copy(data, dAtA[iNdEx:postIndex])
			m.Data = append(m.Data, data)
			iNdEx = postIndex
		case 4:
			if wireType == 0 {
				var v int32
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= int32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
				m.Counts = append(m.Counts, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Counts) == 0 {
					m.Counts = make([]int32, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v int32
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= int32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
					m.Counts[i] = v
					i++
				}
				m.Counts = m.Counts[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Counts", wireType)
			}
		case 5:
			if wireType == 0 {
				var v uint64
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= uint64(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
				m.Sums = append(m.Sums, int64(v))
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Sums) == 0 {
					m.Sums = make([]int64, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v uint64
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= uint64(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
					m.Sums[i] = int64(v)
					i++
				}
				m.Sums = m.Sums[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Sums", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *ApbArrayCounterUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Inc == nil {
				m.Inc = &ApbArrayCounterIncrement{Index: new(int32), Inc: new(int64)}
			}
			if err := m.Inc.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncAll == nil {
				m.IncAll = &ApbArrayCounterIncrementAll{Inc: new(int64)}
			}
			if err := m.IncAll.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncMulti == nil {
				m.IncMulti = &ApbArrayCounterIncrementMulti{}
			}
			if err := m.IncMulti.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncSub == nil {
				m.IncSub = &ApbArrayCounterIncrementSub{}
			}
			if err := m.IncSub.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 5:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Size == nil {
				m.Size = &ApbArrayCounterSetSize{Size: new(int32)}
			}
			if err := m.Size.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 6:
			var v NumberArrayUpdType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= NumberArrayUpdType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.UpdType = v
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbArrayCounterIncrement) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.Index = v
		case 2:
			var v uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
			v2 := int64(v)
			*m.Inc = v2
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbArrayCounterIncrementAll) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
			v2 := int64(v)
			*m.Inc = v2
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbArrayCounterIncrementMulti) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Incs = nil //Don't re-use the slice, as PotionDB will use this slice directly.
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v uint64
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= uint64(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
				m.Incs = append(m.Incs, int64(v))
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Incs) == 0 {
					m.Incs = make([]int64, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v uint64
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= uint64(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
					m.Incs[i] = int64(v)
					i++
				}
				m.Incs = m.Incs[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Incs", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbArrayCounterIncrementSub) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Incs, m.Indexes = nil, nil //Don't re-use the slices, as PotionDB will use these slices directly.
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v int32
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= int32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.Indexes = append(m.Indexes, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Indexes) == 0 {
					m.Indexes = make([]int32, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v int32
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= int32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.Indexes[i] = v
					i++
				}
				m.Indexes = m.Indexes[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Indexes", wireType)
			}
		case 2:
			if wireType == 0 {
				var v uint64
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= uint64(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
				m.Incs = append(m.Incs, int64(v))
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Incs) == 0 {
					m.Incs = make([]int64, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v uint64
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= uint64(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
					m.Incs[i] = int64(v)
					i++
				}
				m.Incs = m.Incs[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Incs", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbArrayCounterSetSize) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.Size = v
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbArrayFloatUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Inc == nil {
				m.Inc = &ApbArrayFloatIncrement{Index: new(int32), Inc: new(float64)}
			}
			if err := m.Inc.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncAll == nil {
				m.IncAll = &ApbArrayFloatIncrementAll{Inc: new(float64)}
			}
			if err := m.IncAll.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncMulti == nil {
				m.IncMulti = &ApbArrayFloatIncrementMulti{}
			}
			if err := m.IncMulti.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncSub == nil {
				m.IncSub = &ApbArrayFloatIncrementSub{}
			}
			if err := m.IncSub.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 5:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncRange == nil {
				m.IncRange = &ApbArrayFloatIncrementRange{From: new(int32), To: new(int32), Inc: new(float64)}
			}
			if err := m.IncRange.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 6:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Size == nil {
				m.Size = &ApbArrayFloatSetSize{Size: new(int32)}
			}
			if err := m.Size.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 7:
			var v NumberArrayUpdType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= NumberArrayUpdType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.UpdType = v
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbArrayFloatIncrement) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.Index = v
		case 2:
			var v uint64
			if (iNdEx + 8) > l {
				return io.ErrUnexpectedEOF
			}
			v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			v2 := float64(math.Float64frombits(v))
			*m.Inc = v2
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbArrayFloatIncrementAll) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v uint64
			if (iNdEx + 8) > l {
				return io.ErrUnexpectedEOF
			}
			v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			v2 := float64(math.Float64frombits(v))
			*m.Inc = v2
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbArrayFloatIncrementMulti) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Incs = nil //Don't re-use the slice, as PotionDB will use this slice directly.
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType == 1 {
				var v uint64
				if (iNdEx + 8) > l {
					return io.ErrUnexpectedEOF
				}
				v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
				iNdEx += 8
				v2 := float64(math.Float64frombits(v))
				m.Incs = append(m.Incs, v2)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				elementCount = packedLen / 8
				if elementCount != 0 && len(m.Incs) == 0 {
					m.Incs = make([]float64, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v uint64
					if (iNdEx + 8) > l {
						return io.ErrUnexpectedEOF
					}
					v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
					iNdEx += 8
					v2 := float64(math.Float64frombits(v))
					m.Incs[i] = v2
					i++
				}
				m.Incs = m.Incs[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Incs", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbArrayFloatIncrementSub) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Incs, m.Indexes = nil, nil //Don't re-use the slices, as PotionDB will use these slices directly.
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v int32
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= int32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.Indexes = append(m.Indexes, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Indexes) == 0 {
					m.Indexes = make([]int32, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v int32
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= int32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.Indexes[i] = v
					i++
				}
				m.Indexes = m.Indexes[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Indexes", wireType)
			}
		case 2:
			if wireType == 1 {
				var v uint64
				if (iNdEx + 8) > l {
					return io.ErrUnexpectedEOF
				}
				v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
				iNdEx += 8
				v2 := float64(math.Float64frombits(v))
				m.Incs = append(m.Incs, v2)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				elementCount = packedLen / 8
				if elementCount != 0 && len(m.Incs) == 0 {
					m.Incs = make([]float64, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v uint64
					if (iNdEx + 8) > l {
						return io.ErrUnexpectedEOF
					}
					v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
					iNdEx += 8
					v2 := float64(math.Float64frombits(v))
					m.Incs[i] = v2
					i++
				}
				m.Incs = m.Incs[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Incs", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbArrayFloatIncrementRange) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.From = v
		case 2:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.To = v
		case 3:
			var v uint64
			if (iNdEx + 8) > l {
				return io.ErrUnexpectedEOF
			}
			v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			v2 := float64(math.Float64frombits(v))
			*m.Inc = v2
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbArrayFloatSetSize) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.Size = v
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}

func (m *ApbMapCounterUpdate) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	hasDec := false
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v DATAType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= DATAType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.DataType = v
		case 2:
			hasDec = true
			var v int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			b := bool(v != 0)
			*m.IsDec = b
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IntOp == nil {
				m.IntOp = &ApbMapIntOp{}
			}
			if err := m.IntOp.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.DoubleOp == nil {
				m.DoubleOp = &ApbMapDoubleOp{}
			}
			if err := m.DoubleOp.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 5:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Init == nil {
				m.Init = &ApbMapCounterInit{Size: new(int32)}
			}
			if err := m.Init.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 6:
			var v NumberArrayUpdType
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= NumberArrayUpdType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.UpdType = v
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	if !hasDec {
		*m.IsDec = false
	}
	return nil
}
func (m *ApbMapIntOp) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Inc == nil {
				m.Inc = &ApbMapIntSingleIncOp{Key: new(int32), Inc: new(int64)}
			}
			if err := m.Inc.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncAll == nil {
				m.IncAll = &ApbMapIntIncAllOp{Inc: new(int64)}
			}
			if err := m.IncAll.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncMulti == nil {
				m.IncMulti = &ApbMapIntIncMultOp{}
			}
			if err := m.IncMulti.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMapDoubleOp) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.Inc == nil {
				m.Inc = &ApbMapDoubleSingleIncOp{Key: new(int32), Inc: new(float64)}
			}
			if err := m.Inc.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncAll == nil {
				m.IncAll = &ApbMapDoubleIncAllOp{Inc: new(float64)}
			}
			if err := m.IncAll.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			var msglen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if m.IncMulti == nil {
				m.IncMulti = &ApbMapDoubleIncMultOp{}
			}
			if err := m.IncMulti.UnmarshalVTUnsafeReuse(dAtA[iNdEx:postIndex], pbBuf); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMapIntSingleIncOp) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Data = nil
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
			*m.Key = v
		case 2:
			var v uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
			v2 := int64(v)
			*m.Inc = v2
		case 3:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Data = append(m.Data, dAtA[iNdEx:postIndex]...)
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMapIntIncAllOp) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Keys, m.Data = nil, nil
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v int32
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= int32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
				m.Keys = append(m.Keys, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Keys) == 0 {
					m.Keys = make([]int32, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v int32
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= int32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
					m.Keys[i] = v
					i++
				}
				m.Keys = m.Keys[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Keys", wireType)
			}
		case 2:
			var v uint64
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
			v2 := int64(v)
			*m.Inc = v2
		case 3:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Data = append(m.Data, dAtA[iNdEx:postIndex]...)
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMapIntIncMultOp) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Keys, m.Inc, m.Data = nil, nil, nil
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v int32
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= int32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
				m.Keys = append(m.Keys, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Keys) == 0 {
					m.Keys = make([]int32, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v int32
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= int32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
					m.Keys[i] = v
					i++
				}
				m.Keys = m.Keys[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Keys", wireType)
			}
		case 2:
			if wireType == 0 {
				var v uint64
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= uint64(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
				m.Inc = append(m.Inc, int64(v))
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Inc) == 0 {
					m.Inc = make([]int64, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v uint64
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= uint64(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = (v >> 1) ^ uint64((int64(v&1)<<63)>>63)
					m.Inc[i] = int64(v)
					i++
				}
				m.Inc = m.Inc[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Inc", wireType)
			}
		case 3:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			data := make([]byte, postIndex-iNdEx)
			copy(data, dAtA[iNdEx:postIndex])
			m.Data = append(m.Data, data)
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMapDoubleSingleIncOp) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Data = nil
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
			*m.Key = v
		case 2:
			var v uint64
			if (iNdEx + 8) > l {
				return io.ErrUnexpectedEOF
			}
			v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			v2 := float64(math.Float64frombits(v))
			*m.Inc = v2
		case 3:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Data = append(m.Data, dAtA[iNdEx:postIndex]...)
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMapDoubleIncAllOp) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Keys, m.Data = nil, nil
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v int32
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= int32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
				m.Keys = append(m.Keys, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Keys) == 0 {
					m.Keys = make([]int32, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v int32
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= int32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
					m.Keys[i] = v
					i++
				}
				m.Keys = m.Keys[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Keys", wireType)
			}
		case 2:
			var v uint64
			if (iNdEx + 8) > l {
				return io.ErrUnexpectedEOF
			}
			v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			v2 := float64(math.Float64frombits(v))
			*m.Inc = v2
		case 3:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			m.Data = append(m.Data, dAtA[iNdEx:postIndex]...)
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMapDoubleIncMultOp) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	m.Keys, m.Inc, m.Data = nil, nil, nil
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v int32
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					v |= int32(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
				m.Keys = append(m.Keys, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.Keys) == 0 {
					m.Keys = make([]int32, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v int32
					for shift := uint(0); ; shift += 7 {
						b := dAtA[iNdEx]
						iNdEx++
						v |= int32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					v = int32((uint32(v) >> 1) ^ uint32(((v&1)<<31)>>31))
					m.Keys[i] = v
					i++
				}
				m.Keys = m.Keys[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Keys", wireType)
			}
		case 2:
			if wireType == 1 {
				var v uint64
				if (iNdEx + 8) > l {
					return io.ErrUnexpectedEOF
				}
				v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
				iNdEx += 8
				v2 := float64(math.Float64frombits(v))
				m.Inc = append(m.Inc, v2)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				postIndex := iNdEx + packedLen
				var elementCount int
				elementCount = packedLen / 8
				if elementCount != 0 && len(m.Inc) == 0 {
					m.Inc = make([]float64, elementCount)
				}
				i := 0
				for iNdEx < postIndex {
					var v uint64
					v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
					iNdEx += 8
					v2 := float64(math.Float64frombits(v))
					m.Inc[i] = v2
					i++
				}
				m.Inc = m.Inc[:i]
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Inc", wireType)
			}
		case 3:
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + byteLen
			data := make([]byte, postIndex-iNdEx)
			copy(data, dAtA[iNdEx:postIndex])
			m.Data = append(m.Data, data)
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
func (m *ApbMapCounterInit) UnmarshalVTUnsafeReuse(dAtA []byte, pbBuf *PbBuffers) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		switch fieldNum {
		case 1:
			var v int32
			for shift := uint(0); ; shift += 7 {
				b := dAtA[iNdEx]
				iNdEx++
				v |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			*m.Size = v
		default:
			iNdEx = preIndex
			skippy, err := protohelpers.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protohelpers.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	return nil
}
