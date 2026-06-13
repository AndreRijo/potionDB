package proto

//Note: Currently unused. So far the code is a straight copy from the generated code.

import (
	fmt "fmt"
	io "io"

	protohelpers "github.com/planetscale/vtprotobuf/protohelpers"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

type ApbReadObjectRespFast struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Resp isApbReadObjectResp_Resp
}

func (x *ApbGetCounterResp) isApbReadObjectResp_Resp()      {}
func (x *ApbGetSetResp) isApbReadObjectResp_Resp()          {}
func (x *ApbGetRegResp) isApbReadObjectResp_Resp()          {}
func (x *ApbGetMVRegResp) isApbReadObjectResp_Resp()        {}
func (x *ApbGetMapResp) isApbReadObjectResp_Resp()          {}
func (x *ApbGetFlagResp) isApbReadObjectResp_Resp()         {}
func (x *ApbGetMaxMinResp) isApbReadObjectResp_Resp()       {}
func (x *ApbGetTopkResp) isApbReadObjectResp_Resp()         {}
func (x *ApbGetLeaderboardResp) isApbReadObjectResp_Resp()  {}
func (x *ApbGetAverageResp) isApbReadObjectResp_Resp()      {}
func (x *ApbGetCounterFloatResp) isApbReadObjectResp_Resp() {}
func (x *ApbGetPairCounterResp) isApbReadObjectResp_Resp()  {}
func (x *ApbGetArrayCounterResp) isApbReadObjectResp_Resp() {}
func (x *ApbGetArrayFloatResp) isApbReadObjectResp_Resp()   {}
func (x *ApbGetMultiArrayResp) isApbReadObjectResp_Resp()   {}
func (x *ApbGetDateResp) isApbReadObjectResp_Resp()         {}
func (x *ApbGetArrayCompResp) isApbReadObjectResp_Resp()    {}
func (x *ApbGetArrayStringResp) isApbReadObjectResp_Resp()  {}
func (x *ApbGetArrayByteResp) isApbReadObjectResp_Resp()    {}
func (x *ApbGetMapCounterResp) isApbReadObjectResp_Resp()   {}
func (x *ApbPartialReadResp) isApbReadObjectResp_Resp()     {}

func (m *ApbReadObjectRespFast) MarshalVT() (dAtA []byte, err error) {
	if m == nil {
		return nil, nil
	}
	size := m.SizeVT()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBufferVT(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ApbReadObjectRespFast) MarshalToVT(dAtA []byte) (int, error) {
	size := m.SizeVT()
	return m.MarshalToSizedBufferVT(dAtA[:size])
}

func (m *ApbReadObjectRespFast) MarshalToSizedBufferVT(dAtA []byte) (int, error) {
	if m == nil {
		return 0, nil
	}
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.unknownFields != nil {
		i -= len(m.unknownFields)
		copy(dAtA[i:], m.unknownFields)
	}
	if vtmsg, ok := m.Resp.(interface {
		MarshalToSizedBufferVT([]byte) (int, error)
	}); ok {
		size, err := vtmsg.MarshalToSizedBufferVT(dAtA[:i])
		if err != nil {
			return 0, err
		}
		i -= size
	}
	return len(dAtA) - i, nil
}

func (m *ApbReadObjectRespFast) SizeVT() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if vtmsg, ok := m.Resp.(interface{ SizeVT() int }); ok {
		n += vtmsg.SizeVT()
	}
	n += len(m.unknownFields)
	return n
}

func (m *ApbReadObjectRespFast) UnmarshalVT(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return protohelpers.ErrIntOverflow
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: ApbReadObjectResp: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ApbReadObjectResp: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Counter", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Counter); ok {
				if err := oneof.Counter.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetCounterResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Counter{Counter: v}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Set", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Set); ok {
				if err := oneof.Set.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetSetResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Set{Set: v}
			}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Reg", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Reg); ok {
				if err := oneof.Reg.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetRegResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Reg{Reg: v}
			}
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Mvreg", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Mvreg); ok {
				if err := oneof.Mvreg.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetMVRegResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Mvreg{Mvreg: v}
			}
			iNdEx = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Map", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Map); ok {
				if err := oneof.Map.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetMapResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Map{Map: v}
			}
			iNdEx = postIndex
		case 6:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Flag", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Flag); ok {
				if err := oneof.Flag.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetFlagResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Flag{Flag: v}
			}
			iNdEx = postIndex
		case 7:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Maxmin", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Maxmin); ok {
				if err := oneof.Maxmin.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetMaxMinResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Maxmin{Maxmin: v}
			}
			iNdEx = postIndex
		case 8:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Topk", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Topk); ok {
				if err := oneof.Topk.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetTopkResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Topk{Topk: v}
			}
			iNdEx = postIndex
		case 9:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Leadb", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Leadb); ok {
				if err := oneof.Leadb.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetLeaderboardResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Leadb{Leadb: v}
			}
			iNdEx = postIndex
		case 10:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Avg", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Avg); ok {
				if err := oneof.Avg.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetAverageResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Avg{Avg: v}
			}
			iNdEx = postIndex
		case 11:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Counterfloat", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Counterfloat); ok {
				if err := oneof.Counterfloat.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetCounterFloatResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Counterfloat{Counterfloat: v}
			}
			iNdEx = postIndex
		case 12:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Paircounter", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Paircounter); ok {
				if err := oneof.Paircounter.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetPairCounterResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Paircounter{Paircounter: v}
			}
			iNdEx = postIndex
		case 13:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Arraycounter", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Arraycounter); ok {
				if err := oneof.Arraycounter.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetArrayCounterResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Arraycounter{Arraycounter: v}
			}
			iNdEx = postIndex
		case 14:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Arrayfloat", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Arrayfloat); ok {
				if err := oneof.Arrayfloat.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetArrayFloatResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Arrayfloat{Arrayfloat: v}
			}
			iNdEx = postIndex
		case 15:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Multiarray", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Multiarray); ok {
				if err := oneof.Multiarray.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetMultiArrayResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Multiarray{Multiarray: v}
			}
			iNdEx = postIndex
		case 16:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Date", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Date); ok {
				if err := oneof.Date.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetDateResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Date{Date: v}
			}
			iNdEx = postIndex
		case 17:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Compactarray", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Compactarray); ok {
				if err := oneof.Compactarray.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetArrayCompResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Compactarray{Compactarray: v}
			}
			iNdEx = postIndex
		case 18:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Stringarray", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Stringarray); ok {
				if err := oneof.Stringarray.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetArrayStringResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Stringarray{Stringarray: v}
			}
			iNdEx = postIndex
		case 19:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Bytearray", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Bytearray); ok {
				if err := oneof.Bytearray.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetArrayByteResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Bytearray{Bytearray: v}
			}
			iNdEx = postIndex
		case 20:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Mapcounter", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Mapcounter); ok {
				if err := oneof.Mapcounter.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetMapCounterResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Mapcounter{Mapcounter: v}
			}
			iNdEx = postIndex
		case 21:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Partread", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Partread); ok {
				if err := oneof.Partread.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbPartialReadResp{}
				if err := v.UnmarshalVT(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Partread{Partread: v}
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
func (m *ApbReadObjectRespFast) UnmarshalVTUnsafe(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return protohelpers.ErrIntOverflow
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: ApbReadObjectResp: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ApbReadObjectResp: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Counter", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Counter); ok {
				if err := oneof.Counter.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetCounterResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Counter{Counter: v}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Set", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Set); ok {
				if err := oneof.Set.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetSetResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Set{Set: v}
			}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Reg", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Reg); ok {
				if err := oneof.Reg.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetRegResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Reg{Reg: v}
			}
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Mvreg", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Mvreg); ok {
				if err := oneof.Mvreg.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetMVRegResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Mvreg{Mvreg: v}
			}
			iNdEx = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Map", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Map); ok {
				if err := oneof.Map.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetMapResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Map{Map: v}
			}
			iNdEx = postIndex
		case 6:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Flag", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Flag); ok {
				if err := oneof.Flag.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetFlagResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Flag{Flag: v}
			}
			iNdEx = postIndex
		case 7:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Maxmin", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Maxmin); ok {
				if err := oneof.Maxmin.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetMaxMinResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Maxmin{Maxmin: v}
			}
			iNdEx = postIndex
		case 8:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Topk", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Topk); ok {
				if err := oneof.Topk.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetTopkResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Topk{Topk: v}
			}
			iNdEx = postIndex
		case 9:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Leadb", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Leadb); ok {
				if err := oneof.Leadb.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetLeaderboardResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Leadb{Leadb: v}
			}
			iNdEx = postIndex
		case 10:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Avg", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Avg); ok {
				if err := oneof.Avg.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetAverageResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Avg{Avg: v}
			}
			iNdEx = postIndex
		case 11:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Counterfloat", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Counterfloat); ok {
				if err := oneof.Counterfloat.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetCounterFloatResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Counterfloat{Counterfloat: v}
			}
			iNdEx = postIndex
		case 12:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Paircounter", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Paircounter); ok {
				if err := oneof.Paircounter.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetPairCounterResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Paircounter{Paircounter: v}
			}
			iNdEx = postIndex
		case 13:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Arraycounter", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Arraycounter); ok {
				if err := oneof.Arraycounter.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetArrayCounterResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Arraycounter{Arraycounter: v}
			}
			iNdEx = postIndex
		case 14:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Arrayfloat", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Arrayfloat); ok {
				if err := oneof.Arrayfloat.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetArrayFloatResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Arrayfloat{Arrayfloat: v}
			}
			iNdEx = postIndex
		case 15:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Multiarray", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Multiarray); ok {
				if err := oneof.Multiarray.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetMultiArrayResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Multiarray{Multiarray: v}
			}
			iNdEx = postIndex
		case 16:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Date", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Date); ok {
				if err := oneof.Date.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetDateResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Date{Date: v}
			}
			iNdEx = postIndex
		case 17:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Compactarray", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Compactarray); ok {
				if err := oneof.Compactarray.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetArrayCompResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Compactarray{Compactarray: v}
			}
			iNdEx = postIndex
		case 18:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Stringarray", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Stringarray); ok {
				if err := oneof.Stringarray.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetArrayStringResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Stringarray{Stringarray: v}
			}
			iNdEx = postIndex
		case 19:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Bytearray", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Bytearray); ok {
				if err := oneof.Bytearray.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetArrayByteResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Bytearray{Bytearray: v}
			}
			iNdEx = postIndex
		case 20:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Mapcounter", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Mapcounter); ok {
				if err := oneof.Mapcounter.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbGetMapCounterResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Mapcounter{Mapcounter: v}
			}
			iNdEx = postIndex
		case 21:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Partread", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protohelpers.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return protohelpers.ErrInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return protohelpers.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if oneof, ok := m.Resp.(*ApbReadObjectResp_Partread); ok {
				if err := oneof.Partread.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			} else {
				v := &ApbPartialReadResp{}
				if err := v.UnmarshalVTUnsafe(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				m.Resp = &ApbReadObjectResp_Partread{Partread: v}
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
