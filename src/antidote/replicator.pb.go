// Code generated by protoc-gen-go. DO NOT EDIT.
// source: replicator.proto

package antidote

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type ProtoStableClock struct {
	SenderID             *int64   `protobuf:"varint,1,req,name=senderID" json:"senderID,omitempty"`
	ReplicaTs            *int64   `protobuf:"varint,2,req,name=replicaTs" json:"replicaTs,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ProtoStableClock) Reset()         { *m = ProtoStableClock{} }
func (m *ProtoStableClock) String() string { return proto.CompactTextString(m) }
func (*ProtoStableClock) ProtoMessage()    {}
func (*ProtoStableClock) Descriptor() ([]byte, []int) {
	return fileDescriptor_e741e2c5586b74b6, []int{0}
}

func (m *ProtoStableClock) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ProtoStableClock.Unmarshal(m, b)
}
func (m *ProtoStableClock) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ProtoStableClock.Marshal(b, m, deterministic)
}
func (m *ProtoStableClock) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ProtoStableClock.Merge(m, src)
}
func (m *ProtoStableClock) XXX_Size() int {
	return xxx_messageInfo_ProtoStableClock.Size(m)
}
func (m *ProtoStableClock) XXX_DiscardUnknown() {
	xxx_messageInfo_ProtoStableClock.DiscardUnknown(m)
}

var xxx_messageInfo_ProtoStableClock proto.InternalMessageInfo

func (m *ProtoStableClock) GetSenderID() int64 {
	if m != nil && m.SenderID != nil {
		return *m.SenderID
	}
	return 0
}

func (m *ProtoStableClock) GetReplicaTs() int64 {
	if m != nil && m.ReplicaTs != nil {
		return *m.ReplicaTs
	}
	return 0
}

type ProtoReplicatePart struct {
	SenderID             *int64          `protobuf:"varint,1,req,name=senderID" json:"senderID,omitempty"`
	Txn                  *ProtoRemoteTxn `protobuf:"bytes,2,req,name=txn" json:"txn,omitempty"`
	PartitionID          *int64          `protobuf:"varint,3,req,name=partitionID" json:"partitionID,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *ProtoReplicatePart) Reset()         { *m = ProtoReplicatePart{} }
func (m *ProtoReplicatePart) String() string { return proto.CompactTextString(m) }
func (*ProtoReplicatePart) ProtoMessage()    {}
func (*ProtoReplicatePart) Descriptor() ([]byte, []int) {
	return fileDescriptor_e741e2c5586b74b6, []int{1}
}

func (m *ProtoReplicatePart) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ProtoReplicatePart.Unmarshal(m, b)
}
func (m *ProtoReplicatePart) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ProtoReplicatePart.Marshal(b, m, deterministic)
}
func (m *ProtoReplicatePart) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ProtoReplicatePart.Merge(m, src)
}
func (m *ProtoReplicatePart) XXX_Size() int {
	return xxx_messageInfo_ProtoReplicatePart.Size(m)
}
func (m *ProtoReplicatePart) XXX_DiscardUnknown() {
	xxx_messageInfo_ProtoReplicatePart.DiscardUnknown(m)
}

var xxx_messageInfo_ProtoReplicatePart proto.InternalMessageInfo

func (m *ProtoReplicatePart) GetSenderID() int64 {
	if m != nil && m.SenderID != nil {
		return *m.SenderID
	}
	return 0
}

func (m *ProtoReplicatePart) GetTxn() *ProtoRemoteTxn {
	if m != nil {
		return m.Txn
	}
	return nil
}

func (m *ProtoReplicatePart) GetPartitionID() int64 {
	if m != nil && m.PartitionID != nil {
		return *m.PartitionID
	}
	return 0
}

type ProtoRemoteTxn struct {
	Timestamp            []byte                `protobuf:"bytes,1,req,name=timestamp" json:"timestamp,omitempty"`
	Upds                 []*ProtoDownstreamUpd `protobuf:"bytes,2,rep,name=upds" json:"upds,omitempty"`
	XXX_NoUnkeyedLiteral struct{}              `json:"-"`
	XXX_unrecognized     []byte                `json:"-"`
	XXX_sizecache        int32                 `json:"-"`
}

func (m *ProtoRemoteTxn) Reset()         { *m = ProtoRemoteTxn{} }
func (m *ProtoRemoteTxn) String() string { return proto.CompactTextString(m) }
func (*ProtoRemoteTxn) ProtoMessage()    {}
func (*ProtoRemoteTxn) Descriptor() ([]byte, []int) {
	return fileDescriptor_e741e2c5586b74b6, []int{2}
}

func (m *ProtoRemoteTxn) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ProtoRemoteTxn.Unmarshal(m, b)
}
func (m *ProtoRemoteTxn) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ProtoRemoteTxn.Marshal(b, m, deterministic)
}
func (m *ProtoRemoteTxn) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ProtoRemoteTxn.Merge(m, src)
}
func (m *ProtoRemoteTxn) XXX_Size() int {
	return xxx_messageInfo_ProtoRemoteTxn.Size(m)
}
func (m *ProtoRemoteTxn) XXX_DiscardUnknown() {
	xxx_messageInfo_ProtoRemoteTxn.DiscardUnknown(m)
}

var xxx_messageInfo_ProtoRemoteTxn proto.InternalMessageInfo

func (m *ProtoRemoteTxn) GetTimestamp() []byte {
	if m != nil {
		return m.Timestamp
	}
	return nil
}

func (m *ProtoRemoteTxn) GetUpds() []*ProtoDownstreamUpd {
	if m != nil {
		return m.Upds
	}
	return nil
}

type ProtoDownstreamUpd struct {
	KeyParams            *ApbBoundObject         `protobuf:"bytes,1,req,name=keyParams" json:"keyParams,omitempty"`
	CounterOp            *ProtoCounterDownstream `protobuf:"bytes,2,opt,name=counterOp" json:"counterOp,omitempty"`
	SetOp                *ProtoSetDownstream     `protobuf:"bytes,3,opt,name=setOp" json:"setOp,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                `json:"-"`
	XXX_unrecognized     []byte                  `json:"-"`
	XXX_sizecache        int32                   `json:"-"`
}

func (m *ProtoDownstreamUpd) Reset()         { *m = ProtoDownstreamUpd{} }
func (m *ProtoDownstreamUpd) String() string { return proto.CompactTextString(m) }
func (*ProtoDownstreamUpd) ProtoMessage()    {}
func (*ProtoDownstreamUpd) Descriptor() ([]byte, []int) {
	return fileDescriptor_e741e2c5586b74b6, []int{3}
}

func (m *ProtoDownstreamUpd) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ProtoDownstreamUpd.Unmarshal(m, b)
}
func (m *ProtoDownstreamUpd) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ProtoDownstreamUpd.Marshal(b, m, deterministic)
}
func (m *ProtoDownstreamUpd) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ProtoDownstreamUpd.Merge(m, src)
}
func (m *ProtoDownstreamUpd) XXX_Size() int {
	return xxx_messageInfo_ProtoDownstreamUpd.Size(m)
}
func (m *ProtoDownstreamUpd) XXX_DiscardUnknown() {
	xxx_messageInfo_ProtoDownstreamUpd.DiscardUnknown(m)
}

var xxx_messageInfo_ProtoDownstreamUpd proto.InternalMessageInfo

func (m *ProtoDownstreamUpd) GetKeyParams() *ApbBoundObject {
	if m != nil {
		return m.KeyParams
	}
	return nil
}

func (m *ProtoDownstreamUpd) GetCounterOp() *ProtoCounterDownstream {
	if m != nil {
		return m.CounterOp
	}
	return nil
}

func (m *ProtoDownstreamUpd) GetSetOp() *ProtoSetDownstream {
	if m != nil {
		return m.SetOp
	}
	return nil
}

type ProtoCounterDownstream struct {
	IsInc                *bool    `protobuf:"varint,1,req,name=isInc" json:"isInc,omitempty"`
	Change               *int32   `protobuf:"varint,2,req,name=change" json:"change,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ProtoCounterDownstream) Reset()         { *m = ProtoCounterDownstream{} }
func (m *ProtoCounterDownstream) String() string { return proto.CompactTextString(m) }
func (*ProtoCounterDownstream) ProtoMessage()    {}
func (*ProtoCounterDownstream) Descriptor() ([]byte, []int) {
	return fileDescriptor_e741e2c5586b74b6, []int{4}
}

func (m *ProtoCounterDownstream) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ProtoCounterDownstream.Unmarshal(m, b)
}
func (m *ProtoCounterDownstream) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ProtoCounterDownstream.Marshal(b, m, deterministic)
}
func (m *ProtoCounterDownstream) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ProtoCounterDownstream.Merge(m, src)
}
func (m *ProtoCounterDownstream) XXX_Size() int {
	return xxx_messageInfo_ProtoCounterDownstream.Size(m)
}
func (m *ProtoCounterDownstream) XXX_DiscardUnknown() {
	xxx_messageInfo_ProtoCounterDownstream.DiscardUnknown(m)
}

var xxx_messageInfo_ProtoCounterDownstream proto.InternalMessageInfo

func (m *ProtoCounterDownstream) GetIsInc() bool {
	if m != nil && m.IsInc != nil {
		return *m.IsInc
	}
	return false
}

func (m *ProtoCounterDownstream) GetChange() int32 {
	if m != nil && m.Change != nil {
		return *m.Change
	}
	return 0
}

type ProtoSetDownstream struct {
	Adds                 []*ProtoValueUnique  `protobuf:"bytes,1,rep,name=adds" json:"adds,omitempty"`
	Rems                 []*ProtoValueUniques `protobuf:"bytes,2,rep,name=rems" json:"rems,omitempty"`
	XXX_NoUnkeyedLiteral struct{}             `json:"-"`
	XXX_unrecognized     []byte               `json:"-"`
	XXX_sizecache        int32                `json:"-"`
}

func (m *ProtoSetDownstream) Reset()         { *m = ProtoSetDownstream{} }
func (m *ProtoSetDownstream) String() string { return proto.CompactTextString(m) }
func (*ProtoSetDownstream) ProtoMessage()    {}
func (*ProtoSetDownstream) Descriptor() ([]byte, []int) {
	return fileDescriptor_e741e2c5586b74b6, []int{5}
}

func (m *ProtoSetDownstream) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ProtoSetDownstream.Unmarshal(m, b)
}
func (m *ProtoSetDownstream) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ProtoSetDownstream.Marshal(b, m, deterministic)
}
func (m *ProtoSetDownstream) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ProtoSetDownstream.Merge(m, src)
}
func (m *ProtoSetDownstream) XXX_Size() int {
	return xxx_messageInfo_ProtoSetDownstream.Size(m)
}
func (m *ProtoSetDownstream) XXX_DiscardUnknown() {
	xxx_messageInfo_ProtoSetDownstream.DiscardUnknown(m)
}

var xxx_messageInfo_ProtoSetDownstream proto.InternalMessageInfo

func (m *ProtoSetDownstream) GetAdds() []*ProtoValueUnique {
	if m != nil {
		return m.Adds
	}
	return nil
}

func (m *ProtoSetDownstream) GetRems() []*ProtoValueUniques {
	if m != nil {
		return m.Rems
	}
	return nil
}

type ProtoValueUnique struct {
	Value                []byte   `protobuf:"bytes,1,req,name=value" json:"value,omitempty"`
	Unique               *uint64  `protobuf:"varint,2,req,name=unique" json:"unique,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ProtoValueUnique) Reset()         { *m = ProtoValueUnique{} }
func (m *ProtoValueUnique) String() string { return proto.CompactTextString(m) }
func (*ProtoValueUnique) ProtoMessage()    {}
func (*ProtoValueUnique) Descriptor() ([]byte, []int) {
	return fileDescriptor_e741e2c5586b74b6, []int{6}
}

func (m *ProtoValueUnique) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ProtoValueUnique.Unmarshal(m, b)
}
func (m *ProtoValueUnique) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ProtoValueUnique.Marshal(b, m, deterministic)
}
func (m *ProtoValueUnique) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ProtoValueUnique.Merge(m, src)
}
func (m *ProtoValueUnique) XXX_Size() int {
	return xxx_messageInfo_ProtoValueUnique.Size(m)
}
func (m *ProtoValueUnique) XXX_DiscardUnknown() {
	xxx_messageInfo_ProtoValueUnique.DiscardUnknown(m)
}

var xxx_messageInfo_ProtoValueUnique proto.InternalMessageInfo

func (m *ProtoValueUnique) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func (m *ProtoValueUnique) GetUnique() uint64 {
	if m != nil && m.Unique != nil {
		return *m.Unique
	}
	return 0
}

type ProtoValueUniques struct {
	Value                []byte   `protobuf:"bytes,1,req,name=value" json:"value,omitempty"`
	Uniques              []uint64 `protobuf:"varint,2,rep,name=uniques" json:"uniques,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ProtoValueUniques) Reset()         { *m = ProtoValueUniques{} }
func (m *ProtoValueUniques) String() string { return proto.CompactTextString(m) }
func (*ProtoValueUniques) ProtoMessage()    {}
func (*ProtoValueUniques) Descriptor() ([]byte, []int) {
	return fileDescriptor_e741e2c5586b74b6, []int{7}
}

func (m *ProtoValueUniques) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ProtoValueUniques.Unmarshal(m, b)
}
func (m *ProtoValueUniques) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ProtoValueUniques.Marshal(b, m, deterministic)
}
func (m *ProtoValueUniques) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ProtoValueUniques.Merge(m, src)
}
func (m *ProtoValueUniques) XXX_Size() int {
	return xxx_messageInfo_ProtoValueUniques.Size(m)
}
func (m *ProtoValueUniques) XXX_DiscardUnknown() {
	xxx_messageInfo_ProtoValueUniques.DiscardUnknown(m)
}

var xxx_messageInfo_ProtoValueUniques proto.InternalMessageInfo

func (m *ProtoValueUniques) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func (m *ProtoValueUniques) GetUniques() []uint64 {
	if m != nil {
		return m.Uniques
	}
	return nil
}

func init() {
	proto.RegisterType((*ProtoStableClock)(nil), "ProtoStableClock")
	proto.RegisterType((*ProtoReplicatePart)(nil), "ProtoReplicatePart")
	proto.RegisterType((*ProtoRemoteTxn)(nil), "ProtoRemoteTxn")
	proto.RegisterType((*ProtoDownstreamUpd)(nil), "ProtoDownstreamUpd")
	proto.RegisterType((*ProtoCounterDownstream)(nil), "ProtoCounterDownstream")
	proto.RegisterType((*ProtoSetDownstream)(nil), "ProtoSetDownstream")
	proto.RegisterType((*ProtoValueUnique)(nil), "ProtoValueUnique")
	proto.RegisterType((*ProtoValueUniques)(nil), "ProtoValueUniques")
}

func init() { proto.RegisterFile("replicator.proto", fileDescriptor_e741e2c5586b74b6) }

var fileDescriptor_e741e2c5586b74b6 = []byte{
	// 423 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x7c, 0x53, 0x4d, 0x6f, 0xd3, 0x40,
	0x10, 0x55, 0x62, 0x07, 0x92, 0x09, 0x2a, 0xed, 0x82, 0x8a, 0x55, 0x71, 0x08, 0x96, 0x80, 0x70,
	0xa0, 0x87, 0x48, 0xdc, 0xa1, 0x89, 0x90, 0x22, 0x21, 0x25, 0xda, 0xb6, 0x20, 0x71, 0xdb, 0xd8,
	0x23, 0x30, 0xb5, 0x77, 0x97, 0xdd, 0x31, 0x94, 0x1f, 0xc4, 0xff, 0x44, 0xfb, 0x11, 0x9c, 0x28,
	0xd0, 0x9b, 0xdf, 0xbc, 0x37, 0x6f, 0xde, 0x8c, 0x6d, 0x38, 0x36, 0xa8, 0xeb, 0xaa, 0x10, 0xa4,
	0xcc, 0xb9, 0x36, 0x8a, 0xd4, 0xd9, 0x91, 0x90, 0x54, 0x95, 0x8a, 0x30, 0xe0, 0xfc, 0x03, 0x1c,
	0xaf, 0xdd, 0xc3, 0x25, 0x89, 0x4d, 0x8d, 0xf3, 0x5a, 0x15, 0x37, 0xec, 0x0c, 0x86, 0x16, 0x65,
	0x89, 0x66, 0xb9, 0xc8, 0x7a, 0x93, 0xfe, 0x34, 0xe1, 0x7f, 0x31, 0x7b, 0x0a, 0xa3, 0xe8, 0x79,
	0x65, 0xb3, 0xbe, 0x27, 0xbb, 0x42, 0xde, 0x02, 0xf3, 0x6e, 0x3c, 0x8e, 0xc5, 0xb5, 0x30, 0x74,
	0xa7, 0xdf, 0x33, 0x48, 0xe8, 0x56, 0x7a, 0xa7, 0xf1, 0xec, 0xe1, 0x79, 0xec, 0x6e, 0x14, 0xe1,
	0xd5, 0xad, 0xe4, 0x8e, 0x63, 0x13, 0x18, 0x6b, 0x61, 0xa8, 0xa2, 0x4a, 0xc9, 0xe5, 0x22, 0x4b,
	0xbc, 0xc3, 0x6e, 0x29, 0xff, 0x04, 0x47, 0xfb, 0x8d, 0x2e, 0x26, 0x55, 0x0d, 0x5a, 0x12, 0x8d,
	0xf6, 0x33, 0x1f, 0xf0, 0xae, 0xc0, 0x5e, 0x42, 0xda, 0xea, 0xd2, 0xe5, 0x4f, 0xa6, 0xe3, 0xd9,
	0xa3, 0x30, 0x75, 0xa1, 0x7e, 0x4a, 0x4b, 0x06, 0x45, 0x73, 0xad, 0x4b, 0xee, 0x05, 0xf9, 0xef,
	0x5e, 0x5c, 0x68, 0x8f, 0x64, 0xaf, 0x61, 0x74, 0x83, 0xbf, 0xd6, 0xc2, 0x88, 0xc6, 0x7a, 0x77,
	0x17, 0xfd, 0x9d, 0xde, 0x5c, 0xa8, 0x56, 0x96, 0xab, 0xcd, 0x37, 0x2c, 0x88, 0x77, 0x0a, 0xf6,
	0x06, 0x46, 0x85, 0x6a, 0x25, 0xa1, 0x59, 0xe9, 0xac, 0x3f, 0xe9, 0x4d, 0xc7, 0xb3, 0x27, 0x61,
	0xe6, 0x3c, 0x94, 0x3b, 0x77, 0xde, 0x29, 0xd9, 0x2b, 0x18, 0x58, 0xa4, 0x95, 0xce, 0x12, 0xdf,
	0x12, 0x63, 0x5e, 0x22, 0xed, 0xc8, 0x83, 0x22, 0x7f, 0x0f, 0xa7, 0xff, 0xf6, 0x63, 0x8f, 0x61,
	0x50, 0xd9, 0xa5, 0x2c, 0x7c, 0xcc, 0x21, 0x0f, 0x80, 0x9d, 0xc2, 0xbd, 0xe2, 0xab, 0x90, 0x5f,
	0xd0, 0x1f, 0x7e, 0xc0, 0x23, 0xca, 0x8b, 0xb8, 0xee, 0xde, 0x10, 0xf6, 0x1c, 0x52, 0x51, 0x96,
	0x6e, 0x53, 0x77, 0xae, 0x93, 0x90, 0xe3, 0xa3, 0xa8, 0x5b, 0xbc, 0x96, 0xd5, 0xf7, 0x16, 0xb9,
	0xa7, 0xd9, 0x0b, 0x48, 0x0d, 0x36, 0xdb, 0xab, 0xb2, 0x03, 0x99, 0xe5, 0x9e, 0xcf, 0xdf, 0xc6,
	0x4f, 0x6e, 0x87, 0x72, 0x31, 0x7f, 0x38, 0x18, 0xdf, 0x55, 0x00, 0x2e, 0x66, 0xeb, 0x79, 0x1f,
	0x33, 0xe5, 0x11, 0xe5, 0x73, 0x38, 0x39, 0x30, 0xff, 0x8f, 0x45, 0x06, 0xf7, 0x43, 0x53, 0xc8,
	0x95, 0xf2, 0x2d, 0xbc, 0x80, 0xcf, 0xc3, 0xed, 0xbf, 0xf0, 0x27, 0x00, 0x00, 0xff, 0xff, 0xd1,
	0x44, 0x8f, 0xdf, 0x28, 0x03, 0x00, 0x00,
}