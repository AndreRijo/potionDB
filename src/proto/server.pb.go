// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.21.12
// source: server.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type WrapperType int32

const (
	WrapperType_STATIC_READ_OBJS WrapperType = 1
	WrapperType_STATIC_READ      WrapperType = 2
	WrapperType_STATIC_UPDATE    WrapperType = 3
	WrapperType_START_TXN        WrapperType = 4
	WrapperType_READ_OBJS        WrapperType = 5
	WrapperType_READ             WrapperType = 6
	WrapperType_UPD              WrapperType = 7
	WrapperType_COMMIT           WrapperType = 8
	WrapperType_ABORT            WrapperType = 9
)

// Enum value maps for WrapperType.
var (
	WrapperType_name = map[int32]string{
		1: "STATIC_READ_OBJS",
		2: "STATIC_READ",
		3: "STATIC_UPDATE",
		4: "START_TXN",
		5: "READ_OBJS",
		6: "READ",
		7: "UPD",
		8: "COMMIT",
		9: "ABORT",
	}
	WrapperType_value = map[string]int32{
		"STATIC_READ_OBJS": 1,
		"STATIC_READ":      2,
		"STATIC_UPDATE":    3,
		"START_TXN":        4,
		"READ_OBJS":        5,
		"READ":             6,
		"UPD":              7,
		"COMMIT":           8,
		"ABORT":            9,
	}
)

func (x WrapperType) Enum() *WrapperType {
	p := new(WrapperType)
	*p = x
	return p
}

func (x WrapperType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (WrapperType) Descriptor() protoreflect.EnumDescriptor {
	return file_server_proto_enumTypes[0].Descriptor()
}

func (WrapperType) Type() protoreflect.EnumType {
	return &file_server_proto_enumTypes[0]
}

func (x WrapperType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Do not use.
func (x *WrapperType) UnmarshalJSON(b []byte) error {
	num, err := protoimpl.X.UnmarshalJSONEnum(x.Descriptor(), b)
	if err != nil {
		return err
	}
	*x = WrapperType(num)
	return nil
}

// Deprecated: Use WrapperType.Descriptor instead.
func (WrapperType) EnumDescriptor() ([]byte, []int) {
	return file_server_proto_rawDescGZIP(), []int{0}
}

type ApbServerConn struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *ApbServerConn) Reset() {
	*x = ApbServerConn{}
	if protoimpl.UnsafeEnabled {
		mi := &file_server_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ApbServerConn) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ApbServerConn) ProtoMessage() {}

func (x *ApbServerConn) ProtoReflect() protoreflect.Message {
	mi := &file_server_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ApbServerConn.ProtoReflect.Descriptor instead.
func (*ApbServerConn) Descriptor() ([]byte, []int) {
	return file_server_proto_rawDescGZIP(), []int{0}
}

type S2SWrapper struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ClientID       *int32                  `protobuf:"varint,1,req,name=clientID" json:"clientID,omitempty"`
	MsgID          *WrapperType            `protobuf:"varint,12,req,name=msgID,enum=WrapperType" json:"msgID,omitempty"`
	StaticReadObjs *ApbStaticReadObjects   `protobuf:"bytes,2,opt,name=staticReadObjs" json:"staticReadObjs,omitempty"`
	StaticRead     *ApbStaticRead          `protobuf:"bytes,3,opt,name=staticRead" json:"staticRead,omitempty"`
	StaticUpd      *ApbStaticUpdateObjects `protobuf:"bytes,4,opt,name=staticUpd" json:"staticUpd,omitempty"`
	StartTxn       *ApbStartTransaction    `protobuf:"bytes,5,opt,name=startTxn" json:"startTxn,omitempty"`
	ReadObjs       *ApbReadObjects         `protobuf:"bytes,6,opt,name=readObjs" json:"readObjs,omitempty"`
	Read           *ApbRead                `protobuf:"bytes,7,opt,name=read" json:"read,omitempty"`
	Upd            *ApbUpdateObjects       `protobuf:"bytes,8,opt,name=upd" json:"upd,omitempty"`
	CommitTxn      *ApbCommitTransaction   `protobuf:"bytes,9,opt,name=commitTxn" json:"commitTxn,omitempty"`
	AbortTxn       *ApbAbortTransaction    `protobuf:"bytes,10,opt,name=abortTxn" json:"abortTxn,omitempty"`
}

func (x *S2SWrapper) Reset() {
	*x = S2SWrapper{}
	if protoimpl.UnsafeEnabled {
		mi := &file_server_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *S2SWrapper) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*S2SWrapper) ProtoMessage() {}

func (x *S2SWrapper) ProtoReflect() protoreflect.Message {
	mi := &file_server_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use S2SWrapper.ProtoReflect.Descriptor instead.
func (*S2SWrapper) Descriptor() ([]byte, []int) {
	return file_server_proto_rawDescGZIP(), []int{1}
}

func (x *S2SWrapper) GetClientID() int32 {
	if x != nil && x.ClientID != nil {
		return *x.ClientID
	}
	return 0
}

func (x *S2SWrapper) GetMsgID() WrapperType {
	if x != nil && x.MsgID != nil {
		return *x.MsgID
	}
	return WrapperType_STATIC_READ_OBJS
}

func (x *S2SWrapper) GetStaticReadObjs() *ApbStaticReadObjects {
	if x != nil {
		return x.StaticReadObjs
	}
	return nil
}

func (x *S2SWrapper) GetStaticRead() *ApbStaticRead {
	if x != nil {
		return x.StaticRead
	}
	return nil
}

func (x *S2SWrapper) GetStaticUpd() *ApbStaticUpdateObjects {
	if x != nil {
		return x.StaticUpd
	}
	return nil
}

func (x *S2SWrapper) GetStartTxn() *ApbStartTransaction {
	if x != nil {
		return x.StartTxn
	}
	return nil
}

func (x *S2SWrapper) GetReadObjs() *ApbReadObjects {
	if x != nil {
		return x.ReadObjs
	}
	return nil
}

func (x *S2SWrapper) GetRead() *ApbRead {
	if x != nil {
		return x.Read
	}
	return nil
}

func (x *S2SWrapper) GetUpd() *ApbUpdateObjects {
	if x != nil {
		return x.Upd
	}
	return nil
}

func (x *S2SWrapper) GetCommitTxn() *ApbCommitTransaction {
	if x != nil {
		return x.CommitTxn
	}
	return nil
}

func (x *S2SWrapper) GetAbortTxn() *ApbAbortTransaction {
	if x != nil {
		return x.AbortTxn
	}
	return nil
}

type S2SWrapperReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ClientID       *int32                    `protobuf:"varint,1,req,name=clientID" json:"clientID,omitempty"`
	MsgID          *WrapperType              `protobuf:"varint,12,req,name=msgID,enum=WrapperType" json:"msgID,omitempty"`
	StaticReadObjs *ApbStaticReadObjectsResp `protobuf:"bytes,2,opt,name=staticReadObjs" json:"staticReadObjs,omitempty"`
	StartTxn       *ApbStartTransactionResp  `protobuf:"bytes,3,opt,name=startTxn" json:"startTxn,omitempty"`
	ReadObjs       *ApbReadObjectsResp       `protobuf:"bytes,4,opt,name=readObjs" json:"readObjs,omitempty"`
	Upd            *ApbOperationResp         `protobuf:"bytes,5,opt,name=upd" json:"upd,omitempty"`
	CommitTxn      *ApbCommitResp            `protobuf:"bytes,6,opt,name=commitTxn" json:"commitTxn,omitempty"`
}

func (x *S2SWrapperReply) Reset() {
	*x = S2SWrapperReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_server_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *S2SWrapperReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*S2SWrapperReply) ProtoMessage() {}

func (x *S2SWrapperReply) ProtoReflect() protoreflect.Message {
	mi := &file_server_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use S2SWrapperReply.ProtoReflect.Descriptor instead.
func (*S2SWrapperReply) Descriptor() ([]byte, []int) {
	return file_server_proto_rawDescGZIP(), []int{2}
}

func (x *S2SWrapperReply) GetClientID() int32 {
	if x != nil && x.ClientID != nil {
		return *x.ClientID
	}
	return 0
}

func (x *S2SWrapperReply) GetMsgID() WrapperType {
	if x != nil && x.MsgID != nil {
		return *x.MsgID
	}
	return WrapperType_STATIC_READ_OBJS
}

func (x *S2SWrapperReply) GetStaticReadObjs() *ApbStaticReadObjectsResp {
	if x != nil {
		return x.StaticReadObjs
	}
	return nil
}

func (x *S2SWrapperReply) GetStartTxn() *ApbStartTransactionResp {
	if x != nil {
		return x.StartTxn
	}
	return nil
}

func (x *S2SWrapperReply) GetReadObjs() *ApbReadObjectsResp {
	if x != nil {
		return x.ReadObjs
	}
	return nil
}

func (x *S2SWrapperReply) GetUpd() *ApbOperationResp {
	if x != nil {
		return x.Upd
	}
	return nil
}

func (x *S2SWrapperReply) GetCommitTxn() *ApbCommitResp {
	if x != nil {
		return x.CommitTxn
	}
	return nil
}

var File_server_proto protoreflect.FileDescriptor

var file_server_proto_rawDesc = []byte{
	0x0a, 0x0c, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x0e,
	0x61, 0x6e, 0x74, 0x69, 0x64, 0x6f, 0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x0f,
	0x0a, 0x0d, 0x41, 0x70, 0x62, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x43, 0x6f, 0x6e, 0x6e, 0x22,
	0xfb, 0x03, 0x0a, 0x0a, 0x53, 0x32, 0x53, 0x57, 0x72, 0x61, 0x70, 0x70, 0x65, 0x72, 0x12, 0x1a,
	0x0a, 0x08, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x49, 0x44, 0x18, 0x01, 0x20, 0x02, 0x28, 0x05,
	0x52, 0x08, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x49, 0x44, 0x12, 0x22, 0x0a, 0x05, 0x6d, 0x73,
	0x67, 0x49, 0x44, 0x18, 0x0c, 0x20, 0x02, 0x28, 0x0e, 0x32, 0x0c, 0x2e, 0x77, 0x72, 0x61, 0x70,
	0x70, 0x65, 0x72, 0x54, 0x79, 0x70, 0x65, 0x52, 0x05, 0x6d, 0x73, 0x67, 0x49, 0x44, 0x12, 0x3d,
	0x0a, 0x0e, 0x73, 0x74, 0x61, 0x74, 0x69, 0x63, 0x52, 0x65, 0x61, 0x64, 0x4f, 0x62, 0x6a, 0x73,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x41, 0x70, 0x62, 0x53, 0x74, 0x61, 0x74,
	0x69, 0x63, 0x52, 0x65, 0x61, 0x64, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x52, 0x0e, 0x73,
	0x74, 0x61, 0x74, 0x69, 0x63, 0x52, 0x65, 0x61, 0x64, 0x4f, 0x62, 0x6a, 0x73, 0x12, 0x2e, 0x0a,
	0x0a, 0x73, 0x74, 0x61, 0x74, 0x69, 0x63, 0x52, 0x65, 0x61, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x0e, 0x2e, 0x41, 0x70, 0x62, 0x53, 0x74, 0x61, 0x74, 0x69, 0x63, 0x52, 0x65, 0x61,
	0x64, 0x52, 0x0a, 0x73, 0x74, 0x61, 0x74, 0x69, 0x63, 0x52, 0x65, 0x61, 0x64, 0x12, 0x35, 0x0a,
	0x09, 0x73, 0x74, 0x61, 0x74, 0x69, 0x63, 0x55, 0x70, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x17, 0x2e, 0x41, 0x70, 0x62, 0x53, 0x74, 0x61, 0x74, 0x69, 0x63, 0x55, 0x70, 0x64, 0x61,
	0x74, 0x65, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x52, 0x09, 0x73, 0x74, 0x61, 0x74, 0x69,
	0x63, 0x55, 0x70, 0x64, 0x12, 0x30, 0x0a, 0x08, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54, 0x78, 0x6e,
	0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x41, 0x70, 0x62, 0x53, 0x74, 0x61, 0x72,
	0x74, 0x54, 0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x08, 0x73, 0x74,
	0x61, 0x72, 0x74, 0x54, 0x78, 0x6e, 0x12, 0x2b, 0x0a, 0x08, 0x72, 0x65, 0x61, 0x64, 0x4f, 0x62,
	0x6a, 0x73, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x41, 0x70, 0x62, 0x52, 0x65,
	0x61, 0x64, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x52, 0x08, 0x72, 0x65, 0x61, 0x64, 0x4f,
	0x62, 0x6a, 0x73, 0x12, 0x1c, 0x0a, 0x04, 0x72, 0x65, 0x61, 0x64, 0x18, 0x07, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x08, 0x2e, 0x41, 0x70, 0x62, 0x52, 0x65, 0x61, 0x64, 0x52, 0x04, 0x72, 0x65, 0x61,
	0x64, 0x12, 0x23, 0x0a, 0x03, 0x75, 0x70, 0x64, 0x18, 0x08, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x11,
	0x2e, 0x41, 0x70, 0x62, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74,
	0x73, 0x52, 0x03, 0x75, 0x70, 0x64, 0x12, 0x33, 0x0a, 0x09, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74,
	0x54, 0x78, 0x6e, 0x18, 0x09, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x41, 0x70, 0x62, 0x43,
	0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x54, 0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e,
	0x52, 0x09, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x54, 0x78, 0x6e, 0x12, 0x30, 0x0a, 0x08, 0x61,
	0x62, 0x6f, 0x72, 0x74, 0x54, 0x78, 0x6e, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e,
	0x41, 0x70, 0x62, 0x41, 0x62, 0x6f, 0x72, 0x74, 0x54, 0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x74,
	0x69, 0x6f, 0x6e, 0x52, 0x08, 0x61, 0x62, 0x6f, 0x72, 0x74, 0x54, 0x78, 0x6e, 0x22, 0xce, 0x02,
	0x0a, 0x0f, 0x53, 0x32, 0x53, 0x57, 0x72, 0x61, 0x70, 0x70, 0x65, 0x72, 0x52, 0x65, 0x70, 0x6c,
	0x79, 0x12, 0x1a, 0x0a, 0x08, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x49, 0x44, 0x18, 0x01, 0x20,
	0x02, 0x28, 0x05, 0x52, 0x08, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x49, 0x44, 0x12, 0x22, 0x0a,
	0x05, 0x6d, 0x73, 0x67, 0x49, 0x44, 0x18, 0x0c, 0x20, 0x02, 0x28, 0x0e, 0x32, 0x0c, 0x2e, 0x77,
	0x72, 0x61, 0x70, 0x70, 0x65, 0x72, 0x54, 0x79, 0x70, 0x65, 0x52, 0x05, 0x6d, 0x73, 0x67, 0x49,
	0x44, 0x12, 0x41, 0x0a, 0x0e, 0x73, 0x74, 0x61, 0x74, 0x69, 0x63, 0x52, 0x65, 0x61, 0x64, 0x4f,
	0x62, 0x6a, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x41, 0x70, 0x62, 0x53,
	0x74, 0x61, 0x74, 0x69, 0x63, 0x52, 0x65, 0x61, 0x64, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73,
	0x52, 0x65, 0x73, 0x70, 0x52, 0x0e, 0x73, 0x74, 0x61, 0x74, 0x69, 0x63, 0x52, 0x65, 0x61, 0x64,
	0x4f, 0x62, 0x6a, 0x73, 0x12, 0x34, 0x0a, 0x08, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54, 0x78, 0x6e,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x18, 0x2e, 0x41, 0x70, 0x62, 0x53, 0x74, 0x61, 0x72,
	0x74, 0x54, 0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x73, 0x70,
	0x52, 0x08, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54, 0x78, 0x6e, 0x12, 0x2f, 0x0a, 0x08, 0x72, 0x65,
	0x61, 0x64, 0x4f, 0x62, 0x6a, 0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x13, 0x2e, 0x41,
	0x70, 0x62, 0x52, 0x65, 0x61, 0x64, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x52, 0x65, 0x73,
	0x70, 0x52, 0x08, 0x72, 0x65, 0x61, 0x64, 0x4f, 0x62, 0x6a, 0x73, 0x12, 0x23, 0x0a, 0x03, 0x75,
	0x70, 0x64, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x41, 0x70, 0x62, 0x4f, 0x70,
	0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x52, 0x03, 0x75, 0x70, 0x64,
	0x12, 0x2c, 0x0a, 0x09, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x54, 0x78, 0x6e, 0x18, 0x06, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x41, 0x70, 0x62, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x52,
	0x65, 0x73, 0x70, 0x52, 0x09, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x54, 0x78, 0x6e, 0x2a, 0x8f,
	0x01, 0x0a, 0x0b, 0x77, 0x72, 0x61, 0x70, 0x70, 0x65, 0x72, 0x54, 0x79, 0x70, 0x65, 0x12, 0x14,
	0x0a, 0x10, 0x53, 0x54, 0x41, 0x54, 0x49, 0x43, 0x5f, 0x52, 0x45, 0x41, 0x44, 0x5f, 0x4f, 0x42,
	0x4a, 0x53, 0x10, 0x01, 0x12, 0x0f, 0x0a, 0x0b, 0x53, 0x54, 0x41, 0x54, 0x49, 0x43, 0x5f, 0x52,
	0x45, 0x41, 0x44, 0x10, 0x02, 0x12, 0x11, 0x0a, 0x0d, 0x53, 0x54, 0x41, 0x54, 0x49, 0x43, 0x5f,
	0x55, 0x50, 0x44, 0x41, 0x54, 0x45, 0x10, 0x03, 0x12, 0x0d, 0x0a, 0x09, 0x53, 0x54, 0x41, 0x52,
	0x54, 0x5f, 0x54, 0x58, 0x4e, 0x10, 0x04, 0x12, 0x0d, 0x0a, 0x09, 0x52, 0x45, 0x41, 0x44, 0x5f,
	0x4f, 0x42, 0x4a, 0x53, 0x10, 0x05, 0x12, 0x08, 0x0a, 0x04, 0x52, 0x45, 0x41, 0x44, 0x10, 0x06,
	0x12, 0x07, 0x0a, 0x03, 0x55, 0x50, 0x44, 0x10, 0x07, 0x12, 0x0a, 0x0a, 0x06, 0x43, 0x4f, 0x4d,
	0x4d, 0x49, 0x54, 0x10, 0x08, 0x12, 0x09, 0x0a, 0x05, 0x41, 0x42, 0x4f, 0x52, 0x54, 0x10, 0x09,
	0x42, 0x14, 0x5a, 0x12, 0x70, 0x6f, 0x74, 0x69, 0x6f, 0x6e, 0x44, 0x42, 0x2f, 0x73, 0x72, 0x63,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
}

var (
	file_server_proto_rawDescOnce sync.Once
	file_server_proto_rawDescData = file_server_proto_rawDesc
)

func file_server_proto_rawDescGZIP() []byte {
	file_server_proto_rawDescOnce.Do(func() {
		file_server_proto_rawDescData = protoimpl.X.CompressGZIP(file_server_proto_rawDescData)
	})
	return file_server_proto_rawDescData
}

var file_server_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_server_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_server_proto_goTypes = []interface{}{
	(WrapperType)(0),                 // 0: wrapperType
	(*ApbServerConn)(nil),            // 1: ApbServerConn
	(*S2SWrapper)(nil),               // 2: S2SWrapper
	(*S2SWrapperReply)(nil),          // 3: S2SWrapperReply
	(*ApbStaticReadObjects)(nil),     // 4: ApbStaticReadObjects
	(*ApbStaticRead)(nil),            // 5: ApbStaticRead
	(*ApbStaticUpdateObjects)(nil),   // 6: ApbStaticUpdateObjects
	(*ApbStartTransaction)(nil),      // 7: ApbStartTransaction
	(*ApbReadObjects)(nil),           // 8: ApbReadObjects
	(*ApbRead)(nil),                  // 9: ApbRead
	(*ApbUpdateObjects)(nil),         // 10: ApbUpdateObjects
	(*ApbCommitTransaction)(nil),     // 11: ApbCommitTransaction
	(*ApbAbortTransaction)(nil),      // 12: ApbAbortTransaction
	(*ApbStaticReadObjectsResp)(nil), // 13: ApbStaticReadObjectsResp
	(*ApbStartTransactionResp)(nil),  // 14: ApbStartTransactionResp
	(*ApbReadObjectsResp)(nil),       // 15: ApbReadObjectsResp
	(*ApbOperationResp)(nil),         // 16: ApbOperationResp
	(*ApbCommitResp)(nil),            // 17: ApbCommitResp
}
var file_server_proto_depIdxs = []int32{
	0,  // 0: S2SWrapper.msgID:type_name -> wrapperType
	4,  // 1: S2SWrapper.staticReadObjs:type_name -> ApbStaticReadObjects
	5,  // 2: S2SWrapper.staticRead:type_name -> ApbStaticRead
	6,  // 3: S2SWrapper.staticUpd:type_name -> ApbStaticUpdateObjects
	7,  // 4: S2SWrapper.startTxn:type_name -> ApbStartTransaction
	8,  // 5: S2SWrapper.readObjs:type_name -> ApbReadObjects
	9,  // 6: S2SWrapper.read:type_name -> ApbRead
	10, // 7: S2SWrapper.upd:type_name -> ApbUpdateObjects
	11, // 8: S2SWrapper.commitTxn:type_name -> ApbCommitTransaction
	12, // 9: S2SWrapper.abortTxn:type_name -> ApbAbortTransaction
	0,  // 10: S2SWrapperReply.msgID:type_name -> wrapperType
	13, // 11: S2SWrapperReply.staticReadObjs:type_name -> ApbStaticReadObjectsResp
	14, // 12: S2SWrapperReply.startTxn:type_name -> ApbStartTransactionResp
	15, // 13: S2SWrapperReply.readObjs:type_name -> ApbReadObjectsResp
	16, // 14: S2SWrapperReply.upd:type_name -> ApbOperationResp
	17, // 15: S2SWrapperReply.commitTxn:type_name -> ApbCommitResp
	16, // [16:16] is the sub-list for method output_type
	16, // [16:16] is the sub-list for method input_type
	16, // [16:16] is the sub-list for extension type_name
	16, // [16:16] is the sub-list for extension extendee
	0,  // [0:16] is the sub-list for field type_name
}

func init() { file_server_proto_init() }
func file_server_proto_init() {
	if File_server_proto != nil {
		return
	}
	file_antidote_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_server_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ApbServerConn); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_server_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*S2SWrapper); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_server_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*S2SWrapperReply); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_server_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_server_proto_goTypes,
		DependencyIndexes: file_server_proto_depIdxs,
		EnumInfos:         file_server_proto_enumTypes,
		MessageInfos:      file_server_proto_msgTypes,
	}.Build()
	File_server_proto = out.File
	file_server_proto_rawDesc = nil
	file_server_proto_goTypes = nil
	file_server_proto_depIdxs = nil
}