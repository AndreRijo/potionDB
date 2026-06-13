package proto

/*

const (
	CRDTType_COUNTER       CRDTType = 0
	CRDTType_COUNTER_FLOAT CRDTType = 1
	CRDTType_ORSET         CRDTType = 2
	CRDTType_LWWREG        CRDTType = 3
	CRDTType_MVREG         CRDTType = 4
	CRDTType_GMAP          CRDTType = 5
	CRDTType_RWSET         CRDTType = 6
	CRDTType_RRMAP         CRDTType = 7
	CRDTType_FATCOUNTER    CRDTType = 8
	CRDTType_FLAG_EW       CRDTType = 9
	CRDTType_FLAG_DW       CRDTType = 10
	CRDTType_FLAG_LWW      CRDTType = 11
	CRDTType_TOPK          CRDTType = 12
	CRDTType_TOPK_RMV      CRDTType = 13
	CRDTType_AVG           CRDTType = 14
	CRDTType_LEADERBOARD   CRDTType = 15
	CRDTType_MAXMIN        CRDTType = 16
	CRDTType_ORMAP         CRDTType = 17
	CRDTType_TOPSUM        CRDTType = 18
	CRDTType_PAIR_COUNTER  CRDTType = 19
	CRDTType_ARRAY_COUNTER CRDTType = 20
	CRDTType_ARRAY_FLOAT   CRDTType = 21
	CRDTType_MULTI_ARRAY   CRDTType = 22
	CRDTType_SIMPLE_DATE   CRDTType = 23
	CRDTType_SETW_DATE     CRDTType = 24
	CRDTType_INCW_DATE     CRDTType = 25
	CRDTType_SET_ONLY_DATE CRDTType = 26
	CRDTType_ARRAY_COMPACT CRDTType = 27
	CRDTType_ARRAY_STRING  CRDTType = 28
	CRDTType_ARRAY_BYTE    CRDTType = 29
	CRDTType_MAP_COUNTER   CRDTType = 30 //Generic counter map. PotionDB's implementation supports both ints and floats under this CRDT (but only a single type per CRDT instance)
	CRDTType_NOOP          CRDTType = 31
	CRDTType_TOPK_RMV_EXT  CRDTType = 32
)
*/

//Shared pointer vars for re-use in protobufs.
/*var (
	P_CRDTType_COUNTER = proto.CRDTType_COUNTER.Enum()
	P_CRDTType_COUNTER_FLOAT = proto.CRDTType_COUNTER_FLOAT.Enum()
	P_CRDTType_ORSET = proto.CRDTType_ORSET.Enum()
	P_CRDTType_LWWREG = proto.CRDTType_LWWREG.Enum()
	P_CRDTType_MVREG = proto.CRDTType_MVREG.Enum()
	P_CRDTType_GMAP = proto.CRDTType_GMAP.Enum()
	P_CRDTType_RWSET = proto.CRDTType_RWSET.Enum()
	P_CRDTType_RRMAP = proto.CRDTType_RRMAP.Enum()
	P_CRDTType_FATCOUNTER = proto.CRDTType_FATCOUNTER.Enum()
	P_CRDTType_FLAG_EW = proto.CRDTType_FLAG_EW.Enum()
	P_CRDTType_FLAG_DW = proto.CRDTType_FLAG_DW.Enum()
	P_CRDTType_FLAG_LWW = proto.CRDTType_FLAG_LWW.Enum()
	P_CRDTType_TOPK = proto.CRDTType_TOPK.Enum()
	P_CRDTType_TOPK_RMV = proto.CRDTType_TOPK_RMV.Enum()
	P_CRDTType_AVG = proto.CRDTType_AVG.Enum()
	P_CRDTType_LEADERBOARD = proto.CRDTType_LEADERBOARD.Enum()
	P_CRDTType_MAXMIN = proto.CRDTType_MAXMIN.Enum()
	P_CRDTType_ORMAP = proto.CRDTType_ORMAP.Enum()
	P_CRDTType_TOPSUM = proto.CRDTType_TOPSUM.Enum()
	P_CRDTType_PAIR_COUNTER = proto.CRDTType_PAIR_COUNTER.Enum()
	P_CRDTType_ARRAY_COUNTER = proto.CRDTType_ARRAY_COUNTER.Enum()
	P_CRDTType_ARRAY_FLOAT = proto.CRDTType_ARRAY_FLOAT.Enum()
	P_CRDTType_MULTI_ARRAY = proto.CRDTType_MULTI_ARRAY.Enum()
	P_CRDTType_SIMPLE_DATE = proto.CRDTType_SIMPLE_DATE.Enum()
	P_CRDTType_SETW_DATE = proto.CRDTType_SETW_DATE.Enum()
	P_CRDTType_INCW_DATE = proto.CRDTType_INCW_DATE.Enum()
	P_CRDTType_SET_ONLY_DATE = proto.CRDTType_SET_ONLY_DATE.Enum()
	P_CRDTType_ARRAY_COMPACT = proto.CRDTType_ARRAY_COMPACT.Enum()
	P_CRDTType_ARRAY_STRING = proto.CRDTType_ARRAY_STRING.Enum()
	P_CRDTType_ARRAY_BYTE = proto.CRDTType_ARRAY_BYTE.Enum()
	P_CRDTType_MAP_COUNTER = proto.CRDTType_MAP_COUNTER.Enum()
	P_CRDTType_NOOP = proto.CRDTType_NOOP.Enum()
	P_CRDTType_TOPK_RMV_EXT = proto.CRDTType_TOPK_RMV_EXT.Enum()
)*/

// Shared pointer vars for re-use in protobufs.
var CRDTType_pointers []*CRDTType = initializeCRDTTypePointers()

func initializeCRDTTypePointers() (pointers []*CRDTType) {
	pointers = make([]*CRDTType, len(CRDTType_name))
	for id := range CRDTType_name {
		pointers[id] = CRDTType(id).Enum()
	}
	return pointers
}

func (x CRDTType) GetSharedPointer() *CRDTType {
	return CRDTType_pointers[x]
}
