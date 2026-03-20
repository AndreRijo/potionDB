package shared

var (
	ReplicaID                   uint16
	SortedReplicaID             uint16
	Buckets                     []string
	PotionDBPort                int
	TmpHistoryDisable           bool = false //If true, new updates will not generate any history information. Useful for initial data loading.
	TRUE_POINTER, FALSE_POINTER      = makeBoolPointer(true), makeBoolPointer(false)
)

const (
	MAX_REPLICA_ID      = uint16(1024)
	BITS_FOR_REPLICA_ID = 10
)

func makeBoolPointer(value bool) *bool {
	return &value
}
