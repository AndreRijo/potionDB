//Keeps a list of the components that should act as "disabled" in some way.
//Think of most of them as debugging tools/ways to check how each feature may affect PotionDB's performance.

package shared

const (
	IsCRDTDisabled          = false //Replaces all CRDTs by EmptyCrdt instances
	IsBCPermSharingDisabled = true
)

// Vars as they come from configs
var (
	//PotionDB
	IsGCDisabled          = false
	IsVMDisabled          = false
	IsReplDisabled        = false
	IsLogDisabled         = false
	IsReadWaitingDisabled = false //If this is true, all reads are returned instantly, ignoring any clock restrictions.
)
