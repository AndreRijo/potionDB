//Keeps a list of the components that should act as "disabled" in some way.
//Think of most of them as debugging tools/ways to check how each feature may affect PotionDB's performance.

package shared

const (
	//CRDTs
	IsVMDisabled   = true
	IsCRDTDisabled = false //Replaces all CRDTs by EmptyCrdt instances
	IsGCDisabled   = false
)

//Vars as they come from configs
var (
	//PotionDB
	IsReplDisabled        = false
	IsLogDisabled         = false
	IsReadWaitingDisabled = false //If this is true, all reads are returned instantly, ignoring any clock restrictions.
)
