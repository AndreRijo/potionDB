//Keeps a list of the components that should act as "disabled" in some way

package shared

const (
	//CRDTs
	IsVMDisabled   = true
	IsCRDTDisabled = false //Replaces all CRDTs by EmptyCrdt instances
	//PotionDB
	IsReplDisabled = false
	IsLogDisabled  = false
)
