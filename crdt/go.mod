module potionDB/crdt

go 1.20

require (
	github.com/AndreRijo/go-tools v0.0.0-20250702122434-f5d7580301a7
	github.com/golang/protobuf v1.5.4
	google.golang.org/protobuf v1.34.2
	potionDB/shared v0.0.0
)

replace potionDB/shared => ../shared

//replace github.com/AndreRijo/go-tools => ../../goTools
