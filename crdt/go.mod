module potionDB/crdt

go 1.22

require (
	github.com/AndreRijo/go-tools v0.0.0-20250702122434-f5d7580301a7
	github.com/golang/protobuf v1.5.4
	github.com/planetscale/vtprotobuf v0.6.0
	google.golang.org/protobuf v1.34.2
	potionDB/shared v0.0.0
)

require (
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
)

replace potionDB/shared => ../shared

replace github.com/AndreRijo/go-tools => ../../goTools
