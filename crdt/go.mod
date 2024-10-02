module potionDB/crdt

go 1.20

require (
	github.com/golang/protobuf v1.5.4
	github.com/twmb/murmur3 v1.1.5
	google.golang.org/protobuf v1.34.2
	potionDB/shared v0.0.0
)

replace potionDB/shared => ../shared
