module potionDB/potionDB

go 1.22

require (
	github.com/AndreRijo/go-tools v0.0.0-20250702122434-f5d7580301a7
	github.com/streadway/amqp v1.1.0
	github.com/twmb/murmur3 v1.1.5
	github.com/zeebo/xxh3 v1.0.2
	google.golang.org/protobuf v1.34.2
	potionDB/crdt v0.0.0
	potionDB/shared v0.0.0
	sqlToKeyValue v0.0.0
	tpch_data_processor v0.0.0
)

require (
	github.com/AndreRijo/memory v0.0.0-00010101000000-000000000000 // indirect
	github.com/antlr/antlr4/runtime/Go/antlr/v4 v4.0.0-20230321174746-8dcc6526cfb1 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	golang.org/x/exp v0.0.0-20220722155223-a9213eeb770e // indirect
)

replace sqlToKeyValue v0.0.0 => ../../sqlToKeyValue

replace potionDB/crdt v0.0.0 => ../crdt

replace potionDB/shared v0.0.0 => ../shared

replace tpch_data_processor v0.0.0 => ../../tpch_data_processor

replace github.com/AndreRijo/go-tools => ../../goTools

replace github.com/AndreRijo/memory v0.0.0-00010101000000-000000000000 => ../../memory
