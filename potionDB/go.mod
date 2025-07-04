module potionDB/potionDB

go 1.16

require (
	github.com/AndreRijo/go-tools v0.0.0-20250702122434-f5d7580301a7
	github.com/streadway/amqp v1.1.0
	github.com/twmb/murmur3 v1.1.5
	google.golang.org/protobuf v1.34.2
	potionDB/crdt v0.0.0
	potionDB/shared v0.0.0
	sqlToKeyValue v0.0.0
	tpch_data_processor v0.0.0
)

replace sqlToKeyValue v0.0.0 => ../../sqlToKeyValue

replace potionDB/crdt v0.0.0 => ../crdt

replace potionDB/shared v0.0.0 => ../shared

replace tpch_data_processor v0.0.0 => ../../tpch_data_processor
