module potionDB

go 1.16

require (
	github.com/golang/protobuf v1.5.2
	github.com/streadway/amqp v1.0.0
	github.com/twmb/murmur3 v1.1.5
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/protobuf v1.26.0
	tpch_data v0.0.0-00010101000000-000000000000
)

replace tpch_data => ../tpch_data
