module github.com/metricq/metricq-source-ipmi

go 1.21

toolchain go1.23.2

require (
	// github.com/bougou/go-ipmi v0.4.1-0.20240219025858-089b836031ea
	github.com/bougou/go-ipmi v0.7.3
	github.com/metricq/metricq-go v0.0.0
)

require (
	github.com/google/uuid v1.3.1 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/rabbitmq/amqp091-go v1.8.1 // indirect
	github.com/rogpeppe/go-internal v1.6.1 // indirect
	golang.org/x/net v0.0.0-20210813160813-60bc85c4be6d // indirect
	google.golang.org/protobuf v1.27.1 // indirect
)

replace github.com/metricq/metricq-go => ../metricq-go
replace github.com/bougou/go-ipmi => ../go-ipmi
