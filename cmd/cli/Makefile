
dirs:
	mkdir -p ./protos

proto: dirs cli.proto
	protoc --go_out=plugins=grpc:./protos cli.proto