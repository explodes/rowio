
dirs:
	mkdir -p ./protos

proto: dirs pbdb.proto
	protoc --go_out=plugins=grpc:./protos pbdb.proto