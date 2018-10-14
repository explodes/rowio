
proto: service.proto
	protoc --go_out=plugins=grpc:. service.proto