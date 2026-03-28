MODULE = github.com/mooyg/walrus

.PHONY: proto
proto:
	protoc --go_out=. --go_opt=module=$(MODULE) \
	       --go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
	       proto/walrus.proto
