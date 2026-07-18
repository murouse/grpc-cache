package grpccache

import (
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"

	pb "github.com/murouse/grpc-cache/pkg/api/murouse/grpc_cache/v1"
)

type CacheFailurePolicy int

const (
	CacheFailurePolicyFallbackToHandler CacheFailurePolicy = 1
	CacheFailurePolicyReturnError       CacheFailurePolicy = 2
)

type Policy struct {
	TTL          time.Duration
	ActorScoped  bool
	ResponseType protoreflect.MessageType
}

func PolicyToModel(p *pb.Policy, responseType protoreflect.MessageType) Policy {
	return Policy{
		TTL:          p.Ttl.AsDuration(),
		ActorScoped:  p.ActorScoped,
		ResponseType: responseType,
	}
}
