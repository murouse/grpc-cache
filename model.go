package grpccache

import (
	"time"

	pb "github.com/murouse/grpc-cache/pkg/api/murouse/grpc-cache/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
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
