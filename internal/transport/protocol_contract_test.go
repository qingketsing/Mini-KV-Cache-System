package transport_test

import (
	"testing"

	minikvv1 "github.com/qingketsing/Mini-KV-Cache-System/gen/go/minikv/v1"
)

func TestProtocolServiceNames(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{
			name: "cache service",
			got:  minikvv1.CacheService_ServiceDesc.ServiceName,
			want: "minikv.v1.CacheService",
		},
		{
			name: "node service",
			got:  minikvv1.NodeService_ServiceDesc.ServiceName,
			want: "minikv.v1.NodeService",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("service name = %q, want %q", tt.got, tt.want)
			}
		})
	}
}

func TestProtocolPutRequestOneof(t *testing.T) {
	request := &minikvv1.PutRequest{
		Frame: &minikvv1.PutRequest_Header{
			Header: &minikvv1.PutHeader{},
		},
	}

	if request.GetHeader() == nil {
		t.Fatal("GetHeader() = nil, want header")
	}
	if request.GetChunk() != nil {
		t.Fatalf("GetChunk() = %v, want nil", request.GetChunk())
	}
}
