package grpcnode

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	minikvv1 "github.com/qingketsing/Mini-KV-Cache-System/gen/go/minikv/v1"
	"github.com/qingketsing/Mini-KV-Cache-System/internal/sharding"
	"github.com/qingketsing/Mini-KV-Cache-System/internal/store"
	"github.com/qingketsing/Mini-KV-Cache-System/internal/transport/grpccommon"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const testShardCount uint32 = 4

var testLimits = grpccommon.Limits{
	ChunkBytes:      4,
	MaxMessageBytes: 128 << 10,
	MaxObjectBytes:  32,
}

func TestNodePutConstructor(t *testing.T) {
	st := newNodeTestStore(t)
	tests := []struct {
		name   string
		store  store.Store
		limits grpccommon.Limits
		shards uint32
	}{
		{name: "nil store", limits: testLimits, shards: testShardCount},
		{name: "invalid limits", store: st, limits: grpccommon.Limits{}, shards: testShardCount},
		{name: "zero shards", store: st, limits: testLimits},
		{name: "non power of two shards", store: st, limits: testLimits, shards: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.store, tt.limits, tt.shards); err == nil {
				t.Fatal("New() succeeded, want error")
			}
		})
	}
}

func TestNodePutSuccess(t *testing.T) {
	tests := []struct {
		name      string
		key       []byte
		value     []byte
		chunks    [][]byte
		ttlMillis uint64
		requestID []byte
		epoch     uint64
	}{
		{name: "zero byte", key: []byte("zero")},
		{name: "one byte", key: []byte("one"), value: []byte("x"), chunks: [][]byte{[]byte("x")}},
		{name: "several chunks", key: []byte("many"), value: []byte("abcdefghij"), chunks: [][]byte{[]byte("abcd"), []byte("efgh"), []byte("ij")}},
		{name: "binary key", key: []byte{0, 1, 0xff, 2}, value: []byte("bin"), chunks: [][]byte{[]byte("bin")}},
		{name: "ttl", key: []byte("ttl"), value: []byte("life"), chunks: [][]byte{[]byte("life")}, ttlMillis: 1500},
		{name: "request id", key: []byte("request"), value: []byte("id"), chunks: [][]byte{[]byte("id")}, requestID: bytes.Repeat([]byte{7}, 16)},
		{name: "nonzero epoch", key: []byte("epoch"), value: []byte("ok"), chunks: [][]byte{[]byte("ok")}, epoch: 99},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newNodeTestStore(t)
			client := newNodeTestClient(t, st)
			started := time.Now()
			stream := openNodePut(t, client, context.Background())
			sendNodeHeader(t, stream, tt.key, uint64(len(tt.value)), tt.ttlMillis, tt.requestID, tt.epoch)
			for sequence, data := range tt.chunks {
				sendNodeChunk(t, stream, uint32(sequence), data)
			}
			response, err := stream.CloseAndRecv()
			if err != nil {
				t.Fatalf("CloseAndRecv() error = %v", err)
			}
			if response.GetValueSize() != uint64(len(tt.value)) {
				t.Fatalf("response size = %d, want %d", response.GetValueSize(), len(tt.value))
			}
			if tt.ttlMillis == 0 {
				if response.GetExpiresAtUnixMilliseconds() != 0 {
					t.Fatalf("response expiry = %d, want zero", response.GetExpiresAtUnixMilliseconds())
				}
			} else {
				wantEarliest := started.Add(time.Duration(tt.ttlMillis) * time.Millisecond).UnixMilli()
				wantLatest := time.Now().Add(time.Duration(tt.ttlMillis) * time.Millisecond).UnixMilli()
				got := response.GetExpiresAtUnixMilliseconds()
				if got < wantEarliest || got > wantLatest {
					t.Fatalf("response expiry = %d, want in [%d, %d]", got, wantEarliest, wantLatest)
				}
			}
			assertStoredNodeValue(t, st, tt.key, tt.value, response.GetExpiresAtUnixMilliseconds())
		})
	}
}

func TestNodePutRejectsMalformedStreams(t *testing.T) {
	oversizedKey := bytes.Repeat([]byte("k"), 1025)
	tests := []struct {
		name   string
		key    []byte
		frames func([]byte) []*minikvv1.NodePutRequest
	}{
		{name: "missing header", key: []byte("missing")},
		{name: "chunk first", key: []byte("chunk-first"), frames: func([]byte) []*minikvv1.NodePutRequest { return []*minikvv1.NodePutRequest{nodeChunk(0, []byte("x"))} }},
		{name: "empty oneof", key: []byte("empty-frame"), frames: func([]byte) []*minikvv1.NodePutRequest { return []*minikvv1.NodePutRequest{{}} }},
		{name: "nil header", key: []byte("nil-header"), frames: func([]byte) []*minikvv1.NodePutRequest {
			return []*minikvv1.NodePutRequest{{Frame: &minikvv1.NodePutRequest_Header{}}}
		}},
		{name: "duplicate header", key: []byte("duplicate-header"), frames: func(key []byte) []*minikvv1.NodePutRequest {
			return []*minikvv1.NodePutRequest{nodeHeader(key, 1, 0, nil, 0, shardFor(t, key)), nodeHeader(key, 1, 0, nil, 0, shardFor(t, key))}
		}},
		{name: "nil route", key: []byte("nil-route"), frames: func(key []byte) []*minikvv1.NodePutRequest {
			frame := nodeHeader(key, 0, 0, nil, 0, 0)
			frame.GetHeader().Route = nil
			return []*minikvv1.NodePutRequest{frame}
		}},
		{name: "nil request", key: []byte("nil-request"), frames: func(key []byte) []*minikvv1.NodePutRequest {
			frame := nodeHeader(key, 0, 0, nil, 0, 0)
			frame.GetHeader().Request = nil
			return []*minikvv1.NodePutRequest{frame}
		}},
		{name: "wrong shard", key: []byte("wrong-shard"), frames: func(key []byte) []*minikvv1.NodePutRequest {
			return []*minikvv1.NodePutRequest{nodeHeader(key, 0, 0, nil, 0, (shardFor(t, key)+1)%testShardCount)}
		}},
		{name: "short stream", key: []byte("short"), frames: func(key []byte) []*minikvv1.NodePutRequest {
			return []*minikvv1.NodePutRequest{nodeHeader(key, 2, 0, nil, 0, shardFor(t, key)), nodeChunk(0, []byte("x"))}
		}},
		{name: "chunk longer than declared", key: []byte("long-chunk"), frames: func(key []byte) []*minikvv1.NodePutRequest {
			return []*minikvv1.NodePutRequest{nodeHeader(key, 1, 0, nil, 0, shardFor(t, key)), nodeChunk(0, []byte("xx"))}
		}},
		{name: "extra chunk", key: []byte("extra"), frames: func(key []byte) []*minikvv1.NodePutRequest {
			return []*minikvv1.NodePutRequest{nodeHeader(key, 1, 0, nil, 0, shardFor(t, key)), nodeChunk(0, []byte("x")), nodeChunk(1, []byte("y"))}
		}},
		{name: "wrong sequence", key: []byte("wrong-sequence"), frames: func(key []byte) []*minikvv1.NodePutRequest {
			return []*minikvv1.NodePutRequest{nodeHeader(key, 1, 0, nil, 0, shardFor(t, key)), nodeChunk(1, []byte("x"))}
		}},
		{name: "duplicate sequence", key: []byte("duplicate-sequence"), frames: func(key []byte) []*minikvv1.NodePutRequest {
			return []*minikvv1.NodePutRequest{nodeHeader(key, 2, 0, nil, 0, shardFor(t, key)), nodeChunk(0, []byte("x")), nodeChunk(0, []byte("y"))}
		}},
		{name: "skipped sequence", key: []byte("skipped-sequence"), frames: func(key []byte) []*minikvv1.NodePutRequest {
			return []*minikvv1.NodePutRequest{nodeHeader(key, 2, 0, nil, 0, shardFor(t, key)), nodeChunk(0, []byte("x")), nodeChunk(2, []byte("y"))}
		}},
		{name: "empty chunk", key: []byte("empty-chunk"), frames: func(key []byte) []*minikvv1.NodePutRequest {
			return []*minikvv1.NodePutRequest{nodeHeader(key, 1, 0, nil, 0, shardFor(t, key)), nodeChunk(0, nil)}
		}},
		{name: "oversized chunk", key: []byte("oversized-chunk"), frames: func(key []byte) []*minikvv1.NodePutRequest {
			return []*minikvv1.NodePutRequest{nodeHeader(key, 5, 0, nil, 0, shardFor(t, key)), nodeChunk(0, []byte("12345"))}
		}},
		{name: "invalid request id", key: []byte("bad-id"), frames: func(key []byte) []*minikvv1.NodePutRequest {
			return []*minikvv1.NodePutRequest{nodeHeader(key, 0, 0, []byte("bad"), 0, shardFor(t, key))}
		}},
		{name: "empty key", key: []byte("unpublished-empty-key"), frames: func([]byte) []*minikvv1.NodePutRequest {
			return []*minikvv1.NodePutRequest{nodeHeader(nil, 0, 0, nil, 0, 0)}
		}},
		{name: "oversized key", key: oversizedKey, frames: func(key []byte) []*minikvv1.NodePutRequest {
			return []*minikvv1.NodePutRequest{nodeHeader(key, 0, 0, nil, 0, 0)}
		}},
		{name: "ttl overflow", key: []byte("ttl-overflow"), frames: func(key []byte) []*minikvv1.NodePutRequest {
			return []*minikvv1.NodePutRequest{nodeHeader(key, 0, ^uint64(0), nil, 0, shardFor(t, key))}
		}},
		{name: "object too large", key: []byte("too-large"), frames: func(key []byte) []*minikvv1.NodePutRequest {
			return []*minikvv1.NodePutRequest{nodeHeader(key, uint64(testLimits.MaxObjectBytes+1), 0, nil, 0, shardFor(t, key))}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newNodeTestStore(t)
			client := newNodeTestClient(t, st)
			stream := openNodePut(t, client, context.Background())
			if tt.frames != nil {
				for _, frame := range tt.frames(tt.key) {
					if err := stream.Send(frame); err != nil {
						break
					}
				}
			}
			_, err := stream.CloseAndRecv()
			assertNodeCode(t, err, codes.InvalidArgument)
			if status.Convert(err).Message() != "invalid request" {
				t.Fatalf("status message = %q, want sanitized invalid request", status.Convert(err).Message())
			}
			assertNodeNotFound(t, st, tt.key)
			if got := st.Stats().Entries; got != 0 {
				t.Fatalf("entries = %d, want zero", got)
			}
		})
	}
}

func TestNodePutCancellationCleansStagingAndDoesNotPublish(t *testing.T) {
	st := newNodeTestStore(t)
	client := newNodeTestClient(t, st)
	ctx, cancel := context.WithCancel(context.Background())
	stream := openNodePut(t, client, ctx)
	key := []byte("cancel")
	sendNodeHeader(t, stream, key, 4, 0, nil, 0)
	sendNodeChunk(t, stream, 0, []byte("ab"))
	waitForNodeStat(t, st, func(stats store.Stats) bool { return stats.StagingBytes == 4 })
	assertNodeNotFound(t, st, key)
	cancel()
	_, err := stream.CloseAndRecv()
	assertNodeCode(t, err, codes.Canceled)
	waitForNodeStat(t, st, func(stats store.Stats) bool { return stats.StagingBytes == 0 })
	assertNodeNotFound(t, st, key)
}

func TestNodePutPartialValueIsInvisible(t *testing.T) {
	st := newNodeTestStore(t)
	client := newNodeTestClient(t, st)
	stream := openNodePut(t, client, context.Background())
	key := []byte("partial")
	sendNodeHeader(t, stream, key, 4, 0, nil, 0)
	sendNodeChunk(t, stream, 0, []byte("ab"))
	waitForNodeStat(t, st, func(stats store.Stats) bool { return stats.StagingBytes == 4 })
	assertNodeNotFound(t, st, key)
	sendNodeChunk(t, stream, 1, []byte("cd"))
	response, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv() error = %v", err)
	}
	if response.GetValueSize() != 4 {
		t.Fatalf("response size = %d, want 4", response.GetValueSize())
	}
	assertStoredNodeValue(t, st, key, []byte("abcd"), 0)
}

func TestNodePutStoreErrors(t *testing.T) {
	t.Run("no capacity", func(t *testing.T) {
		client := newNodeTestClient(t, errorStore{putErr: store.ErrNoCapacity})
		stream := openNodePut(t, client, context.Background())
		sendNodeHeader(t, stream, []byte("capacity"), 0, 0, nil, 0)
		_, err := stream.CloseAndRecv()
		assertNodeCode(t, err, codes.ResourceExhausted)
	})

	t.Run("closed", func(t *testing.T) {
		st := newNodeTestStore(t)
		if err := st.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		client := newNodeTestClient(t, st)
		stream := openNodePut(t, client, context.Background())
		sendNodeHeader(t, stream, []byte("closed"), 0, 0, nil, 0)
		_, err := stream.CloseAndRecv()
		assertNodeCode(t, err, codes.Unavailable)
	})
}

func newNodeTestStore(t *testing.T) store.Store {
	t.Helper()
	cfg := store.DefaultConfig()
	cfg.CapacityBytes = 1 << 20
	cfg.MaxObjectBytes = testLimits.MaxObjectBytes
	cfg.MaxStagingBytes = testLimits.MaxObjectBytes
	cfg.ChunkBytes = testLimits.ChunkBytes
	cfg.ShardCount = testShardCount
	cfg.TTLResolution = time.Millisecond
	cfg.TouchBuffer = 16
	st, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("Store.Close() error = %v", err)
		}
	})
	return st
}

func newNodeTestClient(t *testing.T, st store.Store) minikvv1.NodeServiceClient {
	t.Helper()
	server, err := New(st, testLimits, testShardCount)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(testLimits.MaxMessageBytes),
		grpc.MaxSendMsgSize(testLimits.MaxMessageBytes),
	)
	minikvv1.RegisterNodeServiceServer(grpcServer, server)
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = grpcServer.Serve(listener)
	}()
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(testLimits.MaxMessageBytes),
			grpc.MaxCallRecvMsgSize(testLimits.MaxMessageBytes),
		),
	)
	if err != nil {
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("ClientConn.Close() error = %v", err)
		}
		grpcServer.Stop()
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("Listener.Close() error = %v", err)
		}
		<-serveDone
	})
	return minikvv1.NewNodeServiceClient(conn)
}

func openNodePut(t *testing.T, client minikvv1.NodeServiceClient, ctx context.Context) minikvv1.NodeService_PutClient {
	t.Helper()
	stream, err := client.Put(ctx)
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	return stream
}

func sendNodeHeader(t *testing.T, stream minikvv1.NodeService_PutClient, key []byte, size, ttl uint64, requestID []byte, epoch uint64) {
	t.Helper()
	if err := stream.Send(nodeHeader(key, size, ttl, requestID, epoch, shardFor(t, key))); err != nil {
		t.Fatalf("Send(header) error = %v", err)
	}
}

func sendNodeChunk(t *testing.T, stream minikvv1.NodeService_PutClient, sequence uint32, data []byte) {
	t.Helper()
	if err := stream.Send(nodeChunk(sequence, data)); err != nil {
		t.Fatalf("Send(chunk) error = %v", err)
	}
}

func nodeHeader(key []byte, size, ttl uint64, requestID []byte, epoch uint64, shardID uint32) *minikvv1.NodePutRequest {
	return &minikvv1.NodePutRequest{Frame: &minikvv1.NodePutRequest_Header{Header: &minikvv1.NodePutHeader{
		Route:   &minikvv1.RouteContext{ShardId: shardID, ShardEpoch: epoch},
		Request: &minikvv1.PutHeader{Key: key, ValueSize: size, TtlMilliseconds: ttl, RequestId: requestID},
	}}}
}

func nodeChunk(sequence uint32, data []byte) *minikvv1.NodePutRequest {
	return &minikvv1.NodePutRequest{Frame: &minikvv1.NodePutRequest_Chunk{Chunk: &minikvv1.DataChunk{Sequence: sequence, Data: data}}}
}

func shardFor(t *testing.T, key []byte) uint32 {
	t.Helper()
	shardID, err := sharding.ID(key, testShardCount)
	if err != nil {
		t.Fatalf("sharding.ID() error = %v", err)
	}
	return shardID
}

func assertNodeCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("status code = %s (%v), want %s", got, err, want)
	}
}

func assertNodeNotFound(t *testing.T, st store.Store, key []byte) {
	t.Helper()
	object, err := st.Get(context.Background(), key)
	if object != nil {
		_ = object.Close()
		t.Fatalf("Get(%q) returned an object, want unpublished", key)
	}
	if !errors.Is(err, store.ErrNotFound) && !errors.Is(err, store.ErrInvalidKey) {
		t.Fatalf("Get(%q) error = %v, want not found", key, err)
	}
}

func assertStoredNodeValue(t *testing.T, st store.Store, key, want []byte, expiresAtMillis int64) {
	t.Helper()
	object, err := st.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%q) error = %v", key, err)
	}
	defer object.Close()
	got, err := io.ReadAll(object)
	if err != nil {
		t.Fatalf("ReadAll(Get(%q)) error = %v", key, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("stored value = %v, want %v", got, want)
	}
	expiryMatches := object.Info().ExpiresAt.IsZero()
	if expiresAtMillis != 0 {
		expiryMatches = object.Info().ExpiresAt.UnixMilli() == expiresAtMillis
	}
	if object.Info().Size != int64(len(want)) || !expiryMatches {
		t.Fatalf("stored metadata = %+v, want size %d expiry %d", object.Info(), len(want), expiresAtMillis)
	}
}

func waitForNodeStat(t *testing.T, st store.Store, ready func(store.Stats) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if ready(st.Stats()) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for Store stats; last = %+v", st.Stats())
		}
		time.Sleep(time.Millisecond)
	}
}

type errorStore struct {
	putErr error
}

func (s errorStore) Put(context.Context, []byte, io.Reader, store.PutOptions) (store.ObjectInfo, error) {
	return store.ObjectInfo{}, s.putErr
}
func (errorStore) Get(context.Context, []byte) (store.Object, error) { return nil, store.ErrNotFound }
func (errorStore) Delete(context.Context, []byte) (bool, error)      { return false, nil }
func (errorStore) Stats() store.Stats                                { return store.Stats{} }
func (errorStore) Close() error                                      { return nil }
