package grpccommon

import (
	"context"
	"errors"
	"io"
	"testing"

	minikvv1 "github.com/qingketsing/Mini-KV-Cache-System/gen/go/minikv/v1"
)

func TestNodePutReaderRetainsSuffix(t *testing.T) {
	t.Parallel()

	recv, calls := nodeReceiver(
		nodeChunk(0, "abcd"),
		nodeChunk(1, "ef"),
	)
	reader := NewNodePutReader(context.Background(), 6, 4, recv)

	first := make([]byte, 2)
	requireRead(t, reader, first, "ab")
	if *calls != 1 {
		t.Fatalf("receive calls after first read = %d, want 1", *calls)
	}

	second := make([]byte, 4)
	requireRead(t, reader, second, "cdef")
	if *calls != 2 {
		t.Fatalf("receive calls after second read = %d, want 2", *calls)
	}
	requireReaderError(t, reader, io.EOF)
	if *calls != 3 {
		t.Fatalf("receive calls after EOF probe = %d, want 3", *calls)
	}
}

func TestNodePutReaderMultipleChunks(t *testing.T) {
	t.Parallel()

	recv, _ := nodeReceiver(nodeChunk(0, "ab"), nodeChunk(1, "cde"))
	reader := NewNodePutReader(context.Background(), 5, 3, recv)
	buffer := make([]byte, 5)
	requireRead(t, reader, buffer, "abcde")
	requireReaderError(t, reader, io.EOF)
}

func TestNodePutReaderDoesNotReadAhead(t *testing.T) {
	t.Parallel()

	recv, calls := nodeReceiver(nodeChunk(0, "abcd"))
	reader := NewNodePutReader(context.Background(), 4, 4, recv)
	for i, want := range []string{"a", "b", "c", "d"} {
		buffer := make([]byte, 1)
		requireRead(t, reader, buffer, want)
		if *calls != 1 {
			t.Fatalf("receive calls after byte %d = %d, want 1", i, *calls)
		}
	}
	requireReaderError(t, reader, io.EOF)
	if *calls != 2 {
		t.Fatalf("receive calls after final probe = %d, want 2", *calls)
	}
}

func TestNodePutReaderShortStream(t *testing.T) {
	t.Parallel()

	recv, _ := nodeReceiver(nodeChunk(0, "ab"))
	reader := NewNodePutReader(context.Background(), 3, 3, recv)
	data, err := io.ReadAll(reader)
	if string(data) != "ab" {
		t.Fatalf("ReadAll() data = %q, want %q", data, "ab")
	}
	if !errors.Is(err, errShortStream) {
		t.Fatalf("ReadAll() error = %v, want errors.Is(_, %v)", err, errShortStream)
	}
}

func TestNodePutReaderLongStream(t *testing.T) {
	t.Parallel()

	t.Run("extra frame", func(t *testing.T) {
		recv, _ := nodeReceiver(nodeChunk(0, "ab"), nodeChunk(1, "c"))
		reader := NewNodePutReader(context.Background(), 2, 2, recv)
		buffer := make([]byte, 2)
		requireRead(t, reader, buffer, "ab")
		requireReaderError(t, reader, errLongStream)
	})

	t.Run("frame returned with eof", func(t *testing.T) {
		calls := 0
		reader := NewNodePutReader(context.Background(), 2, 2, func() (*minikvv1.NodePutRequest, error) {
			calls++
			if calls == 1 {
				return nodeChunk(0, "ab"), nil
			}
			return nodeChunk(1, "c"), io.EOF
		})
		buffer := make([]byte, 2)
		requireRead(t, reader, buffer, "ab")
		requireReaderError(t, reader, errLongStream)
	})
}

func TestNodePutReaderRejectsMalformedFrames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		size   int64
		max    int
		frames []*minikvv1.NodePutRequest
		want   error
	}{
		{name: "repeated header", size: 1, max: 2, frames: []*minikvv1.NodePutRequest{nodeHeader()}, want: errMalformedHeader},
		{name: "nil request", size: 1, max: 2, frames: []*minikvv1.NodePutRequest{nil}, want: errMalformedFrame},
		{name: "nil frame", size: 1, max: 2, frames: []*minikvv1.NodePutRequest{{}}, want: errMalformedFrame},
		{name: "nil chunk", size: 1, max: 2, frames: []*minikvv1.NodePutRequest{{Frame: &minikvv1.NodePutRequest_Chunk{}}}, want: errMalformedFrame},
		{name: "empty chunk", size: 1, max: 2, frames: []*minikvv1.NodePutRequest{nodeChunk(0, "")}, want: errEmptyChunk},
		{name: "oversized chunk", size: 3, max: 2, frames: []*minikvv1.NodePutRequest{nodeChunk(0, "abc")}, want: errChunkTooLarge},
		{name: "chunk exceeds remaining", size: 1, max: 2, frames: []*minikvv1.NodePutRequest{nodeChunk(0, "ab")}, want: errLongStream},
		{name: "skipped sequence", size: 1, max: 2, frames: []*minikvv1.NodePutRequest{nodeChunk(1, "a")}, want: errInvalidSequence},
		{name: "duplicate sequence", size: 2, max: 2, frames: []*minikvv1.NodePutRequest{nodeChunk(0, "a"), nodeChunk(0, "b")}, want: errInvalidSequence},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recv, _ := nodeReceiver(test.frames...)
			_, err := io.ReadAll(NewNodePutReader(context.Background(), test.size, test.max, recv))
			if !errors.Is(err, test.want) {
				t.Fatalf("ReadAll() error = %v, want errors.Is(_, %v)", err, test.want)
			}
		})
	}
}

func TestNodePutReaderCancellationBeforeReceive(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	recv, calls := nodeReceiver(nodeChunk(0, "a"), nodeChunk(1, "b"))
	reader := NewNodePutReader(ctx, 2, 1, recv)
	buffer := make([]byte, 1)
	requireRead(t, reader, buffer, "a")
	cancel()
	requireReaderError(t, reader, context.Canceled)
	if *calls != 1 {
		t.Fatalf("receive calls = %d, want 1", *calls)
	}
}

func TestNodePutReaderZeroSize(t *testing.T) {
	t.Parallel()

	t.Run("empty stream", func(t *testing.T) {
		recv, calls := nodeReceiver()
		reader := NewNodePutReader(context.Background(), 0, 1, recv)
		if n, err := reader.Read(nil); n != 0 || err != nil {
			t.Fatalf("Read(nil) = (%d, %v), want (0, nil)", n, err)
		}
		if *calls != 0 {
			t.Fatalf("receive calls after empty consumer read = %d, want 0", *calls)
		}
		requireReaderError(t, reader, io.EOF)
		if *calls != 1 {
			t.Fatalf("receive calls = %d, want 1", *calls)
		}
	})

	t.Run("extra frame", func(t *testing.T) {
		recv, _ := nodeReceiver(nodeChunk(0, "x"))
		reader := NewNodePutReader(context.Background(), 0, 1, recv)
		requireReaderError(t, reader, errLongStream)
	})
}

func TestNodePutReaderPropagatesReceiveError(t *testing.T) {
	t.Parallel()

	want := errors.New("receive failed")
	reader := NewNodePutReader(context.Background(), 1, 1, func() (*minikvv1.NodePutRequest, error) {
		return nil, want
	})
	requireReaderError(t, reader, want)
}

func nodeReceiver(frames ...*minikvv1.NodePutRequest) (func() (*minikvv1.NodePutRequest, error), *int) {
	index := 0
	calls := 0
	return func() (*minikvv1.NodePutRequest, error) {
		calls++
		if index == len(frames) {
			return nil, io.EOF
		}
		frame := frames[index]
		index++
		return frame, nil
	}, &calls
}

func nodeChunk(sequence uint32, data string) *minikvv1.NodePutRequest {
	return &minikvv1.NodePutRequest{
		Frame: &minikvv1.NodePutRequest_Chunk{
			Chunk: &minikvv1.DataChunk{Sequence: sequence, Data: []byte(data)},
		},
	}
}

func nodeHeader() *minikvv1.NodePutRequest {
	return &minikvv1.NodePutRequest{
		Frame: &minikvv1.NodePutRequest_Header{Header: &minikvv1.NodePutHeader{}},
	}
}

func requireRead(t *testing.T, reader io.Reader, buffer []byte, want string) {
	t.Helper()

	n, err := reader.Read(buffer)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got := string(buffer[:n]); got != want {
		t.Fatalf("Read() data = %q, want %q", got, want)
	}
	if n != len(want) {
		t.Fatalf("Read() bytes = %d, want %d", n, len(want))
	}
}

func requireReaderError(t *testing.T, reader io.Reader, want error) {
	t.Helper()

	buffer := make([]byte, 1)
	n, err := reader.Read(buffer)
	if n != 0 {
		t.Fatalf("Read() bytes = %d, want 0", n)
	}
	if !errors.Is(err, want) {
		t.Fatalf("Read() error = %v, want errors.Is(_, %v)", err, want)
	}
}
