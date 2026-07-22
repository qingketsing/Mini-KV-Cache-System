package grpccommon

import (
	"context"
	"errors"
	"fmt"
	"io"

	minikvv1 "github.com/qingketsing/Mini-KV-Cache-System/gen/go/minikv/v1"
)

var errNilReceive = errors.New("grpccommon: nil receive callback")

type nodePutReader struct {
	ctx              context.Context
	remaining        int64
	chunkBytes       int
	receive          func() (*minikvv1.NodePutRequest, error)
	pending          []byte
	expectedSequence uint32
	verifiedEOF      bool
	terminalErr      error
}

// NewNodePutReader adapts Node Put chunk frames following a consumed header to an io.Reader.
func NewNodePutReader(ctx context.Context, size int64, chunkBytes int, receive func() (*minikvv1.NodePutRequest, error)) io.Reader {
	return &nodePutReader{
		ctx:        ctx,
		remaining:  size,
		chunkBytes: chunkBytes,
		receive:    receive,
	}
}

func (r *nodePutReader) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	if r.verifiedEOF {
		return 0, io.EOF
	}
	if r.terminalErr != nil {
		return 0, r.terminalErr
	}
	if r.receive == nil {
		return r.fail(0, errNilReceive)
	}

	read := 0
	for read < len(buffer) {
		if len(r.pending) != 0 {
			copied := copy(buffer[read:], r.pending)
			r.pending = r.pending[copied:]
			r.remaining -= int64(copied)
			read += copied
			if r.remaining == 0 {
				return read, nil
			}
			continue
		}

		if r.remaining == 0 {
			return r.verifyEOF()
		}
		if r.remaining < 0 {
			return r.fail(read, fmt.Errorf("read put stream: %w", errLongStream))
		}
		if err := r.contextError(); err != nil {
			return r.fail(read, err)
		}

		request, err := r.receive()
		if errors.Is(err, io.EOF) {
			return r.fail(read, fmt.Errorf("read put stream: %w", errShortStream))
		}
		if err != nil {
			return r.fail(read, err)
		}
		chunk, err := nodePutChunk(request)
		if err != nil {
			return r.fail(read, err)
		}
		if err := ValidateChunk(chunk, r.expectedSequence, r.chunkBytes); err != nil {
			return r.fail(read, err)
		}
		if int64(len(chunk.GetData())) > r.remaining {
			return r.fail(read, fmt.Errorf("read put stream: %w", errLongStream))
		}

		r.pending = chunk.GetData()
		r.expectedSequence++
	}
	return read, nil
}

func (r *nodePutReader) verifyEOF() (int, error) {
	if err := r.contextError(); err != nil {
		return r.fail(0, err)
	}
	request, err := r.receive()
	if err != nil && !errors.Is(err, io.EOF) {
		return r.fail(0, err)
	}
	if request != nil {
		return r.fail(0, fmt.Errorf("read put stream: %w", errLongStream))
	}
	if errors.Is(err, io.EOF) {
		r.verifiedEOF = true
		return 0, io.EOF
	}
	return r.fail(0, fmt.Errorf("read put stream: %w", errLongStream))
}

func (r *nodePutReader) contextError() error {
	if r.ctx == nil {
		return nil
	}
	return r.ctx.Err()
}

func (r *nodePutReader) fail(read int, err error) (int, error) {
	r.terminalErr = err
	return read, err
}

func nodePutChunk(request *minikvv1.NodePutRequest) (*minikvv1.DataChunk, error) {
	if request == nil {
		return nil, fmt.Errorf("read put stream: %w", errMalformedFrame)
	}
	switch frame := request.GetFrame().(type) {
	case *minikvv1.NodePutRequest_Chunk:
		return frame.Chunk, nil
	case *minikvv1.NodePutRequest_Header:
		return nil, fmt.Errorf("read put stream: %w", errMalformedHeader)
	default:
		return nil, fmt.Errorf("read put stream: %w", errMalformedFrame)
	}
}
