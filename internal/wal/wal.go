// Package wal is an append-only write-ahead log. Each record is
//
//	[uint32 crc | uint32 payloadLen | payload]
//
// where payload is `[varint seqno][uint8 kind][varint keylen][key][varint vallen][value]`
// and the CRC32 is computed over the payload bytes only. Replay tolerates a
// torn trailing record (short header, short payload, or CRC mismatch) by
// stopping silently — those bytes are presumed to be from a crash mid-append.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/owolagbadavid/kdb/internal/kv"
)

var crcTable = crc32.MakeTable(crc32.IEEE)

const (
	headerSize    = 8
	maxPayloadLen = 1 << 28 // 256 MiB; sanity bound to reject corrupt length
)

// Writer is an append-only WAL writer. Appends are buffered; a record is
// durable only after a successful Sync.
type Writer struct {
	f  *os.File
	bw *bufio.Writer
}

// Create opens path for appending, creating it if necessary. Existing
// records are preserved — the caller is expected to have replayed them
// before calling Create.
func Create(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f, bw: bufio.NewWriter(f)}, nil
}

func (w *Writer) Append(e kv.Entry) error {
	payload := encodePayload(e)
	if len(payload) > maxPayloadLen {
		return fmt.Errorf("wal: payload too large (%d bytes)", len(payload))
	}
	var hdr [headerSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], crc32.Checksum(payload, crcTable))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(payload)))
	if _, err := w.bw.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.bw.Write(payload); err != nil {
		return err
	}
	return nil
}

func (w *Writer) Sync() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *Writer) Close() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Close()
}

func encodePayload(e kv.Entry) []byte {
	var u [binary.MaxVarintLen64]byte
	size := binary.MaxVarintLen64 + 1 + binary.MaxVarintLen64 + len(e.Key) + binary.MaxVarintLen64 + len(e.Value)
	buf := make([]byte, 0, size)

	n := binary.PutUvarint(u[:], e.Seqno)
	buf = append(buf, u[:n]...)
	buf = append(buf, byte(e.Kind))
	n = binary.PutUvarint(u[:], uint64(len(e.Key)))
	buf = append(buf, u[:n]...)
	buf = append(buf, e.Key...)
	n = binary.PutUvarint(u[:], uint64(len(e.Value)))
	buf = append(buf, u[:n]...)
	buf = append(buf, e.Value...)
	return buf
}

// Replay invokes fn for each well-formed record in path. Returns nil on
// clean EOF or a torn trailing record. If fn returns an error, replay
// stops and that error is returned.
func Replay(path string, fn func(kv.Entry) error) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

	br := bufio.NewReader(f)
	var hdr [headerSize]byte
	for {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		wantCRC := binary.LittleEndian.Uint32(hdr[0:4])
		payloadLen := binary.LittleEndian.Uint32(hdr[4:8])
		if payloadLen == 0 || payloadLen > maxPayloadLen {
			return nil
		}
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(br, payload); err != nil {
			return nil
		}
		if crc32.Checksum(payload, crcTable) != wantCRC {
			return nil
		}
		e, err := decodePayload(payload)
		if err != nil {
			return nil
		}
		if err := fn(e); err != nil {
			return err
		}
	}
}

func decodePayload(buf []byte) (kv.Entry, error) {
	var e kv.Entry
	seqno, n := binary.Uvarint(buf)
	if n <= 0 {
		return e, errors.New("bad seqno varint")
	}
	e.Seqno = seqno
	buf = buf[n:]
	if len(buf) < 1 {
		return e, errors.New("missing kind")
	}
	e.Kind = kv.Kind(buf[0])
	buf = buf[1:]
	klen, n := binary.Uvarint(buf)
	if n <= 0 {
		return e, errors.New("bad keylen")
	}
	buf = buf[n:]
	if uint64(len(buf)) < klen {
		return e, errors.New("truncated key")
	}
	e.Key = append([]byte(nil), buf[:klen]...)
	buf = buf[klen:]
	vlen, n := binary.Uvarint(buf)
	if n <= 0 {
		return e, errors.New("bad vallen")
	}
	buf = buf[n:]
	if uint64(len(buf)) < vlen {
		return e, errors.New("truncated value")
	}
	if vlen > 0 {
		e.Value = append([]byte(nil), buf[:vlen]...)
	}
	return e, nil
}
