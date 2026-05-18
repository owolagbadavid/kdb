// Package manifest persists the list of live SSTables and their tier as
// an atomically-rewritten on-disk snapshot.
//
// File format (single MANIFEST file, rewritten in full on every change):
//
//	[uint32 crc][uint32 payloadLen][payload]
//
// where payload is:
//
//	[uint8 version][varint numEntries][entry 0]...[entry N-1]
//
// Entry: [varint fileNum][varint tier][varint smallestLen][smallest][varint largestLen][largest]
//
// CRC32 (IEEE) is computed over the payload bytes. Save writes
// MANIFEST.tmp, fsyncs it, renames to MANIFEST, then fsyncs the directory.
package manifest

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	filename    = "MANIFEST"
	tmpFilename = "MANIFEST.tmp"
	version     = 1
)

var crcTable = crc32.MakeTable(crc32.IEEE)

type Entry struct {
	FileNum  uint64
	Tier     int
	Smallest []byte
	Largest  []byte
}

// Load reads the manifest. Returns (nil, nil) when the file does not
// exist — that case is "no live SSTables yet" and the caller treats it
// as an empty list.
func Load(dir string) ([]Entry, error) {
	data, err := os.ReadFile(filepath.Join(dir, filename))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) < 8 {
		return nil, fmt.Errorf("manifest: too short (%d bytes)", len(data))
	}
	wantCRC := binary.LittleEndian.Uint32(data[0:4])
	payloadLen := binary.LittleEndian.Uint32(data[4:8])
	if int(payloadLen)+8 != len(data) {
		return nil, fmt.Errorf("manifest: length mismatch (header=%d, file=%d)", payloadLen, len(data)-8)
	}
	payload := data[8:]
	if crc32.Checksum(payload, crcTable) != wantCRC {
		return nil, fmt.Errorf("manifest: crc mismatch")
	}

	r := bytes.NewReader(payload)
	ver, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if ver != version {
		return nil, fmt.Errorf("manifest: unsupported version %d", ver)
	}
	numEntries, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, numEntries)
	for i := uint64(0); i < numEntries; i++ {
		var e Entry
		fn, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		e.FileNum = fn
		tier, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		e.Tier = int(tier)
		if e.Smallest, err = readLPBytes(r); err != nil {
			return nil, err
		}
		if e.Largest, err = readLPBytes(r); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// Save writes entries to dir atomically. A zero-length entries slice
// produces a valid manifest representing "no live SSTables".
func Save(dir string, entries []Entry) error {
	var payload bytes.Buffer
	payload.WriteByte(version)
	var u [binary.MaxVarintLen64]byte
	nn := binary.PutUvarint(u[:], uint64(len(entries)))
	payload.Write(u[:nn])
	for _, e := range entries {
		nn = binary.PutUvarint(u[:], e.FileNum)
		payload.Write(u[:nn])
		nn = binary.PutUvarint(u[:], uint64(e.Tier))
		payload.Write(u[:nn])
		writeLPBytes(&payload, e.Smallest)
		writeLPBytes(&payload, e.Largest)
	}

	crc := crc32.Checksum(payload.Bytes(), crcTable)
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], crc)
	binary.LittleEndian.PutUint32(header[4:8], uint32(payload.Len()))

	tmpPath := filepath.Join(dir, tmpFilename)
	f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(header[:]); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(payload.Bytes()); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, filepath.Join(dir, filename)); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func readLPBytes(r *bytes.Reader) ([]byte, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func writeLPBytes(w *bytes.Buffer, b []byte) {
	var u [binary.MaxVarintLen64]byte
	nn := binary.PutUvarint(u[:], uint64(len(b)))
	w.Write(u[:nn])
	w.Write(b)
}
