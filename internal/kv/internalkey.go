package kv

import (
	"bytes"
	"encoding/binary"
)

// Internal-key layout:
//
//	[user key bytes][8 bytes BE complemented trailer]
//
// The trailer is the big-endian complement of (seqno<<8 | kind). This
// encoding makes byte-wise comparison sort:
//   - user key ascending,
//   - and for the same user key, seqno descending (newest first),
//   - with kind as a final tiebreaker (rarely meaningful since two
//     records shouldn't share a seqno).
//
// Seek keys are built with KindMax so the seek lands at the position
// "first record with this user key and seqno <= ceiling."
const (
	InternalKeyTrailerLen = 8
	// SeqnoMax fits in the 56 bits available after the kind byte.
	// Use it as the visibility ceiling when reading at the latest
	// state with no explicit snapshot.
	SeqnoMax = (uint64(1) << 56) - 1
	// KindMax is the largest possible kind byte. Used in seek keys
	// so the lookup positions at the boundary "first version with
	// seqno <= ceiling" within the user key.
	KindMax = Kind(0xFF)
)

// MakeInternalKey returns the internal-key bytes for (userKey, seqno, kind).
func MakeInternalKey(userKey []byte, seqno uint64, kind Kind) []byte {
	ik := make([]byte, len(userKey)+InternalKeyTrailerLen)
	copy(ik, userKey)
	packed := (seqno << 8) | uint64(kind)
	binary.BigEndian.PutUint64(ik[len(userKey):], ^packed)
	return ik
}

// DecodeTrailer extracts the seqno and kind from an internal key's trailer.
func DecodeTrailer(ik []byte) (seqno uint64, kind Kind) {
	packed := ^binary.BigEndian.Uint64(ik[len(ik)-InternalKeyTrailerLen:])
	return packed >> 8, Kind(packed & 0xFF)
}

// UserKeyOf returns the user-key slice of an internal key. The result
// aliases ik; copy it if the caller needs to outlive the source.
func UserKeyOf(ik []byte) []byte {
	return ik[:len(ik)-InternalKeyTrailerLen]
}

// CompareInternal orders two internal keys as the LSM expects: user
// key ascending, then trailer ascending (which decodes to seqno
// descending). A plain bytes.Compare on the full internal key would
// be WRONG when one user key is a prefix of another, because the
// shorter key's first trailer byte (typically near 0xFF) ranks against
// the longer key's user-key byte instead of a trailer byte.
func CompareInternal(a, b []byte) int {
	aUK := a[:len(a)-InternalKeyTrailerLen]
	bUK := b[:len(b)-InternalKeyTrailerLen]
	if c := bytes.Compare(aUK, bUK); c != 0 {
		return c
	}
	return bytes.Compare(a[len(a)-InternalKeyTrailerLen:], b[len(b)-InternalKeyTrailerLen:])
}
