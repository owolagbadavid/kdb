package bloom

import (
	"hash/fnv"
	"math"
)

type Filter struct {
	Version uint8
	Bits    []byte
	M, K    uint64
}

// New sizes a filter for n expected items at false-positive rate p (e.g. 0.01).
func New(n uint64, p float64) *Filter {
	m := uint64(math.Ceil(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)))
	k := uint64(math.Max(1, math.Round(float64(m)/float64(n)*math.Ln2)))
	return &Filter{
		Bits: make([]byte, (m+7)/8),
		M:    m,
		K:    k,
	}
}

func (f *Filter) eachPos(sum uint64, fn func(pos uint64) bool) bool {
	h1, h2 := sum&0xffffffff, sum>>32
	if h2 == 0 {
		h2 = 1
	}
	for i := uint64(0); i < f.K; i++ {
		if !fn((h1 + i*h2) % f.M) {
			return false
		}
	}
	return true
}

func (f *Filter) AddHash(sum uint64) {
	f.eachPos(sum, func(pos uint64) bool { f.Bits[pos>>3] |= 1 << (pos & 7); return true })
}
func (f *Filter) Add(key []byte) { f.AddHash(Hash(key)) }

func (f *Filter) Contains(key []byte) bool {
	if f == nil {
		return true
	}
	return f.eachPos(Hash(key), func(pos uint64) bool {
		return f.Bits[pos>>3]&(1<<(pos&7)) != 0
	})
}

func Hash(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	return h.Sum64()
}
