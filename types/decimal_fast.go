package types

import (
	"fmt"
	"math/big"
	"sync"
)

// bigIntPool recycles *big.Int instances used as transient working
// scratch (e.g. fastDecUnmarshalText's chunkBuf). Pool members must
// never be retained after Put — only safe for purely-local scratch.
var bigIntPool = sync.Pool{
	New: func() interface{} { return new(big.Int) },
}

// decChunkBase is 10^18 — the largest power of 10 that fits in uint64.
// Used to chunk decimal-ASCII -> big.Int conversion 18 digits at a time
// via cheap uint64 arithmetic.
var decChunkBase = big.NewInt(1_000_000_000_000_000_000)

// decPow10 caches 10^i as *big.Int for i in [0, 80). 80 covers the
// full 256-bit Dec range (max ~78 decimal digits).
var decPow10 = func() []*big.Int {
	a := make([]*big.Int, 80)
	a[0] = big.NewInt(1)
	ten := big.NewInt(10)
	for i := 1; i < 80; i++ {
		a[i] = new(big.Int).Mul(a[i-1], ten)
	}
	return a
}()

// decDigitCount returns the number of decimal digits required to print z,
// EXCLUDING any sign byte. Equivalent to len(strconv.FormatInt(|z|, 10)),
// but computed via BitLen + a single big.Int comparison against a pre-built
// power-of-10 table instead of running the full base-10 conversion. Lets
// Size() report exact wire length without round-tripping through Marshal.
//
// Algorithm: digit count D is the smallest int such that |z| < 10^D.
// bl * log10(2) ≈ bl * 0.30103 gives D-1 as a lower bound; the
// safe upper-bound starting point is floor(bl*0.30103) + 1, then
// decrement while |z| < 10^(D-1).
func decDigitCount(z *big.Int) int {
	abs := z
	if z.Sign() < 0 {
		abs = new(big.Int).Abs(z)
	}
	if abs.Sign() == 0 {
		return 1
	}
	bl := abs.BitLen()
	d := bl*301/1000 + 1
	if d >= len(decPow10) {
		d = len(decPow10) - 1
	}
	for d > 1 && abs.Cmp(decPow10[d-1]) < 0 {
		d--
	}
	return d
}

// fastDecUnmarshalText parses base-10 ASCII into a big.Int, byte-for-byte
// compatible with big.Int.UnmarshalText. Accepts a leading '-' or '+'
// sign and requires all remaining bytes to be digits 0-9. Like the
// stdlib path it rejects empty input and any non-digit byte.
func fastDecUnmarshalText(data []byte) (*big.Int, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("math/big: cannot unmarshal %q into a *big.Int", data)
	}
	neg := false
	switch data[0] {
	case '-':
		neg = true
		data = data[1:]
	case '+':
		data = data[1:]
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("math/big: cannot unmarshal %q into a *big.Int", data)
	}
	for _, b := range data {
		if b < '0' || b > '9' {
			return nil, fmt.Errorf("math/big: cannot unmarshal %q into a *big.Int", data)
		}
	}

	result := new(big.Int)
	chunkBuf := bigIntPool.Get().(*big.Int)
	defer bigIntPool.Put(chunkBuf)

	const chunkDigits = 18

	// First (most-significant) chunk may be shorter than 18 digits.
	firstLen := len(data) % chunkDigits
	if firstLen == 0 {
		firstLen = chunkDigits
	}
	var c uint64
	for _, b := range data[:firstLen] {
		c = c*10 + uint64(b-'0')
	}
	result.SetUint64(c)

	off := firstLen
	for off < len(data) {
		c = 0
		for _, b := range data[off : off+chunkDigits] {
			c = c*10 + uint64(b-'0')
		}
		result.Mul(result, decChunkBase)
		chunkBuf.SetUint64(c)
		result.Add(result, chunkBuf)
		off += chunkDigits
	}

	if neg {
		result.Neg(result)
	}
	return result, nil
}
