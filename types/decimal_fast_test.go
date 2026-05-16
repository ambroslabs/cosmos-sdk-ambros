package types_test

import (
	"math/big"
	"math/rand"
	"testing"

	sdk "github.com/cosmos/cosmos-sdk/types"
)

// TestDecMarshalUnmarshalByteEquality verifies that the Dec custom
// Marshal/Unmarshal produces byte-for-byte identical output to the
// previous big.Int.MarshalText/UnmarshalText path across a wide range
// of inputs. Wire-format equality is load-bearing: leaf bytes in the
// IAVL tree include the marshaled Dec, so any byte-level drift would
// change AppHashes and break canonical replay.
func TestDecMarshalUnmarshalByteEquality(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	cases := []*big.Int{
		big.NewInt(0),
		big.NewInt(1),
		big.NewInt(-1),
		big.NewInt(9),
		big.NewInt(10),
		big.NewInt(99),
		big.NewInt(100),
		big.NewInt(999_999_999_999_999_999),                // just under 1e18
		new(big.Int).Mul(big.NewInt(1_000_000_000_000_000_000), big.NewInt(1_000_000_000_000_000_000)), // 1e36
	}
	// Add 200 random magnitudes from 1 bit to 256 bits, both signs.
	for i := 0; i < 200; i++ {
		bits := rng.Intn(255) + 1
		n := new(big.Int).Rand(rng, new(big.Int).Lsh(big.NewInt(1), uint(bits)))
		if rng.Intn(2) == 1 {
			n.Neg(n)
		}
		cases = append(cases, n)
	}

	for _, n := range cases {
		want, err := n.MarshalText()
		if err != nil {
			t.Fatalf("stdlib MarshalText(%v): %v", n, err)
		}

		var d sdk.Dec
		// Construct a Dec carrying this big.Int value.
		// Dec.i is unexported; route through Unmarshal which sets it.
		if err := (&d).Unmarshal(want); err != nil {
			t.Fatalf("Dec.Unmarshal(%q): %v", want, err)
		}

		got, err := d.Marshal()
		if err != nil {
			t.Fatalf("Dec.Marshal: %v", err)
		}

		if string(got) != string(want) {
			t.Errorf("byte mismatch for %v:\n  stdlib: %q\n     got: %q", n, want, got)
		}

		// Also exercise MarshalTo + Size pair the way gogoproto does.
		size := (&d).Size()
		if size != len(want) {
			t.Errorf("Size() = %d, want %d for %v", size, len(want), n)
		}
		buf := make([]byte, size)
		nWritten, err := (&d).MarshalTo(buf)
		if err != nil {
			t.Fatalf("MarshalTo: %v", err)
		}
		if nWritten != size || string(buf[:nWritten]) != string(want) {
			t.Errorf("MarshalTo mismatch for %v:\n  stdlib: %q\n     got: %q (n=%d size=%d)",
				n, want, buf[:nWritten], nWritten, size)
		}
	}
}

func BenchmarkDecMarshal(b *testing.B) {
	// Typical distribution-module Dec: scaled by 10^18, ~20-30 digit magnitude
	d := sdk.MustNewDecFromStr("123456789012345678901234567.123456789012345678")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = d.Marshal()
	}
}

func BenchmarkDecUnmarshal(b *testing.B) {
	bz := []byte("123456789012345678901234567123456789012345678")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var d sdk.Dec
		_ = (&d).Unmarshal(bz)
	}
}

func BenchmarkDecMarshalTo(b *testing.B) {
	d := sdk.MustNewDecFromStr("123456789012345678901234567.123456789012345678")
	buf := make([]byte, 80)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = (&d).MarshalTo(buf)
	}
}

// gogoproto pattern: Size() then MarshalTo(). This is what every Dec
// field on every proto message hits. The wins from the Size+MarshalTo
// optimizations show up here, not in standalone Marshal benchmarks.
func BenchmarkDecSizeAndMarshalTo(b *testing.B) {
	d := sdk.MustNewDecFromStr("123456789012345678901234567.123456789012345678")
	buf := make([]byte, 80)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sz := (&d).Size()
		_, _ = (&d).MarshalTo(buf[:sz])
	}
}

func BenchmarkDecSize(b *testing.B) {
	d := sdk.MustNewDecFromStr("123456789012345678901234567.123456789012345678")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = (&d).Size()
	}
}
