package memiavl

import "strings"

// Go 1.20+ stdlib polyfills for the Go 1.18 build (chosen because
// Go 1.18 is the highest toolchain that produces canonical cosmoshub-4
// AppHashes for v4.0.x; see ambroslabs/cosmos-sdk-ambros#23).

func joinErrors(errs ...error) error {
	var msgs []string
	for _, e := range errs {
		if e != nil {
			msgs = append(msgs, e.Error())
		}
	}
	if len(msgs) == 0 {
		return nil
	}
	return &joinedError{msg: strings.Join(msgs, "\n")}
}

type joinedError struct{ msg string }

func (e *joinedError) Error() string { return e.msg }

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func clearChangesetMap(m map[string]*NamedChangeSet) {
	for k := range m {
		delete(m, k)
	}
}
