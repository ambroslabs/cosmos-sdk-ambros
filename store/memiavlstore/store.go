// Package memiavlstore is the v0.42 cosmos-sdk store-adapter that
// wraps a *memiavl.Tree as a types.CommitKVStore. It mirrors the
// cronos-store/memiavlstore package, ported from cosmossdk.io/store
// (v0.50+) APIs to v0.42's store/types and tendermint v0.34.
package memiavlstore

import (
	"fmt"
	"io"

	ics23 "github.com/confio/ics23/go"
	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/libs/log"
	tmprotocrypto "github.com/tendermint/tendermint/proto/tendermint/crypto"

	"github.com/cosmos/cosmos-sdk/store/cachekv"
	"github.com/cosmos/cosmos-sdk/store/memiavl"
	"github.com/cosmos/cosmos-sdk/store/tracekv"
	"github.com/cosmos/cosmos-sdk/store/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	"github.com/cosmos/cosmos-sdk/types/kv"
)

var (
	_ types.KVStore       = (*Store)(nil)
	_ types.CommitStore   = (*Store)(nil)
	_ types.CommitKVStore = (*Store)(nil)
	_ types.Queryable     = (*Store)(nil)
)

// Store implements types.KVStore and types.CommitKVStore by wrapping a
// *memiavl.Tree. Writes are accumulated in changeSet and flushed by the
// owning rootmulti store at Commit time.
type Store struct {
	tree   *memiavl.Tree
	logger log.Logger

	changeSet memiavl.ChangeSet
}

func New(tree *memiavl.Tree, logger log.Logger) *Store {
	return &Store{tree: tree, logger: logger}
}

func (st *Store) SetTree(tree *memiavl.Tree) {
	st.tree = tree
}

// Commit is invalid on a leaf memiavl store; the owning rootmulti
// store coordinates atomic commits across all named trees.
func (st *Store) Commit() types.CommitID {
	panic("memiavl store is not supposed to be committed alone")
}

func (st *Store) LastCommitID() types.CommitID {
	return types.CommitID{
		Version: st.tree.Version(),
		Hash:    st.tree.RootHash(),
	}
}

func (st *Store) SetPruning(_ types.PruningOptions) {
	panic("cannot set pruning options on an initialized memiavl store")
}

func (st *Store) GetPruning() types.PruningOptions {
	panic("cannot get pruning options on an initialized memiavl store")
}

func (st *Store) GetStoreType() types.StoreType {
	return types.StoreTypeIAVL
}

func (st *Store) CacheWrap() types.CacheWrap {
	return cachekv.NewStore(st)
}

func (st *Store) CacheWrapWithTrace(w io.Writer, tc types.TraceContext) types.CacheWrap {
	return cachekv.NewStore(tracekv.NewStore(st, w, tc))
}

// Set buffers a write. Visible only after Commit on the owning rootmulti.
func (st *Store) Set(key, value []byte) {
	types.AssertValidKey(key)
	types.AssertValidValue(value)
	st.changeSet.Pairs = append(st.changeSet.Pairs, &memiavl.KVPair{
		Key: key, Value: value,
	})
}

func (st *Store) Get(key []byte) []byte {
	return st.tree.Get(key)
}

func (st *Store) Has(key []byte) bool {
	return st.tree.Has(key)
}

// Delete buffers a deletion. Visible only after Commit on the owning rootmulti.
func (st *Store) Delete(key []byte) {
	types.AssertValidKey(key)
	st.changeSet.Pairs = append(st.changeSet.Pairs, &memiavl.KVPair{
		Key: key, Delete: true,
	})
}

func (st *Store) Iterator(start, end []byte) types.Iterator {
	return st.tree.Iterator(start, end, true)
}

func (st *Store) ReverseIterator(start, end []byte) types.Iterator {
	return st.tree.Iterator(start, end, false)
}

// SetInitialVersion is coordinated by the owning rootmulti store, not the leaf.
func (st *Store) SetInitialVersion(_ int64) {
	panic("memiavl store's SetInitialVersion is not supposed to be called directly")
}

// PopChangeSet returns the buffered change set and clears the buffer.
// The owning rootmulti store calls this in Commit and feeds the result
// into memiavl.DB.ApplyChangeSets.
func (st *Store) PopChangeSet() memiavl.ChangeSet {
	cs := st.changeSet
	st.changeSet = memiavl.ChangeSet{}
	return cs
}

// WorkingHash returns the in-memory root hash; used by ABCI WorkingHash.
func (st *Store) WorkingHash() []byte {
	return st.tree.RootHash()
}

// Query implements types.Queryable. memiavl only retains the latest
// version (the snapshot, plus the WAL since the snapshot), so any
// historical query at a different version returns an error.
func (st *Store) Query(req abci.RequestQuery) abci.ResponseQuery {
	if len(req.Data) == 0 {
		return sdkerrors.QueryResult(sdkerrors.Wrap(sdkerrors.ErrTxDecode, "query cannot be zero length"))
	}

	if req.Height != 0 && req.Height != st.tree.Version() {
		return sdkerrors.QueryResult(sdkerrors.Wrap(sdkerrors.ErrInvalidHeight, "memiavl only supports queries at the latest version"))
	}

	res := abci.ResponseQuery{
		Height: st.tree.Version(),
	}

	switch req.Path {
	case "/key":
		res.Key = req.Data
		res.Value = st.tree.Get(res.Key)
		if !req.Prove {
			break
		}
		res.ProofOps = getProofFromTree(st.tree, req.Data, res.Value != nil)

	case "/subspace":
		pairs := kv.Pairs{Pairs: make([]kv.Pair, 0)}
		subspace := req.Data
		res.Key = subspace

		iterator := types.KVStorePrefixIterator(st, subspace)
		for ; iterator.Valid(); iterator.Next() {
			pairs.Pairs = append(pairs.Pairs, kv.Pair{Key: iterator.Key(), Value: iterator.Value()})
		}
		iterator.Close()

		bz, err := pairs.Marshal()
		if err != nil {
			panic(fmt.Errorf("failed to marshal KV pairs: %w", err))
		}
		res.Value = bz

	default:
		return sdkerrors.QueryResult(sdkerrors.Wrapf(sdkerrors.ErrUnknownRequest, "unexpected query path: %v", req.Path))
	}

	return res
}

// getProofFromTree returns an ics23 commitment proof for the key,
// wrapped as a tendermint-v0.34 ProofOps for the ABCI response.
func getProofFromTree(tree *memiavl.Tree, key []byte, exists bool) *tmprotocrypto.ProofOps {
	var (
		commitmentProof *ics23.CommitmentProof
		err             error
	)

	if exists {
		commitmentProof, err = tree.GetMembershipProof(key)
		if err != nil {
			panic(fmt.Sprintf("unexpected error for membership proof: %s", err.Error()))
		}
	} else {
		commitmentProof, err = tree.GetNonMembershipProof(key)
		if err != nil {
			panic(fmt.Sprintf("unexpected error for nonexistence proof: %s", err.Error()))
		}
	}

	op := types.NewIavlCommitmentOp(key, commitmentProof)
	return &tmprotocrypto.ProofOps{Ops: []tmprotocrypto.ProofOp{op.ProofOp()}}
}
