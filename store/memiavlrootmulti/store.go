// Package memiavlrootmulti is the v0.42 cosmos-sdk multistore-adapter
// for memiavl. It implements types.CommitMultiStore by wrapping a
// *memiavl.DB so each named iavl substore is backed by a memiavl tree
// with shared versioning, WAL, and async snapshot rewriting.
//
// Ported from cronos-store/store/rootmulti (v0.50+ shape) to v0.42's
// store/types and tendermint v0.34. Listener / metrics / IAVL fast-node
// extensions were dropped since they don't exist in v0.42.
package memiavlrootmulti

import (
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"

	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/libs/log"
	dbm "github.com/tendermint/tm-db"

	"github.com/cosmos/cosmos-sdk/store/cachemulti"
	"github.com/cosmos/cosmos-sdk/store/mem"
	"github.com/cosmos/cosmos-sdk/store/memiavl"
	"github.com/cosmos/cosmos-sdk/store/memiavlstore"
	"github.com/cosmos/cosmos-sdk/store/rootmulti"
	"github.com/cosmos/cosmos-sdk/store/transient"
	"github.com/cosmos/cosmos-sdk/store/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
)

const CommitInfoFileName = "commit_infos"

var (
	_ types.CommitMultiStore = (*Store)(nil)
	_ types.Queryable        = (*Store)(nil)
)

type Store struct {
	dir     string
	db      *memiavl.DB
	logger  log.Logger
	chainId string

	lastCommitInfo *types.CommitInfo

	storesParams map[types.StoreKey]storeParams
	keysByName   map[string]types.StoreKey
	stores       map[types.StoreKey]types.CommitStore

	opts memiavl.Options

	// sdk46Compact retains the v0.46+ root-hash shape (StoreInfos for
	// non-iavl stores merged into the commit-info hash). v0.42 chains
	// pre-Delta also produce this shape, so default true.
	sdk46Compact bool
}

func NewStore(dir string, logger log.Logger, sdk46Compact bool, chainId string) *Store {
	return &Store{
		dir:          dir,
		logger:       logger,
		sdk46Compact: sdk46Compact,
		chainId:      chainId,

		storesParams: make(map[types.StoreKey]storeParams),
		keysByName:   make(map[string]types.StoreKey),
		stores:       make(map[types.StoreKey]types.CommitStore),
	}
}

func (rs *Store) SetMemIAVLOptions(opts memiavl.Options) {
	if opts.Logger == nil {
		opts.Logger = memiavl.Logger(rs.logger.With("module", "memiavl"))
	}
	rs.opts = opts
}

// flush gathers per-store change sets and applies them atomically.
func (rs *Store) flush() error {
	var changeSets []*memiavl.NamedChangeSet
	for key := range rs.stores {
		store := rs.GetCommitStore(key)
		if memiavlStore, ok := store.(*memiavlstore.Store); ok {
			cs := memiavlStore.PopChangeSet()
			if len(cs.Pairs) > 0 {
				changeSets = append(changeSets, &memiavl.NamedChangeSet{
					Name:      key.Name(),
					Changeset: cs,
				})
			}
		}
	}
	sort.SliceStable(changeSets, func(i, j int) bool {
		return changeSets[i].Name < changeSets[j].Name
	})
	return rs.db.ApplyChangeSets(changeSets)
}

func (rs *Store) WorkingHash() []byte {
	if err := rs.flush(); err != nil {
		panic(err)
	}
	commitInfo := convertCommitInfo(rs.db.WorkingCommitInfo())
	if rs.sdk46Compact {
		commitInfo = amendCommitInfo(commitInfo, rs.storesParams)
	}
	return commitInfo.Hash()
}

func (rs *Store) Commit() types.CommitID {
	if err := rs.flush(); err != nil {
		panic(err)
	}

	for _, store := range rs.stores {
		if store.GetStoreType() != types.StoreTypeIAVL {
			_ = store.Commit()
		}
	}

	if _, err := rs.db.Commit(); err != nil {
		panic(err)
	}

	for key := range rs.stores {
		store := rs.stores[key]
		if store.GetStoreType() == types.StoreTypeIAVL {
			store.(*memiavlstore.Store).SetTree(rs.db.TreeByName(key.Name()))
		}
	}

	rs.lastCommitInfo = convertCommitInfo(rs.db.LastCommitInfo())
	if rs.sdk46Compact {
		rs.lastCommitInfo = amendCommitInfo(rs.lastCommitInfo, rs.storesParams)
	}
	if os.Getenv("AMBROS_DEBUG_COMMITINFO") != "" {
		ci := rs.lastCommitInfo
		sorted := make([]types.StoreInfo, len(ci.StoreInfos))
		copy(sorted, ci.StoreInfos)
		sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
		fmt.Fprintf(os.Stderr, "AMBROS_DEBUG memiavl v=%d hash=%X\n", ci.Version, ci.Hash())
		for _, si := range sorted {
			fmt.Fprintf(os.Stderr, "AMBROS_DEBUG memiavl v=%d store=%-12s hash=%X version=%d\n",
				ci.Version, si.Name, si.CommitId.Hash, si.CommitId.Version)
		}
	}
	return rs.lastCommitInfo.CommitID()
}

func (rs *Store) Close() error {
	return rs.db.Close()
}

func (rs *Store) LastCommitID() types.CommitID {
	if rs.lastCommitInfo == nil {
		v, err := memiavl.GetLatestVersion(rs.dir)
		if err != nil {
			panic(fmt.Errorf("failed to get latest version: %w", err))
		}
		return types.CommitID{Version: v}
	}
	return rs.lastCommitInfo.CommitID()
}

func (rs *Store) SetPruning(_ types.PruningOptions) {}

func (rs *Store) GetPruning() types.PruningOptions {
	return types.PruneDefault
}

func (rs *Store) GetStoreType() types.StoreType {
	return types.StoreTypeMulti
}

func (rs *Store) CacheWrap() types.CacheWrap {
	return rs.CacheMultiStore().(types.CacheWrap)
}

func (rs *Store) CacheWrapWithTrace(_ io.Writer, _ types.TraceContext) types.CacheWrap {
	return rs.CacheWrap()
}

func (rs *Store) CacheMultiStore() types.CacheMultiStore {
	stores := make(map[types.StoreKey]types.CacheWrapper)
	for k, v := range rs.stores {
		stores[k] = v
	}
	return cachemulti.NewStore(nil, stores, rs.keysByName, nil, nil)
}

func (rs *Store) CacheMultiStoreWithVersion(version int64) (types.CacheMultiStore, error) {
	if version == 0 || (rs.lastCommitInfo != nil && version == rs.lastCommitInfo.Version) {
		return rs.CacheMultiStore(), nil
	}
	opts := rs.opts
	opts.TargetVersion = uint32(version)
	opts.ReadOnly = true
	db, err := memiavl.Load(rs.dir, opts, rs.chainId)
	if err != nil {
		return nil, err
	}

	stores := make(map[types.StoreKey]types.CacheWrapper)
	for k, store := range rs.stores {
		if store.GetStoreType() != types.StoreTypeIAVL {
			stores[k] = store
		}
	}
	for _, tree := range db.Trees() {
		stores[rs.keysByName[tree.Name]] = memiavlstore.New(tree.Tree, rs.logger)
	}
	return cachemulti.NewStore(nil, stores, rs.keysByName, nil, nil), nil
}

func (rs *Store) GetStore(key types.StoreKey) types.Store {
	s, ok := rs.stores[key]
	if !ok {
		panic(fmt.Sprintf("store does not exist for key: %s", key.Name()))
	}
	return s
}

func (rs *Store) GetKVStore(key types.StoreKey) types.KVStore {
	s, ok := rs.GetStore(key).(types.KVStore)
	if !ok {
		panic(fmt.Sprintf("store with key %v is not KVStore", key))
	}
	return s
}

func (rs *Store) TracingEnabled() bool { return false }

func (rs *Store) SetTracer(_ io.Writer) types.MultiStore { return rs }

func (rs *Store) SetTracingContext(types.TraceContext) types.MultiStore { return rs }

// MountStoreWithDB registers a substore key. The db arg is ignored —
// memiavl keeps every iavl substore in its own MultiTree.
func (rs *Store) MountStoreWithDB(key types.StoreKey, typ types.StoreType, _ dbm.DB) {
	if key == nil {
		panic("MountStoreWithDB() key cannot be nil")
	}
	if _, ok := rs.storesParams[key]; ok {
		panic(fmt.Sprintf("store duplicate store key %v", key))
	}
	if _, ok := rs.keysByName[key.Name()]; ok {
		panic(fmt.Sprintf("store duplicate store key name %v", key))
	}
	rs.storesParams[key] = storeParams{key: key, typ: typ}
	rs.keysByName[key.Name()] = key
}

func (rs *Store) GetCommitStore(key types.StoreKey) types.CommitStore {
	return rs.stores[key]
}

func (rs *Store) GetCommitKVStore(key types.StoreKey) types.CommitKVStore {
	store, ok := rs.GetCommitStore(key).(types.CommitKVStore)
	if !ok {
		panic(fmt.Sprintf("store with key %v is not CommitKVStore", key))
	}
	return store
}

func (rs *Store) LoadLatestVersion() error {
	return rs.LoadVersionAndUpgrade(0, nil)
}

func (rs *Store) LoadLatestVersionAndUpgrade(upgrades *types.StoreUpgrades) error {
	return rs.LoadVersionAndUpgrade(0, upgrades)
}

func (rs *Store) LoadVersionAndUpgrade(version int64, upgrades *types.StoreUpgrades) error {
	if version > math.MaxUint32 {
		return fmt.Errorf("version overflows uint32: %d", version)
	}

	storesKeys := make([]types.StoreKey, 0, len(rs.storesParams))
	for key := range rs.storesParams {
		storesKeys = append(storesKeys, key)
	}
	sort.Slice(storesKeys, func(i, j int) bool {
		return storesKeys[i].Name() < storesKeys[j].Name()
	})

	initialStores := make([]string, 0, len(storesKeys))
	for _, key := range storesKeys {
		if rs.storesParams[key].typ == types.StoreTypeIAVL {
			initialStores = append(initialStores, key.Name())
		}
	}

	opts := rs.opts
	opts.CreateIfMissing = true
	opts.InitialStores = initialStores
	opts.TargetVersion = uint32(version)
	db, err := memiavl.Load(rs.dir, opts, rs.chainId)
	if err != nil {
		return sdkerrors.Wrapf(err, "fail to load memiavl at %s", rs.dir)
	}

	var treeUpgrades []*memiavl.TreeNameUpgrade
	if upgrades != nil {
		for _, name := range upgrades.Deleted {
			treeUpgrades = append(treeUpgrades, &memiavl.TreeNameUpgrade{Name: name, Delete: true})
		}
		for _, name := range upgrades.Added {
			treeUpgrades = append(treeUpgrades, &memiavl.TreeNameUpgrade{Name: name})
		}
		for _, rename := range upgrades.Renamed {
			treeUpgrades = append(treeUpgrades, &memiavl.TreeNameUpgrade{Name: rename.NewKey, RenameFrom: rename.OldKey})
		}
	}

	if len(treeUpgrades) > 0 {
		if err := db.ApplyUpgrades(treeUpgrades); err != nil {
			return err
		}
	}

	newStores := make(map[types.StoreKey]types.CommitStore, len(storesKeys))
	for _, key := range storesKeys {
		newStores[key], err = rs.loadCommitStoreFromParams(db, key, rs.storesParams[key])
		if err != nil {
			return err
		}
	}

	rs.db = db
	rs.stores = newStores
	if db.Version() != 0 {
		rs.lastCommitInfo = convertCommitInfo(db.LastCommitInfo())
		if rs.sdk46Compact {
			rs.lastCommitInfo = amendCommitInfo(rs.lastCommitInfo, rs.storesParams)
		}
	} else {
		rs.lastCommitInfo = &types.CommitInfo{}
	}

	return nil
}

func (rs *Store) loadCommitStoreFromParams(db *memiavl.DB, key types.StoreKey, params storeParams) (types.CommitStore, error) {
	switch params.typ {
	case types.StoreTypeMulti:
		panic("recursive MultiStores not yet supported")
	case types.StoreTypeIAVL:
		tree := db.TreeByName(key.Name())
		if tree == nil {
			return nil, fmt.Errorf("new store is not added in upgrades: %s", key.Name())
		}
		return types.CommitStore(memiavlstore.New(tree, rs.logger)), nil
	case types.StoreTypeDB:
		panic("StoreTypeDB not supported in memiavl multistore")
	case types.StoreTypeTransient:
		if _, ok := key.(*types.TransientStoreKey); !ok {
			return nil, fmt.Errorf("unexpected key type for a TransientStoreKey; got: %s, %T", key.String(), key)
		}
		return transient.NewStore(), nil
	case types.StoreTypeMemory:
		if _, ok := key.(*types.MemoryStoreKey); !ok {
			return nil, fmt.Errorf("unexpected key type for a MemoryStoreKey; got: %s", key.String())
		}
		return mem.NewStore(), nil
	default:
		return nil, fmt.Errorf("unrecognized store type: %v", params.typ)
	}
}

func (rs *Store) LoadVersion(ver int64) error {
	return rs.LoadVersionAndUpgrade(ver, nil)
}

func (rs *Store) SetInterBlockCache(_ types.MultiStorePersistentCache) {}

func (rs *Store) SetInitialVersion(version int64) error {
	return rs.db.SetInitialVersion(version)
}

// Snapshot/Restore (state-sync). Stubbed for the v0.42 replay
// experiment; archival replay doesn't need them. Wire up when serving
// state-sync snapshots from a memiavl node.
func (rs *Store) Snapshot(height uint64, format uint32) (<-chan io.ReadCloser, error) {
	return nil, fmt.Errorf("memiavl rootmulti: Snapshot not implemented")
}

func (rs *Store) Restore(height uint64, format uint32, chunks <-chan io.ReadCloser, ready chan<- struct{}) error {
	return fmt.Errorf("memiavl rootmulti: Restore not implemented")
}

// RollbackToVersion is used by standalone CLI commands. Drops versions
// after target and reloads.
func (rs *Store) RollbackToVersion(target int64) error {
	if target <= 0 {
		return fmt.Errorf("invalid rollback height target: %d", target)
	}
	if target > math.MaxUint32 {
		return fmt.Errorf("rollback height target %d exceeds max uint32", target)
	}
	if rs.db != nil {
		if err := rs.db.Close(); err != nil {
			return err
		}
	}
	opts := rs.opts
	opts.TargetVersion = uint32(target)
	opts.LoadForOverwriting = true
	var err error
	rs.db, err = memiavl.Load(rs.dir, opts, rs.chainId)
	return err
}

func (rs *Store) Query(req abci.RequestQuery) abci.ResponseQuery {
	version := req.Height
	if version == 0 {
		version = rs.db.Version()
	}

	db := rs.db
	if rs.lastCommitInfo != nil && version != rs.lastCommitInfo.Version {
		var err error
		db, err = memiavl.Load(rs.dir, memiavl.Options{TargetVersion: uint32(version), ReadOnly: true}, rs.chainId)
		if err != nil {
			return sdkerrors.QueryResult(sdkerrors.Wrapf(err, "failed to load memiavl at version %d", version))
		}
		defer db.Close()
	}

	storeName, subpath, err := parsePath(req.Path)
	if err != nil {
		return sdkerrors.QueryResult(err)
	}

	tree := db.TreeByName(storeName)
	if tree == nil {
		return sdkerrors.QueryResult(sdkerrors.Wrapf(sdkerrors.ErrUnknownRequest, "no such store: %s", storeName))
	}

	store := types.Queryable(memiavlstore.New(tree, rs.logger))

	req.Path = subpath
	res := store.Query(req)

	if !req.Prove || !rootmulti.RequireProof(subpath) {
		return res
	}

	if res.ProofOps == nil || len(res.ProofOps.Ops) == 0 {
		return sdkerrors.QueryResult(sdkerrors.Wrap(sdkerrors.ErrInvalidRequest, "proof is unexpectedly empty; ensure height has not been pruned"))
	}

	commitInfo := convertCommitInfo(db.LastCommitInfo())
	if rs.sdk46Compact {
		commitInfo = amendCommitInfo(commitInfo, rs.storesParams)
	}
	res.ProofOps.Ops = append(res.ProofOps.Ops, commitInfo.ProofOp(storeName))

	return res
}

func parsePath(path string) (storeName, subpath string, err error) {
	if !strings.HasPrefix(path, "/") {
		return storeName, subpath, sdkerrors.Wrapf(sdkerrors.ErrUnknownRequest, "invalid path: %s", path)
	}

	paths := strings.SplitN(path[1:], "/", 2)
	storeName = paths[0]
	if len(paths) == 2 {
		subpath = "/" + paths[1]
	}
	return storeName, subpath, nil
}

type storeParams struct {
	key types.StoreKey
	typ types.StoreType
}

func mergeStoreInfos(commitInfo *types.CommitInfo, storeInfos []types.StoreInfo) *types.CommitInfo {
	infos := make([]types.StoreInfo, 0, len(commitInfo.StoreInfos)+len(storeInfos))
	infos = append(infos, commitInfo.StoreInfos...)
	infos = append(infos, storeInfos...)
	sort.SliceStable(infos, func(i, j int) bool {
		return infos[i].Name < infos[j].Name
	})
	return &types.CommitInfo{
		Version:    commitInfo.Version,
		StoreInfos: infos,
	}
}

// amendCommitInfo merges StoreInfos for non-iavl stores so the root
// hash matches cosmos-sdk's v0.46+ shape that v0.42 chains also produce.
func amendCommitInfo(commitInfo *types.CommitInfo, params map[types.StoreKey]storeParams) *types.CommitInfo {
	var extraStoreInfos []types.StoreInfo
	for key := range params {
		typ := params[key].typ
		if typ != types.StoreTypeIAVL && typ != types.StoreTypeTransient {
			extraStoreInfos = append(extraStoreInfos, types.StoreInfo{
				Name:     key.Name(),
				CommitId: types.CommitID{},
			})
		}
	}
	return mergeStoreInfos(commitInfo, extraStoreInfos)
}

func convertCommitInfo(commitInfo *memiavl.CommitInfo) *types.CommitInfo {
	storeInfos := make([]types.StoreInfo, len(commitInfo.StoreInfos))
	for i, storeInfo := range commitInfo.StoreInfos {
		storeInfos[i] = types.StoreInfo{
			Name: storeInfo.Name,
			CommitId: types.CommitID{
				Version: storeInfo.CommitId.Version,
				Hash:    storeInfo.CommitId.Hash,
			},
		}
	}
	return &types.CommitInfo{
		Version:    commitInfo.Version,
		StoreInfos: storeInfos,
	}
}
