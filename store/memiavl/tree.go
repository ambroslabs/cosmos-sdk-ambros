package memiavl

import (
	"crypto/sha256"
	"fmt"
	"hash/crc32"
	"math"
	"sync"

	"github.com/cosmos/cosmos-sdk/store/memiavl/cache"
)

var emptyHash = sha256.New().Sum(nil)

func NewCache(cacheSize int) cache.Cache {
	if cacheSize == 0 {
		return nil
	}
	return newSharedCache(cacheSize)
}

const maxSharedCacheShards = 16

type sharedCache struct {
	shards     [maxSharedCacheShards]cacheShard
	shardCount int
	shardMask  uint32
}

type cacheShard struct {
	mu    sync.RWMutex
	cache cache.Cache
}

func newSharedCache(size int) cache.Cache {
	shardCount := 1
	for shardCount < maxSharedCacheShards && (shardCount<<1) <= size {
		shardCount <<= 1
	}

	sc := &sharedCache{
		shardCount: shardCount,
		shardMask:  uint32(shardCount - 1),
	}

	base := size / shardCount
	rem := size % shardCount
	for i := 0; i < shardCount; i++ {
		capacity := base
		if i < rem {
			capacity++
		}
		sc.shards[i].cache = cache.New(capacity)
	}

	return sc
}

func (c *sharedCache) shardForKey(key []byte) *cacheShard {
	if c.shardMask == 0 {
		return &c.shards[0]
	}
	return &c.shards[hashKey(key)&c.shardMask]
}

func (c *sharedCache) Add(node cache.Node) cache.Node {
	if node == nil {
		panic("shared cache: node must not be nil")
	}
	key := node.GetKey()
	shard := c.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.cache == nil {
		return nil
	}
	return shard.cache.Add(node)
}

func (c *sharedCache) Get(key []byte) cache.Node {
	shard := c.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.cache == nil {
		return nil
	}
	return shard.cache.Get(key)
}

func (c *sharedCache) Has(key []byte) bool {
	shard := c.shardForKey(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	if shard.cache == nil {
		return false
	}
	return shard.cache.Has(key)
}

func (c *sharedCache) Remove(key []byte) cache.Node {
	shard := c.shardForKey(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.cache == nil {
		return nil
	}
	return shard.cache.Remove(key)
}

func (c *sharedCache) Len() int {
	total := 0
	for i := 0; i < c.shardCount; i++ {
		shard := &c.shards[i]
		shard.mu.RLock()
		if shard.cache != nil {
			total += shard.cache.Len()
		}
		shard.mu.RUnlock()
	}
	return total
}

func hashKey(key []byte) uint32 {
	return crc32.ChecksumIEEE(key)
}

// Tree verify change sets by replay them to rebuild iavl tree and verify the root hashes
type Tree struct {
	version, cowVersion uint32
	// root node of empty tree is represented as `nil`
	root     Node
	snapshot *Snapshot

	// shared cache wrapper providing thread-safe access to cached nodes
	cache cache.Cache

	// when true, the get and iterator methods could return a slice pointing to mmaped blob files.
	zeroCopy bool

	traverseStateChanges traverseStateChangesFn
}

type traverseStateChangesFn func(startVersion, endVersion int64, fn func(version int64, changeSet *ChangeSet) error) error

type cacheNode struct {
	key, value []byte
}

func (n *cacheNode) GetKey() []byte {
	return n.key
}

// NewEmptyTree creates an empty tree at an arbitrary version.
func NewEmptyTree(version uint64, cacheSize int) *Tree {
	if version >= math.MaxUint32 {
		panic("version overflows uint32")
	}

	return &Tree{
		version: uint32(version),
		// no need to copy if the tree is not backed by snapshot
		zeroCopy: true,
		cache:    NewCache(cacheSize),
	}
}

// New creates an empty tree at genesis version
func New(cacheSize int) *Tree {
	return NewEmptyTree(0, cacheSize)
}

// NewWithInitialVersion creates a empty tree with initial-version,
// it happens when a new store created at the middle of the chain.
func NewWithInitialVersion(initialVersion uint32, cacheSize int) *Tree {
	if initialVersion <= 1 {
		return New(cacheSize)
	}
	return NewEmptyTree(uint64(initialVersion-1), cacheSize)
}

// NewFromSnapshot mmap the blob files and create the root node.
func NewFromSnapshot(snapshot *Snapshot, zeroCopy bool, cacheSize int) *Tree {
	tree := &Tree{
		version:  snapshot.Version(),
		snapshot: snapshot,
		zeroCopy: zeroCopy,
		cache:    NewCache(cacheSize),
	}

	if !snapshot.IsEmpty() {
		tree.root = snapshot.RootNode()
	}

	return tree
}

func (t *Tree) SetZeroCopy(zeroCopy bool) {
	t.zeroCopy = zeroCopy
}

func (t *Tree) IsEmpty() bool {
	return t.root == nil
}

func (t *Tree) SetInitialVersion(initialVersion int64) error {
	if initialVersion >= math.MaxUint32 {
		return fmt.Errorf("version overflows uint32: %d", initialVersion)
	}

	t.setInitialVersion(uint32(initialVersion))
	return nil
}

func (t *Tree) setInitialVersion(initialVersion uint32) {
	if t.version > 0 {
		// initial version has no effect if the tree is already initialized
		return
	}

	if initialVersion < 1 {
		t.version = 0
	} else {
		t.version = initialVersion - 1
	}
}

// Copy returns a snapshot of the tree which won't be modified by further modifications on the main tree,
// the returned new tree can be accessed concurrently with the main tree.
func (t *Tree) Copy(cacheSize int) *Tree {
	if _, ok := t.root.(*MemNode); ok {
		// protect the existing `MemNode`s from get modified in-place
		t.cowVersion = t.version
	}
	newTree := *t
	// recreate cache for the copy to keep eviction state independent per tree
	newTree.cache = NewCache(cacheSize)
	return &newTree
}

// ApplyChangeSet apply the change set of a whole version, and update hashes.
func (t *Tree) ApplyChangeSet(changeSet ChangeSet) {
	for _, pair := range changeSet.Pairs {
		if pair.Delete {
			t.remove(pair.Key)
		} else {
			t.set(pair.Key, pair.Value)
		}
	}
}

func (t *Tree) set(key, value []byte) {
	if value == nil {
		// the value could be nil when replaying changes from write-ahead-log because of protobuf decoding
		value = []byte{}
	}
	if !t.zeroCopy {
		key = cloneBytes(key)
		value = cloneBytes(value)
	}
	t.root, _ = setRecursive(t.root, key, value, t.version+1, t.cowVersion)
	if t.cache != nil {
		t.cache.Add(&cacheNode{key, value})
	}
}

func (t *Tree) remove(key []byte) {
	_, t.root, _ = removeRecursive(t.root, key, t.version+1, t.cowVersion)
	if t.cache != nil {
		t.cache.Remove(key)
	}
}

// SaveVersion increases the version number and optionally updates the hashes
func (t *Tree) SaveVersion(updateHash bool) ([]byte, int64, error) {
	if t.version == uint32(math.MaxUint32) {
		return nil, 0, fmt.Errorf("version overflows uint32: %d", t.version)
	}

	var hash []byte
	if updateHash {
		hash = t.RootHash()
	}

	t.version++
	return hash, int64(t.version), nil
}

// Version returns the current tree version
func (t *Tree) Version() int64 {
	return int64(t.version)
}

// RootHash updates the hashes and return the current root hash,
// it clones the persisted node's bytes, so the returned bytes is safe to retain.
func (t *Tree) RootHash() []byte {
	if t.root == nil {
		return emptyHash
	}
	return t.root.SafeHash()
}

func (t *Tree) GetWithIndex(key []byte) (int64, []byte) {
	if t.root == nil {
		return 0, nil
	}

	value, index := t.root.Get(key)
	if !t.zeroCopy {
		value = cloneBytes(value)
	}
	return int64(index), value
}

func (t *Tree) GetByIndex(index int64) ([]byte, []byte) {
	if index > math.MaxUint32 {
		return nil, nil
	}
	if t.root == nil {
		return nil, nil
	}

	key, value := t.root.GetByIndex(uint32(index))
	if !t.zeroCopy {
		key = cloneBytes(key)
		value = cloneBytes(value)
	}
	return key, value
}

func (t *Tree) Get(key []byte) []byte {
	if t.cache != nil {
		if node := t.cache.Get(key); node != nil {
			value := node.(*cacheNode).value
			if !t.zeroCopy {
				value = cloneBytes(value)
			}
			return value
		}
	}

	_, value := t.GetWithIndex(key)
	if value == nil {
		return nil
	}

	if t.cache != nil {
		cacheKey := key
		cacheValue := value
		if !t.zeroCopy {
			cacheKey = cloneBytes(key)
			cacheValue = cloneBytes(cacheValue)
		}
		t.cache.Add(&cacheNode{cacheKey, cacheValue})
	}
	return value
}

func (t *Tree) Has(key []byte) bool {
	return t.Get(key) != nil
}

func (t *Tree) Iterator(start, end []byte, ascending bool) *Iterator {
	return NewIterator(start, end, ascending, t.root, t.zeroCopy)
}

// TraverseStateChanges iterates the change sets between the given versions (inclusive).
func (t *Tree) TraverseStateChanges(startVersion, endVersion int64, fn func(version int64, changeSet *ChangeSet) error) error {
	if t.traverseStateChanges == nil {
		return fmt.Errorf("TraverseStateChanges not supported")
	}
	return t.traverseStateChanges(startVersion, endVersion, fn)
}

// ScanPostOrder scans the tree in post-order, and call the callback function on each node.
// If the callback function returns false, the scan will be stopped.
func (t *Tree) ScanPostOrder(callback func(node Node) bool) {
	if t.root == nil {
		return
	}

	stack := []*stackEntry{{node: t.root}}

	for len(stack) > 0 {
		entry := stack[len(stack)-1]

		if entry.node.IsLeaf() || entry.expanded {
			callback(entry.node)
			stack = stack[:len(stack)-1]
			continue
		}

		entry.expanded = true
		stack = append(stack, &stackEntry{node: entry.node.Right()})
		stack = append(stack, &stackEntry{node: entry.node.Left()})
	}
}

type stackEntry struct {
	node     Node
	expanded bool
}

// Export returns a snapshot of the tree which won't be corrupted by further modifications on the main tree.
func (t *Tree) Export() *Exporter {
	if t.snapshot != nil && t.version == t.snapshot.Version() {
		// snapshot export algorithm is more efficient
		return t.snapshot.Export()
	}

	// do normal post-order traversal export
	return newExporter(func(callback func(node *ExportNode) bool) {
		t.ScanPostOrder(func(node Node) bool {
			return callback(&ExportNode{
				Key:     node.Key(),
				Value:   node.Value(),
				Version: int64(node.Version()),
				Height:  int8(node.Height()),
			})
		})
	})
}

func (t *Tree) Close() error {
	var err error
	if t.snapshot != nil {
		err = t.snapshot.Close()
		t.snapshot = nil
	}
	t.root = nil
	t.traverseStateChanges = nil
	return err
}

func (t *Tree) setTraverseStateChanges(fn traverseStateChangesFn) {
	t.traverseStateChanges = fn
}

func (db *DB) attachTraverseStateChanges() {
	for i := range db.trees {
		t := db.trees[i]
		if t.Tree == nil {
			continue
		}
		storeName := t.Name
		t.setTraverseStateChanges(func(startVersion, endVersion int64, fn func(int64, *ChangeSet) error) error {
			return db.traverseStateChanges(storeName, startVersion, endVersion, fn)
		})
	}
}

func (db *DB) traverseStateChanges(store string, startVersion, endVersion int64, fn func(int64, *ChangeSet) error) error {
	walLog, initialVersion, lastVersion, snapshotVersion, err := db.walStateForRead()
	if err != nil {
		return err
	}
	if err := waitForWALVersion(walLog, initialVersion, lastVersion, snapshotVersion); err != nil {
		return err
	}
	if endVersion < startVersion {
		return nil
	}
	firstIndex, err := walLog.FirstIndex()
	if err != nil {
		return err
	}
	lastIndex, err := walLog.LastIndex()
	if err != nil {
		return err
	}
	if firstIndex == 0 || lastIndex == 0 {
		return nil
	}
	firstAvailable := walVersion(firstIndex, initialVersion)
	if startVersion < firstAvailable {
		startVersion = firstAvailable
	}
	lastAvailable := walVersion(lastIndex, initialVersion)
	if endVersion <= 0 || endVersion > lastAvailable {
		endVersion = lastAvailable
	}
	// If the requested interval sits entirely before the retained WAL window,
	// clamping start to firstAvailable jumps past endVersion. In that case there
	// is nothing left to traverse.
	if endVersion < startVersion {
		return nil
	}
	startIndex := walIndex(startVersion, initialVersion)
	endIndex := walIndex(endVersion, initialVersion)
	for idx := startIndex; idx <= endIndex; idx++ {
		data, err := walLog.Read(idx)
		if err != nil {
			return err
		}
		var entry WALEntry
		if err := entry.Unmarshal(data); err != nil {
			return err
		}
		version := walVersion(idx, initialVersion)
		var changeSet *ChangeSet
		for j := range entry.Changesets {
			if entry.Changesets[j].Name == store {
				changeSet = &entry.Changesets[j].Changeset
				break
			}
		}
		if changeSet == nil {
			changeSet = &ChangeSet{}
		}
		if err := fn(version, changeSet); err != nil {
			return err
		}
	}
	return nil
}
