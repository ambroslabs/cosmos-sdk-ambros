// Package cache provides the minimal LRU cache that memiavl's
// shard cache and tree expect. It mirrors the surface of
// github.com/cosmos/iavl/cache (Cache, Node, New) without taking
// a transitive dep on iavl v0.19+ — the rest of the SDK is still
// pinned to iavl v0.15.3 for AppHash compatibility.
package cache

import "container/list"

type Node interface {
	GetKey() []byte
}

type Cache interface {
	Add(node Node) Node
	Get(key []byte) Node
	Has(key []byte) bool
	Remove(key []byte) Node
	Len() int
}

type lruCache struct {
	capacity int
	dict     map[string]*list.Element
	ll       *list.List
}

func New(capacity int) Cache {
	return &lruCache{
		capacity: capacity,
		dict:     make(map[string]*list.Element),
		ll:       list.New(),
	}
}

func (c *lruCache) Add(node Node) Node {
	key := string(node.GetKey())
	if e, ok := c.dict[key]; ok {
		c.ll.MoveToFront(e)
		e.Value = node
		return nil
	}
	e := c.ll.PushFront(node)
	c.dict[key] = e
	if c.capacity > 0 && c.ll.Len() > c.capacity {
		oldest := c.ll.Back()
		if oldest != nil {
			c.ll.Remove(oldest)
			evicted := oldest.Value.(Node)
			delete(c.dict, string(evicted.GetKey()))
			return evicted
		}
	}
	return nil
}

func (c *lruCache) Get(key []byte) Node {
	e, ok := c.dict[string(key)]
	if !ok {
		return nil
	}
	c.ll.MoveToFront(e)
	return e.Value.(Node)
}

func (c *lruCache) Has(key []byte) bool {
	_, ok := c.dict[string(key)]
	return ok
}

func (c *lruCache) Remove(key []byte) Node {
	e, ok := c.dict[string(key)]
	if !ok {
		return nil
	}
	c.ll.Remove(e)
	delete(c.dict, string(key))
	return e.Value.(Node)
}

func (c *lruCache) Len() int {
	return c.ll.Len()
}
