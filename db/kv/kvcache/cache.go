// Copyright 2021 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package kvcache

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c2h5oh/datasize"
	btree2 "github.com/tidwall/btree"
	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/db/kv"
)

type CacheValidationResult struct {
	RequestCancelled   bool
	Enabled            bool
	LatestStateBehind  bool
	CacheCleared       bool
	LatestStateID      uint64
	StateKeysOutOfSync [][]byte
	CodeKeysOutOfSync  [][]byte
}

type Cache interface {
	// View - returns CacheView consistent with given kv.Tx
	View(ctx context.Context, tx kv.TemporalTx) (CacheView, error)
	OnNewBlock(sc *remoteproto.StateChangeBatch)
	Len() int
	ValidateCurrentRoot(ctx context.Context, tx kv.TemporalTx) (*CacheValidationResult, error)
}
type CacheView interface {
	Get(k []byte) ([]byte, error)
	GetCode(k []byte) ([]byte, error)
	HasStorage(address common.Address) (bool, error)
}

const (
	DEGREE           = 32
	MAX_WAITS        = 100
	DefaultNumShards = 16
)

type CoherentConfig struct {
	CacheSize       datasize.ByteSize
	CodeCacheSize   datasize.ByteSize
	WaitForNewBlock bool // should we wait 10ms for a new block message to arrive when calling View?
	WithStorage     bool
	MetricsLabel    string
	NewBlockWait    time.Duration // how long wait
	KeepViews       uint64        // keep in memory up to this amount of views, evict older
	NumShards       uint32        // must be power of 2; 0 means DefaultNumShards
}

var DefaultCoherentConfig = CoherentConfig{
	KeepViews:       5,
	NewBlockWait:    5 * time.Millisecond,
	CacheSize:       2 * datasize.GB,
	CodeCacheSize:   2 * datasize.GB,
	MetricsLabel:    "default",
	WithStorage:     true,
	WaitForNewBlock: true,
}

// cacheShard holds one partition of BTrees and eviction lists.
// A single mutex protects both BTree lookup and eviction MoveToFront,
// eliminating the double-lock pattern from the old design.
type cacheShard struct {
	mu         sync.Mutex
	cache      *btree2.BTreeG[*Element]
	codeCache  *btree2.BTreeG[*Element]
	stateEvict *List
	codeEvict  *List
}

func newCacheShard() cacheShard {
	return cacheShard{
		cache:      btree2.NewBTreeG[*Element](Less),
		codeCache:  btree2.NewBTreeG[*Element](Less),
		stateEvict: NewList(),
		codeEvict:  NewList(),
	}
}

// Coherent works on top of Database Transaction and pair Coherent+ReadTransaction must
// provide "Serializable Isolation Level" semantic: all data form consistent db view at moment
// when read transaction started, read data are immutable until end of read transaction, reader can't see newer updates
//
// Pair.Value == nil - is a marker of absence key in db
//
// High-level guarantees:
// - Keys/Values returned by cache are valid/immutable until end of db transaction
// - CacheView is always coherent with given db transaction
//
// Rules of set view.isCanonical value:
//   - method View can't parent.Clone() - because parent view is not coherent with current kv.Tx
//   - only OnNewBlock method may do parent.Clone() and apply StateChanges to create coherent view of kv.Tx
//   - parent.Clone() can't be called if parent.isCanonical=false
//   - only OnNewBlock method can set view.isCanonical=true
//
// Rules of filling eviction lists:
//   - changes in Canonical View SHOULD reflect in eviction lists
//   - changes in Non-Canonical View SHOULD NOT reflect in eviction lists
type Coherent struct {
	// root management (protected by rootMu)
	rootMu               sync.Mutex
	roots                map[uint64]*CoherentRoot
	latestStateVersionID uint64
	latestStateView      *CoherentRoot
	waitExceededCount    atomic.Int32

	// serializes OnNewBlock (protects hasher)
	onNewBlockMu sync.Mutex
	hasher       hash.Hash

	// immutable after init
	cfg       CoherentConfig
	numShards uint32
	shardMask uint32 // numShards - 1

	// metrics
	codeEvictLen metrics.Gauge
	codeKeys     metrics.Gauge
	keys         metrics.Gauge
	evict        metrics.Gauge
	codeMiss     metrics.Counter
	timeout      metrics.Counter
	hits         metrics.Counter
	codeHits     metrics.Counter
	miss         metrics.Counter
}

type CoherentRoot struct {
	shards          []cacheShard
	ready           chan struct{} // close when ready
	readyChanClosed atomic.Bool   // quick check if ready channel is closed
	closeOnce       sync.Once     // protecting `ready` field from double-close
	isCanonical     bool
}

// CoherentView - dumb object, which proxy all requests to Coherent object.
// It's thread-safe, because immutable
type CoherentView struct {
	tx             kv.TemporalTx
	cache          *Coherent
	stateVersionID uint64
}

func (c *CoherentView) Get(k []byte) ([]byte, error) {
	return c.cache.Get(k, c.tx, c.stateVersionID)
}
func (c *CoherentView) GetCode(k []byte) ([]byte, error) {
	return c.cache.GetCode(k, c.tx, c.stateVersionID)
}
func (c *CoherentView) HasStorage(address common.Address) (bool, error) {
	_, _, hasStorage, err := c.tx.HasPrefix(kv.StorageDomain, address[:])
	return hasStorage, err
}

var _ Cache = (*Coherent)(nil)         // compile-time interface check
var _ CacheView = (*CoherentView)(nil) // compile-time interface check

func New(cfg CoherentConfig) *Coherent {
	if cfg.KeepViews == 0 {
		panic("empty config passed")
	}

	numShards := cfg.NumShards
	if numShards == 0 {
		numShards = DefaultNumShards
	}
	if numShards&(numShards-1) != 0 {
		panic("NumShards must be a power of 2")
	}

	return &Coherent{
		roots:     map[uint64]*CoherentRoot{},
		hasher:    sha3.NewLegacyKeccak256(),
		cfg:       cfg,
		numShards: numShards,
		shardMask: numShards - 1,
		miss:      metrics.GetOrCreateCounter(fmt.Sprintf(`cache_total{result="miss",name="%s"}`, cfg.MetricsLabel)),
		hits:      metrics.GetOrCreateCounter(fmt.Sprintf(`cache_total{result="hit",name="%s"}`, cfg.MetricsLabel)),
		timeout:   metrics.GetOrCreateCounter(fmt.Sprintf(`cache_timeout_total{name="%s"}`, cfg.MetricsLabel)),
		keys:      metrics.GetOrCreateGauge(fmt.Sprintf(`cache_keys_total{name="%s"}`, cfg.MetricsLabel)),
		evict:     metrics.GetOrCreateGauge(fmt.Sprintf(`cache_list_total{name="%s"}`, cfg.MetricsLabel)),
		codeMiss:  metrics.GetOrCreateCounter(fmt.Sprintf(`cache_code_total{result="miss",name="%s"}`, cfg.MetricsLabel)),
		codeHits:  metrics.GetOrCreateCounter(fmt.Sprintf(`cache_code_total{result="hit",name="%s"}`, cfg.MetricsLabel)),
		codeKeys:  metrics.GetOrCreateGauge(fmt.Sprintf(`cache_code_keys_total{name="%s"}`, cfg.MetricsLabel)),
		codeEvictLen: metrics.GetOrCreateGauge(fmt.Sprintf(`cache_code_list_total{name="%s"}`, cfg.MetricsLabel)),
	}
}

func (c *Coherent) newRoot() *CoherentRoot {
	shards := make([]cacheShard, c.numShards)
	for i := range shards {
		shards[i] = newCacheShard()
	}
	return &CoherentRoot{
		shards: shards,
		ready:  make(chan struct{}),
	}
}

// shardIndex returns the shard for a given key using first byte.
// Account keys (20 bytes): first byte of address (keccak-derived, pseudo-random).
// Storage keys (60 bytes): first byte of address.
// Code keys (32 bytes): first byte of keccak hash (uniformly random).
func (c *Coherent) shardIndex(k []byte) uint32 {
	if len(k) == 0 {
		return 0
	}
	return uint32(k[0]) & c.shardMask
}

// stateBudgetPerShard returns the per-shard eviction budget for state cache.
func (c *Coherent) stateBudgetPerShard() int {
	b := int(c.cfg.CacheSize.Bytes()) / int(c.numShards)
	if b < 1 {
		b = 1
	}
	return b
}

// codeBudgetPerShard returns the per-shard eviction budget for code cache.
func (c *Coherent) codeBudgetPerShard() int {
	b := int(c.cfg.CodeCacheSize.Bytes()) / int(c.numShards)
	if b < 1 {
		b = 1
	}
	return b
}

// selectOrCreateRoot - used for usual getting root
func (c *Coherent) selectOrCreateRoot(versionID uint64) *CoherentRoot {
	c.rootMu.Lock()
	defer c.rootMu.Unlock()
	r, ok := c.roots[versionID]
	if ok {
		return r
	}

	r = c.newRoot()
	c.roots[versionID] = r
	return r
}

// advanceRoot - used for advancing root onNewBlock.
// Must be called with rootMu held.
func (c *Coherent) advanceRoot(stateVersionID uint64) (r *CoherentRoot) {
	r, rootExists := c.roots[stateVersionID]

	// if nothing has progressed just return the existing root
	if c.latestStateVersionID == stateVersionID && rootExists {
		return r
	}

	if !rootExists {
		r = &CoherentRoot{
			shards: make([]cacheShard, c.numShards),
			ready:  make(chan struct{}),
		}
		c.roots[stateVersionID] = r
	}

	if prevView, ok := c.roots[stateVersionID-1]; ok && prevView.isCanonical {
		// COW clone each shard from previous canonical root
		for i := uint32(0); i < c.numShards; i++ {
			prevView.shards[i].mu.Lock()
			r.shards[i].cache = prevView.shards[i].cache.Copy()
			r.shards[i].codeCache = prevView.shards[i].codeCache.Copy()
			prevView.shards[i].mu.Unlock()
			// Eviction lists carry over by reference from canonical view.
			// New elements added to this root will be tracked in these lists.
			r.shards[i].stateEvict = prevView.shards[i].stateEvict
			r.shards[i].codeEvict = prevView.shards[i].codeEvict
		}
	} else {
		for i := uint32(0); i < c.numShards; i++ {
			if r.shards[i].stateEvict == nil {
				r.shards[i].stateEvict = NewList()
			} else {
				r.shards[i].stateEvict.Init()
			}
			if r.shards[i].codeEvict == nil {
				r.shards[i].codeEvict = NewList()
			} else {
				r.shards[i].codeEvict.Init()
			}
			if r.shards[i].cache == nil {
				r.shards[i].cache = btree2.NewBTreeG[*Element](Less)
				r.shards[i].codeCache = btree2.NewBTreeG[*Element](Less)
			} else {
				r.shards[i].cache.Walk(func(items []*Element) bool {
					for _, item := range items {
						r.shards[i].stateEvict.PushFront(item)
					}
					return true
				})
				r.shards[i].codeCache.Walk(func(items []*Element) bool {
					for _, item := range items {
						r.shards[i].codeEvict.PushFront(item)
					}
					return true
				})
			}
		}
	}
	r.isCanonical = true

	c.evictRoots()
	c.latestStateVersionID = stateVersionID
	c.latestStateView = r

	// Update metrics (sum across shards)
	totalKeys, totalCodeKeys := 0, 0
	totalEvict, totalCodeEvict := 0, 0
	for i := uint32(0); i < c.numShards; i++ {
		totalKeys += r.shards[i].cache.Len()
		totalCodeKeys += r.shards[i].codeCache.Len()
		totalEvict += r.shards[i].stateEvict.Len()
		totalCodeEvict += r.shards[i].codeEvict.Len()
	}
	c.keys.SetInt(totalKeys)
	c.codeKeys.SetInt(totalCodeKeys)
	c.evict.SetInt(totalEvict)
	c.codeEvictLen.SetInt(totalCodeEvict)
	return r
}

func (c *Coherent) OnNewBlock(stateChanges *remoteproto.StateChangeBatch) {
	c.onNewBlockMu.Lock()
	defer c.onNewBlockMu.Unlock()

	c.rootMu.Lock()
	c.waitExceededCount.Store(0) // reset the circuit breaker
	id := stateChanges.StateVersionId
	r := c.advanceRoot(id)
	latestID := c.latestStateVersionID
	c.rootMu.Unlock()

	for _, sc := range stateChanges.ChangeBatch {
		for i := range sc.Changes {
			switch sc.Changes[i].Action {
			case remoteproto.Action_UPSERT:
				addr := gointerfaces.ConvertH160toAddress(sc.Changes[i].Address)
				v := sc.Changes[i].Data
				c.add(addr[:], v, r, id, latestID)
			case remoteproto.Action_UPSERT_CODE:
				addr := gointerfaces.ConvertH160toAddress(sc.Changes[i].Address)
				v := sc.Changes[i].Data
				c.add(addr[:], v, r, id, latestID)
				c.hasher.Reset()
				c.hasher.Write(sc.Changes[i].Code)
				k := make([]byte, 32)
				c.hasher.Sum(k)
				c.addCode(k, sc.Changes[i].Code, r, id, latestID)
			case remoteproto.Action_REMOVE:
				addr := gointerfaces.ConvertH160toAddress(sc.Changes[i].Address)
				c.add(addr[:], nil, r, id, latestID)
			case remoteproto.Action_STORAGE:
				//skip, will check later
			case remoteproto.Action_CODE:
				c.hasher.Reset()
				c.hasher.Write(sc.Changes[i].Code)
				k := make([]byte, 32)
				c.hasher.Sum(k)
				c.addCode(k, sc.Changes[i].Code, r, id, latestID)
			default:
				panic("not implemented yet")
			}
			if c.cfg.WithStorage && len(sc.Changes[i].StorageChanges) > 0 {
				addr := gointerfaces.ConvertH160toAddress(sc.Changes[i].Address)
				for _, change := range sc.Changes[i].StorageChanges {
					loc := gointerfaces.ConvertH256ToHash(change.Location)
					k := make([]byte, 20+8+32)
					copy(k, addr[:])
					binary.BigEndian.PutUint64(k[20:], sc.Changes[i].Incarnation)
					copy(k[20+8:], loc[:])
					c.add(k, change.Data, r, id, latestID)
				}
			}
		}
	}

	r.closeOnce.Do(func() {
		r.readyChanClosed.Store(true)
		close(r.ready) // broadcast
	})
}

func (c *Coherent) View(ctx context.Context, tx kv.TemporalTx) (CacheView, error) {
	id, err := tx.ReadSequence(string(kv.PlainStateVersion))
	if err != nil {
		return nil, err
	}

	r := c.selectOrCreateRoot(id)

	if !c.cfg.WaitForNewBlock || c.waitExceededCount.Load() >= MAX_WAITS {
		return &CoherentView{stateVersionID: id, tx: tx, cache: c}, nil
	}

	if r.readyChanClosed.Load() {
		return &CoherentView{stateVersionID: id, tx: tx, cache: c}, nil
	}

	select {
	case <-r.ready:
	case <-ctx.Done():
	case <-time.After(c.cfg.NewBlockWait):
		c.timeout.Inc()
		c.waitExceededCount.Add(1)
	}

	return &CoherentView{stateVersionID: id, tx: tx, cache: c}, nil
}

func (c *Coherent) getFromCache(k []byte, id uint64, domain kv.Domain) (*Element, *CoherentRoot, error) {
	// Brief lock on rootMu to find the root
	c.rootMu.Lock()
	r, ok := c.roots[id]
	if !ok {
		latestID := c.latestStateVersionID
		c.rootMu.Unlock()
		return nil, nil, fmt.Errorf("too old ViewID: %d, latestStateVersionID=%d", id, latestID)
	}
	isLatest := c.latestStateVersionID == id
	c.rootMu.Unlock()

	// Shard-local lookup (no global contention)
	si := c.shardIndex(k)
	shard := &r.shards[si]

	shard.mu.Lock()
	var it *Element
	if domain == kv.CodeDomain {
		it, _ = shard.codeCache.Get(&Element{K: k})
		if it != nil && isLatest {
			shard.codeEvict.MoveToFront(it)
		}
	} else {
		it, _ = shard.cache.Get(&Element{K: k})
		if it != nil && isLatest {
			shard.stateEvict.MoveToFront(it)
		}
	}
	shard.mu.Unlock()

	return it, r, nil
}

func (c *Coherent) Get(k []byte, tx kv.TemporalTx, id uint64) (v []byte, err error) {
	it, r, err := c.getFromCache(k, id, kv.AccountsDomain)
	if err != nil {
		return nil, err
	}

	if it != nil {
		c.hits.Inc()
		return it.V, nil
	}

	c.miss.Inc()

	if len(k) == 20 {
		v, _, err = tx.GetLatest(kv.AccountsDomain, k)
	} else {
		v, _, err = tx.GetLatest(kv.StorageDomain, k)
	}
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return v, nil
	}

	c.rootMu.Lock()
	latestID := c.latestStateVersionID
	c.rootMu.Unlock()

	v = c.add(common.Copy(k), common.Copy(v), r, id, latestID).V
	return v, nil
}

func (c *Coherent) GetCode(k []byte, tx kv.TemporalTx, id uint64) (v []byte, err error) {
	it, r, err := c.getFromCache(k, id, kv.CodeDomain)
	if err != nil {
		return nil, err
	}
	if it != nil {
		c.codeHits.Inc()
		return it.V, nil
	}

	c.codeMiss.Inc()
	v, _, err = tx.GetLatest(kv.CodeDomain, k)
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return v, nil
	}

	c.rootMu.Lock()
	latestID := c.latestStateVersionID
	c.rootMu.Unlock()

	v = c.addCode(common.Copy(k), common.Copy(v), r, id, latestID).V
	return v, nil
}

func (c *Coherent) add(k, v []byte, r *CoherentRoot, id, latestID uint64) *Element {
	si := c.shardIndex(k)
	shard := &r.shards[si]

	it := &Element{K: k, V: v}

	shard.mu.Lock()
	replaced, _ := shard.cache.Set(it)
	if latestID != id {
		shard.mu.Unlock()
		return it
	}
	if replaced != nil {
		shard.stateEvict.Remove(replaced)
	}
	shard.stateEvict.PushFront(it)

	budget := c.stateBudgetPerShard()
	for shard.stateEvict.Size() > budget {
		e := shard.stateEvict.Back()
		if e != nil {
			shard.stateEvict.Remove(e)
			shard.cache.Delete(e)
		}
	}
	shard.mu.Unlock()
	return it
}

func (c *Coherent) addCode(k, v []byte, r *CoherentRoot, id, latestID uint64) *Element {
	si := c.shardIndex(k)
	shard := &r.shards[si]

	it := &Element{K: k, V: v}

	shard.mu.Lock()
	replaced, _ := shard.codeCache.Set(it)
	if latestID != id {
		shard.mu.Unlock()
		return it
	}
	if replaced != nil {
		shard.codeEvict.Remove(replaced)
	}
	shard.codeEvict.PushFront(it)

	budget := c.codeBudgetPerShard()
	for shard.codeEvict.Size() > budget {
		e := shard.codeEvict.Back()
		if e != nil {
			shard.codeEvict.Remove(e)
			shard.codeCache.Delete(e)
		}
	}
	shard.mu.Unlock()
	return it
}

func (c *Coherent) ValidateCurrentRoot(ctx context.Context, tx kv.TemporalTx) (*CacheValidationResult, error) {

	result := &CacheValidationResult{
		Enabled:          true,
		RequestCancelled: false,
	}

	select {
	case <-ctx.Done():
		result.RequestCancelled = true
		return result, nil
	default:
	}

	stateID, err := tx.ReadSequence(string(kv.PlainStateVersion))
	if err != nil {
		return nil, err
	}

	result.LatestStateID = stateID

	c.rootMu.Lock()
	latestID := c.latestStateVersionID
	c.rootMu.Unlock()

	if stateID > latestID {
		result.LatestStateBehind = true
		return result, nil
	}

	root := c.selectOrCreateRoot(latestID)

	// ensure the root is ready or wait and press on
	select {
	case <-root.ready:
	case <-time.After(c.cfg.NewBlockWait):
	}

	// check context again after potentially waiting for root to be ready
	select {
	case <-ctx.Done():
		result.RequestCancelled = true
		return result, nil
	default:
	}

	clearCache := false

	compare := func(cache *btree2.BTreeG[*Element], domain kv.Domain) (bool, [][]byte, error) {
		keys := make([][]byte, 0)

		for {
			val, ok := cache.PopMax()
			if !ok {
				break
			}

			// check the db
			inDb, _, err := tx.GetLatest(domain, val.K)
			if err != nil {
				return false, keys, err
			}

			if !bytes.Equal(inDb, val.V) {
				keys = append(keys, val.K)
				clearCache = true
			}

			select {
			case <-ctx.Done():
				return true, keys, nil
			default:
			}
		}

		return false, keys, nil
	}

	stateCaches, codeCaches := c.cloneCaches(root)

	// Merge all state shard clones into one for comparison
	mergedState := btree2.NewBTreeG[*Element](Less)
	for _, sc := range stateCaches {
		sc.Walk(func(items []*Element) bool {
			for _, item := range items {
				mergedState.Set(item)
			}
			return true
		})
	}

	mergedCode := btree2.NewBTreeG[*Element](Less)
	for _, cc := range codeCaches {
		cc.Walk(func(items []*Element) bool {
			for _, item := range items {
				mergedCode.Set(item)
			}
			return true
		})
	}

	cancelled, keys, err := compare(mergedState, kv.AccountsDomain)
	if err != nil {
		return nil, err
	}
	result.StateKeysOutOfSync = keys
	if cancelled {
		result.RequestCancelled = true
		return result, nil
	}

	// Note: the original code called compare twice on `cache` for accounts and storage.
	// We preserve the same behavior.
	mergedState2 := btree2.NewBTreeG[*Element](Less)
	for _, sc := range stateCaches {
		sc.Walk(func(items []*Element) bool {
			for _, item := range items {
				mergedState2.Set(item)
			}
			return true
		})
	}
	cancelled, keys, err = compare(mergedState2, kv.StorageDomain)
	if err != nil {
		return nil, err
	}
	result.StateKeysOutOfSync = keys
	if cancelled {
		result.RequestCancelled = true
		return result, nil
	}

	cancelled, keys, err = compare(mergedCode, kv.CodeDomain)
	if err != nil {
		return nil, err
	}
	result.CodeKeysOutOfSync = keys
	if cancelled {
		result.RequestCancelled = true
		return result, nil
	}

	if clearCache {
		c.clearCaches(root)
	}
	result.CacheCleared = clearCache

	return result, nil
}

func (c *Coherent) cloneCaches(r *CoherentRoot) (stateCaches, codeCaches []*btree2.BTreeG[*Element]) {
	stateCaches = make([]*btree2.BTreeG[*Element], c.numShards)
	codeCaches = make([]*btree2.BTreeG[*Element], c.numShards)
	for i := uint32(0); i < c.numShards; i++ {
		r.shards[i].mu.Lock()
		stateCaches[i] = r.shards[i].cache.Copy()
		codeCaches[i] = r.shards[i].codeCache.Copy()
		r.shards[i].mu.Unlock()
	}
	return
}

func (c *Coherent) clearCaches(r *CoherentRoot) {
	for i := range r.shards {
		r.shards[i].mu.Lock()
		r.shards[i].cache.Clear()
		r.shards[i].codeCache.Clear()
		r.shards[i].mu.Unlock()
	}
}

type Stat struct {
	BlockNum  uint64
	BlockHash [32]byte
	Lenght    int
}

func DebugStats(cache Cache) []Stat {
	res := []Stat{}
	casted, ok := cache.(*Coherent)
	if !ok {
		return res
	}
	casted.rootMu.Lock()
	for root, r := range casted.roots {
		totalLen := 0
		for i := range r.shards {
			totalLen += r.shards[i].cache.Len()
		}
		res = append(res, Stat{
			BlockNum: root,
			Lenght:   totalLen,
		})
	}
	casted.rootMu.Unlock()
	sort.Slice(res, func(i, j int) bool { return res[i].BlockNum < res[j].BlockNum })
	return res
}

func AssertCheckValues(ctx context.Context, tx kv.TemporalTx, cache Cache) (int, error) {
	defer func(t time.Time) { fmt.Printf("AssertCheckValues:327: %s\n", time.Since(t)) }(time.Now())
	view, err := cache.View(ctx, tx)
	if err != nil {
		return 0, err
	}
	castedView, ok := view.(*CoherentView)
	if !ok {
		return 0, nil
	}
	casted, ok := cache.(*Coherent)
	if !ok {
		return 0, nil
	}
	checked := 0
	casted.rootMu.Lock()
	root, ok := casted.roots[castedView.stateVersionID]
	casted.rootMu.Unlock()
	if !ok {
		return 0, nil
	}
	for si := range root.shards {
		root.shards[si].mu.Lock()
		root.shards[si].cache.Walk(func(items []*Element) bool {
			for _, item := range items {
				k, v := item.K, item.V
				var dbV []byte
				dbV, err = tx.GetOne(kv.PlainState, k)
				if err != nil {
					return false
				}
				if !bytes.Equal(dbV, v) {
					err = fmt.Errorf("key: %x, has different values: %x != %x", k, v, dbV)
					return false
				}
				checked++
			}
			return true
		})
		root.shards[si].mu.Unlock()
		if err != nil {
			return checked, err
		}
	}
	return checked, err
}

func (c *Coherent) evictRoots() {
	if c.latestStateVersionID <= c.cfg.KeepViews {
		return
	}
	if len(c.roots) < int(c.cfg.KeepViews) {
		return
	}
	to := c.latestStateVersionID - c.cfg.KeepViews
	toDel := make([]uint64, 0, len(c.roots))
	for txID := range c.roots {
		if txID > to {
			continue
		}
		toDel = append(toDel, txID)
	}
	for _, txID := range toDel {
		delete(c.roots, txID)
	}
}

func (c *Coherent) Len() int {
	c.rootMu.Lock()
	r := c.latestStateView
	c.rootMu.Unlock()
	if r == nil {
		return 0
	}
	return c.latestStateViewLen()
}

// Test helpers (unexported, for use in tests within this package)

func (c *Coherent) totalStateEvictLen() int {
	c.rootMu.Lock()
	r := c.latestStateView
	c.rootMu.Unlock()
	if r == nil {
		return 0
	}
	total := 0
	for i := range r.shards {
		total += r.shards[i].stateEvict.Len()
	}
	return total
}

func (c *Coherent) totalStateEvictSize() int {
	c.rootMu.Lock()
	r := c.latestStateView
	c.rootMu.Unlock()
	if r == nil {
		return 0
	}
	total := 0
	for i := range r.shards {
		total += r.shards[i].stateEvict.Size()
	}
	return total
}

func (c *Coherent) totalCodeEvictLen() int {
	c.rootMu.Lock()
	r := c.latestStateView
	c.rootMu.Unlock()
	if r == nil {
		return 0
	}
	total := 0
	for i := range r.shards {
		total += r.shards[i].codeEvict.Len()
	}
	return total
}

func (c *Coherent) latestStateViewLen() int {
	c.rootMu.Lock()
	r := c.latestStateView
	c.rootMu.Unlock()
	if r == nil {
		return 0
	}
	total := 0
	for i := range r.shards {
		total += r.shards[i].cache.Len()
	}
	return total
}

// Element is an element of a linked list.
type Element struct {
	// Next and previous pointers in the doubly-linked list of elements.
	// To simplify the implementation, internally a list l is implemented
	// as a ring, such that &l.root is both the next element of the last
	// list element (l.Back()) and the previous element of the first list
	// element (l.Front()).
	next, prev *Element

	// The list to which this element belongs.
	list *List

	// The value stored with this element.
	K, V []byte
}

func (e *Element) Size() int { return len(e.K) + len(e.V) }

func Less(a, b *Element) bool { return bytes.Compare(a.K, b.K) < 0 }

// ========= copypaste of List implementation from stdlib ========

// Next returns the next list element or nil.
func (e *Element) Next() *Element {
	if p := e.next; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

// Prev returns the previous list element or nil.
func (e *Element) Prev() *Element {
	if p := e.prev; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

// List represents a doubly linked list.
// The zero value for List is an empty list ready to use.
type List struct {
	root Element // sentinel list element, only &root, root.prev, and root.next are used
	len  int     // current list length excluding (this) sentinel element
	size int     // size of items in list in bytes
}

// Init initializes or clears list l.
func (l *List) Init() *List {
	l.root.next = &l.root
	l.root.prev = &l.root
	l.len = 0
	l.size = 0
	return l
}

// New returns an initialized list.
func NewList() *List { return new(List).Init() }

// Len returns the number of elements of list l.
// The complexity is O(1).
func (l *List) Len() int { return l.len }

// Size returns the size of the elements in the list by bytes
func (l *List) Size() int { return l.size }

// Front returns the first element of list l or nil if the list is empty.
func (l *List) Front() *Element {
	if l.len == 0 {
		return nil
	}
	return l.root.next
}

// Back returns the last element of list l or nil if the list is empty.
func (l *List) Back() *Element {
	if l.len == 0 {
		return nil
	}
	return l.root.prev
}

// lazyInit lazily initializes a zero List value.
func (l *List) lazyInit() {
	if l.root.next == nil {
		l.Init()
	}
}

// insert inserts e after at, increments l.len, and returns e.
func (l *List) insert(e, at *Element) *Element {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	e.list = l
	l.len++
	l.size += e.Size()
	return e
}

// insertValue is a convenience wrapper for insert(&Element{Value: v}, at).
func (l *List) insertValue(e, at *Element) *Element {
	return l.insert(e, at)
}

// remove removes e from its list, decrements l.len, and returns e.
func (l *List) remove(e *Element) *Element {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil // avoid memory leaks
	e.prev = nil // avoid memory leaks
	e.list = nil
	l.len--
	l.size -= e.Size()
	return e
}

// move moves e to next to at and returns e.
func (l *List) move(e, at *Element) *Element {
	if e == at {
		return e
	}
	e.prev.next = e.next
	e.next.prev = e.prev

	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e

	return e
}

// Remove removes e from l if e is an element of list l.
// It returns the element value e.Value.
// The element must not be nil.
func (l *List) Remove(e *Element) ([]byte, []byte) {
	if e.list == l {
		// if e.list == l, l must have been initialized when e was inserted
		// in l or l == nil (e is a zero Element) and l.remove will crash
		l.remove(e)
	}
	return e.K, e.V
}

// PushFront inserts a new element e with value v at the front of list l and returns e.
func (l *List) PushFront(e *Element) *Element {
	l.lazyInit()
	return l.insertValue(e, &l.root)
}

// PushBack inserts a new element e with value v at the back of list l and returns e.
func (l *List) PushBack(e *Element) *Element {
	l.lazyInit()
	return l.insertValue(e, l.root.prev)
}

// InsertBefore inserts a new element e with value v immediately before mark and returns e.
// If mark is not an element of l, the list is not modified.
// The mark must not be nil.
func (l *List) InsertBefore(e *Element, mark *Element) *Element {
	if mark.list != l {
		return nil
	}
	// see comment in List.Remove about initialization of l
	return l.insertValue(e, mark.prev)
}

// InsertAfter inserts a new element e with value v immediately after mark and returns e.
// If mark is not an element of l, the list is not modified.
// The mark must not be nil.
func (l *List) InsertAfter(e *Element, mark *Element) *Element {
	if mark.list != l {
		return nil
	}
	// see comment in List.Remove about initialization of l
	return l.insertValue(e, mark)
}

// MoveToFront moves element e to the front of list l.
// If e is not an element of l, the list is not modified.
// The element must not be nil.
func (l *List) MoveToFront(e *Element) {
	if e.list != l || l.root.next == e {
		return
	}
	// see comment in List.Remove about initialization of l
	l.move(e, &l.root)
}

// MoveToBack moves element e to the back of list l.
// If e is not an element of l, the list is not modified.
// The element must not be nil.
func (l *List) MoveToBack(e *Element) {
	if e.list != l || l.root.prev == e {
		return
	}
	// see comment in List.Remove about initialization of l
	l.move(e, l.root.prev)
}

// MoveBefore moves element e to its new position before mark.
// If e or mark is not an element of l, or e == mark, the list is not modified.
// The element and mark must not be nil.
func (l *List) MoveBefore(e, mark *Element) {
	if e.list != l || e == mark || mark.list != l {
		return
	}
	l.move(e, mark.prev)
}

// MoveAfter moves element e to its new position after mark.
// If e or mark is not an element of l, or e == mark, the list is not modified.
// The element and mark must not be nil.
func (l *List) MoveAfter(e, mark *Element) {
	if e.list != l || e == mark || mark.list != l {
		return
	}
	l.move(e, mark)
}
