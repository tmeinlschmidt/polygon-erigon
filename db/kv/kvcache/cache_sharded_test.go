package kvcache

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
)

// --- Shard Distribution Tests ---

func TestShardIndex_Deterministic(t *testing.T) {
	cfg := DefaultCoherentConfig
	c := New(cfg)

	key := []byte{0xAB, 0xCD, 0xEF}
	si1 := c.shardIndex(key)
	si2 := c.shardIndex(key)
	require.Equal(t, si1, si2, "same key must always map to same shard")
}

func TestShardIndex_Distribution(t *testing.T) {
	cfg := DefaultCoherentConfig
	c := New(cfg)

	seen := make(map[uint32]bool)
	// Generate keys with first bytes 0..255
	for i := 0; i < 256; i++ {
		key := []byte{byte(i), 0x00, 0x00}
		si := c.shardIndex(key)
		require.Less(t, si, c.numShards)
		seen[si] = true
	}
	// All shards should be hit
	require.Equal(t, int(c.numShards), len(seen), "all shards should be used")
}

func TestShardIndex_EmptyKey(t *testing.T) {
	cfg := DefaultCoherentConfig
	c := New(cfg)

	require.Equal(t, uint32(0), c.shardIndex(nil))
	require.Equal(t, uint32(0), c.shardIndex([]byte{}))
}

// --- Sharded Eviction Tests ---

func TestShardedEviction_PerShard(t *testing.T) {
	cfg := DefaultCoherentConfig
	cfg.CacheSize = 16 * 20 // 20 bytes per shard with 16 shards
	cfg.NewBlockWait = 0
	c := New(cfg)

	// Advance to root 1
	c.rootMu.Lock()
	r := c.advanceRoot(1)
	latestID := c.latestStateVersionID
	c.rootMu.Unlock()

	// Add keys that map to shard 0 (first byte 0x00, 0x10)
	// Key=5 bytes + Val=1 byte = 6 bytes each. Two items = 12 bytes. Budget = 20. Both fit.
	c.add([]byte{0x00, 1, 2, 3, 4}, []byte{1}, r, 1, latestID)
	c.add([]byte{0x10, 1, 2, 3, 4}, []byte{2}, r, 1, latestID) // same shard (0x00 & 0xF = 0, 0x10 & 0xF = 0)

	// Add a key to a different shard (first byte 0x01)
	c.add([]byte{0x01, 1, 2, 3, 4}, []byte{3}, r, 1, latestID)

	// Shard 0 should have 2 elements, shard 1 should have 1
	shard0 := &r.shards[0]
	shard1 := &r.shards[1]

	shard0.mu.Lock()
	s0Len := shard0.cache.Len()
	shard0.mu.Unlock()

	shard1.mu.Lock()
	s1Len := shard1.cache.Len()
	shard1.mu.Unlock()

	require.Equal(t, 2, s0Len)
	require.Equal(t, 1, s1Len)
}

func TestShardedEviction_BudgetDivision(t *testing.T) {
	cfg := DefaultCoherentConfig
	cfg.CacheSize = 16 * 5 // 5 bytes per shard
	cfg.NewBlockWait = 0
	c := New(cfg)

	c.rootMu.Lock()
	r := c.advanceRoot(1)
	latestID := c.latestStateVersionID
	c.rootMu.Unlock()

	// Add multiple items to shard 0 that exceed budget
	// Key=1 byte + Val=3 bytes = 4 bytes each. Budget=5, so max 1 item
	c.add([]byte{0x00}, []byte{1, 2, 3}, r, 1, latestID) // 4 bytes
	c.add([]byte{0x10}, []byte{4, 5, 6}, r, 1, latestID) // 4 bytes, exceeds budget of 5

	shard0 := &r.shards[0]
	shard0.mu.Lock()
	s0Len := shard0.cache.Len()
	s0EvictSize := shard0.stateEvict.Size()
	shard0.mu.Unlock()

	// Only 1 item should remain (the newer one, 0x10)
	require.Equal(t, 1, s0Len)
	require.LessOrEqual(t, s0EvictSize, 5)
}

// --- Sharded COW Tests ---

func TestShardedCOW_ClonePreservesOldRoot(t *testing.T) {
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)

	// Create root 1 with a value
	c.rootMu.Lock()
	r1 := c.advanceRoot(1)
	c.rootMu.Unlock()

	c.add([]byte{0x01, 0x02}, []byte{10, 20}, r1, 1, 1)

	// Advance to root 2 (COW clone)
	c.rootMu.Lock()
	r2 := c.advanceRoot(2)
	c.rootMu.Unlock()

	// Modify value in root 2
	c.add([]byte{0x01, 0x02}, []byte{30, 40}, r2, 2, 2)

	// Root 1 should still have old value
	si := c.shardIndex([]byte{0x01, 0x02})
	r1.shards[si].mu.Lock()
	it, ok := r1.shards[si].cache.Get(&Element{K: []byte{0x01, 0x02}})
	r1.shards[si].mu.Unlock()

	require.True(t, ok)
	require.Equal(t, []byte{10, 20}, it.V)

	// Root 2 should have new value
	r2.shards[si].mu.Lock()
	it2, ok2 := r2.shards[si].cache.Get(&Element{K: []byte{0x01, 0x02}})
	r2.shards[si].mu.Unlock()

	require.True(t, ok2)
	require.Equal(t, []byte{30, 40}, it2.V)
}

func TestShardedCOW_NewRootGetsUpdates(t *testing.T) {
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)

	k1 := [20]byte{1}
	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId: 1,
		ChangeBatch: []*remoteproto.StateChange{
			{
				Direction: remoteproto.Direction_FORWARD,
				Changes: []*remoteproto.AccountChange{{
					Action:  remoteproto.Action_UPSERT,
					Address: gointerfaces.ConvertAddressToH160(k1),
					Data:    []byte{1, 2, 3},
				}},
			},
		},
	})

	// Verify value is in root 1
	c.rootMu.Lock()
	r := c.roots[1]
	c.rootMu.Unlock()

	si := c.shardIndex(k1[:])
	r.shards[si].mu.Lock()
	it, ok := r.shards[si].cache.Get(&Element{K: k1[:]})
	r.shards[si].mu.Unlock()

	require.True(t, ok)
	require.Equal(t, []byte{1, 2, 3}, it.V)
}

// --- Sharded OnNewBlock Tests ---

func TestShardedOnNewBlock_MultiShard(t *testing.T) {
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)

	// Create changes that hit different shards
	k1 := [20]byte{0x01} // shard 1
	k2 := [20]byte{0x02} // shard 2

	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId: 1,
		ChangeBatch: []*remoteproto.StateChange{
			{
				Direction: remoteproto.Direction_FORWARD,
				Changes: []*remoteproto.AccountChange{
					{
						Action:  remoteproto.Action_UPSERT,
						Address: gointerfaces.ConvertAddressToH160(k1),
						Data:    []byte{10},
					},
					{
						Action:  remoteproto.Action_UPSERT,
						Address: gointerfaces.ConvertAddressToH160(k2),
						Data:    []byte{20},
					},
				},
			},
		},
	})

	c.rootMu.Lock()
	r := c.roots[1]
	c.rootMu.Unlock()

	// Check shard for k1
	si1 := c.shardIndex(k1[:])
	r.shards[si1].mu.Lock()
	it1, ok1 := r.shards[si1].cache.Get(&Element{K: k1[:]})
	r.shards[si1].mu.Unlock()
	require.True(t, ok1)
	require.Equal(t, []byte{10}, it1.V)

	// Check shard for k2
	si2 := c.shardIndex(k2[:])
	r.shards[si2].mu.Lock()
	it2, ok2 := r.shards[si2].cache.Get(&Element{K: k2[:]})
	r.shards[si2].mu.Unlock()
	require.True(t, ok2)
	require.Equal(t, []byte{20}, it2.V)
}

func TestShardedOnNewBlock_GetReturnsFreshValues(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)

	k1 := [20]byte{1}

	var id uint64
	_ = db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		_ = tx.Put(kv.PlainState, k1[:], []byte{1})
		id = tx.ViewID()
		var versionID [8]byte
		binary.BigEndian.PutUint64(versionID[:], id)
		_ = tx.Put(kv.Sequence, kv.PlainStateVersion, versionID[:])
		return nil
	})

	// OnNewBlock with value {2}
	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId: id,
		ChangeBatch: []*remoteproto.StateChange{
			{
				Direction: remoteproto.Direction_FORWARD,
				Changes: []*remoteproto.AccountChange{{
					Action:  remoteproto.Action_UPSERT,
					Address: gointerfaces.ConvertAddressToH160(k1),
					Data:    []byte{2},
				}},
			},
		},
	})

	// Get should return cached value {2}
	_ = db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		view, err := c.View(ctx, tx)
		require.NoError(t, err)
		v, err := view.Get(k1[:])
		require.NoError(t, err)
		require.Equal(t, []byte{2}, v)
		return nil
	})
}

// --- Concurrency Tests ---

func TestConcurrent_GetDifferentShards(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)

	// Write some data and advance the cache
	var id uint64
	_ = db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		for i := 0; i < 16; i++ {
			k := [20]byte{byte(i)}
			_ = tx.Put(kv.PlainState, k[:], []byte{byte(i + 1)})
		}
		id = tx.ViewID()
		var versionID [8]byte
		binary.BigEndian.PutUint64(versionID[:], id)
		_ = tx.Put(kv.Sequence, kv.PlainStateVersion, versionID[:])
		return nil
	})

	changes := make([]*remoteproto.AccountChange, 16)
	for i := 0; i < 16; i++ {
		k := [20]byte{byte(i)}
		changes[i] = &remoteproto.AccountChange{
			Action:  remoteproto.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(k),
			Data:    []byte{byte(i + 1)},
		}
	}
	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId: id,
		ChangeBatch:    []*remoteproto.StateChange{{Direction: remoteproto.Direction_FORWARD, Changes: changes}},
	})

	// Concurrent reads from different shards
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
				view, err := c.View(ctx, tx)
				if err != nil {
					t.Errorf("View error: %v", err)
					return nil
				}
				k := [20]byte{byte(idx)}
				v, err := view.Get(k[:])
				if err != nil {
					t.Errorf("Get error: %v", err)
					return nil
				}
				if len(v) != 1 || v[0] != byte(idx+1) {
					t.Errorf("unexpected value for key %d: %v", idx, v)
				}
				return nil
			})
		}(i)
	}
	wg.Wait()
}

func TestConcurrent_GetSameShard(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)

	k1 := [20]byte{0x00} // all in shard 0

	var id uint64
	_ = db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		_ = tx.Put(kv.PlainState, k1[:], []byte{42})
		id = tx.ViewID()
		var versionID [8]byte
		binary.BigEndian.PutUint64(versionID[:], id)
		_ = tx.Put(kv.Sequence, kv.PlainStateVersion, versionID[:])
		return nil
	})

	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId: id,
		ChangeBatch: []*remoteproto.StateChange{{
			Direction: remoteproto.Direction_FORWARD,
			Changes: []*remoteproto.AccountChange{{
				Action:  remoteproto.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160(k1),
				Data:    []byte{42},
			}},
		}},
	})

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
				view, err := c.View(ctx, tx)
				if err != nil {
					t.Errorf("View error: %v", err)
					return nil
				}
				v, err := view.Get(k1[:])
				if err != nil {
					t.Errorf("Get error: %v", err)
					return nil
				}
				if len(v) != 1 || v[0] != 42 {
					t.Errorf("unexpected value: %v", v)
				}
				return nil
			})
		}()
	}
	wg.Wait()
}

func TestConcurrent_GetWithOnNewBlock(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)

	k1 := [20]byte{1}

	var id uint64
	_ = db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		_ = tx.Put(kv.PlainState, k1[:], []byte{1})
		id = tx.ViewID()
		var versionID [8]byte
		binary.BigEndian.PutUint64(versionID[:], id)
		_ = tx.Put(kv.Sequence, kv.PlainStateVersion, versionID[:])
		return nil
	})

	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId: id,
		ChangeBatch: []*remoteproto.StateChange{{
			Direction: remoteproto.Direction_FORWARD,
			Changes: []*remoteproto.AccountChange{{
				Action:  remoteproto.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160(k1),
				Data:    []byte{1},
			}},
		}},
	})

	var wg sync.WaitGroup

	// Readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
					view, err := c.View(ctx, tx)
					if err != nil {
						return nil // view might not exist yet
					}
					_, _ = view.Get(k1[:])
					return nil
				})
			}
		}()
	}

	// Writer (OnNewBlock)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := uint64(1); j <= 50; j++ {
			c.OnNewBlock(&remoteproto.StateChangeBatch{
				StateVersionId: id + j,
				ChangeBatch: []*remoteproto.StateChange{{
					Direction: remoteproto.Direction_FORWARD,
					Changes: []*remoteproto.AccountChange{{
						Action:  remoteproto.Action_UPSERT,
						Address: gointerfaces.ConvertAddressToH160(k1),
						Data:    []byte{byte(j)},
					}},
				}},
			})
		}
	}()

	wg.Wait()
}

// --- Benchmarks ---

func BenchmarkGet_Hit(b *testing.B) {
	ctx := context.Background()
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)

	dirs := datadir.New(b.TempDir())
	db := temporaltest.NewTestDB(b, dirs)

	k1 := [20]byte{1}
	var id uint64
	_ = db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		_ = tx.Put(kv.PlainState, k1[:], []byte{1, 2, 3})
		id = tx.ViewID()
		var versionID [8]byte
		binary.BigEndian.PutUint64(versionID[:], id)
		_ = tx.Put(kv.Sequence, kv.PlainStateVersion, versionID[:])
		return nil
	})

	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId: id,
		ChangeBatch: []*remoteproto.StateChange{{
			Direction: remoteproto.Direction_FORWARD,
			Changes: []*remoteproto.AccountChange{{
				Action:  remoteproto.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160(k1),
				Data:    []byte{1, 2, 3},
			}},
		}},
	})

	_ = db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		view, _ := c.View(ctx, tx)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = view.Get(k1[:])
		}
		return nil
	})
}

func BenchmarkGet_Hit_Parallel(b *testing.B) {
	ctx := context.Background()
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)

	dirs := datadir.New(b.TempDir())
	db := temporaltest.NewTestDB(b, dirs)

	// Create 16 different keys for different shards
	keys := make([][20]byte, 16)
	changes := make([]*remoteproto.AccountChange, 16)
	for i := 0; i < 16; i++ {
		keys[i] = [20]byte{byte(i)}
		changes[i] = &remoteproto.AccountChange{
			Action:  remoteproto.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(keys[i]),
			Data:    []byte{byte(i + 1)},
		}
	}

	var id uint64
	_ = db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		for i := 0; i < 16; i++ {
			_ = tx.Put(kv.PlainState, keys[i][:], []byte{byte(i + 1)})
		}
		id = tx.ViewID()
		var versionID [8]byte
		binary.BigEndian.PutUint64(versionID[:], id)
		_ = tx.Put(kv.Sequence, kv.PlainStateVersion, versionID[:])
		return nil
	})

	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId: id,
		ChangeBatch:    []*remoteproto.StateChange{{Direction: remoteproto.Direction_FORWARD, Changes: changes}},
	})

	_ = db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		view, _ := c.View(ctx, tx)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				_, _ = view.Get(keys[i%16][:])
				i++
			}
		})
		return nil
	})
}

func BenchmarkOnNewBlock(b *testing.B) {
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)

	// Pre-create the state change batch with 100 changes
	changes := make([]*remoteproto.AccountChange, 100)
	for i := 0; i < 100; i++ {
		k := [20]byte{byte(i)}
		changes[i] = &remoteproto.AccountChange{
			Action:  remoteproto.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(k),
			Data:    []byte{byte(i)},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.OnNewBlock(&remoteproto.StateChangeBatch{
			StateVersionId: uint64(i + 1),
			ChangeBatch: []*remoteproto.StateChange{{
				Direction: remoteproto.Direction_FORWARD,
				Changes:   changes,
			}},
		})
	}
}

func BenchmarkGet_Miss(b *testing.B) {
	ctx := context.Background()
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)

	dirs := datadir.New(b.TempDir())
	db := temporaltest.NewTestDB(b, dirs)

	var id uint64
	_ = db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		for i := 0; i < 1000; i++ {
			k := [20]byte{byte(i >> 8), byte(i)}
			_ = tx.Put(kv.PlainState, k[:], []byte{byte(i)})
		}
		id = tx.ViewID()
		var versionID [8]byte
		binary.BigEndian.PutUint64(versionID[:], id)
		_ = tx.Put(kv.Sequence, kv.PlainStateVersion, versionID[:])
		return nil
	})

	// Advance root so cache is ready
	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId: id,
		ChangeBatch: []*remoteproto.StateChange{{
			Direction: remoteproto.Direction_FORWARD,
			Changes: []*remoteproto.AccountChange{{
				Action:  remoteproto.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160([20]byte{0xFF}),
				Data:    []byte{1},
			}},
		}},
	})

	_ = db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		view, _ := c.View(ctx, tx)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := [20]byte{byte(i >> 8), byte(i)}
			_, _ = view.Get(k[:])
		}
		return nil
	})
}

// BenchmarkShardIndex measures shard selection overhead
func BenchmarkShardIndex(b *testing.B) {
	cfg := DefaultCoherentConfig
	c := New(cfg)
	k := [20]byte{0xAB, 0xCD}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.shardIndex(k[:])
	}
}

func init() {
	// Suppress fmt.Println in tests
	_ = fmt.Sprintf("")
}
