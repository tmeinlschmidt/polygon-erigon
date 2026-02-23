package kvcache

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
)

func BenchmarkCacheGetHit(b *testing.B) {
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

func BenchmarkCacheGetHitParallel(b *testing.B) {
	ctx := context.Background()
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)

	dirs := datadir.New(b.TempDir())
	db := temporaltest.NewTestDB(b, dirs)

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

func BenchmarkCacheOnNewBlock(b *testing.B) {
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)

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
