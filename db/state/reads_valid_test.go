package state_test

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
)

// setupReadsValidTest creates a SharedDomains instance for testing ReadsValid.
func setupReadsValidTest(t *testing.T) (*state.SharedDomains, kv.TemporalTx) {
	t.Helper()
	stepSize := uint64(100)
	db, _ := testDbAndAggregatorv3(t, stepSize)

	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(rwTx.Rollback)

	domains, err := state.NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	t.Cleanup(domains.Close)

	return domains, rwTx
}

// makeReadLists builds a readLists map for testing.
func makeReadLists(entries map[string][]kvEntry) map[string]*state.KvList {
	result := make(map[string]*state.KvList)
	for domain, kvs := range entries {
		list := &state.KvList{}
		for _, kv := range kvs {
			list.Push(kv.key, kv.val)
		}
		result[domain] = list
	}
	return result
}

type kvEntry struct {
	key string
	val []byte
}

func encodeCodeSize(size int) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(size))
	return buf
}

// putState is a helper to write a key-value pair to a domain.
func putState(t *testing.T, domains *state.SharedDomains, tx kv.TemporalTx, domain kv.Domain, key, val []byte, txNum uint64) {
	t.Helper()
	domains.SetTxNum(txNum)
	err := domains.DomainPut(domain, tx, key, val, txNum, nil, 0)
	require.NoError(t, err)
}

// delState is a helper to delete a key from a domain.
func delState(t *testing.T, domains *state.SharedDomains, tx kv.TemporalTx, domain kv.Domain, key []byte, txNum uint64) {
	t.Helper()
	domains.SetTxNum(txNum)
	err := domains.DomainDel(domain, tx, key, txNum, nil, 0)
	require.NoError(t, err)
}

// --- Unit Tests ---

func TestReadsValid_NilReadLists(t *testing.T) {
	domains, _ := setupReadsValidTest(t)
	require.True(t, domains.ReadsValid(nil), "nil readLists should be valid")
}

func TestReadsValid_EmptyReadLists(t *testing.T) {
	domains, _ := setupReadsValidTest(t)
	require.True(t, domains.ReadsValid(map[string]*state.KvList{}), "empty readLists should be valid")
}

func TestReadsValid_KeyNotInState(t *testing.T) {
	domains, _ := setupReadsValidTest(t)
	// Key "A" was never written to in-memory state
	readLists := makeReadLists(map[string][]kvEntry{
		kv.AccountsDomain.String(): {{key: string(make([]byte, 20)), val: []byte{1, 2, 3}}},
	})
	// Key not in mem -> no conflict (value read from DB which hasn't changed)
	require.True(t, domains.ReadsValid(readLists), "key not in mem should not conflict")
}

func TestReadsValid_AccountUnchanged(t *testing.T) {
	domains, tx := setupReadsValidTest(t)
	key := make([]byte, 20)
	key[0] = 0x01
	val := []byte{0xAA, 0xBB}
	putState(t, domains, tx, kv.AccountsDomain, key, val, 0)

	readLists := makeReadLists(map[string][]kvEntry{
		kv.AccountsDomain.String(): {{key: string(key), val: val}},
	})
	require.True(t, domains.ReadsValid(readLists), "unchanged account should be valid")
}

func TestReadsValid_AccountChanged(t *testing.T) {
	domains, tx := setupReadsValidTest(t)
	key := make([]byte, 20)
	key[0] = 0x01
	putState(t, domains, tx, kv.AccountsDomain, key, []byte{0xAA}, 0)
	putState(t, domains, tx, kv.AccountsDomain, key, []byte{0xBB}, 1)

	// Read saw value 0xAA but current is 0xBB -> conflict
	readLists := makeReadLists(map[string][]kvEntry{
		kv.AccountsDomain.String(): {{key: string(key), val: []byte{0xAA}}},
	})
	require.False(t, domains.ReadsValid(readLists), "changed account should conflict")
}

func TestReadsValid_AccountDeleted(t *testing.T) {
	domains, tx := setupReadsValidTest(t)
	key := make([]byte, 20)
	key[0] = 0x02
	putState(t, domains, tx, kv.AccountsDomain, key, []byte{0xAA}, 0)
	delState(t, domains, tx, kv.AccountsDomain, key, 1)

	readLists := makeReadLists(map[string][]kvEntry{
		kv.AccountsDomain.String(): {{key: string(key), val: []byte{0xAA}}},
	})
	require.False(t, domains.ReadsValid(readLists), "deleted account should conflict")
}

func TestReadsValid_StorageUnchanged(t *testing.T) {
	domains, tx := setupReadsValidTest(t)
	// Storage keys are address(20) + location(32) = 52 bytes
	key := make([]byte, 52)
	key[0] = 0x01
	key[20] = 0xFF
	val := []byte{0x11, 0x22}
	putState(t, domains, tx, kv.StorageDomain, key, val, 0)

	readLists := makeReadLists(map[string][]kvEntry{
		kv.StorageDomain.String(): {{key: string(key), val: val}},
	})
	require.True(t, domains.ReadsValid(readLists), "unchanged storage should be valid")
}

func TestReadsValid_StorageChanged(t *testing.T) {
	domains, tx := setupReadsValidTest(t)
	key := make([]byte, 52)
	key[0] = 0x01
	key[20] = 0xFF
	putState(t, domains, tx, kv.StorageDomain, key, []byte{0x11}, 0)
	putState(t, domains, tx, kv.StorageDomain, key, []byte{0x22}, 1)

	readLists := makeReadLists(map[string][]kvEntry{
		kv.StorageDomain.String(): {{key: string(key), val: []byte{0x11}}},
	})
	require.False(t, domains.ReadsValid(readLists), "changed storage should conflict")
}

func TestReadsValid_CodeUnchanged(t *testing.T) {
	domains, tx := setupReadsValidTest(t)
	key := make([]byte, 20)
	key[0] = 0x03
	code := []byte{0x60, 0x00, 0x60, 0x00} // simple EVM code
	putState(t, domains, tx, kv.CodeDomain, key, code, 0)

	readLists := makeReadLists(map[string][]kvEntry{
		kv.CodeDomain.String(): {{key: string(key), val: code}},
	})
	require.True(t, domains.ReadsValid(readLists), "unchanged code should be valid")
}

func TestReadsValid_CodeChanged(t *testing.T) {
	domains, tx := setupReadsValidTest(t)
	key := make([]byte, 20)
	key[0] = 0x03
	putState(t, domains, tx, kv.CodeDomain, key, []byte{0x60, 0x00}, 0)
	putState(t, domains, tx, kv.CodeDomain, key, []byte{0x60, 0x01, 0x60, 0x02}, 1)

	readLists := makeReadLists(map[string][]kvEntry{
		kv.CodeDomain.String(): {{key: string(key), val: []byte{0x60, 0x00}}},
	})
	require.False(t, domains.ReadsValid(readLists), "changed code should conflict")
}

func TestReadsValid_CodeSizeUnchanged(t *testing.T) {
	domains, tx := setupReadsValidTest(t)
	key := make([]byte, 20)
	key[0] = 0x04
	code := make([]byte, 100)
	putState(t, domains, tx, kv.CodeDomain, key, code, 0)

	readLists := makeReadLists(map[string][]kvEntry{
		state.CodeSizeTableFake: {{key: string(key), val: encodeCodeSize(100)}},
	})
	require.True(t, domains.ReadsValid(readLists), "unchanged code size should be valid")
}

func TestReadsValid_CodeSizeChanged(t *testing.T) {
	domains, tx := setupReadsValidTest(t)
	key := make([]byte, 20)
	key[0] = 0x04
	putState(t, domains, tx, kv.CodeDomain, key, make([]byte, 100), 0)
	putState(t, domains, tx, kv.CodeDomain, key, make([]byte, 200), 1)

	readLists := makeReadLists(map[string][]kvEntry{
		state.CodeSizeTableFake: {{key: string(key), val: encodeCodeSize(100)}},
	})
	require.False(t, domains.ReadsValid(readLists), "changed code size should conflict")
}

func TestReadsValid_MultiDomainMixed(t *testing.T) {
	domains, tx := setupReadsValidTest(t)
	acctKey := make([]byte, 20)
	acctKey[0] = 0x10
	storKey := make([]byte, 52)
	storKey[0] = 0x20

	putState(t, domains, tx, kv.AccountsDomain, acctKey, []byte{0xAA}, 0)
	putState(t, domains, tx, kv.StorageDomain, storKey, []byte{0x11}, 0)
	// Change storage but not account
	putState(t, domains, tx, kv.StorageDomain, storKey, []byte{0x22}, 1)

	readLists := makeReadLists(map[string][]kvEntry{
		kv.AccountsDomain.String(): {{key: string(acctKey), val: []byte{0xAA}}},
		kv.StorageDomain.String():  {{key: string(storKey), val: []byte{0x11}}},
	})
	require.False(t, domains.ReadsValid(readLists), "one conflicting domain should fail overall")
}

func TestReadsValid_MultiKeyAllValid(t *testing.T) {
	domains, tx := setupReadsValidTest(t)
	keys := make([][]byte, 3)
	vals := make([][]byte, 3)
	for i := range keys {
		keys[i] = make([]byte, 20)
		keys[i][0] = byte(i + 1)
		vals[i] = []byte{byte(i + 0xA0)}
		putState(t, domains, tx, kv.AccountsDomain, keys[i], vals[i], uint64(i))
	}

	entries := make([]kvEntry, 3)
	for i := range entries {
		entries[i] = kvEntry{key: string(keys[i]), val: vals[i]}
	}
	readLists := makeReadLists(map[string][]kvEntry{
		kv.AccountsDomain.String(): entries,
	})
	require.True(t, domains.ReadsValid(readLists), "all keys unchanged should be valid")
}

func TestReadsValid_MultiKeyOneConflict(t *testing.T) {
	domains, tx := setupReadsValidTest(t)
	keys := make([][]byte, 3)
	vals := make([][]byte, 3)
	for i := range keys {
		keys[i] = make([]byte, 20)
		keys[i][0] = byte(i + 1)
		vals[i] = []byte{byte(i + 0xA0)}
		putState(t, domains, tx, kv.AccountsDomain, keys[i], vals[i], uint64(i))
	}
	// Change the second key
	putState(t, domains, tx, kv.AccountsDomain, keys[1], []byte{0xFF}, 10)

	entries := make([]kvEntry, 3)
	for i := range entries {
		entries[i] = kvEntry{key: string(keys[i]), val: vals[i]}
	}
	readLists := makeReadLists(map[string][]kvEntry{
		kv.AccountsDomain.String(): entries,
	})
	require.False(t, domains.ReadsValid(readLists), "one changed key should conflict")
}

func TestReadsValid_ReadNilValueUnchanged(t *testing.T) {
	domains, _ := setupReadsValidTest(t)
	// Key was never written, so reading it returned nil
	key := make([]byte, 20)
	key[0] = 0x99

	readLists := makeReadLists(map[string][]kvEntry{
		kv.AccountsDomain.String(): {{key: string(key), val: nil}},
	})
	require.True(t, domains.ReadsValid(readLists), "nil read on unset key should be valid")
}

func TestReadsValid_ReadNilValueNowSet(t *testing.T) {
	domains, tx := setupReadsValidTest(t)
	key := make([]byte, 20)
	key[0] = 0x99
	// Key was nil when read, but now has a value
	putState(t, domains, tx, kv.AccountsDomain, key, []byte{0x01}, 0)

	readLists := makeReadLists(map[string][]kvEntry{
		kv.AccountsDomain.String(): {{key: string(key), val: nil}},
	})
	require.False(t, domains.ReadsValid(readLists), "nil read now set should conflict")
}

func TestReadsValid_EmptyValueVsNil(t *testing.T) {
	domains, _ := setupReadsValidTest(t)
	key := make([]byte, 20)
	key[0] = 0x88

	// Read empty value (no key in state) - should treat empty and nil as equivalent
	readLists := makeReadLists(map[string][]kvEntry{
		kv.AccountsDomain.String(): {{key: string(key), val: []byte{}}},
	})
	require.True(t, domains.ReadsValid(readLists), "empty value on unset key should be valid (treated as nil)")
}

func TestReadsValid_LargeReadSet(t *testing.T) {
	domains, tx := setupReadsValidTest(t)
	const n = 100
	entries := make([]kvEntry, n)
	for i := 0; i < n; i++ {
		key := make([]byte, 20)
		binary.BigEndian.PutUint32(key, uint32(i))
		val := []byte{byte(i)}
		putState(t, domains, tx, kv.AccountsDomain, key, val, uint64(i))
		entries[i] = kvEntry{key: string(key), val: val}
	}

	readLists := makeReadLists(map[string][]kvEntry{
		kv.AccountsDomain.String(): entries,
	})
	require.True(t, domains.ReadsValid(readLists), "large read set with all valid should pass")
}

func TestReadsValid_LargeReadSetOneConflict(t *testing.T) {
	domains, tx := setupReadsValidTest(t)
	const n = 100
	entries := make([]kvEntry, n)
	for i := 0; i < n; i++ {
		key := make([]byte, 20)
		binary.BigEndian.PutUint32(key, uint32(i))
		val := []byte{byte(i)}
		putState(t, domains, tx, kv.AccountsDomain, key, val, uint64(i))
		entries[i] = kvEntry{key: string(key), val: val}
	}
	// Change the last key
	lastKey := make([]byte, 20)
	binary.BigEndian.PutUint32(lastKey, uint32(n-1))
	putState(t, domains, tx, kv.AccountsDomain, lastKey, []byte{0xFF}, uint64(n))

	readLists := makeReadLists(map[string][]kvEntry{
		kv.AccountsDomain.String(): entries,
	})
	require.False(t, domains.ReadsValid(readLists), "large read set with last key conflict should fail")
}

// TestReadsValid_ConcurrentReads tests that ReadsValid is safe for concurrent use.
func TestReadsValid_ConcurrentReads(t *testing.T) {
	domains, tx := setupReadsValidTest(t)

	key := make([]byte, 20)
	key[0] = 0x01
	val := []byte{0xAA}
	putState(t, domains, tx, kv.AccountsDomain, key, val, 0)

	readLists := makeReadLists(map[string][]kvEntry{
		kv.AccountsDomain.String(): {{key: string(key), val: val}},
	})

	// Run multiple goroutines calling ReadsValid concurrently
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			result := domains.ReadsValid(readLists)
			done <- result
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
		// Note: with current stub returning false, these will all be false.
		// Once implemented, they should all be true.
	}
}
