// Copyright 2020 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package state

import (
	"github.com/erigontech/erigon-lib/common"
)

// accessListKey is a composite key for flat slot lookups.
// 52 bytes, no pointers — ideal as a map key.
type accessListKey struct {
	addr common.Address // 20 bytes
	slot common.Hash    // 32 bytes
}

type accessList struct {
	addresses map[common.Address]int             // address -> slot count (0 means address present but no slots)
	slots     map[accessListKey]struct{}          // flat (address, slot) lookup
}

// ContainsAddress returns true if the address is in the access list.
func (al *accessList) ContainsAddress(address common.Address) bool {
	_, ok := al.addresses[address]
	return ok
}

// Contains checks if a slot within an account is present in the access list, returning
// separate flags for the presence of the account and the slot respectively.
func (al *accessList) Contains(address common.Address, slot common.Hash) (addressPresent bool, slotPresent bool) {
	_, addrOk := al.addresses[address]
	if !addrOk {
		return false, false
	}
	_, slotOk := al.slots[accessListKey{addr: address, slot: slot}]
	return true, slotOk
}

// newAccessList creates a new accessList.
func newAccessList() *accessList {
	return &accessList{
		addresses: make(map[common.Address]int),
		slots:     make(map[accessListKey]struct{}),
	}
}

// Copy creates an independent copy of an accessList.
func (al *accessList) Copy() *accessList {
	cp := newAccessList()
	for k, v := range al.addresses {
		cp.addresses[k] = v
	}
	for k := range al.slots {
		cp.slots[k] = struct{}{}
	}
	return cp
}

// AddAddress adds an address to the access list, and returns 'true' if the operation
// caused a change (addr was not previously in the list).
func (al *accessList) AddAddress(address common.Address) bool {
	if _, present := al.addresses[address]; present {
		return false
	}
	al.addresses[address] = 0
	return true
}

// AddSlot adds the specified (addr, slot) combo to the access list.
// Return values are:
// - address added
// - slot added
// For any 'true' value returned, a corresponding journal entry must be made.
func (al *accessList) AddSlot(address common.Address, slot common.Hash) (addrChange bool, slotChange bool) {
	_, addrPresent := al.addresses[address]
	key := accessListKey{addr: address, slot: slot}
	if _, slotPresent := al.slots[key]; slotPresent {
		// Both address and slot already present
		return false, false
	}
	// Slot not present, add it
	al.slots[key] = struct{}{}
	if !addrPresent {
		al.addresses[address] = 1
		return true, true
	}
	al.addresses[address]++
	return false, true
}

// DeleteSlot removes an (address, slot)-tuple from the access list.
// This operation needs to be performed in the same order as the addition happened.
// This method is meant to be used  by the journal, which maintains ordering of
// operations.
func (al *accessList) DeleteSlot(address common.Address, slot common.Hash) {
	slotCount, addrOk := al.addresses[address]
	if !addrOk {
		panic("reverting slot change, address not present in list")
	}
	key := accessListKey{addr: address, slot: slot}
	delete(al.slots, key)
	// Decrement slot count
	if slotCount > 0 {
		al.addresses[address] = slotCount - 1
	}
}

// DeleteAddress removes an address from the access list. This operation
// needs to be performed in the same order as the addition happened.
// This method is meant to be used  by the journal, which maintains ordering of
// operations.
func (al *accessList) DeleteAddress(address common.Address) {
	slotCount, addrOk := al.addresses[address]
	if !addrOk {
		panic("reverting address change, address not present in list")
	}
	if slotCount > 0 {
		panic("reverting address change, address has slots")
	}
	delete(al.addresses, address)
}

// SlotsForAddress returns all slot hashes associated with the given address.
// Used only for testing purposes.
func (al *accessList) SlotsForAddress(address common.Address) map[common.Hash]struct{} {
	if _, ok := al.addresses[address]; !ok {
		return nil
	}
	result := make(map[common.Hash]struct{})
	for key := range al.slots {
		if key.addr == address {
			result[key.slot] = struct{}{}
		}
	}
	return result
}
