package rowio

import (
	"bytes"
	"context"
	"sort"
	"sync"

	"github.com/golang/protobuf/proto"
)

var _ RowIO = (*memoryRowIO)(nil)

type memoryRowIO struct {
	mappingMu *sync.RWMutex
	mapping   *sortedKeyMap
}

func NewMemoryRowIO() (RowIO, error) {
	m := &memoryRowIO{
		mappingMu: new(sync.RWMutex),
		mapping:   newSortedKeyMap(),
	}
	return m, nil
}

func (m *memoryRowIO) Set(ctx context.Context, key []byte, value proto.Message) error {
	valueBytes, err := proto.Marshal(value)
	if err != nil {
		return err
	}
	m.mappingMu.Lock()
	if value == nil {
		m.mapping.delete(key)
	} else {
		m.mapping.set(key, valueBytes)
	}
	m.mappingMu.Unlock()
	return nil
}

func (m *memoryRowIO) Get(ctx context.Context, key []byte, value proto.Message) error {
	m.mappingMu.RLock()
	valueBytes, ok := m.mapping.get(key)
	m.mappingMu.RUnlock()
	if !ok {
		return ErrKeyDoesNotExist
	}
	return proto.Unmarshal(valueBytes, value)
}

func (m *memoryRowIO) Scan(ctx context.Context, fromKey, toKey []byte, factory Factory, predicate Predicate) Iterator {
	fromIndex, _ := m.mapping.searchKey(fromKey)
	toIndex, toExists := m.mapping.searchKey(toKey)
	if !toExists {
		toIndex--
	}
	if fromIndex > toIndex {
		return newErrorIterator(ErrIteratorDone)
	}
	index := fromIndex
	f := keyValueIteratorFunc(func() (key []byte, value []byte, more bool, err error) {
		if index > toIndex {
			return nil, nil, false, nil
		}
		key = m.mapping.keys[index]
		index++
		value, exists := m.mapping.get(key)
		if !exists {
			return nil, nil, false, ErrKeyDoesNotExist
		}
		return key, value, index <= toIndex, nil
	})
	return newPredicateIterator(ctx, predicate, factory, f)
}

func (m *memoryRowIO) Close() error {
	m.mapping = nil
	return nil
}

type sortedKeyMap struct {
	mapping map[string][]byte
	keys    [][]byte
}

func newSortedKeyMap() *sortedKeyMap {
	return &sortedKeyMap{
		mapping: make(map[string][]byte),
		keys:    make([][]byte, 0),
	}
}

func (m *sortedKeyMap) has(key []byte) bool {
	keyStr := string(key)
	_, ok := m.mapping[keyStr]
	return ok
}

func (m *sortedKeyMap) get(key []byte) ([]byte, bool) {
	keyStr := string(key)
	value, ok := m.mapping[keyStr]
	return value, ok
}

func (m *sortedKeyMap) set(key []byte, value []byte) {
	keyStr := string(key)
	m.mapping[keyStr] = value
	m.insertKey(key)
}

func (m *sortedKeyMap) delete(key []byte) {
	keyStr := string(key)
	delete(m.mapping, keyStr)
	m.delete(key)
}

func (m *sortedKeyMap) insertKey(key []byte) {
	index, exists := m.searchKey(key)
	if exists {
		return
	}
	m.keys = append(m.keys[:index], append([][]byte{key}, m.keys[index:]...)...)
}

func (m *sortedKeyMap) deleteKey(key []byte) {
	index, exists := m.searchKey(key)
	if !exists {
		return
	}
	copy(m.keys[index:], m.keys[index+1:])
	m.keys[len(m.keys)-1] = nil
	m.keys = m.keys[:len(m.keys)-1]
}

func (m *sortedKeyMap) searchKey(key []byte) (index int, exists bool) {
	index = sort.Search(len(m.keys), func(index int) bool {
		return bytes.Compare(m.keys[index], key) >= 0
	})
	if index < len(m.keys) && bytes.Equal(m.keys[index], key) {
		return index, true
	}
	return index, false
}
