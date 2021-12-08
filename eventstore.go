package eventstore

import (
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	badger "github.com/dgraph-io/badger/v3"
)

type EventStore struct {
	opt  badger.Options
	db   *badger.DB
	keys []int64 // cache for keys in store
}

func NewEventStore(dbpath string) *EventStore {
	es := new(EventStore)
	es.opt = badger.DefaultOptions(dbpath)
	es.opt.Logger = nil
	es.keys = make([]int64, 0)

	return es
}

func (es *EventStore) Init() error {
	if db, err := badger.Open(es.opt); err != nil {
		return fmt.Errorf("open db: %v", err)
	} else {
		es.db = db
	}

	return nil
}

func (es *EventStore) Close() error {
	return es.db.Close()
}

func (es *EventStore) Get(fn func(val []byte) error) error {
	key := es.getKey()
	if key == nil {
		return nil
	}

	txn := es.db.NewTransaction(true)
	defer txn.Discard()

	if key, err := es.get(key, txn, fn); err != nil {
		return err
	} else {
		txn.Delete(key)
		txn.Commit()
	}

	return nil
}

func (es *EventStore) get(
	key []byte,
	txn *badger.Txn,
	fn func(val []byte) error,
) ([]byte, error) {
	if item, err := txn.Get(key); err != nil {
		return key, fmt.Errorf("get: %v", err)
	} else {
		if err := item.Value(fn); err != nil {
			return key, fmt.Errorf("value: %v", err)
		}
	}

	return key, nil
}

func (es *EventStore) Put(event []byte) error {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, uint64(time.Now().UnixNano()))

	err := es.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, event)
	})

	return err
}

func (es *EventStore) scanKeys() {
	es.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		iter := txn.NewIterator(opts)
		defer iter.Close()

		for iter.Rewind(); iter.Valid(); iter.Next() {
			item := iter.Item()
			key := int64(binary.LittleEndian.Uint64(item.Key()))
			es.keys = append(es.keys, key)
		}

		return nil
	})

	if len(es.keys) > 2 {
		sort.Slice(es.keys, func(i, j int) bool {
			return es.keys[i] < es.keys[j]
		})
	}
}

func (es *EventStore) getKey() []byte {
	if len(es.keys) == 0 {
		es.scanKeys()
	}
	if len(es.keys) == 0 {
		return nil
	}

	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, uint64(es.keys[0]))
	es.keys = es.keys[1:]

	return key
}

func (es *EventStore) GetDB() *badger.DB {
	return es.db
}

func (es *EventStore) RunGC(discardRatio float64) error {
	return es.db.RunValueLogGC(discardRatio)
}
