package eventstore

import (
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	badger "github.com/dgraph-io/badger/v3"
)

type EventStore struct {
	opt badger.Options
	db  *badger.DB
}

func NewEventStore(dbpath string) *EventStore {
	es := new(EventStore)
	es.opt = badger.DefaultOptions(dbpath)
	es.opt.Logger = nil

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
	txn := es.db.NewTransaction(true)
	defer txn.Discard()

	if key, err := es.get(txn, fn); err != nil {
		return fmt.Errorf("value: %v", err)
	} else {
		txn.Delete(key)
		txn.Commit()
	}

	return nil
}

func (es *EventStore) get(
	txn *badger.Txn,
	fn func(val []byte) error,
) ([]byte, error) {

	var keys []int64
	es.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		iter := txn.NewIterator(opts)
		defer iter.Close()

		for iter.Rewind(); iter.Valid(); iter.Next() {
			item := iter.Item()
			key := int64(binary.LittleEndian.Uint64(item.Key()))
			keys = append(keys, key)
		}

		return nil
	})

	if len(keys) == 0 {
		return nil, nil
	}

	if len(keys) > 2 {
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})
	}

	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, uint64(keys[0]))
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
