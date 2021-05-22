package eventstore

import (
	"encoding/binary"
	"fmt"
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
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	iter := txn.NewIterator(opts)
	defer iter.Close()

	var key []byte
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()

		if err := item.Value(fn); err != nil {
			return nil, err
		} else {
			key = item.Key()
			break
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
