package standalone_storage

import (
	"fmt"
	"log"
	"strings"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

func genKey(cf string, key []byte) []byte {
	return []byte(fmt.Sprintf("%s_%s", cf, key))
}

func splitKey(key []byte, cf string) []byte {
	return []byte(strings.TrimPrefix(string(key), cf+"_"))
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	return &StandAloneStorage{db: db}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandAloneStorageReader{txn: s.db.NewTransaction(true)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, d := range batch {
			switch d.Data.(type) {
			case storage.Put:
				err := txn.Set(genKey(d.Cf(), d.Key()), d.Value())
				if err != nil {
					return err
				}
				return err
			case storage.Delete:
				err := txn.Delete(genKey(d.Cf(), d.Key()))
				if err != nil {
					return err
				}
				return err
			}

		}
		return nil
	})
}

type StandAloneStorageDBIterator struct {
	iter *badger.Iterator
	cf   string
}

func (i *StandAloneStorageDBIterator) Item() engine_util.DBItem {
	return &StandAloneStorageDBItem{item: i.iter.Item(), cf: i.cf}
}

func (i *StandAloneStorageDBIterator) Valid() bool {
	return i.iter.ValidForPrefix([]byte(i.cf))
}

func (i *StandAloneStorageDBIterator) Next() {
	i.iter.Next()
}

func (i *StandAloneStorageDBIterator) Seek(t []byte) {
	i.iter.Seek(t)
}

func (i *StandAloneStorageDBIterator) Close() {
	i.iter.Close()
}

type StandAloneStorageDBItem struct {
	item *badger.Item
	cf   string
}

func (i *StandAloneStorageDBItem) Key() []byte {
	return splitKey(i.item.Key(), i.cf)
}

func (i *StandAloneStorageDBItem) KeyCopy(dst []byte) []byte {
	key := i.Key()
	return y.SafeCopy(key, dst)
}

func (i *StandAloneStorageDBItem) Value() ([]byte, error) {
	return i.item.Value()
}

func (i *StandAloneStorageDBItem) ValueSize() int {
	return i.item.ValueSize()
}

func (i *StandAloneStorageDBItem) ValueCopy(dst []byte) ([]byte, error) {
	return i.item.ValueCopy(dst)
}

// type StorageReader interface {
// 	// When the key doesn't exist, return nil for the value
// 	GetCF(cf string, key []byte) ([]byte, error)
// 	IterCF(cf string) engine_util.DBIterator
// 	Close()
// }
type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	var result []byte
	item, err := r.txn.Get(genKey(cf, key))
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, err
	}
	if err != nil && err == badger.ErrKeyNotFound {
		return nil, nil
	}
	result, err = item.Value()
	if err != nil {
		return nil, err
	}
	return result, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	iter := r.txn.NewIterator(badger.DefaultIteratorOptions)
	iter.Seek([]byte(cf))
	return &StandAloneStorageDBIterator{iter: iter, cf: cf}
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}
