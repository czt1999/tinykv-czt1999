package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	db := engine_util.CreateDB(conf.DBPath, conf.Raft)
	standAloneStorage := &StandAloneStorage{db: db}
	return standAloneStorage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	reader := BadgerReader{txn: txn}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)
	defer txn.Discard()
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			err := txn.Set(engine_util.KeyWithCF(modify.Cf(), modify.Key()), modify.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := txn.Delete(engine_util.KeyWithCF(modify.Cf(), modify.Key()))
			if err != nil {
				return err
			}
		}
	}
	err := txn.Commit()
	return err
}

type BadgerReader struct {
	txn *badger.Txn
}

func (b BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(b.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (b BadgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, b.txn)
}

func (b BadgerReader) Close() {
	b.txn.Discard()
}
