package server

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := new(kvrpcpb.GetResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return resp, err
	}
	if lock != nil && lock.IsLockedFor(req.Key, req.Version, resp) {
		return resp, nil
	}
	val, err := txn.GetValue(req.Key)
	if err != nil {
		return resp, err
	}
	if val == nil {
		resp.NotFound = true
	}
	resp.Value = val
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := new(kvrpcpb.PrewriteResponse)
	resp.Errors = []*kvrpcpb.KeyError{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	startTS := req.StartVersion
	txn := mvcc.NewMvccTxn(reader, startTS)
	keys := make([][]byte, len(req.Mutations))
	for i, mut := range req.Mutations {
		keys[i] = mut.Key
	}
	lat := latches.NewLatches()
	lat.WaitForLatches(keys)
	defer lat.ReleaseLatches(keys)
	for _, mut := range req.Mutations {
		lock, err := txn.GetLock(mut.Key)
		if err != nil {
			return resp, err
		}
		if lock != nil && lock.Ts != startTS {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Locked: lock.Info(mut.Key)})
			continue
		}
		write, commitTS, err := txn.MostRecentWrite(mut.Key)
		if write != nil && commitTS >= startTS {
			conflict := &kvrpcpb.WriteConflict{
				StartTs:    startTS,
				ConflictTs: commitTS,
				Key:        mut.Key,
				Primary:    req.PrimaryLock,
			}
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Conflict: conflict})
			continue
		}
		newLock := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      startTS,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindPut,
		}
		txn.PutValue(mut.Key, mut.Value)
		txn.PutLock(mut.Key, newLock)
	}
	if err = server.storage.Write(req.Context, txn.Writes()); err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := new(kvrpcpb.CommitResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	startTS, commitTS := req.StartVersion, req.CommitVersion
	txn := mvcc.NewMvccTxn(reader, commitTS)
	lat := latches.NewLatches()
	lat.WaitForLatches(req.Keys)
	defer lat.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		write, recentCommitTS, err := txn.MostRecentWrite(key)
		if err != nil {
			return resp, err
		}
		if write != nil {
			if write.StartTS == startTS {
				if write.Kind == mvcc.WriteKindPut {
					continue
				} else if write.Kind == mvcc.WriteKindRollback {
					resp.Error = &kvrpcpb.KeyError{Abort: "rollbacked"}
				}
			}
			if recentCommitTS > startTS {
				conflict := &kvrpcpb.WriteConflict{
					StartTs:    startTS,
					ConflictTs: commitTS,
					Key:        key,
				}
				resp.Error = &kvrpcpb.KeyError{Conflict: conflict}
				return resp, err
			}
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock == nil {
			resp.Error = &kvrpcpb.KeyError{Abort: "no lock"}
			return resp, err
		}
		if lock.Ts != startTS {
			resp.Error = &kvrpcpb.KeyError{Locked: lock.Info(key)}
			return resp, nil
		}
		newWrite := &mvcc.Write{
			StartTS: startTS,
			Kind:    mvcc.WriteKindPut,
		}
		txn.PutWrite(key, commitTS, newWrite)
		txn.DeleteLock(key)
	}
	if err = server.storage.Write(req.Context, txn.Writes()); err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.ScanResponse)
	resp.Pairs = make([]*kvrpcpb.KvPair, 0)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	for {
		if uint32(len(resp.Pairs)) == req.Limit {
			return resp, nil
		}
		key, val, err := scanner.Next()
		if err != nil {
			return resp, err
		}
		if key == nil && val == nil {
			return resp, nil
		}
		if val == nil {
			continue
		}
		pair := &kvrpcpb.KvPair{Key: key}
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock == nil || !lock.IsLockedFor(key, req.Version, pair) {
			pair.Value = val
		}
		resp.Pairs = append(resp.Pairs, pair)
	}
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.CheckTxnStatusResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	key, ts := req.PrimaryKey, req.CurrentTs

	lat := latches.NewLatches()
	lat.WaitForLatches([][]byte{key})
	defer lat.ReleaseLatches([][]byte{key})

	txn := mvcc.NewMvccTxn(reader, ts)
	lock, err := txn.GetLock(key)
	if err != nil {
		return resp, err
	}
	if lock != nil {
		resp.LockTtl = lock.Ttl
	}
	write, commitTS, err := txn.MostRecentWrite(key)
	if err != nil {
		return resp, err
	}
	if write != nil && write.Kind == mvcc.WriteKindPut {
		resp.CommitVersion = commitTS
	}
	if lock == nil && write == nil {
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		txn.PutWrite(key, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		if err = server.storage.Write(req.Context, txn.Writes()); err != nil {
			return resp, err
		}
		return resp, nil
	}
	if lock != nil && mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(ts) {
		// lock is expired
		resp.Action = kvrpcpb.Action_TTLExpireRollback
		txn.StartTS = lock.Ts
		txn.DeleteLock(key)
		txn.DeleteValue(key)
		txn.PutWrite(key, lock.Ts, &mvcc.Write{
			StartTS: lock.Ts,
			Kind:    mvcc.WriteKindRollback,
		})
		if err = server.storage.Write(req.Context, txn.Writes()); err != nil {
			return resp, err
		}
		return resp, nil
	}
	resp.Action = kvrpcpb.Action_NoAction
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.BatchRollbackResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	ts := req.StartVersion
	txn := mvcc.NewMvccTxn(reader, ts)

	lat := latches.NewLatches()
	lat.WaitForLatches(req.Keys)
	defer lat.ReleaseLatches(req.Keys)

	for _, key := range req.Keys {
		// checks that a key is locked by the current transaction,
		// and if so removes the lock, deletes any value and leaves a rollback indicator as a write
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil && write.Kind == mvcc.WriteKindPut {
			resp.Error = &kvrpcpb.KeyError{
				Abort: fmt.Sprintf("txn %v has been committed", ts),
			}
			return resp, err
		}
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			continue
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts == ts {
			txn.DeleteLock(key)
			txn.DeleteValue(key)
		}
		txn.PutWrite(key, ts, &mvcc.Write{
			StartTS: ts,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	if err = server.storage.Write(req.Context, txn.Writes()); err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.ResolveLockResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	startTS, commitTS := req.StartVersion, req.CommitVersion
	txn := mvcc.NewMvccTxn(reader, startTS)
	// inspects a batch of locked keys and either rolls them all back or commits them all.
	keys := make([][]byte, 0)
	iter := reader.IterCF(engine_util.CfLock)
	for ; iter.Valid(); iter.Next() {
		i := iter.Item()
		rval, _ := i.Value()
		lock, _ := mvcc.ParseLock(rval)
		if lock.Ts == startTS {
			keys = append(keys, i.Key())
		}
	}

	lat := latches.NewLatches()
	lat.WaitForLatches(keys)
	defer lat.ReleaseLatches(keys)

	for _, key := range keys {
		txn.DeleteLock(key)
		if commitTS > 0 {
			txn.PutWrite(key, commitTS, &mvcc.Write{
				StartTS: startTS,
				Kind:    mvcc.WriteKindPut,
			})
		} else {
			txn.DeleteValue(key)
			txn.PutWrite(key, startTS, &mvcc.Write{
				StartTS: startTS,
				Kind:    mvcc.WriteKindRollback,
			})
		}
	}

	if err = server.storage.Write(req.Context, txn.Writes()); err != nil {
		return resp, err
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
