package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	resp := new(kvrpcpb.RawGetResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	if val == nil {
		resp.NotFound = true
		return resp, nil
	}
	resp.Value = val
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	resp := new(kvrpcpb.RawPutResponse)
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	err := server.storage.Write(req.Context, []storage.Modify{
		{Data: put},
	})
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	resp := new(kvrpcpb.RawDeleteResponse)
	del := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	err := server.storage.Write(req.Context, []storage.Modify{
		{Data: del},
	})
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := new(kvrpcpb.RawScanResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	resp.Kvs = make([]*kvrpcpb.KvPair, 0, 1)
	itr := reader.IterCF(req.Cf)
	itr.Seek(req.StartKey)
	for ; itr.Valid() && uint32(len(resp.Kvs)) < req.Limit; itr.Next() {
		item := itr.Item()
		val, _ := item.Value()
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		})
	}
	itr.Close()
	return resp, nil
}
