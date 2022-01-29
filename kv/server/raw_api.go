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
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	res := &kvrpcpb.RawGetResponse{}
	result, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, err
	}
	res.Value = result
	if result == nil {
		res.NotFound = true
	}
	return res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	p := storage.Put{
		Key:   req.GetKey(),
		Cf:    req.GetCf(),
		Value: req.GetValue(),
	}
	m := storage.Modify{Data: p}
	err := server.storage.Write(req.GetContext(), []storage.Modify{m})
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	p := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	m := storage.Modify{Data: p}
	err := server.storage.Write(req.GetContext(), []storage.Modify{m})
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()
	var cnt int
	kvs := make([]*kvrpcpb.KvPair, 0)
	for iter.Valid() && cnt < int(req.Limit) {
		item := iter.Item()
		k := item.Key()
		v, err := item.Value()
		if err != nil {
			return nil, err
		}
		cnt++
		kvs = append(kvs, &kvrpcpb.KvPair{Key: k, Value: v})
		iter.Next()
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
