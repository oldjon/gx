package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	"github.com/pkg/errors"
	etcd "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type KVManager struct {
	client *etcd.Client
	logger *zap.Logger
}

// GetOrSet get key or set key if key does not exist atomic
func (km *KVManager) GetOrSet(ctx context.Context, key string, val []byte) ([]byte, error) {
	path := keysPrefix + key

	logger := km.logger.With(
		zap.String("etcd_path", path),
		zap.String("key", key))

	logger.Info("get or set key in keys manager")

	nval, err := etcdGetOrSet(ctx, km.client, []byte(path), val)
	if err != nil {
		logger.Error("failed to get or set key in keys manager", zap.Error(err))
		return nil, errors.WithStack(err)
	}

	return nval, nil
}

// GetOrGenerate get key or generate key if key does not exist atomic
func (km *KVManager) GetOrGenerate(ctx context.Context, key string, length int) ([]byte, error) {
	val := make([]byte, length)
	_, err := rand.Read(val)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	vals := hex.EncodeToString(val)

	return km.GetOrSet(ctx, key, []byte(vals))
}

// Delete key from keys manager
func (km *KVManager) Delete(ctx context.Context, key string) error {
	path := keysPrefix + key

	logger := km.logger.With(
		zap.String("etcd_path", path),
		zap.String("key", key))

	logger.Info("delete key in keys manager")
	_, err := km.client.Delete(ctx, path)
	if err != nil {
		logger.Error("failed to delete key in keys manager", zap.Error(err))
		return errors.WithStack(err)
	}

	return nil
}

func etcdGetOrSet(ctx context.Context, client *etcd.Client, key []byte, val []byte) ([]byte, error) {
	keys := string(key)
	vals := string(val)

	txn := client.Txn(ctx).
		If(etcd.Compare(etcd.CreateRevision(keys), "=", 0)).
		Then(etcd.OpPut(keys, vals)).
		Else(etcd.OpGet(keys))

	resp, err := txn.Commit()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if resp.Succeeded {
		return val, nil
	}

	return resp.Responses[0].GetResponseRange().Kvs[0].Value, nil
}
