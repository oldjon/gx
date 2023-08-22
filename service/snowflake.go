package service

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/oldjon/gutil"
	gsnowflake "github.com/oldjon/gutil/snowflake"
	com "github.com/oldjon/gx/common"
	"github.com/pkg/errors"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

var (
	errSessionExpire   = fmt.Errorf("snowflake session expired")
	errNoAvailableNode = fmt.Errorf("no available node")
)

type Snowflake interface {
	Next() (uint64, error)
}

type snowflake struct {
	name        string
	nodeInvalid int64
	etcdSession *concurrency.Session
	logger      *zap.Logger

	snowflake *gsnowflake.Snowflake
}

func newSnowflake(ctx context.Context, session *concurrency.Session, logger *zap.Logger, name string) (Snowflake, error) {
	sf := &snowflake{
		etcdSession: session,
		logger:      logger,
		name:        name,
	}

	node, err := sf.generateNode(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	logger.Info("create snowflake", zap.Int("node", node), zap.String("name", name))
	sf.snowflake, err = gsnowflake.New(com.GetSnowflakeClusterID(), uint64(node), com.GetSnowflakeClusterBits())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	go sf.watchSession(ctx)

	return sf, nil
}

// Next return snowflake id
func (sf *snowflake) Next() (uint64, error) {
	if atomic.LoadInt64(&sf.nodeInvalid) != 0 {
		return 0, errSessionExpire
	}

	return sf.snowflake.Next(), nil
}

func (sf *snowflake) watchSession(ctx context.Context) {
	sf.logger.Info("enter snowflake watch session goroutine")
	defer func() {
		sf.logger.Info("exit snowflake watch session goroutine")
	}()

	select {
	case <-sf.etcdSession.Done():
		sf.logger.Info("snowflake session expired")
		atomic.StoreInt64(&sf.nodeInvalid, 1)
	case <-ctx.Done():
		sf.logger.Info("snowflake context done")
		atomic.StoreInt64(&sf.nodeInvalid, 1)
	}
}

func (sf *snowflake) generateNode(ctx context.Context) (int, error) {
	mutex := concurrency.NewMutex(sf.etcdSession, snowflakeLockPrefix+gutil.If(sf.name == "", "", sf.name+"/"))

	// lock with timeout
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer func() {
		cancel()
	}()

	if err := mutex.Lock(ctx); err != nil {
		return 0, errors.WithStack(err)
	}
	defer func() {
		_ = mutex.Unlock(ctx)
	}()

	path := snowflakePrefix + gutil.If(sf.name == "", "", sf.name+"/")

	client := sf.etcdSession.Client()
	resp, err := client.Get(ctx, path, etcd.WithPrefix())
	if err != nil {
		return 0, errors.WithStack(err)
	}

	// loop over response to get all nodes
	var nodes []int
	for _, kv := range resp.Kvs {
		ls := strings.Split(string(kv.Key), "/")
		if len(ls) > 0 {
			nodeStr := ls[len(ls)-1]
			node, err := strconv.Atoi(nodeStr)
			if err != nil {
				sf.logger.Warn("invalid node in snowflake",
					zap.ByteString("path", kv.Key),
					zap.String("node_str", nodeStr))
				continue
			}
			nodes = append(nodes, node)
		}
	}

	// find available node
	node, err := findAvailableNode(nodes)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	// store node to etcd
	nodePath := snowflakePrefix + gutil.If(sf.name == "", "", sf.name+"/") + strconv.Itoa(node)
	if _, err := client.Put(ctx,
		nodePath,
		strconv.Itoa(int(sf.etcdSession.Lease())),
		etcd.WithLease(sf.etcdSession.Lease())); err != nil {
		return 0, errors.WithStack(err)
	}

	return node, nil
}

func findAvailableNode(nodes []int) (int, error) {
	if len(nodes) == 0 {
		return 1, nil
	}

	sort.Ints(nodes)
	lastNode := nodes[len(nodes)-1]
	if lastNode < gsnowflake.NodeMax {
		return lastNode + 1, nil
	}
	for i := 1; i < gsnowflake.NodeMax; i++ {
		exist := false
		for _, node := range nodes {
			if node == i {
				exist = true
				break
			}
		}
		if !exist {
			return i, nil
		}
	}

	return 0, errNoAvailableNode
}
