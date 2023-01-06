package resolver

import (
	"context"
	"encoding/json"
	"sync"

	"google.golang.org/grpc"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"google.golang.org/grpc/codes"
	grpcresolver "google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
)

type builder struct {
	c        *clientv3.Client
	metaData interface{}
}

func (b builder) Build(target grpcresolver.Target, cc grpcresolver.ClientConn, opts grpcresolver.BuildOptions) (grpcresolver.Resolver, error) {
	r := &Resolver{
		c:      b.c,
		target: target.URL.Path,
		cc:     cc,
	}
	if b.metaData != nil {
		md, ok := b.metaData.(map[string]string)
		if ok {
			r.selector = MetaSelector{
				MetaData: md,
			}
		}
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	em, err := endpoints.NewManager(r.c, r.target)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Resolver: failed to new endpoint manager: %s", err)
	}
	r.wch, err = em.NewWatchChannel(r.ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Resolver: failed to new watch channer: %s", err)
	}

	r.wg.Add(1)
	go r.watch()
	return r, nil
}

func (b builder) Scheme() string {
	return "etcd"
}

// NewBuilder creates a Resolver builder.
func NewBuilder(client *clientv3.Client, metaData interface{}) (grpcresolver.Builder, error) {
	return builder{c: client, metaData: metaData}, nil
}

type Resolver struct {
	c      *clientv3.Client
	target string
	cc     grpcresolver.ClientConn
	wch    endpoints.WatchChannel
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	selector Selector // select by metadata
}

func (r *Resolver) watch() {
	defer r.wg.Done()

	allUps := make(map[string]*endpoints.Update)
	for {
		select {
		case <-r.ctx.Done():
			return
		case ups, ok := <-r.wch:
			if !ok {
				return
			}

			for _, up := range ups {
				switch up.Op {
				case endpoints.Add:
					allUps[up.Key] = up
				case endpoints.Delete:
					delete(allUps, up.Key)
				}
			}

			addrs := r.convertToGRPCAddress(allUps)
			_ = r.cc.UpdateState(grpcresolver.State{Addresses: addrs})
		}
	}
}

func (r *Resolver) convertToGRPCAddress(ups map[string]*endpoints.Update) []grpcresolver.Address {
	var addrs []grpcresolver.Address
	for _, up := range ups {
		addr := grpcresolver.Address{
			Addr:     up.Endpoint.Addr,
			Metadata: up.Endpoint.Metadata,
		}
		if r.selector != nil {
			if ok, err := r.selector.Select(addr); err == nil && ok {
				addrs = append(addrs, addr)
				continue
			}
		} else {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

func (r *Resolver) Register(ctx context.Context, target string, addr grpcresolver.Address, leaseId clientv3.LeaseID) error {
	var bys []byte
	bys, err := json.Marshal(addr)
	if err != nil {
		return grpc.Errorf(codes.InvalidArgument, err.Error())
	}
	_, err = r.c.Put(ctx, target+"/"+addr.Addr, string(bys), clientv3.WithLease(leaseId))
	if err != nil {
		return err
	}
	r.keepAlive(ctx, target, addr, leaseId)
	return nil
}

func (r *Resolver) keepAlive(ctx context.Context, target string, addr grpcresolver.Address, leaseId clientv3.LeaseID) {
	c, err := r.c.KeepAlive(ctx, leaseId)
	if err != nil {
		return
	}
	go func() {
		defer func() {
			_, _ = r.c.Revoke(ctx, leaseId)
		}()
		for {
			select {
			case _, ok := <-c:
				if !ok {
					_, _ = r.c.Revoke(ctx, leaseId)
					_ = r.Register(ctx, target, addr, leaseId)
					return
				}
			}
		}
	}()
}

// ResolveNow is a no-op here.
// It's just a hint, Resolver can ignore this if it's not necessary.
func (r *Resolver) ResolveNow(grpcresolver.ResolveNowOptions) {}

func (r *Resolver) Close() {
	r.cancel()
	r.wg.Wait()
}

func NewResolver(client *clientv3.Client) *Resolver {
	return &Resolver{c: client}
}
