// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package naming

import (
	"context"
	"encoding/json"
	"fmt"

	etcd "github.com/coreos/etcd/clientv3"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/naming"
)

var ErrWatcherClosed = fmt.Errorf("naming: watch closed")

// EtcdResolver creates a grpc.Watcher for a target to track its resolution changes.
type EtcdResolver struct {
	// Client is an initialized etcd bot.
	Client *etcd.Client

	// Selector is used to select address based on metadata
	Selector Selector

	// MetaDataMatcher is used to find out endpoint which should be connected to
	Matcher Matcher
}

func (gr *EtcdResolver) Update(ctx context.Context, target string, nm naming.Update, opts ...etcd.OpOption) (err error) {
	switch nm.Op {
	case naming.Add:
		var v []byte
		if v, err = json.Marshal(nm); err != nil {
			return grpc.Errorf(codes.InvalidArgument, err.Error())
		}
		_, err = gr.Client.KV.Put(ctx, target+"/"+nm.Addr, string(v), opts...)
	case naming.Delete:
		_, err = gr.Client.Delete(ctx, target+"/"+nm.Addr, opts...)
	default:
		return grpc.Errorf(codes.InvalidArgument, "naming: bad naming op")
	}
	return err
}

func (gr *EtcdResolver) Resolve(target string) (naming.Watcher, error) {
	ctx, cancel := context.WithCancel(context.Background())
	w := &gRPCWatcher{
		c:        gr.Client,
		target:   target + "/",
		selector: gr.Selector,
		ctx:      ctx,
		cancel:   cancel,
	}

	w.metaDataMatcher = gr.Matcher

	return w, nil
}

type gRPCWatcher struct {
	c      *etcd.Client
	target string
	ctx    context.Context
	cancel context.CancelFunc
	wch    etcd.WatchChan
	err    error

	selector        Selector
	metaDataMatcher Matcher
}

// Next gets the next set of updates from the etcd resolver.
// Calls to Next should be serialized; concurrent calls are not safe since
// there is no way to reconcile the update ordering.
func (gw *gRPCWatcher) Next() ([]*naming.Update, error) {
	if gw.wch == nil {
		// first Next() returns all addresses
		return gw.firstNext()
	}
	if gw.err != nil {
		return nil, gw.err
	}

	// process new events on target/*
	wr, ok := <-gw.wch
	if !ok {
		gw.err = grpc.Errorf(codes.Unavailable, "%s", ErrWatcherClosed)
		return nil, gw.err
	}
	if gw.err = wr.Err(); gw.err != nil {
		return nil, gw.err
	}

	updates := make([]*naming.Update, 0, len(wr.Events))
	for _, e := range wr.Events {
		var jupdate naming.Update
		var err error
		// nolint: exhaustive
		//  lint report wrong because the type is just same ?
		switch e.Type {
		case etcd.EventTypePut:
			err = json.Unmarshal(e.Kv.Value, &jupdate)
			jupdate.Op = naming.Add
		case etcd.EventTypeDelete:
			err = json.Unmarshal(e.PrevKv.Value, &jupdate)
			jupdate.Op = naming.Delete
		}
		if err == nil {
			updates = append(updates, &jupdate)
		}
	}

	updates = gw.selectUpdates(updates)

	// TODO: remove this if gRPC support map as metadata
	gw.fixMetadata(updates)

	return updates, nil
}

func (gw *gRPCWatcher) firstNext() ([]*naming.Update, error) {
	// Use serialized request so resolution still works if the target etcd
	// server is partitioned away from the quorum.
	resp, err := gw.c.Get(gw.ctx, gw.target, etcd.WithPrefix(), etcd.WithSerializable())
	if gw.err = err; err != nil {
		return nil, err
	}

	updates := make([]*naming.Update, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var jupdate naming.Update
		if err := json.Unmarshal(kv.Value, &jupdate); err != nil {
			continue
		}
		updates = append(updates, &jupdate)
	}

	updates = gw.selectUpdates(updates)

	// TODO: remove this if gRPC support map as metadata
	gw.fixMetadata(updates)

	opts := []etcd.OpOption{etcd.WithRev(resp.Header.Revision + 1), etcd.WithPrefix(), etcd.WithPrevKV()}
	gw.wch = gw.c.Watch(gw.ctx, gw.target, opts...)
	return updates, nil
}

func convertMetaData(metadata interface{}) map[string]string {
	// convert metadata to map[string]string
	metam, ok := metadata.(map[string]interface{})
	if !ok {
		return nil
	}

	result := make(map[string]string, len(metam))

	for k, v := range metam {
		field, ok := v.(string)
		if ok {
			result[k] = field
		}
	}

	return result
}

func (gw *gRPCWatcher) selectUpdates(updates []*naming.Update) []*naming.Update {
	if gw.selector != nil {
		filteredUpdates := make([]*naming.Update, 0, len(updates))
		for _, update := range updates {
			addr := Address{Addr: update.Addr, Metadata: update.Metadata}
			if ok, err := gw.selector.Select(addr); err == nil && ok {
				filteredUpdates = append(filteredUpdates, update)
			}
		}
		return filteredUpdates
	}

	if gw.metaDataMatcher != nil {

		filteredUpdates := make([]*naming.Update, 0, len(updates))
		for _, update := range updates {
			if gw.metaDataMatcher.MatchMetaData(convertMetaData(update.Metadata)) {
				filteredUpdates = append(filteredUpdates, update)
			}
		}
		return filteredUpdates
	}

	return updates
}

func (gw *gRPCWatcher) fixMetadata(updates []*naming.Update) {
	for _, update := range updates {
		if update.Metadata != nil {
			metaj, err := json.Marshal(update.Metadata)
			if err != nil {
				continue
			}
			update.Metadata = string(metaj)
		}
	}
}

func (gw *gRPCWatcher) Close() { gw.cancel() }
