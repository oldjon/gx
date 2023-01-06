package grpc

import (
	"context"
	"fmt"
	"sync"

	"github.com/oldjon/gx/modules/grpc/resolver"
	"google.golang.org/grpc"
)

// ConnManager caches grpc connection
type ConnManager struct {
	dialer *Dialer

	mu    sync.RWMutex // protect conns map
	conns map[string]*conn
}

type conn struct {
	mu    sync.RWMutex // used to guard only one gconn is created
	gconn *grpc.ClientConn
}

// NewConnManager create ConnManager from dialer
func NewConnManager(dialer *Dialer) *ConnManager {
	return &ConnManager{
		dialer: dialer,
		conns:  make(map[string]*conn),
	}
}

func (cm *ConnManager) getOrCreateClientConnect(key string) *conn {
	// speed up result
	cm.mu.RLock()
	c, ok := cm.conns[key]
	cm.mu.RUnlock()
	if ok {
		return c
	}

	cm.mu.Lock()
	c, ok = cm.conns[key]
	if !ok {
		c = &conn{}
		cm.conns[key] = c
	}
	cm.mu.Unlock()

	return c
}

func (cm *ConnManager) mkKey(target string, selector resolver.Selector) string {
	return fmt.Sprintf("%s-[%#v]", target, selector)
}

// todo, let ServiceDialerOptions to generate a key
func serviceKey(serviceName string, selector resolver.Selector) string {
	return "gx.1-" + serviceName + "-" + fmt.Sprintf("%#v", selector)
}

// Dial to create a grpc ClientConn by providing fx service path & other filter options
func (cm *ConnManager) Dial(ctx context.Context, serviceName string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	c := cm.getOrCreateClientConnect(serviceName)

	// make sure only one grpc conn is created
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gconn == nil {
		conn, err := cm.dialer.DialService(ctx, serviceName, opts...)
		if err != nil {
			return nil, err
		}
		c.gconn = conn
	}
	return c.gconn, nil
}

// DialWithSelector to create a grpc ClientConn by providing fx service path & other filter options
func (cm *ConnManager) DialWithSelector(ctx context.Context, serviceName string, selector resolver.Selector, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	key := serviceName
	if selector != nil {
		key = serviceKey(serviceName, selector)
	}

	c := cm.getOrCreateClientConnect(key)

	// make sure only one grpc conn is created
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gconn == nil {
		conn, err := cm.dialer.DialWithSelector(ctx, serviceName, selector, opts...)
		if err != nil {
			return nil, err
		}
		c.gconn = conn
	}
	return c.gconn, nil
}
