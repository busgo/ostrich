package registry

import (
	"errors"
	"github.com/busgo/ostrich/pkg/etcd"
	"google.golang.org/grpc/resolver"
	"time"
)

var (
	ErrorEndpointsIsNil = errors.New("endpoints is nil")
	ErrorSchemeIsNil    = errors.New("scheme is nil")
)

type EtcdResolver struct {
	option EtcdResolverOption
	cc resolver.ClientConn
	cli *etcd.Etcd
}

type EtcdResolverOption struct {
	scheme    string
	endpoints []string
	etcdDailTimeout time.Duration
}

type EtcdResolverOptions func(option *EtcdResolverOption)

func WithScheme(scheme string) EtcdResolverOptions {

	return func(option *EtcdResolverOption) {
		option.scheme = scheme
	}
}

func WithEndpoints(endpoints []string) EtcdResolverOptions {

	return func(option *EtcdResolverOption) {
		option.endpoints = endpoints
	}
}

func WithEtcdDailTimeout(dailTimeout time.Duration)EtcdResolverOptions  {

	return func(option *EtcdResolverOption) {
		option.etcdDailTimeout = dailTimeout
	}
}

// new etcd resolver
func NewEtcdResolver(options ...EtcdResolverOptions) (resolver *EtcdResolver, err error) {

	opt := EtcdResolverOption{}

	for _, op := range options {
		op(&opt)
	}

	if opt.endpoints == nil || len(opt.endpoints) == 0 {
		return nil, ErrorEndpointsIsNil
	}

	if opt.scheme == "" {
		return nil, ErrorSchemeIsNil
	}

	if opt.etcdDailTimeout ==0 {
		opt.etcdDailTimeout = time.Second*10
	}
	resolver = &EtcdResolver{option: opt}
	return

}

// Build creates a new resolver for the given target.
//
// gRPC dial calls Build synchronously, and fails if the returned error is
// not nil.
func (r *EtcdResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {

	return r, nil
}

func (r *EtcdResolver)watch(keyPrefix string)  {

}

// Scheme returns the scheme supported by this resolver.
// Scheme is defined at https://github.com/grpc/grpc/blob/master/doc/naming.md.
func (r EtcdResolver) Scheme() string {
	return r.option.scheme
}

// ResolveNow will be called by gRPC to try to resolve the target name
// again. It's just a hint, resolver can ignore this if it's not necessary.
//
// It could be called multiple times concurrently.
func (r EtcdResolver) ResolveNow(resolver.ResolveNowOptions) {

}

// Close closes the resolver.
func (r EtcdResolver) Close() {

}
