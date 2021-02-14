package etcd

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"log"
	"time"
)

type KeyChangeEventType int32

const (
	KeyChangeCreateEventType KeyChangeEventType = 1
	KeyChangeUpdateEventType KeyChangeEventType = 2
	KeyChangeDeleteEventType KeyChangeEventType = 3
)

type KeyChangeEvent struct {
	K         string
	V         string
	EventType KeyChangeEventType
}

type WatchKeyResponse struct {
	Watcher         clientv3.Watcher
	ChangeEventChan chan *KeyChangeEvent
}
type KeepAliveResponse struct {
	Key           string
	Value         string
	TTL           int64
	LeaseId       clientv3.LeaseID
	KeepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
}
type EtcdClient struct {
	option *EtcdClientOption
	cli    *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

type EtcdClientOption struct {
	endpoints   []string
	dailTimeout time.Duration
	userName    string
	password    string
}

type EtcdClientOptions func(option *EtcdClientOption)

func WithEndpoints(endpoints []string) EtcdClientOptions {
	return func(option *EtcdClientOption) {
		option.endpoints = endpoints
	}
}

func WithDailTimeout(dailTimeout time.Duration) EtcdClientOptions {
	return func(option *EtcdClientOption) {
		option.dailTimeout = dailTimeout
	}
}

func WithUserName(userName string) EtcdClientOptions {
	return func(option *EtcdClientOption) {
		option.userName = userName
	}
}

func WithPassword(password string) EtcdClientOptions {
	return func(option *EtcdClientOption) {
		option.password = password
	}
}

func NewEtcdClient(opts ...EtcdClientOptions) (client *EtcdClient, err error) {
	opt := &EtcdClientOption{
		endpoints:   []string{"127.0.0.1:2379"},
		dailTimeout: time.Second * 5,
		userName:    "",
		password:    "",
	}

	for _, option := range opts {
		option(opt)
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   opt.endpoints,
		DialTimeout: opt.dailTimeout,
		Username:    opt.userName,
		Password:    opt.password,
	})

	if err != nil {
		return nil, err
	}

	return &EtcdClient{
		option: opt,
		cli:    cli,
		kv:     clientv3.NewKV(cli),
		lease:  clientv3.NewLease(cli),
	}, nil
}

//  get with  key prefix
func (e *EtcdClient) GetWithKeyPrefix(keyPrefix string) (keys, values []string, err error) {
	response, err := e.kv.Get(context.Background(), keyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}

	if len(response.Kvs) == 0 {
		return
	}

	keys = make([]string, 0)
	values = make([]string, 0)

	for _, kv := range response.Kvs {

		keys = append(keys, string(kv.Key))
		values = append(values, string(kv.Value))
	}

	return keys, values, nil
}

// get with key
func (e *EtcdClient) GetWithKey(key string) (value string, err error) {
	response, err := e.kv.Get(context.Background(), key)
	if err != nil {
		return "", err
	}

	if len(response.Kvs) == 0 {
		return
	}
	return string(response.Kvs[0].Value), nil
}

func (e *EtcdClient) WatchKeyPrefix(keyPrefix string) (response *WatchKeyResponse) {
	watcher := clientv3.NewWatcher(e.cli)
	watchChan := watcher.Watch(context.Background(), keyPrefix, clientv3.WithPrefix())

	watchResponse := &WatchKeyResponse{
		Watcher:         watcher,
		ChangeEventChan: make(chan *KeyChangeEvent, 10),
	}
	go func() {

		for ch := range watchChan {

			if ch.Canceled {
				log.Println("the watch chan is canceled....")
				break
			}

			for _, event := range ch.Events {
				e.handleKeyChangeEvent(event, watchResponse.ChangeEventChan)
			}

		}

	}()
	return watchResponse
}

//
func (e *EtcdClient) handleKeyChangeEvent(event *clientv3.Event, eventChan chan *KeyChangeEvent) {

	changeEvent := &KeyChangeEvent{}

	switch event.Type {

	case mvccpb.PUT:
		changeEvent.K = string(event.Kv.Key)
		changeEvent.V = string(event.Kv.Value)
		if event.IsCreate() {
			changeEvent.EventType = KeyChangeCreateEventType
		} else {
			changeEvent.EventType = KeyChangeUpdateEventType
		}

	case mvccpb.DELETE:
		changeEvent.K = string(event.PrevKv.Key)
		changeEvent.EventType = KeyChangeDeleteEventType
	}
	eventChan <- changeEvent

}

func (e *EtcdClient) Grant(ttl int64) (leaseId clientv3.LeaseID, err error) {

	response, err := e.lease.Grant(context.Background(), ttl)
	if err != nil {
		return 0, err
	}

	return response.ID, nil
}

func (e *EtcdClient) PutWithLeaseId(key, value string, leaseId clientv3.LeaseID) error {

	_, err := e.cli.Put(context.Background(), key, value, clientv3.WithLease(leaseId))
	if err != nil {
		return err
	}

	return nil
}
func (e *EtcdClient) KeepAlive(leaseId clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {

	keepaliveChan, err := e.lease.KeepAlive(context.Background(), leaseId)
	return keepaliveChan, err
}

func (e *EtcdClient) Revoke(leaseId clientv3.LeaseID) (err error) {

	_, err = e.lease.Revoke(context.Background(), leaseId)
	return err
}

// keepAlive with ttl
func (e *EtcdClient) KeepAliveWithTTL(key, value string, ttl int64) (keepAliveResponse *KeepAliveResponse, err error) {

	leaseId, err := e.Grant(ttl)
	if err != nil {
		return nil, err
	}

	err = e.PutWithLeaseId(key, value, leaseId)
	if err != nil {

		_ = e.Revoke(leaseId)
		return nil, err
	}

	keepAliveChan, err := e.KeepAlive(leaseId)

	return &KeepAliveResponse{
		Key:           key,
		Value:         value,
		TTL:           ttl,
		LeaseId:       leaseId,
		KeepAliveChan: keepAliveChan,
	}, err

}

func (e *EtcdClient) Delete(key string) error {

	_, err := e.cli.KV.Delete(context.Background(), key)
	return err
}
