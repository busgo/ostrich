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
	// create key event
	KeyChangeEventCreateType KeyChangeEventType = 1
	// update key event
	KeyChangeEventUpdateType KeyChangeEventType = 2
	// delete key event
	KeyChangeEventDeleteType KeyChangeEventType = 1
)

type KeyChangeEvent struct {
	EventType KeyChangeEventType
	K         []byte
	V         []byte
}

type WatchKeyResponse struct {
	Watcher            clientv3.Watcher
	keyChangeEventChan chan *KeyChangeEvent
}

type Etcd struct {
	cli   *clientv3.Client
	kv    clientv3.KV
	lease clientv3.Lease
}

type EtcdConf struct {
	Endpoints   []string
	DailTimeout time.Duration
	UserName    string
	Password    string
}

func NewEtcdClient(conf *EtcdConf) (*Etcd, error) {

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   conf.Endpoints,
		DialTimeout: conf.DailTimeout,
		Username:    conf.UserName,
		Password:    conf.Password,
	})

	if err != nil {
		return nil, err
	}
	return &Etcd{
		cli:   cli,
		kv:    clientv3.NewKV(cli),
		lease: clientv3.NewLease(cli),
	}, nil

}

func (e *Etcd) GetWithKeyPrefix(keyPrefix string) (keys [][]byte, values [][]byte, err error) {

	getResponse, err := e.kv.Get(context.Background(), keyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}

	keys = make([][]byte, 0)
	values = make([][]byte, 0)

	if len(getResponse.Kvs) == 0 {
		return
	}

	for _, kv := range getResponse.Kvs {

		keys = append(keys, kv.Key)
		values = append(values, kv.Value)
	}
	return

}

func (e *Etcd) WatchWithKeyPrefix(keyPrefix string) (watchResponse *WatchKeyResponse) {

	watcher := clientv3.NewWatcher(e.cli)

	watchChan := watcher.Watch(context.Background(), keyPrefix, clientv3.WithPrefix())

	watchResponse = &WatchKeyResponse{
		Watcher:            watcher,
		keyChangeEventChan: make(chan *KeyChangeEvent, 200),
	}
	go func() {

		defer watcher.Close()
		for ch := range watchChan {

			if ch.Canceled {
				log.Println("the watcher has canceled....")
				break
			}
			for _, event := range ch.Events {
				e.handleKeyChangeEvent(event, watchResponse.keyChangeEventChan)
			}
		}

	}()

	return watchResponse
}

// handle key change event
func (e *Etcd) handleKeyChangeEvent(event *clientv3.Event, keyChangeEventChan chan *KeyChangeEvent) {

	changeEvent := &KeyChangeEvent{
		K: event.Kv.Key,
	}

	switch event.Type {

	case mvccpb.PUT:
		if event.IsCreate() {
			changeEvent.EventType = KeyChangeEventCreateType
		} else {
			changeEvent.EventType = KeyChangeEventUpdateType
		}
		changeEvent.V = event.Kv.Value

	case mvccpb.DELETE:
		changeEvent.EventType = KeyChangeEventDeleteType
	}

	keyChangeEventChan <- changeEvent

}
