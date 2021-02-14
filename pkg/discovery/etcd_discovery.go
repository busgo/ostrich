package discovery

import (
	"fmt"
	"github.com/busgo/ostrich/pkg/constant"
	"github.com/busgo/ostrich/pkg/etcd"
	"github.com/coreos/etcd/clientv3"
	"google.golang.org/grpc/naming"
	"log"
	"strings"
)

type EtcdDiscovery struct {
	cli         *etcd.EtcdClient
	serviceName string
	state       bool
	updates     []*naming.Update
	watcher     clientv3.Watcher
}

//
func NewEtcdDiscovery(cli *etcd.EtcdClient) *EtcdDiscovery {

	return &EtcdDiscovery{
		cli:     cli,
		updates: make([]*naming.Update, 0),
		state:   false,
	}
}

// Resolve creates a Watcher for target.
func (d *EtcdDiscovery) Resolve(target string) (naming.Watcher, error) {
	d.serviceName = strings.TrimSpace(target)
	return d, nil
}

// Next blocks until an update or error happens. It may return one or more
// updates. The first call should get the full set of the results. It should
// return an error if and only if Watcher cannot recover.
func (d *EtcdDiscovery) Next() ([]*naming.Update, error) {

	if !d.state {

		keyPrefix := fmt.Sprintf("/%s/%s/", constant.Scheme, d.serviceName)
		log.Printf("key prefix:%s\n", keyPrefix)
		keys, values, err := d.cli.GetWithKeyPrefix(keyPrefix)
		if err != nil {
			return nil, err
		}

		d.state = true
		d.updates = make([]*naming.Update, 0)
		log.Printf("keys:%d\n", len(keys))
		if len(keys) > 0 {
			for pos, key := range keys {

				log.Printf("key:%s\n", key)
				addr := strings.TrimPrefix(key, keyPrefix)
				log.Printf("addr:%s\n", addr)
				d.updates = append(d.updates, &naming.Update{
					Op:       naming.Add,
					Addr:     strings.TrimSpace(addr),
					Metadata: values[pos],
				})
			}

		}

		go d.subscribe(keyPrefix)

	}
	return d.updates, nil
}

//
func (r *EtcdDiscovery) subscribe(keyPrefix string) {

	watchResponse := r.cli.WatchKeyPrefix(keyPrefix)
	r.watcher = watchResponse.Watcher
	for {
		ch, ok := <-watchResponse.ChangeEventChan
		if !ok || !r.state {
			fmt.Printf("unsubscribe to:%s\n", keyPrefix)
			break
		}
		r.handleServiceChangeNotify(ch)
	}

}

//  handle  service notify change
func (d *EtcdDiscovery) handleServiceChangeNotify(event *etcd.KeyChangeEvent) {

	keyPrefix := fmt.Sprintf("/%s/%s/", constant.Scheme, d.serviceName)

	addr := strings.TrimPrefix(event.K, keyPrefix)

	exists, pos := d.existAddr(addr)

	switch event.EventType {

	case etcd.KeyChangeCreateEventType:
		if !exists {
			d.updates = append(d.updates, &naming.Update{
				Op:       naming.Add,
				Addr:     addr,
				Metadata: event.V,
			})
		}
	case etcd.KeyChangeUpdateEventType:

		if !exists {
			d.updates = append(d.updates, &naming.Update{
				Op:       naming.Add,
				Addr:     addr,
				Metadata: event.V,
			})
		}
	case etcd.KeyChangeDeleteEventType:

		if exists {
			d.updates = append(d.updates[:pos], d.updates[pos+1:]...)
		}
	}

}

// check exist addr
func (d *EtcdDiscovery) existAddr(addr string) (bool, int) {

	if len(d.updates) == 0 {
		return false, -1
	}

	for pos, update := range d.updates {

		if update.Addr == addr {
			return true, pos
		}

	}
	return false, -1
}

// Close closes the Watcher.
func (d *EtcdDiscovery) Close() {

	if !d.state {
		return
	}
	d.watcher.Close()
	d.state = false
}
