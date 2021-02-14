package registry

import (
	"fmt"
	"github.com/busgo/ostrich/pkg/etcd"
	"github.com/coreos/etcd/clientv3"
	"log"
	"time"
)

type ServiceRunner struct {
	instance    *ServiceInstance
	serviceName string
	leaseId     clientv3.LeaseID
	TTL         int64
	state       bool
	cli         *etcd.EtcdClient
}

func NewServiceRunner(instance *ServiceInstance, e *etcd.EtcdClient, ttl int64) *ServiceRunner {

	runner := &ServiceRunner{
		instance: instance,
		TTL:      ttl,
		state:    false,
		cli:      e,
	}

	go runner.lookup()
	return runner

}

func (r *ServiceRunner) lookup() {

	key := fmt.Sprintf("/%s/%s/%s:%d", r.instance.Scheme, r.instance.ServiceName, r.instance.Ip, r.instance.Port)
	//
	if r.state {

		log.Printf("the service %s has register\n", key)
		return
	}
	r.state = true
RETRY:
	if !r.state {
		log.Printf("unregistry to %s\n", key)
		return
	}
	log.Printf("start the registry to:%s\n", key)
	keepAliveResponse, err := r.cli.KeepAliveWithTTL(key, r.instance.Metadata, r.TTL)
	if err != nil {
		log.Printf("lose registry to:%s\n", key)
		time.Sleep(time.Second * 1)
		goto RETRY
	}

	r.leaseId = keepAliveResponse.LeaseId

	for {

		_, ok := <-keepAliveResponse.KeepAliveChan
		if !ok {
			log.Printf("lose the registry to:%s\n", key)
			goto RETRY
		}

		log.Printf("keep alive the registry to:%s\n", key)
	}

}

func (r *ServiceRunner) Close() {

	if !r.state {
		return
	}

	key := fmt.Sprintf("/%s/%s/%s:%d", r.instance.Scheme, r.instance.ServiceName, r.instance.Ip, r.instance.Port)
	r.cli.Revoke(r.leaseId)
	r.leaseId = 0
	r.state = false
	log.Printf("the unregistry to :%s success", key)
}
