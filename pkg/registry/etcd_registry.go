package registry

import (
	"fmt"
	"github.com/busgo/ostrich/pkg/etcd"
	"github.com/coreos/etcd/clientv3"
	"log"
)

type EtcdRegistry struct {
	//
	cli            *etcd.EtcdClient
	scheme         string
	leaseId        clientv3.LeaseID
	state          bool
	serviceRunners map[string]*ServiceRunner
}

func NewEtcdRegistry(cli *etcd.EtcdClient) Registry {

	return &EtcdRegistry{cli: cli,
		serviceRunners: make(map[string]*ServiceRunner)}
}

func (r *EtcdRegistry) Register(instance *ServiceInstance, ttl int64) {
	service := fmt.Sprintf("/%s/%s/%s:%d", instance.Scheme, instance.ServiceName, instance.Ip, instance.Port)

	if _, ok := r.serviceRunners[service]; ok {
		log.Printf("the service :%s has already register", service)
		return
	}

	runner := NewServiceRunner(instance, r.cli, ttl)

	r.serviceRunners[service] = runner
}

//func (r *EtcdRegistry) Register(instance ServiceInstance, ttl int64) {
//
//	key := fmt.Sprintf("/%s/%s/%s:%d", r.Scheme(), instance.ServiceName, instance.Ip, instance.Port)
//
//	if r.state {
//
//		log.Printf("the service has register %s\n",key)
//		return
//	}
//	r.state = true
//RETRY:
//	if  !r.state{
//		log.Printf("unregistry to %s\n",key)
//		return
//	}
//	log.Printf("start the registry to:%s\n", key)
//	keepAliveResponse, err := r.cli.KeepAliveWithTTL(key, instance.Metadata, ttl)
//	if err != nil {
//		log.Printf("lose registry to:%s\n", key)
//		time.Sleep(time.Second * 1)
//		goto RETRY
//	}
//
//	r.leaseId = keepAliveResponse.LeaseId
//
//	for{
//
//		_, ok := <-keepAliveResponse.KeepAliveChan
//		if !ok {
//			log.Printf("lose the registry to:%s\n", key)
//			goto RETRY
//		}
//
//		log.Printf("keep alive the registry to:%s\n", key)
//	}
//
//}

func (r *EtcdRegistry) UnRegister(instance *ServiceInstance) {

	//if !r.state {
	//	return
	//}
	//
	//key := fmt.Sprintf("/%s/%s/%s:%d", r.Scheme(), instance.ServiceName, instance.Ip, instance.Port)
	//r.cli.Revoke(r.leaseId)
	//r.leaseId =0
	//r.state = false
	//log.Printf("the unregistry to :%s success",key)
}
