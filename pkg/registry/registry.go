package registry

type ServiceInstance struct {
	Scheme      string
	ServiceName string
	Ip          string
	Port        int
	Metadata    string
}

type Registry interface {
	Register(instance *ServiceInstance, TTL int64)
	UnRegister(instance *ServiceInstance)
}
