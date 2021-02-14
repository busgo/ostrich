package util

import "net"

func GetLocalIp() (string, error) {

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && ipNet.IP.IsLoopback() {

			return ipNet.IP.String(), nil
		}
	}
	return "", nil
}
