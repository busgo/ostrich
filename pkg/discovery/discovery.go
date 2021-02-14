package discovery

import "google.golang.org/grpc/naming"

type Discovery interface {
	Resolve(target string) (naming.Watcher, error)

	Next() ([]*naming.Update, error)

	Close()
}
