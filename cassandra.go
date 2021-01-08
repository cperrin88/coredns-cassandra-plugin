package cassandra

import (
	"github.com/coredns/coredns/plugin"
	"github.com/miekg/dns"
	"golang.org/x/net/context"
)

type Cassandra struct {
	Next plugin.Handler
}

func (c Cassandra) ServeDNS(ctx context.Context, writer dns.ResponseWriter, msg *dns.Msg) (int, error) {
	panic("implement me")
}

func (c Cassandra) Name() string { return "cassandra" }
