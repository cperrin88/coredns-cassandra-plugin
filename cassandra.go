package cassandra

import (
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/request"
	"github.com/gocql/gocql"
	"github.com/miekg/dns"
	"golang.org/x/net/context"
	"net"
	"strconv"
)

type Cassandra struct {
	Next         plugin.Handler
	ClusterHosts []string
	ClusterKeyspace string
}

func (c Cassandra) ServeDNS(ctx context.Context, writer dns.ResponseWriter, msg *dns.Msg) (int, error) {
	state := request.Request{W: writer, Req: msg}
	a := new(dns.Msg)
	a.SetReply(msg)
	a.Compress = true
	a.Authoritative = true

	cluster := gocql.NewCluster(c.ClusterHosts...)
	cluster.Keyspace = c.ClusterKeyspace
	cluster.Consistency = gocql.Quorum
	session, _ := cluster.CreateSession()
	name := state.QName()
	if name[len(name)-1] != '.' {
		name = name + "."
	}
	defer session.Close()
	var data map[string]string
	var ttl uint
	iter := session.Query(`SELECT data, ttl FROM records WHERE name = ? and type = ?`, name, state.Type()).Iter()
	for iter.Scan(&data, &ttl) {
		rr := dns.TypeToRR[state.QType()]()
		hdr := dns.RR_Header{Name: state.QName(), Rrtype: state.QType(), Class: state.QClass(), Ttl: uint32(ttl)}
		switch rr := rr.(type) {
		case *dns.SOA:
			rr.Hdr = hdr
			i, _ := strconv.ParseUint(data["expire"], 10, 32)
			rr.Expire = uint32(i)
			i, _ = strconv.ParseUint(data["refresh"], 10, 32)
			rr.Refresh = uint32(i)
			i, _ = strconv.ParseUint(data["minttl"], 10, 32)
			rr.Minttl = uint32(i)
			i, _ = strconv.ParseUint(data["serial"], 10, 32)
			rr.Serial = uint32(i)
			i, _ = strconv.ParseUint(data["retry"], 10, 32)
			rr.Retry = uint32(i)
			rr.Mbox = data["mail"]
			rr.Ns = data["primary"]
			break
		case *dns.A:
			rr.Hdr = hdr
			rr.A = net.ParseIP(data["address"])
			break
		case *dns.AAAA:
			rr.Hdr = hdr
			rr.AAAA = net.ParseIP(data["address"])
			break
		case *dns.TXT:
			rr.Hdr = hdr
			rr.Txt = []string{data["txt"]}
			break
		case *dns.CNAME:
			rr.Hdr = hdr
			rr.Target = data["target"]
		case *dns.NS:
			rr.Hdr = hdr
			rr.Ns = data["ns"]
		case *dns.PTR:
			rr.Hdr = hdr
			rr.Ptr = data["ptr"]
		}

		a.Answer = append(a.Answer, rr)
	}

	if len(a.Answer) == 0 {
		return plugin.NextOrFailure(c.Name(), c.Next, ctx, writer, msg)
	}

	return 0, writer.WriteMsg(a)
}

func (c Cassandra) Name() string { return "cassandra" }
