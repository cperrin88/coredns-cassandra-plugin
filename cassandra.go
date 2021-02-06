package cassandra

import (
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/upstream"
	"github.com/coredns/coredns/request"
	"github.com/gocql/gocql"
	"github.com/miekg/dns"
	"golang.org/x/net/context"
	"net"
	"strconv"
	"strings"
)

type Cassandra struct {
	Next            plugin.Handler
	ClusterHosts    []string
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
	cluster.Consistency = gocql.LocalOne
	session, _ := cluster.CreateSession()
	name := state.QName()
	if name[len(name)-1] != '.' {
		name = name + "."
	}
	defer session.Close()
	var data map[string]string
	var ttl uint
	var rrType string
	iter := session.Query(`SELECT data, ttl, type FROM records WHERE name = ? and type IN (?,'CNAME')`, name, state.Type()).Iter()
	for iter.Scan(&data, &ttl, &rrType) {

		rrQType := dns.StringToType[rrType]
		rr := c.createRR(state.QName(), rrQType, data, ttl, state)

		a.Answer = append(a.Answer, rr)

		rrType := strings.ToUpper(rrType)
		if rrType == "CNAME" && rrType != state.Type() {
			a2 := make([]dns.RR, 0)
			iter2 := session.Query(`SELECT data, ttl, type FROM records WHERE name = ? and type IN (?,'CNAME')`, data["target"], state.Type()).Iter()
			var data2 map[string]string
			var ttl2 uint
			var rrType2 string
			for iter2.Scan(&data2, &ttl2, &rrType2) {
				rrQType := dns.StringToType[rrType2]
				rr := c.createRR(data["target"], rrQType, data2, ttl2, state)
				a2 = append(a2, rr)
			}

			if len(a2) != 0 {
				a.Answer = append(a.Answer, a2...)
			}
			up := upstream.New()
			msg, err := up.Lookup(ctx, state, data["target"], state.QType())
			if err != nil {
				plugin.Error("cassandra", err)
			} else {
				a.Answer = append(a.Answer, msg.Answer...)
			}
		}

	}

	if len(a.Answer) == 0 {
		return plugin.NextOrFailure(c.Name(), c.Next, ctx, writer, msg)
	}

	return 0, writer.WriteMsg(a)
}

func (c Cassandra) Name() string { return "cassandra" }

func (c Cassandra) createRR(name string, rrQType uint16, data map[string]string, ttl uint, state request.Request) dns.RR {
	rr := dns.TypeToRR[rrQType]()
	hdr := dns.RR_Header{Name: name, Rrtype: rrQType, Class: state.QClass(), Ttl: uint32(ttl)}
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
	return rr
}
