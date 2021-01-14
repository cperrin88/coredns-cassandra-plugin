package cassandra

import (
	"bytes"
	"context"
	"github.com/coredns/coredns/plugin/pkg/dnstest"
	"github.com/coredns/coredns/plugin/test"
	"github.com/gocql/gocql"
	"github.com/miekg/dns"
	golog "log"
	"testing"
	"time"
)

func setupDB() {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Consistency = gocql.One
	session, _ := cluster.CreateSession()

	session.Query(`DROP KEYSPACE coredns_test`).Exec()
	if err := session.Query(`CREATE KEYSPACE coredns_test WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};`).Exec(); err != nil {
		panic(err)
	}
	if err := session.Query(`create table coredns_test.records
(
    name text,
    type text,
    id timeuuid,
    data map<text, text>,
    "in" text,
    ttl int,
    primary key ((name, type), id)
)
    with caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
     and compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
     and compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
     and dclocal_read_repair_chance = 0.1;`).Exec(); err != nil {
		panic(err)
	}
	if err := session.Query(`INSERT INTO coredns_test.records (name, type, id, data, "in", ttl) VALUES ('example.org.', 'A', now(), {'address': '127.0.0.1'}, 'IN', 30);`).Exec(); err != nil {
		panic(err)
	}
	if err := session.Query(`INSERT INTO coredns_test.records (name, type, id, data, "in", ttl) VALUES ('example.org.', 'AAAA', now(), {'address': '::1'}, 'IN', 30);`).Exec(); err != nil {
		panic(err)
	}
	if err := session.Query(`INSERT INTO coredns_test.records (name, type, id, data, "in", ttl) VALUES ('example.org.', 'AAAA', now(), {'address': '::2'}, 'IN', 30);`).Exec(); err != nil {
		panic(err)
	}
	if err := session.Query(`INSERT INTO coredns_test.records (name, type, id, data, "in", ttl) VALUES ('txt.example.org.', 'TXT', now(), {'txt': 'Example'}, 'IN', 30);`).Exec(); err != nil {
		panic(err)
	}
	if err := session.Query(`INSERT INTO coredns_test.records (name, type, id, data, "in", ttl) VALUES ('example.org.', 'SOA', now(), {'expire': '1800', 'mail': 'hostmaster.example.org.', 'minttl': '600', 'primary': 'ns01.example.org.', 'refresh': '3600', 'retry': '1800', 'serial': '1'}, 'IN', 30);`).Exec(); err != nil {
		panic(err)
	}
	if err := session.Query(`INSERT INTO coredns_test.records (name, type, id, data, "in", ttl) VALUES ('cname.example.org.', 'CNAME', now(), {'target': 'example.org.'}, 'IN', 30);`).Exec(); err != nil {
		panic(err)
	}
}

func tearDownDB() {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Timeout = time.Second * 30
	session, _ := cluster.CreateSession()

	if err := session.Query(`DROP KEYSPACE coredns_test`).Exec(); err != nil {
		panic(err)
	}
}

func TestCassandra(t *testing.T) {
	setupDB()
	defer tearDownDB()
	tests := []struct {
		qname        string
		qtype        uint16
		wantRetCode  int
		wantAnswer   []string
		wantMsgRCode int
		wantNS       []string
		expectedErr  error
	}{
		{
			qname:      "example.org.",
			qtype:      dns.TypeA,
			wantAnswer: []string{"example.org.	30	IN	A	127.0.0.1"},
		},
		{
			qname: "example.org.",
			qtype: dns.TypeAAAA,
			wantAnswer: []string{
				"example.org.	30	IN	AAAA	::1",
				"example.org.	30	IN	AAAA	::2",
			},
		},
		{
			qname:      "cname.example.org.",
			qtype:      dns.TypeCNAME,
			wantAnswer: []string{"cname.example.org.	30	IN	CNAME	example.org."},
		},
		{
			qname: "cname.example.org.",
			qtype: dns.TypeA,
			wantAnswer: []string{
				"cname.example.org.	30	IN	CNAME	example.org.",
				"example.org.	30	IN	A	127.0.0.1",
			},
		},
	}

	cassandraTest := Cassandra{Next: test.ErrorHandler(), ClusterHosts: []string{"127.0.0.1"}, ClusterKeyspace: "coredns_test"}

	b := &bytes.Buffer{}
	golog.SetOutput(b)

	ctx := context.TODO()

	for ti, tc := range tests {
		r := new(dns.Msg)
		r.SetQuestion(tc.qname, tc.qtype)

		rec := dnstest.NewRecorder(&test.ResponseWriter{})

		cassandraTest.ServeDNS(ctx, rec, r)

		if len(tc.wantAnswer) != len(rec.Msg.Answer) {
			t.Errorf("Test %d: Unexpected number of Answers. Want: %d, got: %d", ti, len(tc.wantAnswer), len(rec.Msg.Answer))
		} else {
			for i, gotAnswer := range rec.Msg.Answer {
				if gotAnswer.String() != tc.wantAnswer[i] {
					t.Errorf("Test %d: Unexpected answer.\nWant:\n\t%s\nGot:\n\t%s", ti, tc.wantAnswer[i], gotAnswer)
				}
			}
		}

	}
}
