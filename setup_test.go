package cassandra

import (
	"github.com/coredns/caddy"
	"testing"
)

func TestSetupWhoami(t *testing.T) {
	c := caddy.NewTestController("dns", `cassandra {
	hosts 127.0.0.1
	keyspace coredns
}`)
	if err := setup(c); err != nil {
		t.Fatalf("Expected no errors, but got: %v", err)
	}
}