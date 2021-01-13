package cassandra

import (
	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
)

// init registers this plugin.
func init() { plugin.Register("cassandra", setup) }

// setup is the function that gets called when the config parser see the token "example". Setup is responsible
// for parsing any extra options the example plugin may have. The first token this function sees is "example".
func setup(c *caddy.Controller) error {
	hosts := make([]string, 0, 3)
	var keyspace string
	c.Next()
	for c.NextBlock() {
		switch c.Val() {
		case "hosts":
			hosts = append(hosts, c.RemainingArgs()...)
			break
		case "keyspace":
			keyspace = c.RemainingArgs()[0]
			break
		default:
			return plugin.Error("cassandra", c.Errf("unknown property %q", c.Val()))
		}
	}

	if c.NextArg() {
		return plugin.Error("cassandra", c.ArgErr())
	}

	// Add the Plugin to CoreDNS, so Servers can use it in their plugin chain.
	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		return Cassandra{Next: next, ClusterHosts: hosts, ClusterKeyspace: keyspace}
	})

	// All OK, return a nil error.
	return nil
}
