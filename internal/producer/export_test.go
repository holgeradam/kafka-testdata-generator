package producer

import "github.com/twmb/franz-go/pkg/kgo"

var (
	AckConfig = ackConfig
	BuildOpts = buildOpts
)

// SetNewClient swaps the client constructor and returns the restore func.
func SetNewClient(f func(...kgo.Opt) (*kgo.Client, error)) (restore func()) {
	orig := newClient
	newClient = f
	return func() { newClient = orig }
}

// Opts returns the Options the Producer stored.
func (p *Producer) Opts() Options { return p.opts }
