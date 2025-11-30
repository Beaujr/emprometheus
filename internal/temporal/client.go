package temporal

import (
	"context"
	"crypto/tls"
	temporalsdk "go.temporal.io/sdk/client"
	"log/slog"
	"strings"
)

type HeaderProvider struct {
	headers map[string]string
}

func (h HeaderProvider) GetHeaders(ctx context.Context) (map[string]string, error) {
	return h.headers, nil
}

func NewClient(logger *slog.Logger, address, ns string, useTLS bool) (temporalsdk.Client, error) {
	var connectionOptions temporalsdk.ConnectionOptions
	if useTLS {
		connectionOptions = temporalsdk.ConnectionOptions{
			TLS: &tls.Config{InsecureSkipVerify: true},
		}
	}
	host := address
	if strings.Index(address, ":") > 0 {
		host = address[0:strings.Index(address, ":")]
	}
	temporalClient, err := temporalsdk.Dial(temporalsdk.Options{
		HostPort:          address,
		Namespace:         ns,
		Logger:            logger,
		ConnectionOptions: connectionOptions,
		HeadersProvider:   &HeaderProvider{headers: map[string]string{"host": host}},
	})
	return temporalClient, err
}
