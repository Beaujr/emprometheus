package temporal

import (
	"context"
	"crypto/tls"
	"log/slog"
	"strings"

	"github.com/beaujr/emprometheus/internal/telemetry"
	temporalsdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
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
	tracingInterceptor, err := opentelemetry.NewTracingInterceptor(
		opentelemetry.TracerOptions{
			Tracer: telemetry.GetTraceProvider().Tracer("temporal"),
		},
	)
	if err != nil {
		return nil, err
	}
	temporalClient, err := temporalsdk.Dial(temporalsdk.Options{
		HostPort:          address,
		Namespace:         ns,
		Logger:            logger,
		ConnectionOptions: connectionOptions,
		HeadersProvider:   &HeaderProvider{headers: map[string]string{"host": host}},
		Interceptors:      []interceptor.ClientInterceptor{tracingInterceptor},
	})
	return temporalClient, err
}
