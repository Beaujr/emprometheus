package prometheus

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"
	p "go.opentelemetry.io/otel/exporters/prometheus"
	api "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"log"
	"log/slog"
	"net/http"
)

var (
	meterProvider *metric.MeterProvider
)

func init() {
	e, err := p.New()
	if err != nil {
		log.Fatal(err)
	}
	meterProvider = metric.NewMeterProvider(
		metric.WithReader(e),
	)
}

func Serve(logger *slog.Logger) error {
	logger.Info("serving metrics at localhost:2223/metrics")
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/health", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		return
	})
	err := http.ListenAndServe(":2223", nil)
	if err != nil {
		logger.Error("failed serving metrics", slog.String("error", err.Error()))
		return err
	}
	return nil
}

func GetMeter(name string) api.Meter {
	return meterProvider.Meter(name)
}

func GetMeterProvider() api.MeterProvider {
	return meterProvider
}
