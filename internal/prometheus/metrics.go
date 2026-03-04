package prometheus

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log/slog"
	"net/http"
)

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
