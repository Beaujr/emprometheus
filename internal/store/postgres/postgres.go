package postgres

import (
	"database/sql"
	"os"

	"github.com/XSAM/otelsql"
	"github.com/beaujr/emprometheus/internal/prometheus"
	"github.com/beaujr/emprometheus/internal/telemetry"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

func New(dsn string) (*sql.DB, error) {
	if err := os.Setenv("OTEL_SEMCONV_STABILITY_OPT_IN", "database/dup"); err != nil {
		return nil, err
	}
	attrs := append(otelsql.AttributesFromDSN(dsn), semconv.DBSystemPostgreSQL)
	opts := []otelsql.Option{
		otelsql.WithSQLCommenter(true),
		otelsql.WithMeterProvider(prometheus.GetMeterProvider()),
		otelsql.WithAttributes(
			attrs...,
		),
	}
	if tp := telemetry.GetTraceProvider(); tp != nil {
		opts = append(opts, otelsql.WithTracerProvider(tp))
	}
	db, err := otelsql.Open("postgres", dsn, opts...)
	if err != nil {
		return nil, err
	}

	// Register DB stats to meter
	_, err = otelsql.RegisterDBStatsMetrics(db, otelsql.WithAttributes(
		attrs...,
	))
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}
