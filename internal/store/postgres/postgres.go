package postgres

import (
	"database/sql"
	"github.com/XSAM/otelsql"
	"github.com/beaujr/emprometheus/internal/prometheus"
	api "go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"os"
)

var (
	meter api.Meter
)

const meterName = "github.com/beaujr/emprometheus/internal/store"

func init() {
	meter = prometheus.GetMeter(meterName)
}

func New(dsn string) (*sql.DB, error) {
	if err := os.Setenv("OTEL_SEMCONV_STABILITY_OPT_IN", "database/dup"); err != nil {
		return nil, err
	}
	attrs := append(otelsql.AttributesFromDSN(dsn), semconv.DBSystemPostgreSQL)

	db, err := otelsql.Open("postgres", dsn, otelsql.WithAttributes(
		attrs...,
	), otelsql.WithMeterProvider(prometheus.GetMeterProvider()), otelsql.WithSQLCommenter(true))
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
