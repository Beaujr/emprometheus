package postgres

import (
	"database/sql"
	"github.com/XSAM/otelsql"
	"github.com/beaujr/emprometheus/internal/prometheus"
	api "go.opentelemetry.io/otel/metric"
)

var (
	meter api.Meter
)

const meterName = "github.com/beaujr/emprometheus/internal/store"

func init() {
	meter = prometheus.GetMeter(meterName)
}

func New(dsn string) (*sql.DB, error) {
	attrs := append(otelsql.AttributesFromDSN(dsn))

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
