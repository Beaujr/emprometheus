package store

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/beaujr/emprometheus/internal/store/sqlc"
	"github.com/beaujr/emprometheus/internal/types"
)

type (
	Select = func(start time.Time) ([]Row, error)
)

var ErrNotFound = errors.New("not found")

type Store interface {
	OptimizationStore
	MinimalStore
	StateAccessorStore
	Close() error
}

type OptimizationStore interface {
	Insert(context.Context, Row) error
	Upsert(context.Context, Row) error
	Delete(context.Context, Row) error
	InsertOptimization(context.Context, types.OptimizationResult) error
	SelectOptimization(ctx context.Context, start time.Time, optimization string) ([]types.OptimizationResult, error)
	DeleteAnyOptimization(context.Context, time.Time) error
}

type MinimalStore interface {
	Select(ctx context.Context, start time.Time) ([]Row, error)
	Find(ctx context.Context, start time.Time) (Row, error)
	SetActualSoc(ctx context.Context, socTime time.Time, soc float64) error
}

type StateStore interface {
	SetSoc(ctx context.Context, soc int64) error
	SetDeviceMode(ctx context.Context, deviceMode string) error
	SetGridCharge(ctx context.Context, gridCharge string) error
	GetSoc(ctx context.Context) (int64, error)
	GetDeviceMode(ctx context.Context) (string, error)
	GetGridCharge(ctx context.Context) (string, error)
	SetLoadFirstStopDischarge(ctx context.Context, soc int64) error
	GetLoadFirstStopDischarge(ctx context.Context) (int64, error)
}

type StateAccessorStore interface {
	SetBatteryFirstGridCharge(ctx context.Context, enabled string) error
	SetSOC(ctx context.Context, soc int64) error
	SetDeviceMode(ctx context.Context, mode string) error
	GetBatteryFirstGridCharge(ctx context.Context) (string, error)
	GetSOC(ctx context.Context) (int64, error)
	GetDeviceMode(ctx context.Context) (string, error)
	SetBatteryFirstGridChargeTarget(ctx context.Context, enabled string) error
	SetSOCTarget(ctx context.Context, soc int64) error
	SetDeviceModeTarget(ctx context.Context, mode string) error
	GetBatteryFirstGridChargeTarget(ctx context.Context) (string, error)
	GetSOCTarget(ctx context.Context) (int64, error)
	GetDeviceModeTarget(ctx context.Context) (string, error)
	SetLoadFirstStopDischarge(ctx context.Context, soc int64) error
	GetLoadFirstStopDischarge(ctx context.Context) (int64, error)
}

type Row struct {
	Optimization     string
	Time             time.Time
	WorkMode         string
	GridCharge       string
	StopDischargeSoc float64
	TargetSoc        float64
}

func (r Row) toInsert() sqlc.InsertScheduleParams {
	return sqlc.InsertScheduleParams{
		Optimization:     r.Optimization,
		Time:             r.Time,
		WorkMode:         r.WorkMode,
		GridCharge:       r.GridCharge,
		StopDischargeSoc: r.StopDischargeSoc,
		TargetSoc:        r.TargetSoc,
	}
}

func (r Row) String() string {
	return fmt.Sprintf("%s,%s,%s,%s,%.2f,%.2f", r.Optimization, r.Time.Format(time.RFC3339), r.WorkMode, r.GridCharge, r.StopDischargeSoc, r.TargetSoc)
}

func (r Row) StringWithTimezone(loc *time.Location) string {
	return fmt.Sprintf("%s,%s,%s,%s,%.2f,%.2f", r.Optimization, r.Time.In(loc).Format(time.RFC3339), r.WorkMode, r.GridCharge, r.StopDischargeSoc, r.TargetSoc*100)
}

func RowFromString(line string) (Row, error) {
	r := Row{}
	csvreader := csv.NewReader(strings.NewReader(line))
	record, err := csvreader.ReadAll()
	if err != nil {
		return r, err
	}

	for _, v := range record {
		r.Optimization = v[0]
		t, err := time.Parse(time.RFC3339, v[1])
		if err != nil {
			return r, err
		}
		r.Time = t
		r.WorkMode = v[2]
		r.GridCharge = v[3]
		StopDischargeSoc, err := strconv.ParseFloat(v[4], 64)
		if err != nil {
			return r, err
		}
		r.StopDischargeSoc = StopDischargeSoc
		TargetSoc, err := strconv.ParseFloat(v[5], 64)
		if err != nil {
			return r, err
		}
		r.TargetSoc = TargetSoc

	}
	return r, nil
}

type storeConfig struct {
	p      *PostgresStore
	f      *FileStore
	logger *slog.Logger
}

type Option = func(s *storeConfig) error

func WithDB(ctx context.Context, db *sql.DB) Option {
	return func(s *storeConfig) error {
		s.p = &PostgresStore{logger: s.logger, postgres: sqlc.New(db)}
		return s.p.checkDB(ctx)
	}
}

func WithFilestore(dir, name string) Option {
	return func(s *storeConfig) error {
		f, err := newFileStore(s.logger, dir, name)
		if err != nil {
			return err
		}
		s.f = f
		return nil
	}
}

func New(logger *slog.Logger, opts ...Option) (Store, error) {
	s := &storeConfig{logger: logger}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}

	if s.p != nil {
		return s.p, nil
	}
	if s.f != nil {
		return s.f, nil
	}
	return nil, ErrNotFound
}
