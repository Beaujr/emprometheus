package store

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/beaujr/emprometheus/internal/emhass"
)

type Find = func(time.Time) (Row, error)

var ErrNotFound = errors.New("not found")

type Store interface {
	OptimizationStore
	MinimalStore
	StateAccessorStore
	Close() error
}

type OptimizationStore interface {
	Insert(Row) error
	Upsert(row Row) error
	Delete(row Row) error
	InsertOptimization(emhass.OptimizationResult) error
	SelectOptimization(start time.Time, optimization string) ([]emhass.OptimizationResult, error)
	DeleteAnyOptimization(start time.Time) error
}

type MinimalStore interface {
	Select(start time.Time) ([]Row, error)
	Find(start time.Time) (Row, error)
	SetActualSoc(socTime time.Time, soc float64) error
}

type StateStore interface {
	SetSoc(soc int64) error
	SetDeviceMode(deviceMode string) error
	SetGridCharge(gridCharge string) error
	GetSoc() (int64, error)
	GetDeviceMode() (string, error)
	GetGridCharge() (string, error)
	SetLoadFirstStopDischarge(soc int64) error
	GetLoadFirstStopDischarge() (int64, error)
}

type StateAccessorStore interface {
	SetBatteryFirstGridCharge(enabled string) error
	SetSOC(soc int64) error
	SetDeviceMode(mode string) error
	GetBatteryFirstGridCharge() (string, error)
	GetSOC() (int64, error)
	GetDeviceMode() (string, error)
	SetBatteryFirstGridChargeTarget(enabled string) error
	SetSOCTarget(soc int64) error
	SetDeviceModeTarget(mode string) error
	GetBatteryFirstGridChargeTarget() (string, error)
	GetSOCTarget() (int64, error)
	GetDeviceModeTarget() (string, error)
	SetLoadFirstStopDischarge(soc int64) error
	GetLoadFirstStopDischarge() (int64, error)
}

type Row struct {
	Optimization     string
	Time             time.Time
	WorkMode         string
	GridCharge       string
	StopDischargeSOC float64
	TargetSOC        float64
}

func (r Row) String() string {
	return fmt.Sprintf("%s,%s,%s,%s,%.2f,%.2f", r.Optimization, r.Time.Format(time.RFC3339), r.WorkMode, r.GridCharge, r.StopDischargeSOC, r.TargetSOC)
}

func (r Row) StringWithTimezone(loc *time.Location) string {
	return fmt.Sprintf("%s,%s,%s,%s,%.2f,%.2f", r.Optimization, r.Time.In(loc).Format(time.RFC3339), r.WorkMode, r.GridCharge, r.StopDischargeSOC, r.TargetSOC)
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
		stopDischargeSOC, err := strconv.ParseFloat(v[4], 64)
		if err != nil {
			return r, err
		}
		r.StopDischargeSOC = stopDischargeSOC
		targetSOC, err := strconv.ParseFloat(v[5], 64)
		if err != nil {
			return r, err
		}
		r.TargetSOC = targetSOC

	}
	return r, nil
}

type storeConfig struct {
	p *PostgresStore
	f *FileStore
}

type Option = func(s *storeConfig) error

func WithDB(db *sql.DB) Option {
	return func(s *storeConfig) error {
		s.p = &PostgresStore{db: db}
		return s.p.migrations()
	}
}

func WithFilestore(dir, name string) Option {
	return func(s *storeConfig) error {
		f, err := newFileStore(dir, name)
		if err != nil {
			return err
		}
		s.f = f
		return nil
	}
}

func New(opts ...Option) (Store, error) {
	s := &storeConfig{}
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
