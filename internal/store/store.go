package store

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
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
	InsertOptimization(OptimizationResult) error
	SelectOptimization(start time.Time, optimization string) ([]OptimizationResult, error)
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
}

type Row struct {
	Optimization     string
	Time             time.Time
	WorkMode         string
	GridCharge       string
	StopDischargeSOC float64
	TargetSOC        float64
}

type OptimizationResult struct {
	Optimization  string
	Time          time.Time
	PPV           float64
	PLoad         float64
	PGridPos      float64
	PGridNeg      float64
	PGrid         float64
	PBatt         float64
	SOCOpt        float64
	UnitLoadCost  float64
	UnitProdPrice float64
	CostProfit    float64
	CostFunProfit float64
	OptimStatus   string
}

func (o OptimizationResult) String() string {
	return fmt.Sprintf("%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%s",
		o.Optimization,
		o.Time.Format("2006-01-02 15:04:05-07:00"),
		o.PPV,
		o.PLoad,
		o.PGridPos,
		o.PGridNeg,
		o.PGrid,
		o.PBatt,
		o.SOCOpt,
		o.UnitLoadCost,
		o.UnitProdPrice,
		o.CostProfit,
		o.CostFunProfit,
		o.OptimStatus,
	)
}

func OptimizationFromString(line string) (OptimizationResult, error) {
	csvreader := csv.NewReader(strings.NewReader(line))
	record, err := csvreader.ReadAll()
	if err != nil {
		return OptimizationResult{}, err
	}
	for _, r := range record {
		t, err := time.Parse("2006-01-02 15:04:05-07:00", r[1])
		if err != nil {
			return OptimizationResult{}, nil
		}

		pPV, err := strconv.ParseFloat(r[2], 64)
		if err != nil {
			return OptimizationResult{}, nil
		}
		pLoad, err := strconv.ParseFloat(r[3], 64)
		if err != nil {
			return OptimizationResult{}, nil
		}
		pGridPos, err := strconv.ParseFloat(r[4], 64)
		if err != nil {
			return OptimizationResult{}, nil
		}
		pGridNeg, err := strconv.ParseFloat(r[5], 64)
		if err != nil {
			return OptimizationResult{}, nil
		}
		pGrid, err := strconv.ParseFloat(r[6], 64)
		if err != nil {
			return OptimizationResult{}, nil
		}
		pBatt, err := strconv.ParseFloat(r[7], 64)
		if err != nil {
			return OptimizationResult{}, nil
		}
		socOpt, err := strconv.ParseFloat(r[8], 64)
		if err != nil {
			return OptimizationResult{}, nil
		}
		unitLoadCost, err := strconv.ParseFloat(r[9], 64)
		if err != nil {
			return OptimizationResult{}, nil
		}
		unitProdPrice, err := strconv.ParseFloat(r[10], 64)
		if err != nil {
			return OptimizationResult{}, nil
		}
		costProfit, err := strconv.ParseFloat(r[11], 64)
		if err != nil {
			return OptimizationResult{}, nil
		}
		costFunProfit, err := strconv.ParseFloat(r[12], 64)
		if err != nil {
			return OptimizationResult{}, nil
		}

		return OptimizationResult{
			Optimization:  r[0],
			Time:          t,
			PPV:           pPV,
			PLoad:         pLoad,
			PGridPos:      pGridPos,
			PGridNeg:      pGridNeg,
			PGrid:         pGrid,
			PBatt:         pBatt,
			SOCOpt:        socOpt,
			UnitLoadCost:  unitLoadCost,
			UnitProdPrice: unitProdPrice,
			CostProfit:    costProfit,
			CostFunProfit: costFunProfit,
			OptimStatus:   r[13],
		}, nil
	}
	return OptimizationResult{}, ErrNotFound
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
