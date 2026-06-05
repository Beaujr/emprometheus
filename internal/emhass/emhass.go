package emhass

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

var (
	ErrNotFound       = errors.New("not found")
	ErrColumnNotFound = errors.New("column not found")
	OptimHeaders      = []string{optimization, timestamp, p_PV, p_Load, p_grid_pos, p_grid_neg, p_grid, p_batt, soc_opt, unit_load_cost, unit_prod_price, cost_profit, optim_status}
	CostFuncHeaders   = []string{cost_fun_profit, cost_fun_cost}
)

const (
	optimization = "optimization"
	timestamp    = "timestamp"
	p_PV         = "P_PV"
	p_Load       = "P_Load"
	p_grid_pos   = "P_grid_pos"
	p_grid_neg   = "P_grid_neg"
	p_grid       = "P_grid"
	p_batt       = "P_batt"
	soc_opt      = "SOC_opt"
	//soc_deficit_cost = "soc_deficit_cost"
	unit_load_cost  = "unit_load_cost"
	unit_prod_price = "unit_prod_price"
	//maximum_power_from_grid = "maximum_power_from_grid"
	//maximum_power_to_grid = "maximum_power_to_grid"
	cost_profit     = "cost_profit"
	cost_fun_cost   = "cost_fun_cost"
	cost_fun_profit = "cost_fun_profit"
	optim_status    = "optim_status"
)

type OptimizationResult struct {
	Optimization  string
	time          time.Time
	pPV           float64
	pLoad         float64
	pGridPos      float64
	pGridNeg      float64
	pGrid         float64
	pBatt         float64
	socOpt        float64
	unitLoadCost  float64
	unitProdPrice float64
	costProfit    float64
	costFunProfit float64
	optimStatus   string
}

func (o OptimizationResult) String() string {
	return fmt.Sprintf("%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%s",
		o.Optimization,
		o.time.Format("2006-01-02 15:04:05-07:00"),
		o.pPV,
		o.pLoad,
		o.pGridPos,
		o.pGridNeg,
		o.pGrid,
		o.pBatt,
		o.socOpt,
		o.unitLoadCost,
		o.unitProdPrice,
		o.costProfit,
		o.costFunProfit,
		o.optimStatus,
	)
}

func NewOptimizationResult(time time.Time, socOpt, unitLoadCost, pPV, pBatt float64) OptimizationResult {
	return OptimizationResult{time: time, socOpt: socOpt, unitLoadCost: unitLoadCost, pPV: pPV, pBatt: pBatt}
}

func (o OptimizationResult) Time() time.Time {
	return o.time
}

func (o OptimizationResult) PPV() float64 {
	return o.pPV
}

func (o OptimizationResult) PLoad() float64 {
	return o.pLoad
}

func (o OptimizationResult) PGrid() float64 {
	return o.pGrid
}

func (o OptimizationResult) PGridPos() float64 {
	return o.pGridPos
}

func (o OptimizationResult) PGridNeg() float64 {
	return o.pGridNeg
}

func (o OptimizationResult) UnitLoadCost() float64 {
	return o.unitLoadCost
}

func (o OptimizationResult) SOCOpt() float64 {
	return o.socOpt
}

func (o OptimizationResult) PBatt() float64 {
	return o.pBatt
}

func (o OptimizationResult) UnitProdPrice() float64 {
	return o.unitProdPrice
}

func (o OptimizationResult) CostProfit() float64 {
	return o.costProfit
}

func (o OptimizationResult) CostFunProfit() float64 {
	return o.costFunProfit
}

func (o OptimizationResult) OptimStatus() string {
	return o.optimStatus
}

func ReadOptimizationResults(logger *slog.Logger, reader *bufio.Scanner, forecastMethod string) ([]OptimizationResult, error) {
	idx := 0
	headerMapping := map[string]int{}
	var results []OptimizationResult
	for reader.Scan() {
		if idx == 0 {
			idx++
			line := fmt.Sprintf("%s,%s", optimization, strings.TrimSpace(reader.Text()))
			csvreader := csv.NewReader(strings.NewReader(line))
			headers, err := csvreader.Read()
			if err != nil {
				return nil, err
			}
			for index, key := range headers {
				headerMapping[key] = index
			}
			logger.Info("headers mapped", slog.Any("headerMapping", headerMapping))
			for _, header := range OptimHeaders {
				if _, ok := headerMapping[header]; !ok {
					// cost fun headers depend on config
					if strings.HasPrefix(header, "cost_fun") {

					}
					return nil, ErrColumnNotFound
				}
			}
			continue
		}
		o, err := OptimizationFromString(logger, headerMapping, fmt.Sprintf("%s,%s", forecastMethod, reader.Text()))
		if err != nil {
			logger.Error("failed reading from string", slog.String("error", err.Error()))
			return nil, err
		}
		results = append(results, o)

	}
	return results, nil
}

func OptimizationFromString(logger *slog.Logger, mapping map[string]int, line string) (OptimizationResult, error) {
	csvreader := csv.NewReader(strings.NewReader(line))
	r, err := csvreader.Read()
	if err != nil {
		return OptimizationResult{}, err
	}
	logger.Info("reading", slog.String("key", timestamp))
	t, err := time.Parse("2006-01-02 15:04:05-07:00", r[mapping[timestamp]])
	if err != nil {
		return OptimizationResult{}, err
	}
	logger.Info("reading", slog.String("key", p_PV))
	pPV, err := strconv.ParseFloat(r[mapping[p_PV]], 64)
	if err != nil {
		return OptimizationResult{}, err
	}

	logger.Info("reading", slog.String("key", p_Load))
	pLoad, err := strconv.ParseFloat(r[mapping[p_Load]], 64)
	if err != nil {
		return OptimizationResult{}, err
	}

	logger.Info("reading", slog.String("key", p_grid_pos))
	pGridPos, err := strconv.ParseFloat(r[mapping[p_grid_pos]], 64)
	if err != nil {
		return OptimizationResult{}, err
	}

	logger.Info("reading", slog.String("key", p_grid_neg))
	pGridNeg, err := strconv.ParseFloat(r[mapping[p_grid_neg]], 64)
	if err != nil {
		return OptimizationResult{}, err
	}

	logger.Info("reading", slog.String("key", p_grid))
	pGrid, err := strconv.ParseFloat(r[mapping[p_grid]], 64)
	if err != nil {
		return OptimizationResult{}, err
	}

	logger.Info("reading", slog.String("key", p_batt))
	pBatt, err := strconv.ParseFloat(r[mapping[p_batt]], 64)
	if err != nil {
		return OptimizationResult{}, err
	}

	logger.Info("reading", slog.String("key", soc_opt))
	socOpt, err := strconv.ParseFloat(r[mapping[soc_opt]], 64)
	if err != nil {
		return OptimizationResult{}, err
	}

	logger.Info("reading", slog.String("key", unit_load_cost))
	unitLoadCost, err := strconv.ParseFloat(r[mapping[unit_load_cost]], 64)
	if err != nil {
		return OptimizationResult{}, err
	}
	logger.Info("reading", slog.String("key", unit_prod_price))
	unitProdPrice, err := strconv.ParseFloat(r[mapping[unit_prod_price]], 64)
	if err != nil {
		return OptimizationResult{}, err
	}

	logger.Info("reading", slog.String("key", cost_profit))
	costProfit, err := strconv.ParseFloat(r[mapping[cost_profit]], 64)
	if err != nil {
		return OptimizationResult{}, err
	}

	logger.Info("reading", slog.String("key", "cost_fun_*"))
	costFunProfit := 0.0
	for _, costFunc := range CostFuncHeaders {
		if _, ok := mapping[costFunc]; ok {
			cost, err := strconv.ParseFloat(r[mapping[costFunc]], 64)
			if err != nil {
				return OptimizationResult{}, err
			}
			costFunProfit = cost
		}
	}

	return OptimizationResult{
		Optimization:  r[0],
		time:          t,
		pPV:           pPV,
		pLoad:         pLoad,
		pGridPos:      pGridPos,
		pGridNeg:      pGridNeg,
		pGrid:         pGrid,
		pBatt:         pBatt,
		socOpt:        socOpt,
		unitLoadCost:  unitLoadCost,
		unitProdPrice: unitProdPrice,
		costProfit:    costProfit,
		costFunProfit: costFunProfit,
		optimStatus:   r[mapping[optim_status]],
	}, nil
}
