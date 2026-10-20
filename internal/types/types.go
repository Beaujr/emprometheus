package types

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/beaujr/emprometheus/internal/store/sqlc"
)

var (
	OptimHeaders    = []string{optimization, timestamp, p_PV, p_Load, p_grid_pos, p_grid_neg, p_grid, p_batt, soc_opt, unit_load_cost, unit_prod_price, cost_profit, optim_status}
	CostFuncHeaders = []string{cost_fun_profit, cost_fun_cost}
)

const (
	optimization    = "optimization"
	timestamp       = "timestamp"
	p_PV            = "P_PV"
	p_Load          = "P_Load"
	p_grid_pos      = "P_grid_pos"
	p_grid_neg      = "P_grid_neg"
	p_grid          = "P_grid"
	p_batt          = "P_batt"
	soc_opt         = "SOC_opt"
	unit_load_cost  = "unit_load_cost"
	unit_prod_price = "unit_prod_price"
	cost_profit     = "cost_profit"
	cost_fun_cost   = "cost_fun_cost"
	cost_fun_profit = "cost_fun_profit"
	optim_status    = "optim_status"
)

type OptimizationResult struct {
	sqlc.OptimizationResult
}

func (o OptimizationResult) String() string {
	return fmt.Sprintf("%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%s",
		o.Optimization,
		o.Time().Format("2006-01-02 15:04:05-07:00"),
		o.PPV(),
		o.PLoad(),
		o.PGridPos(),
		o.PGridNeg(),
		o.PGrid(),
		o.PBatt(),
		o.SOCOpt(),
		o.UnitLoadCost(),
		o.UnitProdPrice(),
		o.CostProfit(),
		o.CostFunProfit(),
		o.OptimStatus(),
	)
}

func NewOptimizationResult(time time.Time, socOpt, unitLoadCost, pPV, pBatt float64) OptimizationResult {
	return OptimizationResult{sqlc.OptimizationResult{
		Time:         time,
		PPv:          pPV,
		PBatt:        pBatt,
		SocOpt:       socOpt,
		UnitLoadCost: unitLoadCost,
	}}
}

func NewOptimizationResultFull(optimization string, t time.Time, pPV, pLoad, pGridPos, pGridNeg, pGrid, pBatt, socOpt, unitLoadCost, unitProdPrice, costProfit, costFunProfit float64, optimStatus string) OptimizationResult {
	return OptimizationResult{sqlc.OptimizationResult{
		Optimization:  optimization,
		Time:          t,
		PPv:           pPV,
		PLoad:         pLoad,
		PGridPos:      pGridPos,
		PGridNeg:      pGridNeg,
		PGrid:         pGrid,
		PBatt:         pBatt,
		SocOpt:        socOpt,
		UnitLoadCost:  unitLoadCost,
		UnitProdPrice: unitProdPrice,
		CostProfit:    costProfit,
		CostFunProfit: costFunProfit,
		OptimStatus:   optimStatus,
	}}
}

func (o OptimizationResult) Time() time.Time {
	return o.OptimizationResult.Time
}

func (o OptimizationResult) PPV() float64 {
	return o.OptimizationResult.PPv
}

func (o OptimizationResult) PLoad() float64 {
	return o.OptimizationResult.PLoad
}

func (o OptimizationResult) PGrid() float64 {
	return o.OptimizationResult.PGrid
}

func (o OptimizationResult) PGridPos() float64 {
	return o.OptimizationResult.PGridPos
}

func (o OptimizationResult) PGridNeg() float64 {
	return o.OptimizationResult.PGridNeg
}

func (o OptimizationResult) UnitLoadCost() float64 {
	return o.OptimizationResult.UnitLoadCost
}

func (o OptimizationResult) SOCOpt() float64 {
	return o.OptimizationResult.SocOpt
}

func (o OptimizationResult) PBatt() float64 {
	return o.OptimizationResult.PBatt
}

func (o OptimizationResult) UnitProdPrice() float64 {
	return o.OptimizationResult.UnitProdPrice
}

func (o OptimizationResult) CostProfit() float64 {
	return o.OptimizationResult.CostProfit
}

func (o OptimizationResult) CostFunProfit() float64 {
	return o.OptimizationResult.CostFunProfit
}

func (o OptimizationResult) OptimStatus() string {
	return o.OptimizationResult.OptimStatus
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
					logger.Warn("optional column absent from emhass output", slog.String("column", header))
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

	var pBatt float64
	if idx, ok := mapping[p_batt]; ok {
		logger.Info("reading", slog.String("key", p_batt))
		pBatt, err = strconv.ParseFloat(r[idx], 64)
		if err != nil {
			return OptimizationResult{}, err
		}
	}

	var socOpt float64
	if idx, ok := mapping[soc_opt]; ok {
		logger.Info("reading", slog.String("key", soc_opt))
		socOpt, err = strconv.ParseFloat(r[idx], 64)
		if err != nil {
			return OptimizationResult{}, err
		}
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
		sqlc.OptimizationResult{
			Optimization:  r[0],
			Time:          t,
			PPv:           pPV,
			PLoad:         pLoad,
			PGridPos:      pGridPos,
			PGridNeg:      pGridNeg,
			PGrid:         pGrid,
			PBatt:         pBatt,
			SocOpt:        socOpt,
			UnitLoadCost:  unitLoadCost,
			UnitProdPrice: unitProdPrice,
			CostProfit:    costProfit,
			CostFunProfit: costFunProfit,
			OptimStatus:   r[mapping[optim_status]],
		}}, nil
}
