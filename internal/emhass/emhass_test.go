package emhass

import (
	"bufio"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestOptimizationFromString(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	validMapping := map[string]int{}
	csvHeaders := []string{optimization, "timestamp", "P_PV", "P_Load", "P_grid_pos", "P_grid_neg", "P_grid", "P_batt", "SOC_opt", "soc_deficit_cost", "unit_load_cost", "unit_prod_price", "maximum_power_from_grid", "maximum_power_to_grid", "cost_profit", "cost_fun_profit", "optim_status"}
	for idx, val := range csvHeaders {
		validMapping[val] = idx
	}

	validLine := "opt1,2024-01-01 12:00:00+00:00,1058.6904660263276,23.789941102433158,0.0,-1034.9005249238944,-1034.9005249238944,0.0,0.6,0.0,0.3834,0.15,9000.0,9000.0,0.15523507873858416,0.15523507873858416,Optimal"

	tests := []struct {
		name       string
		mapping    map[string]int
		line       string
		wantErr    error
		validateFn func(t *testing.T, result OptimizationResult)
	}{
		{
			name:    "success",
			mapping: validMapping,
			line:    validLine,
			validateFn: func(t *testing.T, result OptimizationResult) {
				expectedTime, _ := time.Parse(
					"2006-01-02 15:04:05-07:00",
					"2024-01-01 12:00:00+00:00",
				)

				if result.Optimization != "opt1" {
					t.Fatalf("expected Optimization=opt1 got=%s", result.Optimization)
				}

				if !result.time.Equal(expectedTime) {
					t.Fatalf("unexpected time: %v", result.time)
				}

				if result.pPV != 1058.6904660263276 {
					t.Fatalf("expected pPV=1.1 got=%v", result.pPV)
				}

				if result.pLoad != 23.789941102433158 {
					t.Fatalf("expected pLoad=2.2 got=%v", result.pLoad)
				}

				if result.optimStatus != "Optimal" {
					t.Fatalf("expected optimStatus=SUCCESS got=%s", result.optimStatus)
				}
			},
		},
		{
			name:    "invalid timestamp",
			mapping: validMapping,
			line:    "opt1,invalidtime,1058.6904660263276,23.789941102433158,0.0,-1034.9005249238944,-1034.9005249238944,0.0,0.6,0.0,0.3834,0.15,9000.0,9000.0,0.15523507873858416,0.15523507873858416,Optimal",
			wantErr: &time.ParseError{},
		},
		{
			name:    "invalid float",
			mapping: validMapping,
			line:    "opt1,invalidtime,test,23.789941102433158,0.0,-1034.9005249238944,-1034.9005249238944,0.0,0.6,0.0,0.3834,0.15,9000.0,9000.0,0.15523507873858416,0.15523507873858416,Optimal",
			wantErr: errors.New(""),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := OptimizationFromString(logger, tt.mapping, tt.line)

			if tt.wantErr != nil {
				if err == nil {
					t.Fatalf("expected error got nil")
				}

				switch tt.wantErr.(type) {
				case *time.ParseError:
					var parseErr *time.ParseError
					if !errors.As(err, &parseErr) {
						t.Fatalf("expected time.ParseError got %T", err)
					}
				default:
					if tt.wantErr == ErrNotFound {
						if !errors.Is(err, tt.wantErr) {
							t.Fatalf("expected error %v got %v", tt.wantErr, err)
						}
					}
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.validateFn != nil {
				tt.validateFn(t, result)
			}
		})
	}
}

func TestReadOptimizationResults(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	csvData := `timestamp,P_PV,P_Load,P_grid_pos,P_grid_neg,P_grid,P_batt,SOC_opt,unit_load_cost,unit_prod_price,cost_profit,cost_fun_cost,optim_status
2024-01-01 12:00:00+00:00,1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8,9.9,10.1,11.2,SUCCESS
2024-01-01 13:00:00+00:00,11.1,12.2,13.3,14.4,15.5,16.6,17.7,18.8,19.9,20.1,21.2,FAILED`

	scanner := bufio.NewScanner(strings.NewReader(csvData))

	results, err := ReadOptimizationResults(logger, scanner, "forecast-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results got %d", len(results))
	}

	first := results[0]

	if first.Optimization != "forecast-a" {
		t.Fatalf("expected Optimization=forecast-a got=%s", first.Optimization)
	}

	if first.pPV != 1.1 {
		t.Fatalf("expected first pPV=1.1 got=%v", first.pPV)
	}

	if first.optimStatus != "SUCCESS" {
		t.Fatalf("expected first optimStatus=SUCCESS got=%s", first.optimStatus)
	}

	second := results[1]

	if second.pPV != 11.1 {
		t.Fatalf("expected second pPV=11.1 got=%v", second.pPV)
	}

	if second.optimStatus != "FAILED" {
		t.Fatalf("expected second optimStatus=FAILED got=%s", second.optimStatus)
	}
}
