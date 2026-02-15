package provider

import (
	"errors"
)

const (
	CSVFileName     = "data_load_cost_forecast.csv"
	CSVScheduleName = "schedule.csv"
	CSVForecastName = "opt_res_latest.csv"
	ActionForecast  = "dayahead-optim"
	ActionMPC       = "naive-mpc-optim"
)

var TariffNotAvailable = errors.New("future tariff not available")

type RateFetcher = func() error
