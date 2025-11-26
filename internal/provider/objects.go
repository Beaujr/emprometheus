package provider

import (
	"errors"
)

const CSVFileName = "data_load_cost_forecast.csv"

var TariffNotAvailable = errors.New("future tariff not available")

type RateFetcher = func() error
