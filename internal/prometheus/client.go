package prometheus

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

type Reporter interface {
	GetRange(ctx context.Context, query string, start, end time.Time, step time.Duration) (model.Matrix, error)
	//Query(ctx context.Context, query string, ts time.Time, opts ...v1.Option) (model.Value, v1.Warnings, error)
}

var ErrNoRows = errors.New("no rows in result set")

func New(api v1.API) *reporter {
	return &reporter{api: api}
}

type reporter struct {
	api v1.API
}

func (r *reporter) Query(ctx context.Context, query string, ts time.Time, opts ...v1.Option) (model.Value, v1.Warnings, error) {
	return r.api.Query(ctx, query, ts, opts...)
}

func (r *reporter) GetRange(ctx context.Context, query string, start, end time.Time, step time.Duration) (model.Matrix, error) {
	grid, warning, err := r.api.QueryRange(ctx, query, v1.Range{
		Start: start,
		End:   end,
		Step:  step,
	})
	if err != nil {
		return nil, err
	}
	if warning != nil {
		err = fmt.Errorf("warning")
		for _, warn := range warning {
			err = fmt.Errorf("warning %s: %w", warn, err)
		}
		return nil, err
	}
	if m, ok := grid.(model.Matrix); ok {
		if m.Len() == 0 {
			return nil, ErrNoRows
		}
		return m, nil
	}
	return nil, fmt.Errorf("unexpected type: %s", grid.Type().String())
}

// GetRange is a wrapper to handle ErrNoRows scenario and push the start back by 1 day
func GetRange(ctx context.Context, r Reporter, query string, start time.Time, end time.Time, step int64) (model.Matrix, error) {
	values, err := r.GetRange(ctx, query, start, end, time.Second*time.Duration(step))
	if err != nil {
		if errors.Is(err, ErrNoRows) {
			// its empty due to no results, lets set the start to the previous day values as a backup
			return r.GetRange(ctx, query, start.Add(-24*time.Hour), end, time.Second*time.Duration(step))
		}
		return nil, err
	}
	return values, nil
}
