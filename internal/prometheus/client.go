package prometheus

import (
	"context"
	"fmt"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"time"
)

type Reporter interface {
	GetRange(ctx context.Context, query string, start, end time.Time, step time.Duration) (model.Matrix, error)
	Query(ctx context.Context, query string, ts time.Time, opts ...v1.Option) (model.Value, v1.Warnings, error)
}

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
			return nil, fmt.Errorf("no series returned")
		}
		return m, nil
	}
	return nil, fmt.Errorf("unexpected type: %s", grid.Type().String())
}
