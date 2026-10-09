package solarapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/prometheus/common/model"
)

type Reporter struct {
	baseURL string
	client  *http.Client
}

func New(baseURL string, c *http.Client) *Reporter {
	return &Reporter{
		baseURL: baseURL,
		client:  c,
	}
}

func (r *Reporter) GetRange(ctx context.Context, query string, start, end time.Time, step time.Duration) (model.Matrix, error) {
	return r.history(ctx, query, start, end, step)
}

func (r *Reporter) history(ctx context.Context, query string, start, end time.Time, step time.Duration) (model.Matrix, error) {
	params := url.Values{}
	params.Set("topic", fmt.Sprintf("total/%s", query))
	params.Set("from", start.UTC().Format(time.RFC3339))
	params.Set("to", end.UTC().Format(time.RFC3339))
	params.Set("group_by", formatDuration(step))

	endpoint := r.baseURL + "/api/v1/history?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request solar assistant history: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("solar assistant history returned HTTP %d", resp.StatusCode)
	}
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var d data
	err = json.Unmarshal(out, &d)
	if err != nil {
		return nil, err
	}

	return toMatrix(d)
}

type data struct {
	Name   string  `json:"name"`
	Unit   string  `json:"unit"`
	Device string  `json:"device"`
	Group  string  `json:"group"`
	Topic  string  `json:"topic"`
	Points [][]any `json:"points"`
}

func toMatrix(d data) (model.Matrix, error) {
	sampleStream := &model.SampleStream{
		Metric: model.Metric{
			"__name__": model.LabelValue(d.Topic),
			"device":   model.LabelValue(d.Device),
			"group":    model.LabelValue(d.Group),
			"unit":     model.LabelValue(d.Unit),
		},
	}

	for i, point := range d.Points {
		if len(point) != 2 {
			return nil, fmt.Errorf("point %d: expected timestamp and value", i)
		}

		timestamp, ok := point[0].(string)
		if !ok {
			return nil, fmt.Errorf("point %d: timestamp must be a string", i)
		}

		t, err := time.Parse(time.RFC3339, timestamp)
		if err != nil {
			return nil, fmt.Errorf("point %d: parse timestamp: %w", i, err)
		}

		var value float64
		switch v := point[1].(type) {
		case float64:
			value = v
		case int:
			value = float64(v)
		case json.Number:
			value, err = v.Float64()
			if err != nil {
				return nil, fmt.Errorf("point %d: parse value: %w", i, err)
			}
		default:
			return nil, fmt.Errorf("point %d: value must be numeric", i)
		}

		sampleStream.Values = append(sampleStream.Values, model.SamplePair{
			Timestamp: model.TimeFromUnixNano(t.UnixNano()),
			Value:     model.SampleValue(value),
		})
	}

	return model.Matrix{sampleStream}, nil
}
func formatDuration(d time.Duration) string {
	switch {
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", int64(d/time.Hour))
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", int64(d/time.Minute))
	case d%time.Second == 0:
		return fmt.Sprintf("%ds", int64(d/time.Second))
	default:
		return d.String()
	}
}
