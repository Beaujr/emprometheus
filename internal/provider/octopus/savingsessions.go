package octopus

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"
	_ "time/tzdata"
)

type SavingSessionResponse struct {
	Data Data `json:"data"`
}
type Events struct {
	ID                       int           `json:"id"`
	Code                     string        `json:"code"`
	StartAt                  time.Time     `json:"startAt"`
	EndAt                    time.Time     `json:"endAt"`
	EventType                string        `json:"eventType"`
	CapacityStatus           interface{}   `json:"capacityStatus"`
	RewardPerKwhInOctoPoints int           `json:"rewardPerKwhInOctoPoints"`
	TargetRegion             []interface{} `json:"targetRegion"`
}
type SignedUpMeterPoint struct {
	RegionID int `json:"regionId"`
}
type JoinedEvents struct {
	EventID   int       `json:"eventId"`
	StartAt   time.Time `json:"startAt"`
	EndAt     time.Time `json:"endAt"`
	EventType string    `json:"eventType"`
}
type Account struct {
	SignedUpMeterPoint SignedUpMeterPoint `json:"signedUpMeterPoint"`
	JoinedEvents       []JoinedEvents     `json:"joinedEvents"`
}
type SavingSessions struct {
	Events  []Events `json:"events"`
	Account Account  `json:"account"`
}
type Data struct {
	SavingSessions SavingSessions `json:"savingSessions"`
}

// fetchHappyHours targets the specific flexibility campaign via GraphQL
func (o *Octopus) graphqlSavingSessions(ctx context.Context, client *http.Client) (*SavingSessionResponse, error) {
	query := `query($accountNumber: String!) {
      savingSessions {
        events(includeDev: false) {
          id
          code
          startAt
          endAt
          eventType
          capacityStatus
          rewardPerKwhInOctoPoints
          targetRegion {
            regionId
          }
        }
        account(accountNumber: $accountNumber) {
          signedUpMeterPoint {
            regionId
          }
          joinedEvents {
            eventId
            startAt
            endAt
            eventType
          }
        }
      }
    }`
	reqPayload := GraphQLRequest{
		Query: query,
		Variables: map[string]interface{}{
			"accountNumber": o.accountNo,
		},
	}

	jsonBytes, err := json.Marshal(reqPayload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", GraphQLEndpoint, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err = resp.Body.Close(); err != nil {
			o.logger.Error(err.Error())
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	o.logger.Info("savingsession", slog.String("response", string(body)))
	var hhResp SavingSessionResponse
	if err = json.Unmarshal(body, &hhResp); err != nil {
		return nil, err
	}

	return &hhResp, nil
}

func (o *Octopus) fetchSavingSessions(ctx context.Context) ([]Result, error) {
	c, err := o.getKrakenClient(ctx)
	if err != nil {
		return nil, err
	}

	events, err := o.graphqlSavingSessions(ctx, c)
	if err != nil {
		return nil, err
	}
	var results []Result

    uk, err := time.LoadLocation("Europe/London")
    if err != nil {
    	return nil, err
    }

    now := time.Now().In(uk)

    toUKTime := func(t time.Time) time.Time {
    	return time.Date(
    		t.Year(), t.Month(), t.Day(),
    		t.Hour(), t.Minute(), t.Second(),
    		t.Nanosecond(), uk,
    	)
    }

    for _, edge := range events.Data.SavingSessions.Events {
    	if edge.EventType != "WEEKEND_HAPPY_HOUR" &&
    		edge.EventType != "TURN_UP" {
    		continue
    	}

    	startAt := toUKTime(edge.StartAt)
    	if startAt.Before(now) {
    		continue
    	}

    	results = append(results, Result{
    		ValueExcVat: 0,
    		ValueIncVat: 0, // TURN_UP is only free for above-average usage; TODO: model better
    		ValidFrom:   startAt,
	    	ValidTo:     toUKTime(edge.EndAt),
    	})
    }

	return results, nil
}
