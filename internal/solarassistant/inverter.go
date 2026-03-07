package solarassistant

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	p "go.opentelemetry.io/otel/exporters/prometheus"
	"log/slog"

	"github.com/beaujr/emprometheus/internal/scheduler"
	"go.opentelemetry.io/otel/sdk/metric"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	api "go.opentelemetry.io/otel/metric"
	"log"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"
)

const (
	TopicBatteryFirstGridCharge      = "solar_assistant/inverter_1/battery_first_grid_charge/set"
	TopicWorkModePriority            = "solar_assistant/inverter_1/work_mode_priority/set"
	TopicLoadFirstStopDischarge      = "solar_assistant/inverter_1/load_first_stop_discharge/set"
	TopicSOCState                    = "solar_assistant/total/battery_state_of_charge/state"
	TopicDeviceModeState             = "solar_assistant/inverter_1/device_mode/state"
	TopicBatteryFirstGridChargeState = "solar_assistant/inverter_1/battery_first_grid_charge/state"
	meterName                        = "github.com/beaujr/emprometheus"
)

var (
	mqttServer                     = flag.String("mqtt_server", "tcp://mqtt.whickerx.info:9229", "mqtt server address")
	mqttUser                       = flag.String("mqtt_user", "", "mqtt user")
	mqttPassword                   = flag.String("mqtt_password", "", "mqtt password")
	workmodes                      = []string{scheduler.WorkModeLoadFirst, scheduler.WorkModeBatteryFirst, scheduler.WorkModeGridFirst}
	stateOfCharge                  api.Int64ObservableGauge
	processSuccess, processFailure api.Int64Counter
	meter                          api.Meter
)

func init() {
	exporter, err := p.New()
	if err != nil {
		log.Fatal(err)
	}
	provider := metric.NewMeterProvider(
		metric.WithReader(exporter),
	)
	meter = provider.Meter(meterName)

	stateOfCharge, err = meter.Int64ObservableGauge("soc", api.WithDescription("State of charge"))
	if err != nil {
		log.Fatal(err)
	}
	processSuccess, err = meter.Int64Counter("success", api.WithDescription("successful interactions with inverter"))
	if err != nil {
		log.Fatal(err)
	}
	processFailure, err = meter.Int64Counter("failure", api.WithDescription("failed interactions with inverter"))
	if err != nil {
		log.Fatal(err)
	}

}

type state struct {
	soc                    int64
	deviceMode, gridCharge string
}

type stateHandler struct {
	sync.Mutex
	state  *state
	target *state
}

func (h *stateHandler) SetBatteryFirstGridCharge(enabled string) {
	h.Lock()
	defer h.Unlock()
	h.state.gridCharge = enabled
}

func (h *stateHandler) SetSOC(soc int64) {
	h.Lock()
	defer h.Unlock()
	h.state.soc = soc
}

func (h *stateHandler) SetDeviceMode(mode string) {
	h.Lock()
	defer h.Unlock()
	h.state.deviceMode = mode
}

func (h *stateHandler) GetBatteryFirstGridCharge() string {
	h.Lock()
	defer h.Unlock()
	return h.state.gridCharge
}

func (h *stateHandler) GetSOC() int64 {
	h.Lock()
	defer h.Unlock()
	return h.state.soc
}

func (h *stateHandler) GetDeviceMode() string {
	h.Lock()
	defer h.Unlock()
	return h.state.deviceMode
}

func (h *stateHandler) SetBatteryFirstGridChargeTarget(enabled string) {
	h.Lock()
	defer h.Unlock()
	h.target.gridCharge = enabled
}

func (h *stateHandler) SetSOCTarget(soc int64) {
	h.Lock()
	defer h.Unlock()
	h.target.soc = soc
}

func (h *stateHandler) SetDeviceModeTarget(mode string) {
	h.Lock()
	defer h.Unlock()
	h.target.deviceMode = mode
}
func (h *stateHandler) GetBatteryFirstGridChargeTarget() string {
	h.Lock()
	defer h.Unlock()
	return h.target.gridCharge
}

func (h *stateHandler) GetSOCTarget() int64 {
	h.Lock()
	defer h.Unlock()
	return h.target.soc
}

func (h *stateHandler) GetDeviceModeTarget() string {
	h.Lock()
	defer h.Unlock()
	return h.target.deviceMode
}

type SolarAssistant struct {
	client       mqtt.Client
	stateHandler *stateHandler
	logger       *slog.Logger
	reg          api.Registration
}

func (sa *SolarAssistant) Start(_ context.Context) error {
	if token := sa.client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (sa *SolarAssistant) Stop(_ context.Context) error {
	if sa.reg != nil {
		if err := sa.reg.Unregister(); err != nil {
			return err
		}
	}

	if sa.client != nil {
		sa.client.Disconnect(250)
	}
	return nil
}

func (sa *SolarAssistant) Process(ctx context.Context, batteryfirstgridcharge string, workmodepriority string, soc int64) error {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(error); ok {
				processFailure.Add(ctx, 1)
				return
			}
		}

	}()
	if err := sa.SetBatteryFirstGridCharge(batteryfirstgridcharge); err != nil {
		return err
	}
	if err := sa.SetWorkModePriority(workmodepriority); err != nil {
		return err
	}

	switch workmodepriority {
	case scheduler.WorkModeBatteryFirst:
		// why would I want to maintain the higher SOC for target?
		//currentSoc, err := sa.GetCurrentSOC()
		//if err != nil {
		//	return err
		//}
		//if currentSoc > soc {
		//	soc = currentSoc
		//}
		if err := sa.SetLoadFirstStopDischarge(soc); err != nil {
			return err
		}
	default:
		// always allow discharging to 10 (minimum) unless explicitly grid charging
		if err := sa.SetLoadFirstStopDischarge(soc); err != nil {
			return err
		}
	}
	processSuccess.Add(ctx, 1)
	return nil
}
func (sa *SolarAssistant) SetBatteryFirstGridCharge(enabled string) error {
	if enabled != scheduler.BatteryFirstGridChargeEnabled && enabled != scheduler.BatteryFirstGridChargeDisabled {
		return fmt.Errorf("unknown Battery First Grid Charge: %s", enabled)
	}
	if enabled == sa.stateHandler.GetBatteryFirstGridCharge() {
		return nil
	}
	if token := sa.client.Publish(TopicBatteryFirstGridCharge, 0, false, enabled); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	sa.stateHandler.SetBatteryFirstGridChargeTarget(enabled)
	return nil
}

func (sa *SolarAssistant) SetWorkModePriority(workmode string) error {
	if !slices.Contains(workmodes, workmode) {
		return fmt.Errorf("unknown work mode: %s", workmode)
	}

	if workmode == sa.stateHandler.GetDeviceMode() {
		return nil
	}

	if token := sa.client.Publish(TopicWorkModePriority, 0, false, workmode); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	sa.stateHandler.SetDeviceModeTarget(workmode)
	return nil
}

func (sa *SolarAssistant) SetLoadFirstStopDischarge(soc int64) error {
	if soc < 10 {
		soc = 10
	}

	if token := sa.client.Publish(TopicLoadFirstStopDischarge, 0, false, fmt.Sprintf("%d", soc)); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	sa.SetTargetSOC(soc)
	return nil
}

type Option = func(s *SolarAssistant) error

func WithMqttClient(client mqtt.Client) Option {
	return func(s *SolarAssistant) error {
		s.client = client
		return nil
	}
}

func WithOTEL() Option {
	return func(s *SolarAssistant) error {
		reg, err := meter.RegisterCallback(s.observe, stateOfCharge)
		if err != nil {
			return err
		}
		s.reg = reg
		return nil
	}
}

func New(logger *slog.Logger, opts ...Option) (*SolarAssistant, error) {
	if !flag.Parsed() {
		flag.Parse()
	}
	sa := &SolarAssistant{stateHandler: &stateHandler{state: &state{}, target: &state{}}, logger: logger}
	for _, opt := range opts {
		if err := opt(sa); err != nil {
			return nil, err
		}
	}

	if sa.client == nil {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		mqttOpts := mqtt.NewClientOptions().AddBroker(*mqttServer).SetClientID(hostname).SetCleanSession(true)
		if len(*mqttUser) > 0 {
			mqttOpts.SetCredentialsProvider(func() (username string, password string) {
				return *mqttUser, *mqttPassword
			})
		}
		mqttOpts.SetKeepAlive(60 * time.Second)
		mqttOpts.SetPingTimeout(60 * time.Second)
		mqttOpts.SetOnConnectHandler(sa.subscribers)
		mqttOpts.SetAutoReconnect(true)
		mqttOpts.SetResumeSubs(true)
		tlsConfig := &tls.Config{InsecureSkipVerify: true, ClientAuth: tls.NoClientCert}
		mqttOpts.SetTLSConfig(tlsConfig)
		sa.client = mqtt.NewClient(mqttOpts)
	}

	return sa, nil
}

func (sa *SolarAssistant) observe(_ context.Context, obs api.Observer) error {
	soc, err := sa.GetCurrentSOC()
	if err != nil {
		return err
	}
	obs.ObserveInt64(stateOfCharge, soc)
	return nil

}

func (sa *SolarAssistant) subscribers(c mqtt.Client) {
	if token := c.Subscribe(TopicSOCState, 0, func(client mqtt.Client, msg mqtt.Message) {
		batterySOC, err := strconv.ParseFloat(string(msg.Payload()), 32)
		if err != nil {
			return
		}
		if err = sa.SetCurrentSOC(int64(batterySOC)); err != nil {
			sa.logger.Warn("error setting soc", slog.Int64("soc", int64(batterySOC)))
		}
	}); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	if token := c.Subscribe(TopicDeviceModeState, 0, func(client mqtt.Client, msg mqtt.Message) {
		sa.SetCurrentDeviceMode(string(msg.Payload()))
	}); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	if token := c.Subscribe(TopicBatteryFirstGridChargeState, 0, func(client mqtt.Client, msg mqtt.Message) {
		sa.SetCurrentBatteryFirstGridCharge(string(msg.Payload()))
	}); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
}

func (sa *SolarAssistant) SetCurrentSOC(soc int64) error {
	sa.stateHandler.SetSOC(soc)
	// if the target is less or equal to current soc, revert to grid
	if soc >= sa.stateHandler.target.soc &&
		sa.stateHandler.state.gridCharge == scheduler.BatteryFirstGridChargeEnabled &&
		sa.stateHandler.state.deviceMode == scheduler.WorkModeBatteryFirst {
		// set to load first
		if err := sa.SetWorkModePriority(scheduler.WorkModeLoadFirst); err != nil {
			return err
		}
		// disable grid charging
		if err := sa.SetBatteryFirstGridCharge(scheduler.BatteryFirstGridChargeDisabled); err != nil {
			return err
		}
	}
	return nil
}

func (sa *SolarAssistant) SetCurrentDeviceMode(mode string) {
	sa.stateHandler.SetDeviceMode(mode)
}

func (sa *SolarAssistant) SetCurrentBatteryFirstGridCharge(gridFirstBatteryCharge string) {
	sa.stateHandler.SetBatteryFirstGridCharge(gridFirstBatteryCharge)
}

func (sa *SolarAssistant) GetCurrentSOC() (int64, error) {
	return sa.stateHandler.GetSOC(), nil // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) GetCurrentDeviceMode() string {
	return sa.stateHandler.GetDeviceMode() // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) GetCurrentBatteryFirstGridCharge() string {
	return sa.stateHandler.GetBatteryFirstGridCharge() // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) GetTargetSOC() (int64, error) {
	return sa.stateHandler.GetSOCTarget(), nil // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) SetTargetSOC(soc int64) {
	sa.stateHandler.SetSOCTarget(soc) // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) GetTargetDeviceMode() string {
	return sa.stateHandler.GetDeviceModeTarget() // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) GetTargetBatteryFirstGridCharge() string {
	return sa.stateHandler.GetBatteryFirstGridChargeTarget() // remove error later? or keep for compatibility?
}
