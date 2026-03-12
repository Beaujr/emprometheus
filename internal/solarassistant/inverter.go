package solarassistant

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/beaujr/emprometheus/internal/prometheus"
	"github.com/beaujr/emprometheus/internal/store"
	"go.opentelemetry.io/otel/attribute"
	"log/slog"

	"github.com/beaujr/emprometheus/internal/scheduler"
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
	meterName                        = "github.com/beaujr/emprometheus/internal/solarassistant"
)

var (
	mqttServer                                                          = flag.String("mqtt_server", "tcp://mqtt.whickerx.info:9229", "mqtt server address")
	mqttUser                                                            = flag.String("mqtt_user", "", "mqtt user")
	mqttPassword                                                        = flag.String("mqtt_password", "", "mqtt password")
	workmodes                                                           = []string{scheduler.WorkModeLoadFirst, scheduler.WorkModeBatteryFirst, scheduler.WorkModeGridFirst}
	stateOfCharge                                                       api.Int64ObservableGauge
	processSuccess, processFailure                                      api.Int64Counter
	batteryFirstGridChargeEnabled, workModePriority, stopDischargeAtSOC api.Int64ObservableGauge
	meter                                                               api.Meter
)

func init() {
	meter = prometheus.GetMeter(meterName)
	var err error
	stateOfCharge, err = meter.Int64ObservableGauge("soc", api.WithDescription("State of charge"))
	if err != nil {
		log.Fatal(err)
	}
	batteryFirstGridChargeEnabled, err = meter.Int64ObservableGauge("battery_first_grid_charge_enabled", api.WithDescription("Allow grid charging of battery"))
	if err != nil {
		log.Fatal(err)
	}
	workModePriority, err = meter.Int64ObservableGauge("work_mode_priority", api.WithDescription("Which load is priority, grid, battery or load"))
	if err != nil {
		log.Fatal(err)
	}
	stopDischargeAtSOC, err = meter.Int64ObservableGauge("stop_discharge_soc", api.WithDescription("State of charge"))
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

func (s *state) SetSoc(soc int64) error {
	s.soc = soc
	return nil
}

func (s *state) SetDeviceMode(deviceMode string) error {
	s.deviceMode = deviceMode
	return nil
}

func (s *state) SetGridCharge(gridCharge string) error {
	s.gridCharge = gridCharge
	return nil
}
func (s *state) GetSoc() (int64, error) {
	return s.soc, nil
}

func (s *state) GetDeviceMode() (string, error) {
	return s.deviceMode, nil
}

func (s *state) GetGridCharge() (string, error) {
	return s.gridCharge, nil
}

type stateHandler struct {
	sync.Mutex
	state  store.StateStore
	target store.StateStore
	parent store.StateAccessorStore
}

func (h *stateHandler) Load() error {
	socTarget, err := h.parent.GetSOCTarget()
	if err != nil {
		return err
	}
	err = h.target.SetSoc(socTarget)
	if err != nil {
		return err
	}
	soc, err := h.parent.GetSOC()
	err = h.state.SetSoc(soc)

	deviceMode, err := h.parent.GetDeviceMode()
	if err != nil {
		return err
	}
	err = h.state.SetDeviceMode(deviceMode)
	deviceModeTarget, err := h.parent.GetDeviceModeTarget()
	if err != nil {
		return err
	}
	err = h.target.SetDeviceMode(deviceModeTarget)
	if err != nil {
		return err
	}
	gridCharge, err := h.parent.GetBatteryFirstGridCharge()
	if err != nil {
		return err
	}
	err = h.state.SetGridCharge(gridCharge)
	if err != nil {
		return err
	}
	gridChargeTarget, err := h.parent.GetBatteryFirstGridChargeTarget()
	if err != nil {
		return err
	}
	err = h.target.SetGridCharge(gridChargeTarget)
	if err != nil {
		return err
	}
	return nil
}

func (h *stateHandler) SetBatteryFirstGridCharge(enabled string) error {
	h.Lock()
	defer h.Unlock()
	sEnabled, err := h.state.GetGridCharge()
	if err != nil {
		return err
	}
	if enabled != sEnabled {
		if h.parent != nil {
			h.parent.SetBatteryFirstGridCharge(enabled)
		}
		h.state.SetGridCharge(enabled)
	}
	return nil
}

func (h *stateHandler) SetSOC(soc int64) error {
	h.Lock()
	defer h.Unlock()
	sSoc, err := h.state.GetSoc()
	if err != nil {
		return err
	}
	if soc != sSoc {
		if h.parent != nil {
			h.parent.SetSOC(soc)
		}
		h.state.SetSoc(soc)
	}
	return nil
}

func (h *stateHandler) SetDeviceMode(mode string) error {
	h.Lock()
	defer h.Unlock()
	sMode, err := h.state.GetDeviceMode()
	if err != nil {
		return nil
	}
	if mode != sMode {
		if h.parent != nil {
			err = h.parent.SetDeviceMode(mode)
			if err != nil {
				return nil
			}
		}
		err = h.state.SetDeviceMode(mode)
		if err != nil {
			return nil
		}
	}
	return nil
}

func (h *stateHandler) GetBatteryFirstGridCharge() (string, error) {
	h.Lock()
	defer h.Unlock()
	return h.state.GetGridCharge()
}

func (h *stateHandler) GetSOC() (int64, error) {
	h.Lock()
	defer h.Unlock()
	return h.state.GetSoc()
}

func (h *stateHandler) GetDeviceMode() (string, error) {
	h.Lock()
	defer h.Unlock()
	return h.state.GetDeviceMode()
}

func (h *stateHandler) SetBatteryFirstGridChargeTarget(enabled string) error {
	h.Lock()
	defer h.Unlock()
	sEnabled, err := h.state.GetGridCharge()
	if err != nil {
		return err
	}
	if enabled != sEnabled {
		if h.parent != nil {
			if err = h.parent.SetBatteryFirstGridChargeTarget(enabled); err != nil {
				return err
			}
		}
		if err = h.target.SetGridCharge(enabled); err != nil {
			return err
		}
	}
	return nil
}

func (h *stateHandler) SetSOCTarget(soc int64) error {
	h.Lock()
	defer h.Unlock()
	tSoc, err := h.target.GetSoc()
	if err != nil {
		return err
	}
	if soc != tSoc {
		if h.parent != nil {
			if err = h.parent.SetSOCTarget(soc); err != nil {
				return err
			}
		}
		if err = h.target.SetSoc(soc); err != nil {
			return err
		}
	}
	return nil
}

func (h *stateHandler) SetDeviceModeTarget(mode string) error {
	h.Lock()
	defer h.Unlock()
	sMode, err := h.target.GetDeviceMode()
	if err != nil {
		return err
	}
	if sMode != mode {
		if h.parent != nil {
			if err = h.parent.SetDeviceModeTarget(mode); err != nil {
				return err
			}
		}
		if err = h.target.SetDeviceMode(mode); err != nil {
			return err
		}
	}
	return nil
}
func (h *stateHandler) GetBatteryFirstGridChargeTarget() (string, error) {
	h.Lock()
	defer h.Unlock()
	return h.target.GetGridCharge()
}

func (h *stateHandler) GetSOCTarget() (int64, error) {
	h.Lock()
	defer h.Unlock()
	return h.target.GetSoc()
}

func (h *stateHandler) GetDeviceModeTarget() (string, error) {
	h.Lock()
	defer h.Unlock()
	return h.target.GetDeviceMode()
}

type SolarAssistant struct {
	client       mqtt.Client
	stateHandler store.StateAccessorStore
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

	sEnabled, err := sa.stateHandler.GetBatteryFirstGridCharge()
	if err != nil {
		return err
	}
	if enabled == sEnabled {
		return nil
	}
	if token := sa.client.Publish(TopicBatteryFirstGridCharge, 0, false, enabled); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	if err = sa.stateHandler.SetBatteryFirstGridChargeTarget(enabled); err != nil {
		return err
	}
	return nil
}

func (sa *SolarAssistant) SetWorkModePriority(workmode string) error {
	if !slices.Contains(workmodes, workmode) {
		return fmt.Errorf("unknown work mode: %s", workmode)
	}

	sWorkMode, err := sa.stateHandler.GetDeviceMode()
	if err != nil {
		return err
	}
	if workmode == sWorkMode {
		return nil
	}

	if token := sa.client.Publish(TopicWorkModePriority, 0, false, workmode); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return sa.stateHandler.SetDeviceModeTarget(workmode)
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
		reg, err := meter.RegisterCallback(s.observe, stateOfCharge, batteryFirstGridChargeEnabled, workModePriority, stopDischargeAtSOC)
		if err != nil {
			return err
		}
		s.reg = reg
		return nil
	}
}

func WithStateHandler(handler store.StateAccessorStore) Option {
	return func(s *SolarAssistant) error {
		sh := &stateHandler{
			state:  &state{},
			target: &state{},
			parent: handler,
		}
		sh.Load()
		s.stateHandler = sh
		return nil
	}
}

func New(logger *slog.Logger, opts ...Option) (*SolarAssistant, error) {
	if !flag.Parsed() {
		flag.Parse()
	}
	s := &state{}
	t := &state{}
	sa := &SolarAssistant{stateHandler: &stateHandler{state: s, target: t}, logger: logger}
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
	currentAttrs := attribute.Key("state").String(store.StateCurrent)
	targetAttrs := attribute.Key("state").String(store.StateTarget)
	obs.ObserveInt64(stateOfCharge, soc, api.WithAttributes(currentAttrs))
	deviceMode, err := sa.GetCurrentDeviceMode()
	if err != nil {
		return err
	}
	switch deviceMode {
	case scheduler.WorkModeLoadFirst:
		obs.ObserveInt64(workModePriority, 1, api.WithAttributes(currentAttrs))
	case scheduler.WorkModeGridFirst:
		obs.ObserveInt64(workModePriority, 2, api.WithAttributes(currentAttrs))
	case scheduler.WorkModeBatteryFirst:
		obs.ObserveInt64(workModePriority, 3, api.WithAttributes(currentAttrs))
	}
	targetDeviceMode, err := sa.GetTargetDeviceMode()
	if err != nil {
		return err
	}
	switch targetDeviceMode {
	case scheduler.WorkModeLoadFirst:
		obs.ObserveInt64(workModePriority, 1, api.WithAttributes(targetAttrs))
	case scheduler.WorkModeGridFirst:
		obs.ObserveInt64(workModePriority, 2, api.WithAttributes(targetAttrs))
	case scheduler.WorkModeBatteryFirst:
		obs.ObserveInt64(workModePriority, 3, api.WithAttributes(targetAttrs))
	}
	currentBatteryFirstGridCharge, err := sa.GetCurrentBatteryFirstGridCharge()
	if err != nil {
		return err
	}
	switch currentBatteryFirstGridCharge {
	case scheduler.BatteryFirstGridChargeEnabled:
		obs.ObserveInt64(batteryFirstGridChargeEnabled, 1, api.WithAttributes(currentAttrs))
	default:
		obs.ObserveInt64(batteryFirstGridChargeEnabled, 0, api.WithAttributes(currentAttrs))
	}

	targetBatteryFirstGridCharge, err := sa.GetTargetBatteryFirstGridCharge()
	if err != nil {
		return err
	}
	switch targetBatteryFirstGridCharge {
	case scheduler.BatteryFirstGridChargeEnabled:
		obs.ObserveInt64(batteryFirstGridChargeEnabled, 1, api.WithAttributes(targetAttrs))
	default:
		obs.ObserveInt64(batteryFirstGridChargeEnabled, 0, api.WithAttributes(targetAttrs))
	}
	tSoc, err := sa.GetTargetSOC()
	if err != nil {
		return err
	}
	obs.ObserveInt64(stopDischargeAtSOC, tSoc, api.WithAttributes(targetAttrs))
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
	tSoc, err := sa.stateHandler.GetSOCTarget()
	if err != nil {
		return err
	}

	sBatteryFirstGridCharge, err := sa.stateHandler.GetBatteryFirstGridCharge()
	if err != nil {
		return err
	}

	sDeviceMode, err := sa.stateHandler.GetDeviceMode()
	if err != nil {
		return err
	}
	if soc >= tSoc &&
		sBatteryFirstGridCharge == scheduler.BatteryFirstGridChargeEnabled &&
		sDeviceMode == scheduler.WorkModeBatteryFirst {
		// set to load first
		if err = sa.SetWorkModePriority(scheduler.WorkModeLoadFirst); err != nil {
			return err
		}
		// disable grid charging
		if err = sa.SetBatteryFirstGridCharge(scheduler.BatteryFirstGridChargeDisabled); err != nil {
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
	return sa.stateHandler.GetSOC() // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) GetCurrentDeviceMode() (string, error) {
	return sa.stateHandler.GetDeviceMode() // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) GetCurrentBatteryFirstGridCharge() (string, error) {
	return sa.stateHandler.GetBatteryFirstGridCharge() // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) GetTargetSOC() (int64, error) {
	return sa.stateHandler.GetSOCTarget() // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) SetTargetSOC(soc int64) {
	sa.stateHandler.SetSOCTarget(soc) // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) GetTargetDeviceMode() (string, error) {
	return sa.stateHandler.GetDeviceModeTarget() // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) GetTargetBatteryFirstGridCharge() (string, error) {
	return sa.stateHandler.GetBatteryFirstGridChargeTarget() // remove error later? or keep for compatibility?
}
