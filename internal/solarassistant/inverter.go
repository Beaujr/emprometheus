package solarassistant

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"

	"github.com/beaujr/emprometheus/internal/prometheus"
	"github.com/beaujr/emprometheus/internal/store"
	"go.opentelemetry.io/otel/attribute"

	"log"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/beaujr/emprometheus/internal/scheduler"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	api "go.opentelemetry.io/otel/metric"
)

const (
	TopicBatteryFirstGridCharge      = "solar_assistant/inverter_1/battery_first_grid_charge/set"
	TopicWorkModePriority            = "solar_assistant/inverter_1/work_mode_priority/set"
	TopicLoadFirstStopDischarge      = "solar_assistant/inverter_1/load_first_stop_discharge/set"
	TopicLoadFirstStopDischargeState = "solar_assistant/inverter_1/load_first_stop_discharge/state"
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

func (sa *SolarAssistant) Status(ctx context.Context) (batteryfirstgridcharge string, workmodepriority string, soc int64, err error) {
	soc, err = sa.GetTargetSoc(ctx)
	if err != nil {
		return "", "", 0, err
	}
	batteryfirstgridcharge, err = sa.GetTargetBatteryFirstGridCharge(ctx)
	if err != nil {
		return "", "", 0, err
	}

	workmodepriority, err = sa.GetTargetDeviceMode(ctx)
	if err != nil {
		return "", "", 0, err
	}
	return batteryfirstgridcharge, workmodepriority, soc, nil
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
	if err := sa.SetBatteryFirstGridCharge(ctx, batteryfirstgridcharge); err != nil {
		return err
	}
	if err := sa.SetWorkModePriority(ctx, workmodepriority); err != nil {
		return err
	}

	switch workmodepriority {
	case scheduler.WorkModeBatteryFirst:
		if err := sa.SetLoadFirstStopDischarge(ctx, soc); err != nil {
			return err
		}
	default:
		// always allow discharging to 10 (minimum) unless explicitly grid charging
		if err := sa.SetLoadFirstStopDischarge(ctx, soc); err != nil {
			return err
		}
	}
	processSuccess.Add(ctx, 1)
	return nil
}
func (sa *SolarAssistant) SetBatteryFirstGridCharge(ctx context.Context, enabled string) error {
	if enabled != scheduler.BatteryFirstGridChargeEnabled && enabled != scheduler.BatteryFirstGridChargeDisabled {
		return fmt.Errorf("unknown Battery First Grid Charge: %s", enabled)
	}

	sEnabled, err := sa.stateHandler.GetBatteryFirstGridCharge(ctx)
	if err != nil {
		return err
	}
	if enabled == sEnabled {
		return nil
	}
	if token := sa.client.Publish(TopicBatteryFirstGridCharge, 0, false, enabled); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	if err = sa.stateHandler.SetBatteryFirstGridChargeTarget(ctx, enabled); err != nil {
		return err
	}
	return nil
}

func (sa *SolarAssistant) SetWorkModePriority(ctx context.Context, workmode string) error {
	if !slices.Contains(workmodes, workmode) {
		return fmt.Errorf("unknown work mode: %s", workmode)
	}

	sWorkMode, err := sa.stateHandler.GetDeviceMode(ctx)
	if err != nil {
		return err
	}
	if workmode == sWorkMode {
		return nil
	}

	if token := sa.client.Publish(TopicWorkModePriority, 0, false, workmode); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return sa.stateHandler.SetDeviceModeTarget(ctx, workmode)
}

func (sa *SolarAssistant) SetLoadFirstStopDischarge(ctx context.Context, soc int64) error {
	if soc < 10 {
		soc = 10
	}

	start := time.Now()
	if token := sa.client.Publish(TopicLoadFirstStopDischarge, 0, false, fmt.Sprintf("%d", soc)); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	timeout := time.After(time.Minute)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	logger := sa.logger.With(slog.Int64("want", soc))
applied:
	for {
		loadFirstStopDischarge, err := sa.stateHandler.GetLoadFirstStopDischarge(ctx)
		if err != nil {
			return err
		}

		if loadFirstStopDischarge == soc {
			logger.Info("applied", slog.Duration("seconds", time.Since(start)))
			break applied
		}
		select {
		case <-timeout:
			return fmt.Errorf("timeout waiting for LoadFirstStopDischarge to become %d", soc)
		case <-ticker.C:
			logger.Info("waiting for LoadFirstStopDischarge to apply", slog.Int64("have", loadFirstStopDischarge))
		}
	}

	return sa.SetTargetSoc(ctx, soc)
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

func WithStateHandler(ctx context.Context, handler store.StateAccessorStore) Option {
	return func(s *SolarAssistant) error {
		sh := &stateHandler{
			state:  &state{},
			target: &state{},
			parent: handler,
		}
		if err := sh.Load(ctx); err != nil {
			return err
		}
		s.stateHandler = sh
		return nil
	}
}

func New(ctx context.Context, logger *slog.Logger, opts ...Option) (*SolarAssistant, error) {
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
		subFunc := func(c mqtt.Client) {
			sa.subscribers(ctx, c)
		}
		mqttOpts.SetOnConnectHandler(subFunc)
		mqttOpts.SetAutoReconnect(true)
		mqttOpts.SetResumeSubs(true)
		tlsConfig := &tls.Config{InsecureSkipVerify: true, ClientAuth: tls.NoClientCert}
		mqttOpts.SetTLSConfig(tlsConfig)
		sa.client = mqtt.NewClient(mqttOpts)
	}

	return sa, nil
}

func (sa *SolarAssistant) observe(ctx context.Context, obs api.Observer) error {
	soc, err := sa.GetCurrentSOC(ctx)
	if err != nil {
		return err
	}
	currentAttrs := attribute.Key("state").String(store.StateCurrent)
	targetAttrs := attribute.Key("state").String(store.StateTarget)
	obs.ObserveInt64(stateOfCharge, soc, api.WithAttributes(currentAttrs))
	deviceMode, err := sa.GetCurrentDeviceMode(ctx)
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
	targetDeviceMode, err := sa.GetTargetDeviceMode(ctx)
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
	currentBatteryFirstGridCharge, err := sa.GetCurrentBatteryFirstGridCharge(ctx)
	if err != nil {
		return err
	}
	switch currentBatteryFirstGridCharge {
	case scheduler.BatteryFirstGridChargeEnabled:
		obs.ObserveInt64(batteryFirstGridChargeEnabled, 1, api.WithAttributes(currentAttrs))
	default:
		obs.ObserveInt64(batteryFirstGridChargeEnabled, 0, api.WithAttributes(currentAttrs))
	}

	targetBatteryFirstGridCharge, err := sa.GetTargetBatteryFirstGridCharge(ctx)
	if err != nil {
		return err
	}
	switch targetBatteryFirstGridCharge {
	case scheduler.BatteryFirstGridChargeEnabled:
		obs.ObserveInt64(batteryFirstGridChargeEnabled, 1, api.WithAttributes(targetAttrs))
	default:
		obs.ObserveInt64(batteryFirstGridChargeEnabled, 0, api.WithAttributes(targetAttrs))
	}
	tSoc, err := sa.GetTargetSoc(ctx)
	if err != nil {
		return err
	}
	obs.ObserveInt64(stopDischargeAtSOC, tSoc, api.WithAttributes(targetAttrs))
	return nil

}

func (sa *SolarAssistant) subscribers(ctx context.Context, c mqtt.Client) {
	if token := c.Subscribe(TopicSOCState, 0, func(client mqtt.Client, msg mqtt.Message) {
		batterySOC, err := strconv.ParseFloat(string(msg.Payload()), 32)
		if err != nil {
			return
		}
		if err = sa.SetCurrentSOC(ctx, int64(batterySOC)); err != nil {
			sa.logger.Warn("error setting soc", slog.Int64("soc", int64(batterySOC)))
		}
	}); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	if token := c.Subscribe(TopicDeviceModeState, 0, func(client mqtt.Client, msg mqtt.Message) {
		if err := sa.SetCurrentDeviceMode(ctx, string(msg.Payload())); err != nil {
			sa.logger.Warn("error setting device mode", slog.String("payload", string(msg.Payload())))
		}
	}); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	if token := c.Subscribe(TopicBatteryFirstGridChargeState, 0, func(client mqtt.Client, msg mqtt.Message) {
		if err := sa.SetCurrentBatteryFirstGridCharge(ctx, string(msg.Payload())); err != nil {
			sa.logger.Warn("error setting battery charge", slog.String("payload", string(msg.Payload())))
		}
	}); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := c.Subscribe(TopicLoadFirstStopDischargeState, 0, func(client mqtt.Client, msg mqtt.Message) {
		soc, err := strconv.ParseInt(string(msg.Payload()), 10, 64)
		if err != nil {
			sa.logger.Error(err.Error(), slog.String("payload", string(msg.Payload())))
			return
		}
		if err = sa.SetLoadFirstStopDischargeState(ctx, soc); err != nil {
			sa.logger.Error(err.Error(), slog.String("payload", string(msg.Payload())))
			return
		}
	}); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
}

func (sa *SolarAssistant) SetCurrentSOC(ctx context.Context, soc int64) error {
	if err := sa.stateHandler.SetSOC(ctx, soc); err != nil {
		return err
	}
	// if the target is less or equal to current soc, revert to grid
	tSoc, err := sa.stateHandler.GetSOCTarget(ctx)
	if err != nil {
		return err
	}

	sBatteryFirstGridCharge, err := sa.stateHandler.GetBatteryFirstGridCharge(ctx)
	if err != nil {
		return err
	}

	sDeviceMode, err := sa.stateHandler.GetDeviceMode(ctx)
	if err != nil {
		return err
	}
	if soc >= tSoc &&
		sBatteryFirstGridCharge == scheduler.BatteryFirstGridChargeEnabled &&
		sDeviceMode == scheduler.WorkModeBatteryFirst {
		// set to load first
		if err = sa.SetWorkModePriority(ctx, scheduler.WorkModeLoadFirst); err != nil {
			return err
		}
		// disable grid charging
		if err = sa.SetBatteryFirstGridCharge(ctx, scheduler.BatteryFirstGridChargeDisabled); err != nil {
			return err
		}
	}
	return nil
}

func (sa *SolarAssistant) SetCurrentDeviceMode(ctx context.Context, mode string) error {
	return sa.stateHandler.SetDeviceMode(ctx, mode)
}

func (sa *SolarAssistant) SetCurrentBatteryFirstGridCharge(ctx context.Context, gridFirstBatteryCharge string) error {
	return sa.stateHandler.SetBatteryFirstGridCharge(ctx, gridFirstBatteryCharge)
}

func (sa *SolarAssistant) SetLoadFirstStopDischargeState(ctx context.Context, soc int64) error {
	return sa.stateHandler.SetLoadFirstStopDischarge(ctx, soc)
}

func (sa *SolarAssistant) GetCurrentSOC(ctx context.Context) (int64, error) {
	return sa.stateHandler.GetSOC(ctx) // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) GetCurrentDeviceMode(ctx context.Context) (string, error) {
	return sa.stateHandler.GetDeviceMode(ctx) // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) GetCurrentBatteryFirstGridCharge(ctx context.Context) (string, error) {
	return sa.stateHandler.GetBatteryFirstGridCharge(ctx) // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) GetTargetSoc(ctx context.Context) (int64, error) {
	return sa.stateHandler.GetSOCTarget(ctx) // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) SetTargetSoc(ctx context.Context, soc int64) error {
	return sa.stateHandler.SetSOCTarget(ctx, soc) // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) GetTargetDeviceMode(ctx context.Context) (string, error) {
	return sa.stateHandler.GetDeviceModeTarget(ctx) // remove error later? or keep for compatibility?
}

func (sa *SolarAssistant) GetTargetBatteryFirstGridCharge(ctx context.Context) (string, error) {
	return sa.stateHandler.GetBatteryFirstGridChargeTarget(ctx) // remove error later? or keep for compatibility?
}
