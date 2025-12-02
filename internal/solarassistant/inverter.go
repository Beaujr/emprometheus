package solarassistant

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/beaujr/emprometheus/internal/scheduler"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"
)

const (
	topicBatteryFirstGridCharge      = "solar_assistant/inverter_1/battery_first_grid_charge/set"
	topicWorkModePriority            = "solar_assistant/inverter_1/work_mode_priority/set"
	topicLoadFirstStopDischarge      = "solar_assistant/inverter_1/load_first_stop_discharge/set"
	topicSOCState                    = "solar_assistant/total/battery_state_of_charge/state"
	topicDeviceModeState             = "solar_assistant/inverter_1/device_mode/state"
	topicBatteryFirstGridChargeState = "solar_assistant/inverter_1/battery_first_grid_charge/state"
)

var (
	mqttServer   = flag.String("mqtt_server", "tcp://mqtt.whickerx.info:9229", "mqtt server address")
	mqttUser     = flag.String("mqtt_user", "", "mqtt user")
	mqttPassword = flag.String("mqtt_password", "", "mqtt password")
	workmodes    = []string{scheduler.WorkModeLoadFirst, scheduler.WorkModeBatteryFirst, scheduler.WorkModeGridFirst}
)

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
}

func (sa *SolarAssistant) Start(ctx context.Context) error {
	if token := sa.client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (sa *SolarAssistant) SetBatteryFirstGridCharge(enabled string) error {
	if enabled != scheduler.BatteryFirstGridChargeEnabled && enabled != scheduler.BatteryFirstGridChargeDisabled {
		return fmt.Errorf("unknown Batter yFirst Grid Charge: %s", enabled)
	}
	if token := sa.client.Publish(topicBatteryFirstGridCharge, 0, false, enabled); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	sa.stateHandler.SetBatteryFirstGridChargeTarget(enabled)
	return nil
}

func (sa *SolarAssistant) SetWorkModePriority(workmode string) error {
	if !slices.Contains(workmodes, workmode) {
		return fmt.Errorf("unknown work mode: %s", workmode)
	}

	if token := sa.client.Publish(topicWorkModePriority, 0, false, workmode); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	sa.stateHandler.SetDeviceModeTarget(workmode)
	return nil
}

func (sa *SolarAssistant) SetLoadFirstStopDischarge(soc int64) error {
	if soc < 10 {
		soc = 10
	}
	if token := sa.client.Publish(topicLoadFirstStopDischarge, 0, false, fmt.Sprintf("%d", soc)); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	sa.stateHandler.SetSOCTarget(soc)
	return nil
}

func New() (*SolarAssistant, error) {
	if !flag.Parsed() {
		flag.Parse()
	}
	sa := &SolarAssistant{stateHandler: &stateHandler{state: &state{}, target: &state{}}}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	opts := mqtt.NewClientOptions().AddBroker(*mqttServer).SetClientID(hostname).SetCleanSession(true)
	if len(*mqttUser) > 0 {
		opts.SetCredentialsProvider(func() (username string, password string) {
			return *mqttUser, *mqttPassword
		})
	}

	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(60 * time.Second)
	opts.SetOnConnectHandler(sa.subscribers)
	opts.SetAutoReconnect(true)
	opts.SetResumeSubs(true)
	tlsConfig := &tls.Config{InsecureSkipVerify: true, ClientAuth: tls.NoClientCert}
	opts.SetTLSConfig(tlsConfig)
	sa.client = mqtt.NewClient(opts)
	return sa, nil
}

func (sa *SolarAssistant) subscribers(c mqtt.Client) {
	if token := c.Subscribe(topicSOCState, 0, sa.socHandler); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	if token := c.Subscribe(topicDeviceModeState, 0, sa.deviceModeHandler); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	if token := c.Subscribe(topicBatteryFirstGridChargeState, 0, sa.gridFirstBatteryChargerHandler); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
}

func (sa *SolarAssistant) socHandler(client mqtt.Client, msg mqtt.Message) {
	soc, err := strconv.ParseInt(string(msg.Payload()), 10, 64)
	if err != nil {
		return
	}
	sa.stateHandler.SetSOC(soc)
	// if the target is less or equal to current soc, revert to grid
	if soc >= sa.stateHandler.target.soc &&
		sa.stateHandler.state.gridCharge == scheduler.BatteryFirstGridChargeEnabled &&
		sa.stateHandler.state.deviceMode == scheduler.WorkModeBatteryFirst {
		// set to load first
		sa.SetWorkModePriority(scheduler.WorkModeLoadFirst)
		// disable grid charging
		sa.SetBatteryFirstGridCharge(scheduler.BatteryFirstGridChargeDisabled)
	}
}

func (sa *SolarAssistant) deviceModeHandler(client mqtt.Client, msg mqtt.Message) {
	sa.stateHandler.SetDeviceMode(string(msg.Payload()))
}

func (sa *SolarAssistant) gridFirstBatteryChargerHandler(client mqtt.Client, msg mqtt.Message) {
	sa.stateHandler.SetBatteryFirstGridCharge(string(msg.Payload()))
}

func (sa *SolarAssistant) GetSOC() (int64, error) {
	return sa.stateHandler.GetSOC(), nil // remove error later? or keep for compatibility?
}
