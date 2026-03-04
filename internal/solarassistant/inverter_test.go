package solarassistant_test

import (
	"github.com/beaujr/emprometheus/internal/scheduler"
	"github.com/beaujr/emprometheus/internal/solarassistant"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"log/slog"
	"strconv"
	"testing"
	"time"
)

type fakemsg struct {
	topic, payload string
}

func (fk fakemsg) Duplicate() bool {
	return true
}

func (fk fakemsg) Qos() byte {
	return 0
}

func (fk fakemsg) Retained() bool {
	return false
}

func (fk fakemsg) Topic() string {
	return fk.topic
}

func (fk fakemsg) MessageID() uint16 {
	return 0
}

func (fk fakemsg) Payload() []byte {
	return []byte(fk.payload)
}

func (fk fakemsg) Ack() {
}

type fakeclient struct {
	plant scheduler.ControllablePowerPlant
}

func (f fakeclient) IsConnected() bool {
	return true
}

func (f fakeclient) IsConnectionOpen() bool {
	return true
}

func (f fakeclient) Connect() mqtt.Token {
	return &mqtt.ConnectToken{}
}

func (f fakeclient) Disconnect(quiesce uint) {
	return
}

func (f fakeclient) Publish(topic string, qos byte, retained bool, msg interface{}) mqtt.Token {
	switch topic {
	case solarassistant.TopicSOCState:
		batterySOC, err := strconv.ParseFloat(string(msg.([]byte)), 32)
		if err != nil {
			return fakeToken{}
		}
		f.plant.SetCurrentSOC(int64(batterySOC))
	case solarassistant.TopicWorkModePriority:
		f.plant.SetCurrentDeviceMode(msg.(string))
	case solarassistant.TopicBatteryFirstGridCharge:
		f.plant.SetCurrentBatteryFirstGridCharge(msg.(string))
	}
	return fakeToken{}
}

func (f fakeclient) Subscribe(topic string, qos byte, callback mqtt.MessageHandler) mqtt.Token {
	return fakeToken{}
}

func (f fakeclient) SubscribeMultiple(filters map[string]byte, callback mqtt.MessageHandler) mqtt.Token {
	return fakeToken{}
}

func (f fakeclient) Unsubscribe(topics ...string) mqtt.Token {
	return fakeToken{}
}

func (f fakeclient) AddRoute(topic string, callback mqtt.MessageHandler) {
	return
}

func (f fakeclient) OptionsReader() mqtt.ClientOptionsReader {
	return mqtt.ClientOptionsReader{}
}

type fakeToken struct {
}

func (f fakeToken) Wait() bool {
	return true
}

func (f fakeToken) WaitTimeout(duration time.Duration) bool {
	return true
}

func (f fakeToken) Done() <-chan struct{} {
	return nil
}

func (f fakeToken) Error() error {
	return nil
}

func TestSolarAssistant(t *testing.T) {
	var p scheduler.ControllablePowerPlant
	fc := fakeclient{plant: p}
	s, err := solarassistant.New(slog.New(slog.DiscardHandler), solarassistant.WithMqttClient(&fc))
	if err != nil {
		t.Fatal(err)
		return
	}
	fc.plant = s

	if err = s.SetLoadFirstStopDischarge(100); err != nil {
		t.Fatal(err)
		return
	}
	if err = s.SetWorkModePriority(scheduler.WorkModeLoadFirst); err != nil {
		t.Fatal(err)
		return
	}
	if err = s.SetLoadFirstStopDischarge(100); err != nil {
		t.Fatal(err)
		return
	}
	if err = s.SetCurrentSOC(100); err != nil {
		t.Fatal(err)
	}
	soc, err := s.GetCurrentSOC()
	if err != nil {
		t.Fatal(err)
		return
	}
	if soc != 100 {
		t.Fail()
	}
}

var scenarios = []struct {
	name                           string
	initialBattery                 int64
	initialBatteryFirstGridCharge  string
	initialWorkModePriority        string
	targetBattery                  int64
	targetBatteryFirstGridCharge   string
	targetWorkModePriority         string
	currentBattery                 int64
	expectedBattery                int64
	expectedBatteryFirstGridCharge string
	expectedWorkModePriority       string
}{
	{
		name:                           "Battery Charging, Current SOC Below target",
		initialBattery:                 10,
		initialBatteryFirstGridCharge:  scheduler.BatteryFirstGridChargeDisabled,
		initialWorkModePriority:        scheduler.WorkModeLoadFirst,
		targetBattery:                  100,
		targetBatteryFirstGridCharge:   scheduler.BatteryFirstGridChargeEnabled,
		targetWorkModePriority:         scheduler.WorkModeBatteryFirst,
		currentBattery:                 11,
		expectedBattery:                11,
		expectedBatteryFirstGridCharge: scheduler.BatteryFirstGridChargeEnabled,
		expectedWorkModePriority:       scheduler.WorkModeBatteryFirst,
	},
	{
		name:                           "Already Charged Scenario",
		initialBattery:                 100,
		initialBatteryFirstGridCharge:  scheduler.BatteryFirstGridChargeDisabled,
		initialWorkModePriority:        scheduler.WorkModeLoadFirst,
		targetBattery:                  50,
		targetBatteryFirstGridCharge:   scheduler.BatteryFirstGridChargeEnabled,
		targetWorkModePriority:         scheduler.WorkModeBatteryFirst,
		currentBattery:                 99,
		expectedBattery:                99,
		expectedBatteryFirstGridCharge: scheduler.BatteryFirstGridChargeDisabled,
		expectedWorkModePriority:       scheduler.WorkModeLoadFirst,
	},
	{
		name:                           "Discharge below target Scenario",
		initialBattery:                 50,
		initialBatteryFirstGridCharge:  scheduler.BatteryFirstGridChargeDisabled,
		initialWorkModePriority:        scheduler.WorkModeLoadFirst,
		targetBattery:                  50,
		targetBatteryFirstGridCharge:   scheduler.BatteryFirstGridChargeEnabled,
		targetWorkModePriority:         scheduler.WorkModeBatteryFirst,
		currentBattery:                 40,
		expectedBattery:                40,
		expectedBatteryFirstGridCharge: scheduler.BatteryFirstGridChargeEnabled,
		expectedWorkModePriority:       scheduler.WorkModeBatteryFirst,
	},
	{
		name:                           "Discharge above target Scenario",
		initialBattery:                 50,
		initialBatteryFirstGridCharge:  scheduler.BatteryFirstGridChargeDisabled,
		initialWorkModePriority:        scheduler.WorkModeLoadFirst,
		targetBattery:                  60,
		targetBatteryFirstGridCharge:   scheduler.BatteryFirstGridChargeEnabled,
		targetWorkModePriority:         scheduler.WorkModeBatteryFirst,
		currentBattery:                 70,
		expectedBattery:                70,
		expectedBatteryFirstGridCharge: scheduler.BatteryFirstGridChargeDisabled,
		expectedWorkModePriority:       scheduler.WorkModeLoadFirst,
	},
	{
		name:                           "Start Discharging",
		initialBattery:                 100,
		initialBatteryFirstGridCharge:  scheduler.BatteryFirstGridChargeEnabled,
		initialWorkModePriority:        scheduler.WorkModeBatteryFirst,
		targetBattery:                  10,
		targetBatteryFirstGridCharge:   scheduler.BatteryFirstGridChargeDisabled,
		targetWorkModePriority:         scheduler.WorkModeLoadFirst,
		currentBattery:                 40,
		expectedBattery:                40,
		expectedBatteryFirstGridCharge: scheduler.BatteryFirstGridChargeDisabled,
		expectedWorkModePriority:       scheduler.WorkModeLoadFirst,
	},
}

func TestIntervertCurrentSOCHigherThanTargetSOC(t *testing.T) {
	var p scheduler.ControllablePowerPlant
	fc := fakeclient{plant: p}
	s, err := solarassistant.New(slog.New(slog.DiscardHandler), solarassistant.WithMqttClient(&fc))
	if err != nil {
		t.Fatal(err)
		return
	}
	fc.plant = s
	for _, tt := range scenarios {
		t.Run(tt.name, func(t *testing.T) {
			// init
			if err = s.SetCurrentSOC(tt.initialBattery); err != nil {
				t.Fatal(err)
				return
			}
			s.SetCurrentDeviceMode(tt.initialWorkModePriority)
			s.SetCurrentBatteryFirstGridCharge(tt.initialBatteryFirstGridCharge)

			// set inverter state
			if err = s.Process(t.Context(), tt.targetBatteryFirstGridCharge, tt.targetWorkModePriority, tt.targetBattery); err != nil {
				t.Fatal(err)
				return
			}

			// set some realtime change
			//s.SetCurrentDeviceMode(tt.currentWorkModePriority)
			//s.SetCurrentBatteryFirstGridCharge(tt.currentBatteryFirstGridCharge)
			s.SetCurrentSOC(tt.currentBattery)

			// review targets being handled
			//targetSoC, err := s.GetTargetSOC()
			//if err != nil {
			//	t.Fatal(err)
			//	return
			//}
			//if targetSoC != tt.targetBattery {
			//	t.Fail()
			//}
			//
			//if s.GetTargetBatteryFirstGridCharge() != tt.targetBatteryFirstGridCharge {
			//	t.Fail()
			//}
			//if s.GetTargetDeviceMode() != tt.targetWorkModePriority {
			//	t.Fail()
			//}

			soc, err := s.GetCurrentSOC()
			if err != nil {
				t.Fatal(err)
				return
			}
			if soc != tt.expectedBattery {
				t.Fatal(err)
				return
			}

			if s.GetCurrentDeviceMode() != tt.expectedWorkModePriority {
				t.Fail()
			}
			if s.GetCurrentBatteryFirstGridCharge() != tt.expectedBatteryFirstGridCharge {
				t.Fail()
			}
		})
	}
}
