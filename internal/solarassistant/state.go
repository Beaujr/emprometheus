package solarassistant

import (
	"sync"

	"github.com/beaujr/emprometheus/internal/store"
)

type state struct {
	soc, loadFirstStopDischarge int64
	deviceMode, gridCharge      string
}

func (s *state) GetLoadFirstStopDischarge() (int64, error) {
	return s.loadFirstStopDischarge, nil
}

func (s *state) SetLoadFirstStopDischarge(soc int64) error {
	s.loadFirstStopDischarge = soc
	return nil
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

func (h *stateHandler) GetLoadFirstStopDischarge() (int64, error) {
	h.Lock()
	defer h.Unlock()
	return h.state.GetLoadFirstStopDischarge()

}

func (h *stateHandler) SetLoadFirstStopDischarge(soc int64) error {
	h.Lock()
	defer h.Unlock()
	current, err := h.state.GetLoadFirstStopDischarge()
	if err != nil {
		return err
	}
	if soc != current {
		if h.parent != nil {
			if err = h.parent.SetLoadFirstStopDischarge(soc); err != nil {
				return err
			}

		}
		if err = h.state.SetLoadFirstStopDischarge(soc); err != nil {
			return err
		}
	}
	return nil
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
	loadFirstStopDischarge, err := h.parent.GetLoadFirstStopDischarge()
	if err != nil {
		return err
	}
	// set the local mirror of the mqtt topic to 10 (the minimum)
	err = h.state.SetLoadFirstStopDischarge(loadFirstStopDischarge)
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
			if err = h.parent.SetBatteryFirstGridCharge(enabled); err != nil {
				return err
			}
		}
		if err = h.state.SetGridCharge(enabled); err != nil {
			return err
		}
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
			if err = h.parent.SetSOC(soc); err != nil {
				return err
			}
		}
		if err = h.state.SetSoc(soc); err != nil {
			return err
		}
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
