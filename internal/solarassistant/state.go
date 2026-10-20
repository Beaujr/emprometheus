package solarassistant

import (
	"context"
	"sync"

	"github.com/beaujr/emprometheus/internal/store"
)

type state struct {
	soc, loadFirstStopDischarge int64
	deviceMode, gridCharge      string
}

func (s *state) GetLoadFirstStopDischarge(ctx context.Context) (int64, error) {
	return s.loadFirstStopDischarge, nil
}

func (s *state) SetLoadFirstStopDischarge(ctx context.Context, soc int64) error {
	s.loadFirstStopDischarge = soc
	return nil
}

func (s *state) SetSoc(ctx context.Context, soc int64) error {
	s.soc = soc
	return nil
}

func (s *state) SetDeviceMode(ctx context.Context, deviceMode string) error {
	s.deviceMode = deviceMode
	return nil
}

func (s *state) SetGridCharge(ctx context.Context, gridCharge string) error {
	s.gridCharge = gridCharge
	return nil
}
func (s *state) GetSoc(ctx context.Context) (int64, error) {
	return s.soc, nil
}

func (s *state) GetDeviceMode(ctx context.Context) (string, error) {
	return s.deviceMode, nil
}

func (s *state) GetGridCharge(ctx context.Context) (string, error) {
	return s.gridCharge, nil
}

type stateHandler struct {
	sync.Mutex
	state  store.StateStore
	target store.StateStore
	parent store.StateAccessorStore
}

func (h *stateHandler) GetLoadFirstStopDischarge(ctx context.Context) (int64, error) {
	h.Lock()
	defer h.Unlock()
	return h.state.GetLoadFirstStopDischarge(ctx)

}

func (h *stateHandler) SetLoadFirstStopDischarge(ctx context.Context, soc int64) error {
	h.Lock()
	defer h.Unlock()
	current, err := h.state.GetLoadFirstStopDischarge(ctx)
	if err != nil {
		return err
	}
	if soc != current {
		if h.parent != nil {
			if err = h.parent.SetLoadFirstStopDischarge(ctx, soc); err != nil {
				return err
			}

		}
		if err = h.state.SetLoadFirstStopDischarge(ctx, soc); err != nil {
			return err
		}
	}
	return nil
}

func (h *stateHandler) Load(ctx context.Context) error {
	socTarget, err := h.parent.GetSOCTarget(ctx)
	if err != nil {
		return err
	}
	err = h.target.SetSoc(ctx, socTarget)
	if err != nil {
		return err
	}
	soc, err := h.parent.GetSOC(ctx)
	if err != nil {
		return err
	}
	err = h.state.SetSoc(ctx, soc)
	if err != nil {
		return err
	}
	deviceMode, err := h.parent.GetDeviceMode(ctx)
	if err != nil {
		return err
	}
	err = h.state.SetDeviceMode(ctx, deviceMode)
	if err != nil {
		return err
	}
	deviceModeTarget, err := h.parent.GetDeviceModeTarget(ctx)
	if err != nil {
		return err
	}
	err = h.target.SetDeviceMode(ctx, deviceModeTarget)
	if err != nil {
		return err
	}
	gridCharge, err := h.parent.GetBatteryFirstGridCharge(ctx)
	if err != nil {
		return err
	}
	err = h.state.SetGridCharge(ctx, gridCharge)
	if err != nil {
		return err
	}
	gridChargeTarget, err := h.parent.GetBatteryFirstGridChargeTarget(ctx)
	if err != nil {
		return err
	}
	err = h.target.SetGridCharge(ctx, gridChargeTarget)
	if err != nil {
		return err
	}
	loadFirstStopDischarge, err := h.parent.GetLoadFirstStopDischarge(ctx)
	if err != nil {
		return err
	}
	// set the local mirror of the mqtt topic to 10 (the minimum)
	err = h.state.SetLoadFirstStopDischarge(ctx, loadFirstStopDischarge)
	if err != nil {
		return err
	}
	return nil
}

func (h *stateHandler) SetBatteryFirstGridCharge(ctx context.Context, enabled string) error {
	h.Lock()
	defer h.Unlock()
	sEnabled, err := h.state.GetGridCharge(ctx)
	if err != nil {
		return err
	}
	if enabled != sEnabled {
		if h.parent != nil {
			if err = h.parent.SetBatteryFirstGridCharge(ctx, enabled); err != nil {
				return err
			}
		}
		if err = h.state.SetGridCharge(ctx, enabled); err != nil {
			return err
		}
	}
	return nil
}

func (h *stateHandler) SetSOC(ctx context.Context, soc int64) error {
	h.Lock()
	defer h.Unlock()
	sSoc, err := h.state.GetSoc(ctx)
	if err != nil {
		return err
	}
	if soc != sSoc {
		if h.parent != nil {
			if err = h.parent.SetSOC(ctx, soc); err != nil {
				return err
			}
		}
		if err = h.state.SetSoc(ctx, soc); err != nil {
			return err
		}
	}
	return nil
}

func (h *stateHandler) SetDeviceMode(ctx context.Context, mode string) error {
	h.Lock()
	defer h.Unlock()
	sMode, err := h.state.GetDeviceMode(ctx)
	if err != nil {
		return nil
	}
	if mode != sMode {
		if h.parent != nil {
			err = h.parent.SetDeviceMode(ctx, mode)
			if err != nil {
				return nil
			}
		}
		err = h.state.SetDeviceMode(ctx, mode)
		if err != nil {
			return nil
		}
	}
	return nil
}

func (h *stateHandler) GetBatteryFirstGridCharge(ctx context.Context) (string, error) {
	h.Lock()
	defer h.Unlock()
	return h.state.GetGridCharge(ctx)
}

func (h *stateHandler) GetSOC(ctx context.Context) (int64, error) {
	h.Lock()
	defer h.Unlock()
	return h.state.GetSoc(ctx)
}

func (h *stateHandler) GetDeviceMode(ctx context.Context) (string, error) {
	h.Lock()
	defer h.Unlock()
	return h.state.GetDeviceMode(ctx)
}

func (h *stateHandler) SetBatteryFirstGridChargeTarget(ctx context.Context, enabled string) error {
	h.Lock()
	defer h.Unlock()
	sEnabled, err := h.state.GetGridCharge(ctx)
	if err != nil {
		return err
	}
	if enabled != sEnabled {
		if h.parent != nil {
			if err = h.parent.SetBatteryFirstGridChargeTarget(ctx, enabled); err != nil {
				return err
			}
		}
		if err = h.target.SetGridCharge(ctx, enabled); err != nil {
			return err
		}
	}
	return nil
}

func (h *stateHandler) SetSOCTarget(ctx context.Context, soc int64) error {
	h.Lock()
	defer h.Unlock()
	tSoc, err := h.target.GetSoc(ctx)
	if err != nil {
		return err
	}
	if soc != tSoc {
		if h.parent != nil {
			if err = h.parent.SetSOCTarget(ctx, soc); err != nil {
				return err
			}
		}
		if err = h.target.SetSoc(ctx, soc); err != nil {
			return err
		}
	}
	return nil
}

func (h *stateHandler) SetDeviceModeTarget(ctx context.Context, mode string) error {
	h.Lock()
	defer h.Unlock()
	sMode, err := h.target.GetDeviceMode(ctx)
	if err != nil {
		return err
	}
	if sMode != mode {
		if h.parent != nil {
			if err = h.parent.SetDeviceModeTarget(ctx, mode); err != nil {
				return err
			}
		}
		if err = h.target.SetDeviceMode(ctx, mode); err != nil {
			return err
		}
	}
	return nil
}
func (h *stateHandler) GetBatteryFirstGridChargeTarget(ctx context.Context) (string, error) {
	h.Lock()
	defer h.Unlock()
	return h.target.GetGridCharge(ctx)
}

func (h *stateHandler) GetSOCTarget(ctx context.Context) (int64, error) {
	h.Lock()
	defer h.Unlock()
	return h.target.GetSoc(ctx)
}

func (h *stateHandler) GetDeviceModeTarget(ctx context.Context) (string, error) {
	h.Lock()
	defer h.Unlock()
	return h.target.GetDeviceMode(ctx)
}
