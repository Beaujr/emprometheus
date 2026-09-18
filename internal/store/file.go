package store

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/beaujr/emprometheus/internal/types"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// fileState is the JSON-serialised inverter settings persisted by FileStore.
type fileState struct {
	CurrentSOC                    int64  `json:"current_soc"`
	CurrentDeviceMode             string `json:"current_device_mode"`
	CurrentGridCharge             string `json:"current_grid_charge"`
	CurrentLoadFirstStopDischarge int64  `json:"current_load_first_stop_discharge"`
	TargetSOC                     int64  `json:"target_soc"`
	TargetDeviceMode              string `json:"target_device_mode"`
	TargetGridCharge              string `json:"target_grid_charge"`
}

// defaultFileState mirrors the seed values used by the Postgres migration.
var defaultFileState = fileState{
	CurrentSOC:                    10,
	CurrentDeviceMode:             "Load First",
	CurrentGridCharge:             "Disabled",
	CurrentLoadFirstStopDischarge: 10,
	TargetSOC:                     10,
	TargetDeviceMode:              "Load First",
	TargetGridCharge:              "Disabled",
}

// FileStore is a file-backed implementation of store.Store.
//
//   - Schedule rows are stored as CSV lines in filepath.
//   - Inverter state is persisted as JSON in statepath.
//   - Optimization results are stored as CSV lines in optimpath.
type FileStore struct {
	filepath  string // schedule CSV
	statepath string // inverter state JSON
	optimpath string // optimization results CSV
	mu        sync.RWMutex
}

// newFileStore creates a FileStore rooted at dir/name. The state and
// optimisation files are derived from name automatically.
func newFileStore(dir string, name string) (*FileStore, error) {
	base := strings.TrimSuffix(name, filepath.Ext(name))
	fs := &FileStore{
		filepath:  filepath.Join(dir, name),
		statepath: filepath.Join(dir, base+"_state.json"),
		optimpath: filepath.Join(dir, base+"_optimization.csv"),
	}
	file, err := os.OpenFile(fs.filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	return fs, file.Close()
}

func (f *FileStore) Close() error { return nil }

// ── state file helpers ────────────────────────────────────────────────────────

func (f *FileStore) readState() (fileState, error) {
	data, err := os.ReadFile(f.statepath)
	if os.IsNotExist(err) {
		return defaultFileState, nil
	}
	if err != nil {
		return fileState{}, err
	}
	var s fileState
	if err = json.Unmarshal(data, &s); err != nil {
		return fileState{}, err
	}
	return s, nil
}

func (f *FileStore) writeState(s fileState) error {
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return os.WriteFile(f.statepath, data, 0600)
}

// ── CSV file helpers ──────────────────────────────────────────────────────────

func (f *FileStore) readLines(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var lines []string
	for _, line := range strings.Split(string(data), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines, nil
}

func (f *FileStore) writeLines(path string, lines []string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, line := range lines {
		if _, err = file.WriteString(line + "\n"); err != nil {
			return err
		}
	}
	return nil
}

// parseOptimizationLine parses a line in the format produced by
// OptimizationResult.String() (fixed 14-column CSV).
func parseOptimizationLine(line string) (types.OptimizationResult, error) {
	r := csv.NewReader(strings.NewReader(line))
	record, err := r.Read()
	if err != nil {
		return types.OptimizationResult{}, err
	}
	if len(record) < 14 {
		return types.OptimizationResult{}, fmt.Errorf("expected 14 fields, got %d", len(record))
	}
	t, err := time.Parse("2006-01-02 15:04:05-07:00", record[1])
	if err != nil {
		return types.OptimizationResult{}, fmt.Errorf("parse time: %w", err)
	}
	indices := [11]int{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	var vals [11]float64
	for i, idx := range indices {
		vals[i], err = strconv.ParseFloat(record[idx], 64)
		if err != nil {
			return types.OptimizationResult{}, fmt.Errorf("parse field %d: %w", idx, err)
		}
	}
	return types.NewOptimizationResultFull(
		record[0], t,
		vals[0], vals[1], vals[2], vals[3], vals[4], vals[5],
		vals[6], vals[7], vals[8], vals[9], vals[10],
		record[13],
	), nil
}

// ── OptimizationStore ─────────────────────────────────────────────────────────

func (f *FileStore) InsertOptimization(row types.OptimizationResult) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	lines, err := f.readLines(f.optimpath)
	if err != nil {
		return err
	}
	rowTimeStr := row.Time().Format("2006-01-02 15:04:05-07:00")
	updated := false
	for i, line := range lines {
		rec, err := csv.NewReader(strings.NewReader(line)).Read()
		if err == nil && len(rec) >= 2 && rec[0] == row.Optimization && rec[1] == rowTimeStr {
			lines[i] = row.String()
			updated = true
			break
		}
	}
	if !updated {
		lines = append(lines, row.String())
	}
	return f.writeLines(f.optimpath, lines)
}

func (f *FileStore) SelectOptimization(start time.Time, optimization string) ([]types.OptimizationResult, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	lines, err := f.readLines(f.optimpath)
	if err != nil {
		return nil, err
	}
	var results []types.OptimizationResult
	for _, line := range lines {
		row, err := parseOptimizationLine(line)
		if err != nil {
			return nil, err
		}
		if row.Optimization == optimization && !row.Time().Before(start) {
			results = append(results, row)
		}
	}
	return results, nil
}

func (f *FileStore) DeleteAnyOptimization(start time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	lines, err := f.readLines(f.optimpath)
	if err != nil {
		return err
	}
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		rec, err := csv.NewReader(strings.NewReader(line)).Read()
		if err != nil || len(rec) < 2 {
			filtered = append(filtered, line)
			continue
		}
		t, err := time.Parse("2006-01-02 15:04:05-07:00", rec[1])
		if err != nil || !t.Equal(start) {
			filtered = append(filtered, line)
		}
	}
	return f.writeLines(f.optimpath, filtered)
}

// SetActualSoc is a no-op for FileStore: OptimizationResult does not expose
// an actual-SOC field, and the inverter workflow only logs a warning on failure.
func (f *FileStore) SetActualSoc(_ time.Time, _ float64) error { return nil }

// ── MinimalStore / schedule rows ──────────────────────────────────────────────

func (f *FileStore) Insert(row Row) error { return f.Upsert(row) }

func (f *FileStore) Upsert(row Row) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	lines, err := f.readLines(f.filepath)
	if err != nil {
		return err
	}
	key := row.Time.Format(time.RFC3339)
	found := false
	for i, line := range lines {
		if strings.Contains(line, key) {
			lines[i] = row.String()
			found = true
			break
		}
	}
	if !found {
		lines = append(lines, row.String())
	}
	return f.writeLines(f.filepath, lines)
}

func (f *FileStore) Delete(row Row) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	lines, err := f.readLines(f.filepath)
	if err != nil {
		return err
	}
	key := row.Time.Format(time.RFC3339)
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		if !strings.Contains(line, key) {
			filtered = append(filtered, line)
		}
	}
	return f.writeLines(f.filepath, filtered)
}

func (f *FileStore) Find(start time.Time) (Row, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	lines, err := f.readLines(f.filepath)
	if err != nil {
		return Row{}, err
	}
	key := start.Format(time.RFC3339)
	for _, line := range lines {
		if strings.Contains(line, key) {
			return RowFromString(line)
		}
	}
	return Row{}, ErrNotFound
}

func (f *FileStore) Select(start time.Time) ([]Row, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	lines, err := f.readLines(f.filepath)
	if err != nil {
		return nil, err
	}
	var rows []Row
	for _, line := range lines {
		row, err := RowFromString(line)
		if err != nil {
			return nil, err
		}
		if row.Time.After(start) {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// ── StateAccessorStore ────────────────────────────────────────────────────────

func (f *FileStore) SetSOC(soc int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, err := f.readState()
	if err != nil {
		return err
	}
	s.CurrentSOC = soc
	return f.writeState(s)
}

func (f *FileStore) GetSOC() (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s, err := f.readState()
	return s.CurrentSOC, err
}

func (f *FileStore) SetDeviceMode(mode string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, err := f.readState()
	if err != nil {
		return err
	}
	s.CurrentDeviceMode = mode
	return f.writeState(s)
}

func (f *FileStore) GetDeviceMode() (string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s, err := f.readState()
	return s.CurrentDeviceMode, err
}

func (f *FileStore) SetBatteryFirstGridCharge(enabled string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, err := f.readState()
	if err != nil {
		return err
	}
	s.CurrentGridCharge = enabled
	return f.writeState(s)
}

func (f *FileStore) GetBatteryFirstGridCharge() (string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s, err := f.readState()
	return s.CurrentGridCharge, err
}

func (f *FileStore) SetBatteryFirstGridChargeTarget(enabled string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, err := f.readState()
	if err != nil {
		return err
	}
	s.TargetGridCharge = enabled
	return f.writeState(s)
}

func (f *FileStore) GetBatteryFirstGridChargeTarget() (string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s, err := f.readState()
	return s.TargetGridCharge, err
}

func (f *FileStore) SetSOCTarget(soc int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, err := f.readState()
	if err != nil {
		return err
	}
	s.TargetSOC = soc
	return f.writeState(s)
}

func (f *FileStore) GetSOCTarget() (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s, err := f.readState()
	return s.TargetSOC, err
}

func (f *FileStore) SetDeviceModeTarget(mode string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, err := f.readState()
	if err != nil {
		return err
	}
	s.TargetDeviceMode = mode
	return f.writeState(s)
}

func (f *FileStore) GetDeviceModeTarget() (string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s, err := f.readState()
	return s.TargetDeviceMode, err
}

func (f *FileStore) SetLoadFirstStopDischarge(soc int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, err := f.readState()
	if err != nil {
		return err
	}
	s.CurrentLoadFirstStopDischarge = soc
	return f.writeState(s)
}

func (f *FileStore) GetLoadFirstStopDischarge() (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s, err := f.readState()
	return s.CurrentLoadFirstStopDischarge, err
}
