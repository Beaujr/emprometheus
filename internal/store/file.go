package store

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type FileStore struct {
	filepath string
}

func (f FileStore) SelectOptimization(start time.Time, optimization string) ([]OptimizationResult, error) {
	//TODO implement me
	panic("implement me")
}

func (f FileStore) SetActualSoc(socTime time.Time, soc float64) error {
	//TODO implement me
	panic("implement me")
}

func (f FileStore) SetBatteryFirstGridCharge(enabled string) error {
	//TODO implement me
	panic("implement me")
}

func (f FileStore) SetSOC(soc int64) error {
	//TODO implement me
	panic("implement me")
}

func (f FileStore) SetDeviceMode(mode string) error {
	//TODO implement me
	panic("implement me")
}

func (f FileStore) GetBatteryFirstGridCharge() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (f FileStore) GetSOC() (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (f FileStore) GetDeviceMode() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (f FileStore) SetBatteryFirstGridChargeTarget(enabled string) error {
	//TODO implement me
	panic("implement me")
}

func (f FileStore) SetSOCTarget(soc int64) error {
	//TODO implement me
	panic("implement me")
}

func (f FileStore) SetDeviceModeTarget(mode string) error {
	//TODO implement me
	panic("implement me")
}

func (f FileStore) GetBatteryFirstGridChargeTarget() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (f FileStore) GetSOCTarget() (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (f FileStore) GetDeviceModeTarget() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (f FileStore) InsertOptimization(row OptimizationResult) error {
	return nil
}

func (f FileStore) Insert(row Row) error {
	file, err := os.OpenFile(f.filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(row.String() + "\n")
	return err
}

func (f FileStore) Find(start time.Time) (Row, error) {
	var row Row
	file, err := os.Open(f.filepath)
	if err != nil {
		return row, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), start.Format(time.RFC3339)) {
			return RowFromString(scanner.Text())
		}
	}
	if err = scanner.Err(); err != nil {
		return row, err
	}
	return row, ErrNotFound
}

func (f FileStore) Upsert(row Row) error {
	file, err := os.Open(f.filepath)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = scanner.Err(); err != nil {
		return err
	}
	found := false
	for i, line := range lines {
		if strings.Contains(line, row.Time.Format(time.RFC3339)) {
			lines[i] = row.String()
			found = true
			break
		}
	}
	if len(lines) == 0 || !found {
		lines = append(lines, row.String())
	}

	file, err = os.OpenFile(f.filepath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
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
func (f FileStore) Select(start time.Time) ([]Row, error) {
	var rows []Row
	file, err := os.Open(f.filepath)
	defer file.Close()
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		row, err := RowFromString(scanner.Text())
		if err != nil {
			return nil, err
		}
		if row.Time.After(start) {
			rows = append(rows, row)
		}
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

func newFileStore(dir string, name string) (*FileStore, error) {
	fileloc := filepath.Join(dir, name)
	fs := &FileStore{filepath: fileloc}
	file, err := os.OpenFile(fileloc, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0777)
	defer file.Close()
	if err != nil {
		if os.IsExist(err) {
			return fs, nil
		}
		return nil, err
	}
	return fs, nil
}

func (f FileStore) Close() error {
	return nil
}
