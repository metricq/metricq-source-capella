package command

import (
	"encoding/binary"
	"fmt"
	"time"
)

type EnergyReading struct {
	timestamp_s  uint32
	timestamp_ms uint16
	energy_j     uint32
	energy_mj    uint16
}

func (r *EnergyReading) Unpack(buf []byte) error {
	if len(buf) != 14 {
		return fmt.Errorf("unexpected buffer length: %v != 14", len(buf))
	}

	r.timestamp_s = binary.LittleEndian.Uint32(buf[8:12])
	r.timestamp_ms = binary.LittleEndian.Uint16(buf[12:14])
	r.energy_j = binary.LittleEndian.Uint32(buf[2:6])
	r.energy_mj = binary.LittleEndian.Uint16(buf[6:8])

	return nil
}

func (r *EnergyReading) Time() time.Time {
	return time.Unix(int64(r.timestamp_s), int64(r.timestamp_ms)*1000*1000)
}

func (r *EnergyReading) Energy() float64 {
	return float64(r.energy_j) + float64(r.energy_mj)/1000
}
