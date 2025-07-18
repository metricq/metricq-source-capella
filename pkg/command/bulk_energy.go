package command

import (
	"encoding/binary"
	"fmt"
	"time"
)

type BulkEnergyReading struct {
	timestamp_s  uint32
	timestamp_ms uint16
	energy_j     uint32
	readings     []uint16
}

func (r *BulkEnergyReading) Unpack(buf []byte) error {
	if len(buf) != 210 {
		return fmt.Errorf("unexpected buffer length: %v != 210", len(buf))
	}

	r.timestamp_s = binary.LittleEndian.Uint32(buf[0:4])
	r.timestamp_ms = binary.LittleEndian.Uint16(buf[4:6])
	r.energy_j = binary.LittleEndian.Uint32(buf[6:10])

	r.readings = make([]uint16, 100)

	for i := 10; i < 210; i += 2 {
		r.readings[(i-10)/2] = binary.LittleEndian.Uint16(buf[i : i+2])
	}

	return nil
}

func (r *BulkEnergyReading) Timestamp() time.Time {
    time, _ := r.Time(0)
    return time
}

func (r *BulkEnergyReading) Time(i int) (time.Time, error) {
	if i < 0 || i > 99 {
		return time.Time{}, fmt.Errorf("invalid index: %v", i)
	}

	return time.Unix(
		int64(r.timestamp_s),
		(int64(i)*10+int64(r.timestamp_ms))*int64(time.Millisecond),
	), nil
}

func (r *BulkEnergyReading) Energy(i int) (int64, error) {
	if i < 0 || i > 99 {
		return 0, fmt.Errorf("invalid index: %v", i)
	}

	return int64(r.energy_j) + int64(r.readings[i]), nil
}
