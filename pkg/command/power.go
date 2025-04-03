package command

import (
	"encoding/binary"
	"fmt"
	"time"
)

// PowerReading represents a single power command response
type PowerReading struct {
	timestamp_s  uint32
	timestamp_ms uint16
	cpu_power    uint16
	gpu_power    uint16
	// they are not used, so I don't bother!
	// riser1_power uint16
	// riser2_power uint16
}

func (r *PowerReading) Unpack(buf []byte) error {
	if len(buf) != 14 {
		return fmt.Errorf("unexpected buffer length: %v != 14", len(buf))
	}

	r.timestamp_s = binary.LittleEndian.Uint32(buf[0:4])
	r.timestamp_ms = binary.LittleEndian.Uint16(buf[4:6])
	r.cpu_power = binary.LittleEndian.Uint16(buf[6:8])
	r.gpu_power = binary.LittleEndian.Uint16(buf[8:10])

	// r.riser1_power = binary.LittleEndian.Uint16(buf[10:12])
	// r.riser2_power = binary.LittleEndian.Uint16(buf[12:14])

	return nil
}

// Time returns the reported time stamp in device time
func (r *PowerReading) Time() time.Time {
	return time.Unix(int64(r.timestamp_s), int64(r.timestamp_ms)*1000*1000)
}

func (r *PowerReading) NodePower() float64 {
	return r.CpuPower() + r.GpuPower()
}

func (r *PowerReading) GpuPower() float64 {
	return float64(r.gpu_power)
}

func (r *PowerReading) CpuPower() float64 {
	return float64(r.cpu_power)
}
