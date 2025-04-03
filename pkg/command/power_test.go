package command

import (
	"testing"
	"time"
)

func TestPowerUnpack(t *testing.T) {
	r := PowerReading{}
	if err := r.Unpack([]byte{}); err == nil {
		t.Errorf("expected an error")
	}

	if err := r.Unpack([]byte{0x1f, 0x19, 0x9a, 0x60, 0x49, 0x01, 0xb5, 0x00, 0x05, 0x01, 0x19, 0x00, 0x00, 0x00}); err != nil {
		t.Errorf("unexpected an error: %v", err)
	}

	if got, want := r.timestamp_s, uint32(1620711711); got != want {
		t.Errorf("timestamp_s=%v, want=%v", got, want)
	}

	if got, want := r.Time(), time.UnixMilli(1620711711329); got != want {
		t.Errorf("time stamp=%s, want=%s", got, want)
	}

	if got, want := r.GpuPower(), float64(261); got != want {
		t.Errorf("gpu_power=%v, want=%v", got, want)
	}

	if got, want := r.CpuPower(), float64(181); got != want {
		t.Errorf("cpu_power=%v, want=%v", got, want)
	}

	if got, want := r.NodePower(), float64(181+261); got != want {
		t.Errorf("node_power=%v, want=%v", got, want)
	}
}
