package command

import (
	"testing"
	"time"
)

func TestEnergyUnpack(t *testing.T) {
	r := EnergyReading{}
	if err := r.Unpack([]byte{}); err == nil {
		t.Errorf("expected an error")
	}

	if err := r.Unpack([]byte{0xa7, 0x00, 0x75, 0xe8, 0x77, 0x01, 0xe4, 0x02, 0x9d, 0xa2, 0xdd, 0x5a, 0x3b, 0x01}); err != nil {
		t.Errorf("unexpected an error: %v", err)
	}

	if got, want := r.timestamp_s, uint32(1524474525); got != want {
		t.Errorf("timestamp_s=%v, want=%v", got, want)
	}

	if got, want := r.Time(), time.UnixMilli(1524474525315); got != want {
		t.Errorf("time stamp=%s, want=%s", got, want)
	}

	if got, want := r.Energy(), float64(24635509.740); got != want {
		t.Errorf("energy=%v, want=%v", got, want)
	}
}
