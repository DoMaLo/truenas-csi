package driver

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestProbeMount(t *testing.T) {
	dir := t.TempDir()
	if err := probeMount(dir); err != nil {
		t.Errorf("probeMount() on an empty directory = %v, want nil", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "f"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := probeMount(dir); err != nil {
		t.Errorf("probeMount() on a populated directory = %v, want nil", err)
	}
	if err := probeMount(dir); err != nil {
		t.Errorf("a finished probe must not block the next one, got %v", err)
	}
}

func TestProbeMount_PendingProbeIsNotRepeated(t *testing.T) {
	dir := t.TempDir()
	key, err := probeKey(dir)
	if err != nil {
		t.Fatal(err)
	}
	probesInFlight.Store(key, struct{}{})
	t.Cleanup(func() { probesInFlight.Delete(key) })

	if err := probeMount(dir); err != errProbePending {
		t.Errorf("probeMount() with a probe in flight = %v, want %v", err, errProbePending)
	}
}

// A probe left hanging on an old mount must not be mistaken for one on whatever
// is mounted at the same path later.
func TestProbeMount_PendingProbeIsPerDevice(t *testing.T) {
	dir := t.TempDir()
	probesInFlight.Store(dir+"|0", struct{}{}) // another device's probe at the same path
	t.Cleanup(func() { probesInFlight.Delete(dir + "|0") })

	if err := probeMount(dir); err != nil {
		t.Errorf("probeMount() = %v, want nil: the pending probe belongs to another device", err)
	}
}

func TestProbeMount_MissingPath(t *testing.T) {
	err := probeMount(filepath.Join(t.TempDir(), "gone"))
	if err == nil || isDeadMountError(err) {
		t.Errorf("probeMount() on a missing path = %v, want a non-storage error", err)
	}
}

func TestIsDeadMountError(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errProbeTimeout, true},
		{errProbePending, true},
		{&os.PathError{Op: "readdirent", Path: "/x", Err: syscall.EIO}, true},
		{fmt.Errorf("wrapped: %w", syscall.EUCLEAN), true},
		{syscall.ENOTCONN, true},
		{&os.PathError{Op: "open", Path: "/x", Err: syscall.ENOENT}, false},
		{syscall.EACCES, false},
	}
	for _, tt := range tests {
		if got := isDeadMountError(tt.err); got != tt.want {
			t.Errorf("isDeadMountError(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}
