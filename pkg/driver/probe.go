package driver

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"time"
)

// mountProbeTimeout bounds how long a caller waits for a read probe. I/O against
// a device whose session is down blocks until the kernel gives up on it, which
// can take minutes.
const mountProbeTimeout = 5 * time.Second

var (
	errProbeTimeout = errors.New("read probe timed out")
	errProbePending = errors.New("an earlier read probe has not returned")
)

// probesInFlight holds the mounts with a probe still running, keyed by path and
// device. A probe stuck in uninterruptible I/O cannot be cancelled, so a second
// one is never started for the same mount: the caller is told the earlier one is
// pending instead, which keeps a hung device from accumulating one goroutine per
// check. The device is part of the key so that a probe left hanging on a mount
// that was since replaced at the same path does not condemn the new one.
var probesInFlight sync.Map

func probeKey(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return fmt.Sprintf("%s|%d", path, uint64(st.Dev)), nil //nolint:unconvert // width differs by platform
	}
	return path, nil
}

// probeMount reads the directory at path, the cheapest operation that still
// reaches the filesystem rather than the page cache. Overridable in tests.
var probeMount = func(path string) error {
	key, err := probeKey(path)
	if err != nil {
		return err
	}
	if _, busy := probesInFlight.LoadOrStore(key, struct{}{}); busy {
		return errProbePending
	}
	done := make(chan error, 1)
	go func() {
		err := readDirProbe(path)
		probesInFlight.Delete(key)
		done <- err
	}()

	timer := time.NewTimer(mountProbeTimeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return errProbeTimeout
	}
}

func readDirProbe(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	if _, err := dir.Readdirnames(1); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// isDeadMountError reports whether a probe error means the filesystem has lost
// its backing storage. Everything else - a missing path, a permission problem -
// says nothing about the storage and must not get a mount torn down.
func isDeadMountError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errProbeTimeout) || errors.Is(err, errProbePending) {
		return true
	}
	for _, errno := range []syscall.Errno{
		syscall.EIO,
		syscall.ENOTCONN,
		syscall.ESTALE,
		syscall.ENXIO,
		syscall.ENODEV,
		syscall.EUCLEAN, // XFS reports metadata it can no longer trust this way
	} {
		if errors.Is(err, errno) {
			return true
		}
	}
	return false
}

// isSlowProbeError reports whether err only says the probe did not answer in time,
// as opposed to the filesystem reporting a failure.
func isSlowProbeError(err error) bool {
	return errors.Is(err, errProbeTimeout) || errors.Is(err, errProbePending)
}
