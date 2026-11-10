package driver

import (
	"errors"
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

// probesInFlight holds the paths with a probe still running. A probe stuck in
// uninterruptible I/O cannot be cancelled, so a second one is never started for
// the same path: the caller is told the earlier one is pending instead, which
// keeps a hung device from accumulating one goroutine per check.
var probesInFlight sync.Map

// probeMount reads the directory at path, the cheapest operation that still
// reaches the filesystem rather than the page cache. Overridable in tests.
var probeMount = func(path string) error {
	if _, busy := probesInFlight.LoadOrStore(path, struct{}{}); busy {
		return errProbePending
	}
	done := make(chan error, 1)
	go func() {
		err := readDirProbe(path)
		probesInFlight.Delete(path)
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
