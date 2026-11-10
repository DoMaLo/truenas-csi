package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	iscsilib "github.com/kubernetes-csi/csi-lib-iscsi/iscsi"
	"k8s.io/mount-utils"
)

const (
	// DefaultMountHealInterval is how often the node plugin scans staged block
	// mounts after a storage-path outage. Set TRUENAS_MOUNT_HEAL_INTERVAL=0 to
	// disable. A network blip that leaves XFS/ext4 dead while kubelet still
	// believes the volume is staged never re-enters NodeStageVolume; this loop
	// tears the dead mounts down and rebuilds them once the portal is back.
	DefaultMountHealInterval = 30 * time.Second

	defaultCSIPluginsPath = "/var/lib/kubelet/plugins/kubernetes.io/csi"

	mountHealProbeFile   = ".truenas-csi-heal-probe"
	mountHealProbeTimeout = 3 * time.Second
	portalDialTimeout     = 2 * time.Second
)

// writeProbeMount tries a short write against path to catch XFS/ext4 mounts that
// still look mounted (and are not ST_RDONLY) but return EIO after an iSCSI outage.
// Overridable in tests.
var writeProbeMount = func(path string) error {
	if path == "" {
		return fmt.Errorf("empty path")
	}
	probe := filepath.Join(path, mountHealProbeFile)
	done := make(chan error, 1)
	go func() {
		if err := os.WriteFile(probe, []byte("ok"), 0o600); err != nil {
			done <- err
			return
		}
		done <- os.Remove(probe)
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(mountHealProbeTimeout):
		return fmt.Errorf("write probe timed out after %s", mountHealProbeTimeout)
	}
}

// mountHealIntervalFromEnv reads TRUENAS_MOUNT_HEAL_INTERVAL. Empty uses the
// default; "0" / "0s" disables the healer.
func mountHealIntervalFromEnv() time.Duration {
	raw := strings.TrimSpace(os.Getenv("TRUENAS_MOUNT_HEAL_INTERVAL"))
	if raw == "" {
		return DefaultMountHealInterval
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return DefaultMountHealInterval
	}
	return d
}

func csiPluginsPathFromEnv() string {
	if p := strings.TrimSpace(os.Getenv("TRUENAS_KUBELET_CSI_PATH")); p != "" {
		return p
	}
	return defaultCSIPluginsPath
}

// StartMountHealer periodically repairs staged iSCSI/NVMe-oF mounts left unusable
// after a transient path to storage (for example a router reboot between workers
// and TrueNAS). Safe to call once from Driver.Run; no-op when the interval is 0.
func (s *NodeServer) StartMountHealer(ctx context.Context) {
	interval := mountHealIntervalFromEnv()
	if interval <= 0 {
		s.driver.Log().Info("Mount healer disabled", "env", "TRUENAS_MOUNT_HEAL_INTERVAL")
		return
	}
	pluginsPath := csiPluginsPathFromEnv()
	s.driver.Log().Info("Starting mount healer", "interval", interval.String(), "csiPluginsPath", pluginsPath)

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		// Run once shortly after start so recovery does not wait a full interval.
		s.healStaleMounts(ctx, pluginsPath)
		for {
			select {
			case <-ctx.Done():
				s.driver.Log().Info("Mount healer stopped")
				return
			case <-ticker.C:
				s.healStaleMounts(ctx, pluginsPath)
			}
		}
	}()
}

type stagedBlockMount struct {
	StagingPath string
	Device      string
	FSType      string
	VolumeID    string
	Protocol    string
	PublishCtx  map[string]string
	Portals     []string
}

func (s *NodeServer) healStaleMounts(ctx context.Context, pluginsPath string) {
	mounts, err := s.mounter.List()
	if err != nil {
		s.driver.Log().Error(err, "Mount healer: failed to list mounts")
		return
	}

	candidates := s.findStagedBlockMounts(mounts, pluginsPath)
	for _, cand := range candidates {
		if reason := unusableStagedMountReason(cand.StagingPath, false); reason == "" {
			continue
		} else {
			s.driver.Log().Info("Mount healer found unusable staging mount",
				"stagingPath", cand.StagingPath, "volumeId", cand.VolumeID, "reason", reason)
		}

		if !portalsReachable(cand.Portals) {
			s.driver.Log().V(LogLevelDebug).Info("Mount healer: portal still unreachable, deferring",
				"volumeId", cand.VolumeID, "portals", cand.Portals)
			continue
		}

		if err := s.healOneStagedMount(ctx, cand, mounts); err != nil {
			s.driver.Log().Error(err, "Mount healer: failed to repair staging mount",
				"stagingPath", cand.StagingPath, "volumeId", cand.VolumeID)
		}
	}
}

func (s *NodeServer) findStagedBlockMounts(mounts []mount.MountPoint, pluginsPath string) []stagedBlockMount {
	pluginsPath = filepath.Clean(pluginsPath)
	var out []stagedBlockMount
	seen := map[string]struct{}{}

	for _, m := range mounts {
		path := filepath.Clean(m.Path)
		if !strings.HasPrefix(path, pluginsPath+string(os.PathSeparator)) {
			continue
		}
		if filepath.Base(path) != "globalmount" {
			continue
		}
		if !strings.HasPrefix(m.Device, devicePrefixDev) {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}

		cand := stagedBlockMount{
			StagingPath: path,
			Device:      m.Device,
			FSType:      m.Type,
		}
		s.enrichFromConnector(&cand)
		out = append(out, cand)
	}
	return out
}

func (s *NodeServer) enrichFromConnector(cand *stagedBlockMount) {
	devName := filepath.Base(cand.Device)

	entries, err := os.ReadDir(connectorDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		path := filepath.Join(connectorDir, name)
		switch {
		case strings.HasSuffix(name, nvmeConnectorExt):
			data, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			var info nvmeConnectorInfo
			if err := json.Unmarshal(data, &info); err != nil {
				continue
			}
			if info.DevicePath == "" || filepath.Base(info.DevicePath) != devName {
				if info.DevicePath != cand.Device {
					continue
				}
			}
			cand.VolumeID = info.VolumeID
			cand.Protocol = ProtocolNVMeOF
			// NVMe connector lacks portal fields needed to restage; healer will
			// only tear the mount down and let kubelet republish with full context.
			return
		case strings.HasSuffix(name, ".connector"):
			data, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			var c iscsilib.Connector
			if err := json.Unmarshal(data, &c); err != nil {
				continue
			}
			if !connectorOwnsDevice(&c, devName) {
				continue
			}
			cand.VolumeID = c.VolumeName
			cand.Protocol = ProtocolISCSI
			cand.Portals = append([]string(nil), c.TargetPortals...)
			cand.PublishCtx = iscsiPublishContextFromConnector(&c)
			return
		}
	}
}

func connectorOwnsDevice(c *iscsilib.Connector, devName string) bool {
	if c.MountTargetDevice != nil && c.MountTargetDevice.Name == devName {
		return true
	}
	for _, d := range c.Devices {
		if d.Name == devName {
			return true
		}
	}
	return false
}

func iscsiPublishContextFromConnector(c *iscsilib.Connector) map[string]string {
	portal := ""
	if len(c.TargetPortals) > 0 {
		portal = c.TargetPortals[0]
	}
	ctx := map[string]string{
		PublishContextProtocol:     ProtocolISCSI,
		PublishContextTargetIQN:    c.TargetIqn,
		PublishContextTargetPortal: portal,
		PublishContextLUN:          strconv.FormatInt(int64(c.Lun), 10),
	}
	return ctx
}

func portalsReachable(portals []string) bool {
	if len(portals) == 0 {
		// No portal known (e.g. NVMe tear-down path): still attempt local cleanup
		// so kubelet can restage once the path recovers.
		return true
	}
	for _, portal := range portals {
		if portalReachable(portal) {
			return true
		}
	}
	return false
}

func portalReachable(portal string) bool {
	if portal == "" {
		return false
	}
	hostPort := portal
	if _, _, err := net.SplitHostPort(portal); err != nil {
		hostPort = net.JoinHostPort(portal, "3260")
	}
	conn, err := net.DialTimeout("tcp", hostPort, portalDialTimeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func (s *NodeServer) healOneStagedMount(ctx context.Context, cand stagedBlockMount, mounts []mount.MountPoint) error {
	volumeID := cand.VolumeID
	if volumeID == "" {
		volumeID = cand.StagingPath
	}
	lockKey := fmt.Sprintf("%s-%s", volumeID, cand.StagingPath)
	if !s.TryAcquireLock(lockKey) {
		return fmt.Errorf("volume lock busy")
	}
	defer s.ReleaseLock(lockKey)

	// Re-check under the lock: a concurrent Stage/Publish may have fixed it.
	if reason := unusableStagedMountReason(cand.StagingPath, false); reason == "" {
		return nil
	}

	consumers := consumerMounts(mounts, cand.StagingPath, cand.Device)
	// Unmount deepest paths first (nested binds, if any).
	sort.Slice(consumers, func(i, j int) bool {
		return len(consumers[i]) > len(consumers[j])
	})

	for _, target := range consumers {
		s.driver.Log().Info("Mount healer unmounting consumer", "path", target, "volumeId", volumeID)
		if err := s.mounter.Unmount(target); err != nil {
			s.driver.Log().Error(err, "Mount healer: consumer unmount failed", "path", target)
		}
	}

	handler := s.handlerForMount(cand.StagingPath)
	if handler == nil {
		switch cand.Protocol {
		case ProtocolNVMeOF:
			handler = s.nvmeofHandler
		default:
			handler = s.iscsiHandler
		}
	}

	if err := handler.Unstage(ctx, &UnstageRequest{VolumeID: volumeID, StagingPath: cand.StagingPath}); err != nil {
		return fmt.Errorf("unstage: %w", err)
	}

	// NVMe connector does not carry portal/publish context; leave restage to kubelet.
	if cand.Protocol == ProtocolNVMeOF || len(cand.PublishCtx) == 0 {
		s.driver.Log().Info("Mount healer tore down unusable mount; kubelet will restage",
			"volumeId", volumeID, "stagingPath", cand.StagingPath, "protocol", cand.Protocol)
		return nil
	}

	fsType := cand.FSType
	if fsType == "" {
		fsType = DefaultFSType
	}
	if _, err := handler.Stage(ctx, &StageRequest{
		VolumeID:       volumeID,
		StagingPath:    cand.StagingPath,
		FSType:         fsType,
		PublishContext: cand.PublishCtx,
		VolumeContext:  map[string]string{},
	}); err != nil {
		return fmt.Errorf("restage: %w", err)
	}

	for _, target := range consumers {
		if err := os.MkdirAll(target, 0o750); err != nil {
			s.driver.Log().Error(err, "Mount healer: recreate consumer path failed", "path", target)
			continue
		}
		if err := s.mounter.Mount(cand.StagingPath, target, "", []string{mountOptionBind}); err != nil {
			s.driver.Log().Error(err, "Mount healer: rebind consumer failed", "path", target)
			continue
		}
		s.driver.Log().Info("Mount healer rebound consumer", "path", target, "volumeId", volumeID)
	}

	s.driver.Log().Info("Mount healer repaired staging mount",
		"volumeId", volumeID, "stagingPath", cand.StagingPath, "consumers", len(consumers))
	return nil
}

// consumerMounts returns bind/pod mounts that reference the same block device or
// the staging path itself, excluding the staging path.
func consumerMounts(mounts []mount.MountPoint, stagingPath, device string) []string {
	stagingPath = filepath.Clean(stagingPath)
	var out []string
	seen := map[string]struct{}{}
	for _, m := range mounts {
		path := filepath.Clean(m.Path)
		if path == stagingPath {
			continue
		}
		if m.Device != device && filepath.Clean(m.Device) != stagingPath {
			continue
		}
		// Prefer kubelet pod volume paths; skip unrelated mounts of the same disk.
		if !strings.Contains(path, "/pods/") && !strings.Contains(path, "/var/lib/kubelet/") {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		out = append(out, path)
	}
	return out
}
