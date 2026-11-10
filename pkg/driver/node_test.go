package driver

import (
	"context"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/go-logr/logr"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"
)

// useTempConnectorDir points connectorDir at a scratch directory for the duration of
// a test, so connector files can be created and inspected without touching the host.
func useTempConnectorDir(t *testing.T) string {
	t.Helper()
	orig := connectorDir
	connectorDir = t.TempDir()
	t.Cleanup(func() { connectorDir = orig })
	return connectorDir
}

// stubStatfs replaces the statfs call with one that reports the given flags, or an
// error when statErr is non-nil.
func stubStatfs(t *testing.T, flags int64, statErr error) {
	t.Helper()
	orig := statfs
	statfs = func(path string, stat *syscall.Statfs_t) error {
		if statErr != nil {
			return statErr
		}
		stat.Flags = flags
		return nil
	}
	t.Cleanup(func() { statfs = orig })
}

// stubMountProbe replaces the read probe used by unusableStagedMountReason.
func stubMountProbe(t *testing.T, err error) {
	t.Helper()
	orig := probeMount
	probeMount = func(string) error { return err }
	t.Cleanup(func() { probeMount = orig })
}

// newTestNodeServer builds a NodeServer backed by a fake mounter, with all three
// protocol handlers wired to it.
func newTestNodeServer(t *testing.T, mounts []mount.MountPoint) (*NodeServer, *mount.FakeMounter) {
	t.Helper()
	log := logr.Discard()
	fake := mount.NewFakeMounter(mounts)
	safe := &mount.SafeFormatAndMount{Interface: fake, Exec: exec.New()}

	driver := &Driver{
		log: log,
		volumeCaps: []*csi.VolumeCapability_AccessMode{
			{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY},
		},
	}

	return &NodeServer{
		driver:        driver,
		mounter:       fake,
		iscsiHandler:  &ISCSIHandler{mounter: safe, resizer: mount.NewResizeFs(safe.Exec), log: log},
		nvmeofHandler: &NVMeOFHandler{mounter: safe, resizer: mount.NewResizeFs(safe.Exec), log: log},
		nfsHandler:    NewNFSHandler(fake, log),
	}, fake
}

// A connector file whose volume was staged before the connector directory was
// persisted on the host is gone after a container restart. Unstage must still reach
// the protocol's own handler: dispatching to NFS, whose Unstage does nothing, would
// report success while leaving the mount and the session in place.
func TestHandlerForVolume_RecoversProtocolFromMount(t *testing.T) {
	useTempConnectorDir(t)
	const stagingPath = "/var/lib/kubelet/plugins/kubernetes.io/csi/csi.truenas.io/abc/globalmount"

	tests := []struct {
		name     string
		mount    *mount.MountPoint
		expected string
	}{
		{
			name:     "iSCSI block device",
			mount:    &mount.MountPoint{Device: "/dev/sda", Path: stagingPath, Type: "ext4"},
			expected: ProtocolISCSI,
		},
		{
			name:     "multipath block device",
			mount:    &mount.MountPoint{Device: "/dev/dm-3", Path: stagingPath, Type: "xfs"},
			expected: ProtocolISCSI,
		},
		{
			name:     "NVMe-oF namespace",
			mount:    &mount.MountPoint{Device: "/dev/nvme0n1", Path: stagingPath, Type: "ext4"},
			expected: ProtocolNVMeOF,
		},
		{
			name:     "NFS export",
			mount:    &mount.MountPoint{Device: "10.0.0.1:/mnt/tank/vol", Path: stagingPath, Type: "nfs4"},
			expected: ProtocolNFS,
		},
		{
			name:     "nothing mounted falls back to NFS",
			mount:    nil,
			expected: ProtocolNFS,
		},
		{
			name:     "unrelated mount falls back to NFS",
			mount:    &mount.MountPoint{Device: "/dev/sdb", Path: "/some/other/path", Type: "ext4"},
			expected: ProtocolNFS,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mounts []mount.MountPoint
			if tt.mount != nil {
				mounts = []mount.MountPoint{*tt.mount}
			}
			ns, _ := newTestNodeServer(t, mounts)

			if got := ns.handlerForVolume("tank/pvc-abc", stagingPath).Protocol(); got != tt.expected {
				t.Errorf("handlerForVolume() protocol = %q, want %q", got, tt.expected)
			}
		})
	}
}

// The connector file is authoritative when it exists, even if the mount table would
// suggest something else.
func TestHandlerForVolume_PrefersConnectorFile(t *testing.T) {
	dir := useTempConnectorDir(t)
	const (
		volumeID    = "tank/pvc-abc"
		stagingPath = "/var/lib/kubelet/staging/globalmount"
	)

	// An NFS-looking mount that must not win over an iSCSI connector file.
	mounts := []mount.MountPoint{{Device: "10.0.0.1:/mnt/tank/vol", Path: stagingPath, Type: "nfs4"}}
	ns, _ := newTestNodeServer(t, mounts)

	if err := os.WriteFile(filepath.Join(dir, sanitizeISCSIVolumeID(volumeID)+".connector"), []byte("{}"), 0o600); err != nil {
		t.Fatalf("failed to write connector file: %v", err)
	}
	if got := ns.handlerForVolume(volumeID, stagingPath).Protocol(); got != ProtocolISCSI {
		t.Errorf("handlerForVolume() protocol = %q, want %q", got, ProtocolISCSI)
	}

	// NVMe-oF wins over iSCSI when both connector files are present.
	if err := os.WriteFile(nvmeConnectorPath(volumeID), []byte("{}"), 0o600); err != nil {
		t.Fatalf("failed to write NVMe connector file: %v", err)
	}
	if got := ns.handlerForVolume(volumeID, stagingPath).Protocol(); got != ProtocolNVMeOF {
		t.Errorf("handlerForVolume() protocol = %q, want %q", got, ProtocolNVMeOF)
	}
}

func TestReadOnlyRequested(t *testing.T) {
	mountCap := func(mode csi.VolumeCapability_AccessMode_Mode, flags ...string) *csi.VolumeCapability {
		return &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{MountFlags: flags},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode},
		}
	}

	tests := []struct {
		name       string
		capability *csi.VolumeCapability
		want       bool
	}{
		{"single node writer", mountCap(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER), false},
		{"single node reader only", mountCap(csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY), true},
		{"multi node reader only", mountCap(csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY), true},
		{"writer with ro mount flag", mountCap(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, "ro"), true},
		{"writer with other mount flags", mountCap(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, "noatime"), false},
		{"block volume", &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := readOnlyRequested(tt.capability); got != tt.want {
				t.Errorf("readOnlyRequested() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUnusableStagedMountReason(t *testing.T) {
	tests := []struct {
		name       string
		flags      int64
		statErr    error
		probeErr   error
		readOnly   bool
		wantReason bool
	}{
		{name: "healthy read-write mount"},
		{name: "read-only mount when read-write was requested", flags: statfsFlagReadOnly, wantReason: true},
		{name: "read-only mount when read-only was requested", flags: statfsFlagReadOnly, readOnly: true},
		{name: "statfs failure", statErr: syscall.EIO, wantReason: true},
		{name: "read probe I/O error", probeErr: syscall.EIO, wantReason: true},
		{name: "read probe I/O error on a read-only volume", probeErr: syscall.EIO, readOnly: true, wantReason: true},
		// Stage logs out of the session on this verdict; a slow volume keeps it.
		{name: "read probe timed out", probeErr: errProbeTimeout},
		{name: "read probe still pending", probeErr: errProbePending},
		{name: "read probe error unrelated to storage", probeErr: syscall.EACCES},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubStatfs(t, tt.flags, tt.statErr)
			stubMountProbe(t, tt.probeErr)
			reason := unusableStagedMountReason("/staging", tt.readOnly)
			if (reason != "") != tt.wantReason {
				t.Errorf("unusableStagedMountReason() = %q, want a reason: %v", reason, tt.wantReason)
			}
		})
	}
}

// stageRequest builds a filesystem NodeStageVolume request for an iSCSI volume.
func stageRequest(volumeID, stagingPath string) *csi.NodeStageVolumeRequest {
	return &csi.NodeStageVolumeRequest{
		VolumeId:          volumeID,
		StagingTargetPath: stagingPath,
		PublishContext:    map[string]string{PublishContextProtocol: ProtocolISCSI},
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: DefaultFSType},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
	}
}

// stagingDir creates a staging path that the fake mounter will accept as mounted.
func stagingDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "globalmount")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("failed to create staging dir: %v", err)
	}
	// The fake mounter resolves symlinks before matching mount points.
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatalf("failed to resolve staging dir: %v", err)
	}
	return resolved
}

func TestNodeStageVolume_ReusesHealthyMount(t *testing.T) {
	useTempConnectorDir(t)
	staging := stagingDir(t)
	stubStatfs(t, 0, nil)
	stubMountProbe(t, nil)

	ns, fake := newTestNodeServer(t, []mount.MountPoint{{Device: "/dev/sda", Path: staging, Type: "ext4"}})

	if _, err := ns.NodeStageVolume(context.Background(), stageRequest("tank/pvc-abc", staging)); err != nil {
		t.Fatalf("NodeStageVolume() on a healthy mount should succeed, got: %v", err)
	}
	if len(fake.GetLog()) != 0 {
		t.Errorf("a healthy staging mount must be left alone, got actions: %v", fake.GetLog())
	}
}

// After a storage-side interruption the staging mount survives as a read-only,
// aborted filesystem. Reusing it hands every pod an EROFS filesystem that no
// appliance-side repair can fix, so stage must tear it down and mount again.
func TestNodeStageVolume_RestagesUnusableMount(t *testing.T) {
	useTempConnectorDir(t)
	staging := stagingDir(t)
	stubStatfs(t, statfsFlagReadOnly, nil)
	stubMountProbe(t, nil)

	ns, fake := newTestNodeServer(t, []mount.MountPoint{{Device: "/dev/sda", Path: staging, Type: "ext4"}})

	// Staging cannot complete without a reachable target, but the request must get
	// past the idempotency check rather than reporting the stale mount as staged.
	if _, err := ns.NodeStageVolume(context.Background(), stageRequest("tank/pvc-abc", staging)); err == nil {
		t.Fatal("NodeStageVolume() should not have reported the unusable mount as staged")
	}

	var unmounted bool
	for _, action := range fake.GetLog() {
		if action.Action == mount.FakeActionUnmount && action.Target == staging {
			unmounted = true
		}
	}
	if !unmounted {
		t.Errorf("the unusable staging mount was not unmounted, actions: %v", fake.GetLog())
	}
}

// A failed logout must not take the connector file with it: the file is the only
// record that the volume is iSCSI, and losing it sends later unstage attempts to
// the wrong handler.
func TestCleanupISCSISession_KeepsConnectorWhenLogoutFails(t *testing.T) {
	dir := useTempConnectorDir(t)
	const volumeID = "tank/pvc-abc"

	cpath := filepath.Join(dir, sanitizeISCSIVolumeID(volumeID)+".connector")
	connector := `{"volume_name":"pvc-abc","target_iqn":"iqn.2000-01.io.truenas:pvc-abc","target_portal":["10.0.0.1:3260"]}`
	if err := os.WriteFile(cpath, []byte(connector), 0o600); err != nil {
		t.Fatalf("failed to write connector file: %v", err)
	}

	orig := logoutISCSITarget
	logoutISCSITarget = func(string, []string) error { return syscall.EIO }
	t.Cleanup(func() { logoutISCSITarget = orig })

	handler := &ISCSIHandler{log: logr.Discard()}
	if err := handler.cleanupISCSISession(volumeID); err == nil {
		t.Fatal("cleanupISCSISession() should report the failed logout")
	}
	if _, err := os.Stat(cpath); err != nil {
		t.Errorf("connector file was removed despite the failed logout: %v", err)
	}
}

func TestCleanupISCSISession_RemovesConnectorOnSuccess(t *testing.T) {
	dir := useTempConnectorDir(t)
	const volumeID = "tank/pvc-abc"

	cpath := filepath.Join(dir, sanitizeISCSIVolumeID(volumeID)+".connector")
	connector := `{"volume_name":"pvc-abc","target_iqn":"iqn.2000-01.io.truenas:pvc-abc","target_portal":["10.0.0.1:3260"]}`
	if err := os.WriteFile(cpath, []byte(connector), 0o600); err != nil {
		t.Fatalf("failed to write connector file: %v", err)
	}

	var loggedOutIQN string
	orig := logoutISCSITarget
	logoutISCSITarget = func(iqn string, _ []string) error {
		loggedOutIQN = iqn
		return nil
	}
	t.Cleanup(func() { logoutISCSITarget = orig })

	handler := &ISCSIHandler{log: logr.Discard()}
	if err := handler.cleanupISCSISession(volumeID); err != nil {
		t.Fatalf("cleanupISCSISession() = %v, want nil", err)
	}
	if loggedOutIQN != "iqn.2000-01.io.truenas:pvc-abc" {
		t.Errorf("logged out of %q, want the connector's target IQN", loggedOutIQN)
	}
	if _, err := os.Stat(cpath); !os.IsNotExist(err) {
		t.Errorf("connector file should have been removed, stat error: %v", err)
	}
}

// No connector file means there is nothing to log out of, which must not be an error.
func TestCleanupISCSISession_NoConnector(t *testing.T) {
	useTempConnectorDir(t)

	handler := &ISCSIHandler{log: logr.Discard()}
	if err := handler.cleanupISCSISession("tank/pvc-missing"); err != nil {
		t.Errorf("cleanupISCSISession() with no connector file = %v, want nil", err)
	}
}

func TestIsNoISCSIObjectsFound(t *testing.T) {
	noObjs := exitError(t, iscsiadmExitNoObjsFound)
	otherFailure := exitError(t, 1)

	if !isNoISCSIObjectsFound(noObjs) {
		t.Errorf("exit %d should be treated as nothing to log out of", iscsiadmExitNoObjsFound)
	}
	if isNoISCSIObjectsFound(otherFailure) {
		t.Error("exit 1 should be treated as a real failure")
	}
	if isNoISCSIObjectsFound(syscall.EIO) {
		t.Error("a non-exit error should be treated as a real failure")
	}
}

// exitError runs a command that exits with the given code and returns the resulting
// error, so tests see the same error type iscsiadm failures arrive as.
func exitError(t *testing.T, code int) error {
	t.Helper()
	err := osexec.Command("sh", "-c", "exit "+strconv.Itoa(code)).Run()
	if err == nil {
		t.Fatalf("expected a non-zero exit for code %d", code)
	}
	return err
}

// csi-lib-iscsi reports only an exit status, so a host that cannot run iscsiadm
// surfaced as a bare "exit status 127" with nothing to act on. The command's stderr
// is what names the real problem.
func TestWithCommandOutput(t *testing.T) {
	cmd := osexec.Command("sh", "-c", "echo 'nsenter: failed to execute iscsiadm: No such file or directory' >&2; exit 127")
	if _, err := cmd.Output(); err == nil {
		t.Fatal("expected the command to fail")
	} else {
		enriched := withCommandOutput(err)
		if !strings.Contains(enriched.Error(), "failed to execute iscsiadm") {
			t.Errorf("stderr was not surfaced: %v", enriched)
		}
		if !strings.Contains(enriched.Error(), "exit status 127") {
			t.Errorf("exit status was lost: %v", enriched)
		}
	}

	// Errors that carry no command output are passed through untouched.
	plain := syscall.EIO
	if got := withCommandOutput(plain); got != plain {
		t.Errorf("withCommandOutput() = %v, want the original error", got)
	}
}

// TrueNAS storage is reachable from every node, so the driver must not report an
// accessible topology. A reported segment becomes node affinity on the PV, and a
// node label holds one value per key, so any volume whose segment does not match
// what the nodes advertise can never be scheduled.
func TestNodeGetInfo_ReportsNoTopology(t *testing.T) {
	ns, _ := newTestNodeServer(t, nil)
	ns.driver.nodeID = "worker-1"

	resp, err := ns.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatalf("NodeGetInfo() = %v", err)
	}
	if resp.NodeId != "worker-1" {
		t.Errorf("NodeId = %q, want %q", resp.NodeId, "worker-1")
	}
	if resp.AccessibleTopology != nil {
		t.Errorf("NodeGetInfo reported topology %v, want none", resp.AccessibleTopology.Segments)
	}
}

// The plugin must not claim topology support while reporting no topology: the
// combination is what lets the provisioner pin PVs to labels no node carries.
func TestPluginCapabilities_ExcludeAccessibilityConstraints(t *testing.T) {
	d := &Driver{}
	d.initializeCapabilities()

	for _, c := range d.pluginCaps {
		if c.GetService().GetType() == csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS {
			t.Error("driver advertises VOLUME_ACCESSIBILITY_CONSTRAINTS but reports no topology")
		}
	}
}

func publishRequest(protocol, staging, target string) *csi.NodePublishVolumeRequest {
	return &csi.NodePublishVolumeRequest{
		VolumeId:          "tank/pvc-abc",
		StagingTargetPath: staging,
		TargetPath:        target,
		PublishContext:    map[string]string{PublishContextProtocol: protocol},
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: DefaultFSType},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
	}
}

func unmounted(fake *mount.FakeMounter, target string) bool {
	for _, action := range fake.GetLog() {
		if action.Action == mount.FakeActionUnmount && action.Target == target {
			return true
		}
	}
	return false
}

// A bind mount that outlived a storage outage is released so Publish can mount it
// again from a healthy staging path.
func TestNodePublishVolume_ReplacesDeadBlockTarget(t *testing.T) {
	useTempConnectorDir(t)
	target := stagingDir(t)
	stubStatfs(t, 0, nil)
	stubMountProbe(t, syscall.EIO)

	ns, fake := newTestNodeServer(t, []mount.MountPoint{{Device: "/dev/sda", Path: target, Type: "xfs"}})

	// Publish itself cannot finish here because nothing is staged; what matters is
	// that the request did not report the dead mount as published.
	if _, err := ns.NodePublishVolume(context.Background(), publishRequest(ProtocolISCSI, "/not/staged", target)); err == nil {
		t.Fatal("NodePublishVolume() reported a dead mount as published")
	}
	if !unmounted(fake, target) {
		t.Errorf("the dead publish mount was not released, actions: %v", fake.GetLog())
	}
}

func TestNodePublishVolume_LeavesHealthyBlockTarget(t *testing.T) {
	useTempConnectorDir(t)
	target := stagingDir(t)
	stubStatfs(t, 0, nil)
	stubMountProbe(t, nil)

	ns, fake := newTestNodeServer(t, []mount.MountPoint{{Device: "/dev/sda", Path: target, Type: "xfs"}})

	if _, err := ns.NodePublishVolume(context.Background(), publishRequest(ProtocolISCSI, "/not/staged", target)); err != nil {
		t.Fatalf("NodePublishVolume() on a healthy mount = %v, want nil", err)
	}
	if unmounted(fake, target) {
		t.Errorf("a healthy publish mount must be left alone, actions: %v", fake.GetLog())
	}
}

// An NFS mount whose server is away cannot be told apart from a slow one, and
// tearing it down would not help, so NFS keeps the plain idempotent answer.
func TestNodePublishVolume_NeverProbesNFSTarget(t *testing.T) {
	target := stagingDir(t)
	stubStatfs(t, 0, syscall.EIO)
	stubMountProbe(t, syscall.EIO)

	ns, fake := newTestNodeServer(t, []mount.MountPoint{{Device: "nas:/export", Path: target, Type: "nfs4"}})

	if _, err := ns.NodePublishVolume(context.Background(), publishRequest(ProtocolNFS, "", target)); err != nil {
		t.Fatalf("NodePublishVolume() on an existing NFS mount = %v, want nil", err)
	}
	if unmounted(fake, target) {
		t.Errorf("an NFS publish mount must never be released, actions: %v", fake.GetLog())
	}
}

// eio is what stat returns under a shut-down XFS mount, as seen in the outage.
func eio(path string) error {
	return &os.PathError{Op: "stat", Path: path, Err: syscall.EIO}
}

// After the outage kubelet could not even stat the dead mounts. Unstage must
// unmount such a staging path all the same, or the volume is never released.
func TestNodeUnstageVolume_UnmountsUnreadableStagingMount(t *testing.T) {
	useTempConnectorDir(t)
	staging := stagingDir(t)
	ns, fake := newTestNodeServer(t, []mount.MountPoint{{Device: "/dev/sda", Path: staging, Type: "xfs"}})
	fake.MountCheckErrors = map[string]error{staging: eio(staging)}

	if _, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId: "tank/pvc-abc", StagingTargetPath: staging,
	}); err != nil {
		t.Fatalf("NodeUnstageVolume() on an unreadable mount = %v, want nil", err)
	}
	if !unmounted(fake, staging) {
		t.Errorf("the unreadable staging mount was not unmounted, actions: %v", fake.GetLog())
	}
}

func TestNodeStageVolume_RestagesUnreadableMount(t *testing.T) {
	useTempConnectorDir(t)
	staging := stagingDir(t)
	ns, fake := newTestNodeServer(t, []mount.MountPoint{{Device: "/dev/sda", Path: staging, Type: "xfs"}})
	fake.MountCheckErrors = map[string]error{staging: eio(staging)}

	// Staging cannot complete without a target; it must get past the check.
	_, err := ns.NodeStageVolume(context.Background(), stageRequest("tank/pvc-abc", staging))
	if err != nil && strings.Contains(err.Error(), "failed to check staging path") {
		t.Fatalf("NodeStageVolume() stopped at the mount check: %v", err)
	}
	if !unmounted(fake, staging) {
		t.Errorf("the unreadable staging mount was not replaced, actions: %v", fake.GetLog())
	}
}

// A volume whose staging mount is gone or dead while kubelet still believes it
// staged is staged again by Publish, instead of failing with "volume not staged"
// until someone intervenes.
func TestNodePublishVolume_StagesAgainWhenStagingIsMissing(t *testing.T) {
	useTempConnectorDir(t)
	staging := filepath.Join(t.TempDir(), "globalmount") // removed, as on w-01/w-06
	target := filepath.Join(t.TempDir(), "mount")
	ns, _ := newTestNodeServer(t, nil)

	_, err := ns.NodePublishVolume(context.Background(), publishRequest(ProtocolISCSI, staging, target))
	if err == nil {
		t.Fatal("NodePublishVolume() succeeded without a target to stage from")
	}
	// The stand-in publish context has no target, so staging fails on that, which
	// shows Publish went on to stage rather than giving up.
	if !strings.Contains(err.Error(), "target portal and IQN are required") {
		t.Errorf("NodePublishVolume() = %v, want it to have tried staging the volume", err)
	}
}

func TestNodePublishVolume_StagesAgainWhenStagingIsUnreadable(t *testing.T) {
	useTempConnectorDir(t)
	staging := stagingDir(t)
	target := filepath.Join(t.TempDir(), "mount")
	ns, fake := newTestNodeServer(t, []mount.MountPoint{{Device: "/dev/sda", Path: staging, Type: "xfs"}})
	fake.MountCheckErrors = map[string]error{staging: eio(staging)}

	if _, err := ns.NodePublishVolume(context.Background(), publishRequest(ProtocolISCSI, staging, target)); err == nil {
		t.Fatal("NodePublishVolume() published from an unreadable staging mount")
	}
	if !unmounted(fake, staging) {
		t.Errorf("the unreadable staging mount was not replaced, actions: %v", fake.GetLog())
	}
}

func TestNodePublishVolume_ReplacesUnreadableBlockTarget(t *testing.T) {
	useTempConnectorDir(t)
	target := stagingDir(t)
	ns, fake := newTestNodeServer(t, []mount.MountPoint{{Device: "/dev/sda", Path: target, Type: "xfs"}})
	fake.MountCheckErrors = map[string]error{target: eio(target)}

	_, _ = ns.NodePublishVolume(context.Background(), publishRequest(ProtocolISCSI, "/not/staged", target))
	if !unmounted(fake, target) {
		t.Errorf("the unreadable publish mount was not released, actions: %v", fake.GetLog())
	}
}

func TestCheckMountPoint(t *testing.T) {
	dir := stagingDir(t)
	missing := filepath.Join(t.TempDir(), "gone")
	fake := mount.NewFakeMounter([]mount.MountPoint{{Device: "/dev/sda", Path: dir}})

	if mounted, corrupt, err := checkMountPoint(fake, dir); !mounted || corrupt != nil || err != nil {
		t.Errorf("healthy mount: mounted=%v corrupt=%v err=%v", mounted, corrupt, err)
	}
	if mounted, corrupt, err := checkMountPoint(fake, missing); mounted || corrupt != nil || err != nil {
		t.Errorf("missing path: mounted=%v corrupt=%v err=%v", mounted, corrupt, err)
	}
	fake.MountCheckErrors = map[string]error{dir: eio(dir)}
	if mounted, corrupt, err := checkMountPoint(fake, dir); !mounted || corrupt == nil || err != nil {
		t.Errorf("unreadable mount: mounted=%v corrupt=%v err=%v", mounted, corrupt, err)
	}
	other := &os.PathError{Op: "stat", Path: dir, Err: syscall.ELOOP}
	fake.MountCheckErrors = map[string]error{dir: other}
	if _, _, err := checkMountPoint(fake, dir); err == nil {
		t.Error("an error unrelated to a dead mount must be returned")
	}
}
