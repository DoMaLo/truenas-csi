package driver

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	iscsilib "github.com/kubernetes-csi/csi-lib-iscsi/iscsi"
	"k8s.io/mount-utils"
)

const (
	testStaging = "/var/lib/kubelet/plugins/kubernetes.io/csi/csi.truenas.io/abc/globalmount"
	testPodUID  = "11111111-2222-3333-4444-555555555555"
)

func testPodMount(uid string) string {
	return "/var/lib/kubelet/pods/" + uid + "/volumes/kubernetes.io~csi/pvc-abc/mount"
}

type fakePodAPI struct {
	pods    []healPod
	listed  int
	deleted []string
}

func (f *fakePodAPI) ListNodePods(context.Context, string) ([]healPod, error) {
	f.listed++
	return f.pods, nil
}

func (f *fakePodAPI) DeletePod(_ context.Context, pod healPod) error {
	f.deleted = append(f.deleted, pod.Namespace+"/"+pod.Name)
	return nil
}

type healerFixture struct {
	healer    *mountHealer
	pods      *fakePodAPI
	now       time.Time
	reachable bool
}

func newHealerFixture(t *testing.T, infos []mount.MountInfo, pods ...healPod) *healerFixture {
	t.Helper()
	useTempConnectorDir(t)
	ns, _ := newTestNodeServer(t, nil)
	ns.driver.name = "csi.truenas.io"
	ns.driver.nodeID = "w-01"
	ns.driver.iscsiPortal = "192.0.2.10:3260"

	f := &healerFixture{pods: &fakePodAPI{pods: pods}, now: time.Unix(1_700_000_000, 0), reachable: true}
	f.healer = newMountHealerWithAPI(ns, MountHealConfig{Interval: time.Minute}, f.pods)
	f.healer.listMounts = func() ([]mount.MountInfo, error) { return infos, nil }
	f.healer.portalReachable = func(string) bool { return f.reachable }
	f.healer.now = func() time.Time { return f.now }
	return f
}

func stagedWithConsumer(source string, mountOpts ...string) []mount.MountInfo {
	if len(mountOpts) == 0 {
		mountOpts = []string{"rw", "relatime"}
	}
	return []mount.MountInfo{
		{Major: 8, Minor: 16, Source: source, MountPoint: testStaging, FsType: "xfs", MountOptions: mountOpts},
		{Major: 8, Minor: 16, Source: source, MountPoint: testPodMount(testPodUID), FsType: "xfs", MountOptions: mountOpts},
		{Major: 8, Minor: 16, Source: source, MountPoint: "/mnt/unrelated", FsType: "xfs"},
		{Major: 8, Minor: 32, Source: "/dev/sdc", MountPoint: testPodMount("other-pod"), FsType: "xfs"},
	}
}

func controlledPod(name string) healPod {
	return healPod{Namespace: "monitoring", Name: name, UID: testPodUID, Controlled: true}
}

func TestStagedMounts_MatchesConsumersByDevice(t *testing.T) {
	f := newHealerFixture(t, nil)
	got := f.healer.stagedMounts(stagedWithConsumer("/dev/sdb"))
	if len(got) != 1 {
		t.Fatalf("stagedMounts() returned %d mounts, want 1", len(got))
	}
	if got[0].info.MountPoint != testStaging {
		t.Errorf("staging path = %q, want %q", got[0].info.MountPoint, testStaging)
	}
	if !slices.Equal(got[0].consumerPods, []string{testPodUID}) {
		t.Errorf("consumer pods = %v, want [%s]", got[0].consumerPods, testPodUID)
	}
}

func TestStagedMounts_IgnoresOtherDriversAndNonBlockSources(t *testing.T) {
	f := newHealerFixture(t, nil)
	infos := []mount.MountInfo{
		{Major: 8, Minor: 1, Source: "/dev/sdd", MountPoint: "/var/lib/kubelet/plugins/kubernetes.io/csi/other.csi.io/x/globalmount"},
		{Major: 0, Minor: 50, Source: "nas:/export", MountPoint: "/var/lib/kubelet/plugins/kubernetes.io/csi/csi.truenas.io/y/globalmount"},
		{Major: 8, Minor: 2, Source: "/dev/sde", MountPoint: "/var/lib/kubelet/plugins/kubernetes.io/csi/csi.truenas.io/z/other"},
	}
	if got := f.healer.stagedMounts(infos); len(got) != 0 {
		t.Errorf("stagedMounts() = %v, want none", got)
	}
}

func TestPodUIDFromVolumePath(t *testing.T) {
	f := newHealerFixture(t, nil)
	tests := map[string]string{
		testPodMount(testPodUID): testPodUID,
		"/var/lib/kubelet/pods/" + testPodUID + "/volumes/kubernetes.io~empty-dir/x":     "",
		"/var/lib/kubelet/pods/" + testPodUID + "/volumes/kubernetes.io~csi/pvc/mount/x": "",
		"/mnt/pods/" + testPodUID + "/volumes/kubernetes.io~csi/pvc/mount":               "",
	}
	for path, want := range tests {
		if got := f.healer.podUIDFromVolumePath(path); got != want {
			t.Errorf("podUIDFromVolumePath(%q) = %q, want %q", path, got, want)
		}
	}
}

func TestScan_HealthyMountIsLeftAlone(t *testing.T) {
	stubStatfs(t, 0, nil)
	stubMountProbe(t, nil)
	f := newHealerFixture(t, stagedWithConsumer("/dev/sdb"), controlledPod("loki-0"))

	f.healer.scan(context.Background())

	if f.pods.listed != 0 || len(f.pods.deleted) != 0 {
		t.Errorf("healthy mount must not touch pods, listed=%d deleted=%v", f.pods.listed, f.pods.deleted)
	}
}

func TestScan_DeletesControlledConsumerOfDeadMount(t *testing.T) {
	stubStatfs(t, 0, nil)
	stubMountProbe(t, syscall.EIO)
	f := newHealerFixture(t, stagedWithConsumer("/dev/sdb"), controlledPod("loki-0"))

	f.healer.scan(context.Background())

	if !slices.Equal(f.pods.deleted, []string{"monitoring/loki-0"}) {
		t.Errorf("deleted = %v, want [monitoring/loki-0]", f.pods.deleted)
	}
}

func TestScan_ErrorRemountedReadOnly(t *testing.T) {
	stubStatfs(t, statfsFlagReadOnly, nil)
	stubMountProbe(t, nil)
	f := newHealerFixture(t, stagedWithConsumer("/dev/sdb"), controlledPod("loki-0"))

	f.healer.scan(context.Background())

	if len(f.pods.deleted) != 1 {
		t.Errorf("an ext4 volume aborted to read-only must be healed, deleted = %v", f.pods.deleted)
	}
}

func TestScan_IntentionallyReadOnlyMountIsLeftAlone(t *testing.T) {
	stubStatfs(t, statfsFlagReadOnly, nil)
	stubMountProbe(t, nil)
	f := newHealerFixture(t, stagedWithConsumer("/dev/sdb", "ro", "relatime"), controlledPod("loki-0"))

	f.healer.scan(context.Background())

	if len(f.pods.deleted) != 0 {
		t.Errorf("a volume mounted read-only on request must be left alone, deleted = %v", f.pods.deleted)
	}
}

func TestScan_WaitsWhileStorageIsUnreachable(t *testing.T) {
	stubStatfs(t, 0, nil)
	stubMountProbe(t, syscall.EIO)
	f := newHealerFixture(t, stagedWithConsumer("/dev/sdb"), controlledPod("loki-0"))
	f.reachable = false

	f.healer.scan(context.Background())
	if len(f.pods.deleted) != 0 {
		t.Fatalf("pods must not be restarted while storage is unreachable, deleted = %v", f.pods.deleted)
	}

	f.reachable = true
	f.healer.scan(context.Background())
	if len(f.pods.deleted) != 1 {
		t.Errorf("pods must be restarted once storage is back, deleted = %v", f.pods.deleted)
	}
}

func TestScan_SkipsUnmanagedAndTerminatingPods(t *testing.T) {
	stubStatfs(t, 0, nil)
	stubMountProbe(t, syscall.EIO)

	for name, pod := range map[string]healPod{
		"unmanaged":   {Namespace: "default", Name: "bare", UID: testPodUID},
		"terminating": {Namespace: "default", Name: "leaving", UID: testPodUID, Controlled: true, Terminating: true},
	} {
		t.Run(name, func(t *testing.T) {
			f := newHealerFixture(t, stagedWithConsumer("/dev/sdb"), pod)
			f.healer.scan(context.Background())
			if len(f.pods.deleted) != 0 {
				t.Errorf("deleted = %v, want none", f.pods.deleted)
			}
		})
	}
}

func TestScan_CooldownBetweenRounds(t *testing.T) {
	stubStatfs(t, 0, nil)
	stubMountProbe(t, syscall.EIO)
	f := newHealerFixture(t, stagedWithConsumer("/dev/sdb"), controlledPod("loki-0"))

	f.healer.scan(context.Background())
	f.now = f.now.Add(time.Minute)
	f.healer.scan(context.Background())
	if len(f.pods.deleted) != 1 {
		t.Fatalf("a second round within the cooldown must not delete again, deleted = %v", f.pods.deleted)
	}

	f.now = f.now.Add(defaultMountHealCooldown)
	f.healer.scan(context.Background())
	if len(f.pods.deleted) != 2 {
		t.Errorf("a mount still dead after the cooldown must be retried, deleted = %v", f.pods.deleted)
	}
}

func TestPortalsFor_PrefersISCSIConnector(t *testing.T) {
	f := newHealerFixture(t, nil)

	connector := iscsilib.Connector{
		VolumeName:        "SSD/k8s/csi/pvc-abc",
		TargetPortals:     []string{"198.51.100.7:3260"},
		MountTargetDevice: &iscsilib.Device{Name: "sdb"},
		Devices:           []iscsilib.Device{{Name: "sdb"}},
	}
	data, err := json.Marshal(connector)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(connectorDir, sanitizeISCSIVolumeID(connector.VolumeName)+".connector")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	if got := f.healer.portalsFor(mount.MountInfo{Source: "/dev/sdb"}); !slices.Equal(got, connector.TargetPortals) {
		t.Errorf("portalsFor(/dev/sdb) = %v, want %v", got, connector.TargetPortals)
	}
	if got := f.healer.portalsFor(mount.MountInfo{Source: "/dev/sdz"}); !slices.Equal(got, []string{"192.0.2.10:3260"}) {
		t.Errorf("portalsFor(/dev/sdz) = %v, want the configured portal", got)
	}
}

func TestPortalsFor_AddsDefaultPortPerProtocol(t *testing.T) {
	f := newHealerFixture(t, nil)
	f.healer.node.driver.iscsiPortal = "192.0.2.10"
	f.healer.node.driver.nvmeofPortal = "192.0.2.10"

	if got := f.healer.portalsFor(mount.MountInfo{Source: "/dev/sdz"}); !slices.Equal(got, []string{"192.0.2.10:3260"}) {
		t.Errorf("iSCSI portal = %v, want port 3260", got)
	}
	if got := f.healer.portalsFor(mount.MountInfo{Source: "/dev/nvme0n1"}); !slices.Equal(got, []string{"192.0.2.10:4420"}) {
		t.Errorf("NVMe-oF portal = %v, want port 4420", got)
	}
}

func TestWithDefaultPort(t *testing.T) {
	tests := map[string]string{
		"192.0.2.10":      "192.0.2.10:3260",
		"192.0.2.10:3261": "192.0.2.10:3261",
		"fd00::1":         "[fd00::1]:3260",
		"[fd00::1]:3260":  "[fd00::1]:3260",
		"[fd00::1]":       "[fd00::1]:3260",
	}
	for in, want := range tests {
		if got := withDefaultPort(in, "3260"); got != want {
			t.Errorf("withDefaultPort(%q) = %q, want %q", in, got, want)
		}
	}
}

// A device name can belong to another volume's connector file after logins have
// come and gone, so the session's IQN decides when sysfs knows it.
func TestPortalsFor_MatchesConnectorByTargetIQN(t *testing.T) {
	f := newHealerFixture(t, nil)

	sys := t.TempDir()
	orig := sysfsRoot
	sysfsRoot = sys
	t.Cleanup(func() { sysfsRoot = orig })
	devDir := filepath.Join(sys, "devices", "platform", "host3", "session7", "target3:0:0", "3:0:0:0", "block", "sdb")
	sessionDir := filepath.Join(sys, "class", "iscsi_session", "session7")
	for _, dir := range []string{devDir, sessionDir, filepath.Join(sys, "block")} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(devDir, filepath.Join(sys, "block", "sdb")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sessionDir, "targetname"), []byte("iqn.test:current\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	write := func(name, iqn, portal string) {
		data, err := json.Marshal(iscsilib.Connector{
			VolumeName:        name,
			TargetIqn:         iqn,
			TargetPortals:     []string{portal},
			MountTargetDevice: &iscsilib.Device{Name: "sdb"}, // stale: the name was reused
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(connectorDir, sanitizeISCSIVolumeID(name)+".connector"), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("a-old", "iqn.test:old", "198.51.100.1:3260")
	write("b-current", "iqn.test:current", "198.51.100.2:3260")

	if got := f.healer.portalsFor(mount.MountInfo{Source: "/dev/sdb"}); !slices.Equal(got, []string{"198.51.100.2:3260"}) {
		t.Errorf("portalsFor(/dev/sdb) = %v, want the portal of the connector with the session's IQN", got)
	}
}

func TestScan_SlowProbeNeedsRepeatedFailures(t *testing.T) {
	stubStatfs(t, 0, nil)
	stubMountProbe(t, errProbeTimeout)
	f := newHealerFixture(t, stagedWithConsumer("/dev/sdb"), controlledPod("loki-0"))

	for i := 1; i < softFailThreshold; i++ {
		f.healer.scan(context.Background())
		if len(f.pods.deleted) != 0 {
			t.Fatalf("scan %d: a volume that is merely slow must not lose its pods yet, deleted = %v", i, f.pods.deleted)
		}
	}
	f.healer.scan(context.Background())
	if len(f.pods.deleted) != 1 {
		t.Errorf("a volume that never answers must be healed after %d scans, deleted = %v", softFailThreshold, f.pods.deleted)
	}
}

func TestScan_RecoveredVolumeResetsSlowCount(t *testing.T) {
	stubStatfs(t, 0, nil)
	f := newHealerFixture(t, stagedWithConsumer("/dev/sdb"), controlledPod("loki-0"))

	for i := 0; i < softFailThreshold-1; i++ {
		stubMountProbe(t, errProbeTimeout)
		f.healer.scan(context.Background())
	}
	stubMountProbe(t, nil)
	f.healer.scan(context.Background())
	stubMountProbe(t, errProbeTimeout)
	f.healer.scan(context.Background())

	if len(f.pods.deleted) != 0 {
		t.Errorf("slow scans separated by a healthy one must not add up, deleted = %v", f.pods.deleted)
	}
}

func TestScan_IOErrorActsImmediately(t *testing.T) {
	stubStatfs(t, 0, nil)
	stubMountProbe(t, syscall.EIO)
	f := newHealerFixture(t, stagedWithConsumer("/dev/sdb"), controlledPod("loki-0"))

	f.healer.scan(context.Background())
	if len(f.pods.deleted) != 1 {
		t.Errorf("an I/O error is conclusive, deleted = %v", f.pods.deleted)
	}
}

func TestScan_GivesUpOnVolumeThatStaysDead(t *testing.T) {
	stubStatfs(t, 0, nil)
	stubMountProbe(t, syscall.EIO)
	f := newHealerFixture(t, stagedWithConsumer("/dev/sdb"), controlledPod("loki-0"))

	for i := 0; i < maxHealRestarts+3; i++ {
		f.now = f.now.Add(defaultMountHealCooldown)
		f.healer.scan(context.Background())
	}
	if len(f.pods.deleted) != maxHealRestarts {
		t.Errorf("deleted %d times, want it capped at %d", len(f.pods.deleted), maxHealRestarts)
	}
}

func TestScan_VolumeThatIsGoneIsForgotten(t *testing.T) {
	stubStatfs(t, 0, nil)
	stubMountProbe(t, syscall.EIO)
	infos := stagedWithConsumer("/dev/sdb")
	f := newHealerFixture(t, infos, controlledPod("loki-0"))

	f.healer.scan(context.Background())
	if len(f.healer.state) != 1 {
		t.Fatalf("state = %v, want one entry", f.healer.state)
	}
	f.healer.listMounts = func() ([]mount.MountInfo, error) { return nil, nil }
	f.healer.scan(context.Background())
	if len(f.healer.state) != 0 {
		t.Errorf("state = %v, want it dropped once the volume is unstaged", f.healer.state)
	}
}

func TestScan_ConsumerMissingFromNodePods(t *testing.T) {
	stubStatfs(t, 0, nil)
	stubMountProbe(t, syscall.EIO)
	f := newHealerFixture(t, stagedWithConsumer("/dev/sdb"), healPod{Namespace: "x", Name: "other", UID: "different", Controlled: true})

	f.healer.scan(context.Background())
	if len(f.pods.deleted) != 0 {
		t.Errorf("deleted = %v, want none", f.pods.deleted)
	}
}
