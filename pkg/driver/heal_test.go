package driver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	iscsilib "github.com/kubernetes-csi/csi-lib-iscsi/iscsi"
	"k8s.io/mount-utils"
)

func TestConsumerMounts(t *testing.T) {
	staging := "/var/lib/kubelet/plugins/kubernetes.io/csi/csi.truenas.io/abc/globalmount"
	pod := "/var/lib/kubelet/pods/uuid/volumes/kubernetes.io~csi/pvc-abc/mount"
	mounts := []mount.MountPoint{
		{Device: "/dev/sda", Path: staging, Type: "xfs"},
		{Device: "/dev/sda", Path: pod, Type: "xfs"},
		{Device: "/dev/sda", Path: "/mnt/unrelated", Type: "xfs"},
	}
	got := consumerMounts(mounts, staging, "/dev/sda")
	if len(got) != 1 || got[0] != pod {
		t.Fatalf("consumerMounts() = %v, want [%s]", got, pod)
	}
}

func TestFindStagedBlockMounts_EnrichesFromISCSIConnector(t *testing.T) {
	dir := useTempConnectorDir(t)
	plugins := t.TempDir()
	staging := filepath.Join(plugins, "csi.truenas.io", "hash", "globalmount")
	if err := os.MkdirAll(staging, 0o750); err != nil {
		t.Fatal(err)
	}

	connector := iscsilib.Connector{
		VolumeName:    "SSD/k8s/csi/pvc-abc",
		TargetIqn:     "iqn.2025-10.ru.example:pvc-abc",
		TargetPortals: []string{"192.168.135.131:3260"},
		Lun:           0,
		MountTargetDevice: &iscsilib.Device{Name: "sdb"},
		Devices:           []iscsilib.Device{{Name: "sdb"}},
	}
	data, err := json.Marshal(connector)
	if err != nil {
		t.Fatal(err)
	}
	cpath := filepath.Join(dir, sanitizeISCSIVolumeID(connector.VolumeName)+".connector")
	if err := os.WriteFile(cpath, data, 0o600); err != nil {
		t.Fatal(err)
	}

	ns, _ := newTestNodeServer(t, []mount.MountPoint{
		{Device: "/dev/sdb", Path: staging, Type: "xfs"},
	})
	cands := ns.findStagedBlockMounts([]mount.MountPoint{
		{Device: "/dev/sdb", Path: staging, Type: "xfs"},
	}, plugins)
	if len(cands) != 1 {
		t.Fatalf("candidates = %d, want 1", len(cands))
	}
	if cands[0].VolumeID != connector.VolumeName {
		t.Errorf("VolumeID = %q, want %q", cands[0].VolumeID, connector.VolumeName)
	}
	if cands[0].Protocol != ProtocolISCSI {
		t.Errorf("Protocol = %q, want %q", cands[0].Protocol, ProtocolISCSI)
	}
	if cands[0].PublishCtx[PublishContextTargetPortal] != "192.168.135.131:3260" {
		t.Errorf("publish context portal = %q", cands[0].PublishCtx[PublishContextTargetPortal])
	}
}

func TestMountHealIntervalFromEnv(t *testing.T) {
	t.Setenv("TRUENAS_MOUNT_HEAL_INTERVAL", "")
	if got := mountHealIntervalFromEnv(); got != DefaultMountHealInterval {
		t.Errorf("default = %v, want %v", got, DefaultMountHealInterval)
	}
	t.Setenv("TRUENAS_MOUNT_HEAL_INTERVAL", "0")
	if got := mountHealIntervalFromEnv(); got != 0 {
		t.Errorf("disabled = %v, want 0", got)
	}
	t.Setenv("TRUENAS_MOUNT_HEAL_INTERVAL", "15s")
	if got := mountHealIntervalFromEnv(); got.Seconds() != 15 {
		t.Errorf("custom = %v, want 15s", got)
	}
}
