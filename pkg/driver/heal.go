package driver

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	iscsilib "github.com/kubernetes-csi/csi-lib-iscsi/iscsi"
	"k8s.io/mount-utils"
)

const (
	defaultMountHealCooldown = 10 * time.Minute
	defaultKubeletDir        = "/var/lib/kubelet"
	defaultISCSIPort         = "3260"
	portalDialTimeout        = 2 * time.Second
)

// Mount healer outcomes, used as the metric label. A fixed set keeps cardinality
// bounded.
const (
	healActionUnusable          = "unusable_mount"
	healActionPortalUnreachable = "portal_unreachable"
	healActionNoConsumers       = "no_consumers"
	healActionPodDeleted        = "pod_deleted"
	healActionSkippedUnmanaged  = "pod_skipped_unmanaged"
	healActionDeleteFailed      = "pod_delete_failed"
)

// MountHealConfig configures the node plugin's mount healer.
type MountHealConfig struct {
	// Interval between scans. Zero disables the healer.
	Interval time.Duration
	// Cooldown is the minimum time between two rounds of pod deletions for the
	// same staged volume, which gives kubelet time to restage it.
	Cooldown time.Duration
	// KubeletDir is the kubelet root directory.
	KubeletDir string
}

// mountHealer recovers volumes whose staged filesystem died while workloads were
// still using it, typically after the network path to TrueNAS went down for
// longer than the iSCSI replacement timeout.
//
// Kubelet does not notice: the volume stays staged and published, so it never
// calls NodeStageVolume again and the pods keep hitting I/O errors. Remounting
// from the node plugin cannot fix this either, because every running container
// holds the dead mount in its own mount namespace. What does fix it is restarting
// the pods: kubelet then unpublishes and unstages the volume, and the next
// NodeStageVolume replaces the dead staging mount (see unusableStagedMountReason).
// The healer automates exactly that, and leaves all mount and session handling to
// the regular CSI calls.
type mountHealer struct {
	node        *NodeServer
	pods        podAPI
	cfg         MountHealConfig
	stagingRoot string
	podsRoot    string
	lastAction  map[string]time.Time

	listMounts      func() ([]mount.MountInfo, error)
	portalReachable func(portal string) bool
	now             func() time.Time
}

func newMountHealer(node *NodeServer, cfg MountHealConfig) (*mountHealer, error) {
	pods, err := newInClusterPodAPI()
	if err != nil {
		return nil, err
	}
	return newMountHealerWithAPI(node, cfg, pods), nil
}

func newMountHealerWithAPI(node *NodeServer, cfg MountHealConfig, pods podAPI) *mountHealer {
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = defaultMountHealCooldown
	}
	if cfg.KubeletDir == "" {
		cfg.KubeletDir = defaultKubeletDir
	}
	kubeletDir := filepath.Clean(cfg.KubeletDir)
	return &mountHealer{
		node:        node,
		pods:        pods,
		cfg:         cfg,
		stagingRoot: filepath.Join(kubeletDir, "plugins", "kubernetes.io", "csi", node.driver.name),
		podsRoot:    filepath.Join(kubeletDir, "pods"),
		lastAction:  map[string]time.Time{},
		listMounts: func() ([]mount.MountInfo, error) {
			return mount.ParseMountInfo("/proc/self/mountinfo")
		},
		portalReachable: dialPortal,
		now:             time.Now,
	}
}

func (h *mountHealer) run(ctx context.Context) {
	log := h.node.driver.Log()
	log.Info("Mount healer started", "interval", h.cfg.Interval.String(), "cooldown", h.cfg.Cooldown.String(),
		"stagingRoot", h.stagingRoot)

	ticker := time.NewTicker(h.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.scan(ctx)
		}
	}
}

// stagedMount is a staging mount of this driver together with the pod volume
// mounts bound from it.
type stagedMount struct {
	info         mount.MountInfo
	consumerPods []string // pod UIDs
}

func (h *mountHealer) scan(ctx context.Context) {
	log := h.node.driver.Log()

	infos, err := h.listMounts()
	if err != nil {
		log.Error(err, "Mount healer could not read the mount table")
		return
	}

	var podsOnNode []healPod
	podsLoaded := false

	for _, staged := range h.stagedMounts(infos) {
		path := staged.info.MountPoint
		if last, ok := h.lastAction[path]; ok && h.now().Sub(last) < h.cfg.Cooldown {
			continue
		}

		// A filesystem mounted read-only on request carries "ro" in its per-mount
		// options. One the kernel turned read-only after an I/O error keeps "rw"
		// there and only its superblock is read-only, which statfs then reports.
		readOnly := slices.Contains(staged.info.MountOptions, mountOptionReadOnly)
		reason := unusableStagedMountReason(path, readOnly)
		if reason == "" {
			delete(h.lastAction, path)
			continue
		}
		h.record(healActionUnusable)

		// Restarting pods while storage is still unreachable only moves the
		// failure from I/O errors to a stage that cannot complete.
		if portals := h.portalsFor(staged.info); len(portals) > 0 && !h.anyReachable(portals) {
			h.record(healActionPortalUnreachable)
			log.V(LogLevelInfo).Info("Staged volume is unusable but storage is still unreachable, waiting",
				"stagingPath", path, "reason", reason, "portals", portals)
			continue
		}

		if len(staged.consumerPods) == 0 {
			h.record(healActionNoConsumers)
			log.Info("Staged volume is unusable and no pod on this node uses it; kubelet unstages it",
				"stagingPath", path, "reason", reason)
			h.lastAction[path] = h.now()
			continue
		}

		if !podsLoaded {
			podsOnNode, err = h.pods.ListNodePods(ctx, h.node.driver.NodeID())
			if err != nil {
				log.Error(err, "Mount healer could not list the pods on this node")
				return
			}
			podsLoaded = true
		}

		log.Info("Staged volume is unusable, restarting the pods that use it",
			"stagingPath", path, "reason", reason, "pods", len(staged.consumerPods))
		h.restartConsumers(ctx, path, staged.consumerPods, podsOnNode)
		h.lastAction[path] = h.now()
	}

	for path, last := range h.lastAction {
		if h.now().Sub(last) >= h.cfg.Cooldown {
			delete(h.lastAction, path)
		}
	}
}

func (h *mountHealer) restartConsumers(ctx context.Context, stagingPath string, uids []string, podsOnNode []healPod) {
	log := h.node.driver.Log()
	for _, uid := range uids {
		idx := slices.IndexFunc(podsOnNode, func(p healPod) bool { return p.UID == uid })
		if idx < 0 {
			// The pod is gone and kubelet is tearing its volume down already.
			continue
		}
		pod := podsOnNode[idx]
		switch {
		case pod.Terminating:
			continue
		case !pod.Controlled:
			// Nothing would recreate it, so deleting it would lose the workload.
			h.record(healActionSkippedUnmanaged)
			log.Info("Pod uses an unusable volume but has no controller to recreate it, restart it manually",
				"pod", pod.Namespace+"/"+pod.Name, "stagingPath", stagingPath)
		default:
			if err := h.pods.DeletePod(ctx, pod); err != nil {
				h.record(healActionDeleteFailed)
				log.Error(err, "Mount healer could not delete pod", "pod", pod.Namespace+"/"+pod.Name)
				continue
			}
			h.record(healActionPodDeleted)
			log.Info("Mount healer deleted pod so its volume is staged again",
				"pod", pod.Namespace+"/"+pod.Name, "stagingPath", stagingPath)
		}
	}
}

// stagedMounts returns this driver's filesystem staging mounts and, for each, the
// UIDs of the pods whose volume mounts share its device.
func (h *mountHealer) stagedMounts(infos []mount.MountInfo) []stagedMount {
	type devKey struct{ major, minor int }
	byDev := map[devKey]*stagedMount{}
	var order []devKey

	for _, info := range infos {
		path := filepath.Clean(info.MountPoint)
		if !strings.HasPrefix(path, h.stagingRoot+string(os.PathSeparator)) || filepath.Base(path) != "globalmount" {
			continue
		}
		if !strings.HasPrefix(info.Source, devicePrefixDev) {
			continue
		}
		key := devKey{info.Major, info.Minor}
		if _, ok := byDev[key]; !ok {
			info.MountPoint = path
			byDev[key] = &stagedMount{info: info}
			order = append(order, key)
		}
	}

	for _, info := range infos {
		staged, ok := byDev[devKey{info.Major, info.Minor}]
		if !ok {
			continue
		}
		if uid := h.podUIDFromVolumePath(info.MountPoint); uid != "" && !slices.Contains(staged.consumerPods, uid) {
			staged.consumerPods = append(staged.consumerPods, uid)
		}
	}

	out := make([]stagedMount, 0, len(order))
	for _, key := range order {
		out = append(out, *byDev[key])
	}
	return out
}

// podUIDFromVolumePath extracts the pod UID from a CSI pod volume mount,
// <kubelet>/pods/<uid>/volumes/kubernetes.io~csi/<pv>/mount.
func (h *mountHealer) podUIDFromVolumePath(path string) string {
	rel, err := filepath.Rel(h.podsRoot, filepath.Clean(path))
	if err != nil || strings.HasPrefix(rel, "..") {
		return ""
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	if len(parts) != 5 || parts[1] != "volumes" || parts[2] != "kubernetes.io~csi" || parts[4] != "mount" {
		return ""
	}
	return parts[0]
}

// portalsFor returns the storage portals a staged volume depends on: the ones in
// its iSCSI connector file when one matches its device, else the driver's
// configured portals for the device's protocol.
func (h *mountHealer) portalsFor(info mount.MountInfo) []string {
	if strings.HasPrefix(info.Source, devicePrefixNVMe) {
		if p := h.node.driver.nvmeofPortal; p != "" {
			return []string{p}
		}
		return nil
	}
	if portals := iscsiPortalsForDevice(filepath.Base(info.Source)); len(portals) > 0 {
		return portals
	}
	if p := h.node.driver.iscsiPortal; p != "" {
		return []string{p}
	}
	return nil
}

func iscsiPortalsForDevice(devName string) []string {
	entries, err := os.ReadDir(connectorDir)
	if err != nil {
		return nil
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".connector") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(connectorDir, entry.Name()))
		if err != nil {
			continue
		}
		var c iscsilib.Connector
		if err := json.Unmarshal(data, &c); err != nil {
			continue
		}
		owns := c.MountTargetDevice != nil && c.MountTargetDevice.Name == devName
		owns = owns || slices.ContainsFunc(c.Devices, func(d iscsilib.Device) bool { return d.Name == devName })
		if owns {
			return c.TargetPortals
		}
	}
	return nil
}

func (h *mountHealer) anyReachable(portals []string) bool {
	return slices.ContainsFunc(portals, h.portalReachable)
}

func dialPortal(portal string) bool {
	addr := portal
	if _, _, err := net.SplitHostPort(portal); err != nil {
		addr = net.JoinHostPort(portal, defaultISCSIPort)
	}
	conn, err := net.DialTimeout("tcp", addr, portalDialTimeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func (h *mountHealer) record(action string) {
	h.node.driver.metrics.RecordMountHeal(action)
}
