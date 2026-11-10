package driver

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	iscsilib "github.com/kubernetes-csi/csi-lib-iscsi/iscsi"
	"github.com/truenas/truenas-csi/pkg/client"
	"k8s.io/mount-utils"
)

const (
	defaultMountHealCooldown = 10 * time.Minute
	defaultKubeletDir        = "/var/lib/kubelet"
	portalDialTimeout        = 2 * time.Second

	// softFailThreshold is how many scans in a row a volume must fail by not
	// answering in time before it is acted on. A volume under heavy load can be
	// slow without being dead; one that returns I/O errors is acted on at once.
	softFailThreshold = 3

	// maxHealRestarts bounds the rounds of pod restarts for a volume that stays
	// dead afterwards. That points at a damaged filesystem rather than the network,
	// which restarting the same pods again will not fix.
	maxHealRestarts = 3
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
	healActionListFailed        = "list_pods_failed"
	healActionConsumerNotFound  = "consumer_not_found"
	healActionGaveUp            = "gave_up"
)

var iscsiSessionInPath = regexp.MustCompile(`/session(\d+)/`)

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
	state       map[string]*healState // by staging path

	listMounts      func() ([]mount.MountInfo, error)
	portalReachable func(portal string) bool
	now             func() time.Time
}

// healState is what the healer remembers about one staged volume while it is
// unhealthy. It is dropped as soon as the volume checks out, or goes away.
type healState struct {
	softFails   int       // consecutive scans that failed by timeout only
	restarts    int       // rounds of pod restarts so far
	lastRestart time.Time // start of the cooldown
	gaveUp      bool
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
		state:       map[string]*healState{},
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
	seen := map[string]bool{}
	reachable := map[string]bool{}
	anyReachable := func(portals []string) bool {
		return slices.ContainsFunc(portals, func(p string) bool {
			if ok, known := reachable[p]; known {
				return ok
			}
			reachable[p] = h.portalReachable(p)
			return reachable[p]
		})
	}

	for _, staged := range h.stagedMounts(infos) {
		path := staged.info.MountPoint
		seen[path] = true

		st := h.state[path]
		if st != nil && !st.lastRestart.IsZero() && h.now().Sub(st.lastRestart) < h.cfg.Cooldown {
			continue
		}

		// While storage is unreachable there is nothing to do: restarting pods
		// would only trade I/O errors for a stage that cannot complete. The mount
		// is not even probed then, as a read would sit in the kernel until the
		// session's replacement timeout, holding the mount busy.
		if portals := h.portalsFor(staged.info); len(portals) > 0 && !anyReachable(portals) {
			h.record(healActionPortalUnreachable)
			log.V(LogLevelInfo).Info("Storage for a staged volume is unreachable, waiting",
				"stagingPath", path, "portals", portals)
			continue
		}

		// A filesystem mounted read-only on request carries "ro" in its per-mount
		// options. One the kernel turned read-only after an I/O error keeps "rw"
		// there and only its superblock is read-only, which statfs then reports.
		readOnly := slices.Contains(staged.info.MountOptions, mountOptionReadOnly)
		reason, soft := stagedMountVerdict(path, readOnly)
		if reason == "" {
			delete(h.state, path)
			continue
		}
		h.record(healActionUnusable)
		if st == nil {
			st = &healState{}
			h.state[path] = st
		}

		if soft {
			st.softFails++
			if st.softFails < softFailThreshold {
				log.V(LogLevelInfo).Info("Staged volume did not answer in time, checking again next scan",
					"stagingPath", path, "reason", reason, "consecutive", st.softFails, "threshold", softFailThreshold)
				continue
			}
		} else {
			st.softFails = 0
		}

		if st.restarts >= maxHealRestarts {
			if !st.gaveUp {
				st.gaveUp = true
				h.record(healActionGaveUp)
				log.Error(nil, "Staged volume is still unusable after restarting its pods, leaving it alone; "+
					"the filesystem may need repair",
					"stagingPath", path, "reason", reason, "restarts", st.restarts)
			}
			continue
		}

		if len(staged.consumerPods) == 0 {
			h.record(healActionNoConsumers)
			log.Info("Staged volume is unusable and no pod on this node uses it; kubelet unstages it",
				"stagingPath", path, "reason", reason)
			st.lastRestart = h.now()
			continue
		}

		if !podsLoaded {
			podsOnNode, err = h.pods.ListNodePods(ctx, h.node.driver.NodeID())
			if err != nil {
				h.record(healActionListFailed)
				log.Error(err, "Mount healer could not list the pods on this node")
				return
			}
			podsLoaded = true
		}

		log.Info("Staged volume is unusable, restarting the pods that use it",
			"stagingPath", path, "reason", reason, "pods", len(staged.consumerPods))
		if h.restartConsumers(ctx, path, staged.consumerPods, podsOnNode) > 0 {
			st.restarts++
		}
		st.lastRestart = h.now()
	}

	// A volume that is no longer staged is being handled by kubelet already.
	for path := range h.state {
		if !seen[path] {
			delete(h.state, path)
		}
	}
}

// restartConsumers deletes the controller-owned pods among uids and returns how
// many deletions it issued.
func (h *mountHealer) restartConsumers(ctx context.Context, stagingPath string, uids []string, podsOnNode []healPod) int {
	log := h.node.driver.Log()
	deleted := 0
	for _, uid := range uids {
		idx := slices.IndexFunc(podsOnNode, func(p healPod) bool { return p.UID == uid })
		if idx < 0 {
			// Usually a pod that is already gone, whose volume kubelet is tearing
			// down. It also happens when the node ID is not the Kubernetes node name.
			h.record(healActionConsumerNotFound)
			log.Info("Pod using an unusable volume is not among the pods of this node",
				"podUID", uid, "nodeID", h.node.driver.NodeID(), "stagingPath", stagingPath)
			continue
		}
		pod := podsOnNode[idx]
		switch {
		case pod.Terminating:
			continue
		case !pod.Controlled:
			// Nothing would recreate it, so deleting it would lose the workload.
			h.record(healActionSkippedUnmanaged)
			log.Info("Pod uses an unusable volume but no workload controller will recreate it, restart it manually",
				"pod", pod.Namespace+"/"+pod.Name, "stagingPath", stagingPath)
		default:
			if err := h.pods.DeletePod(ctx, pod); err != nil {
				h.record(healActionDeleteFailed)
				log.Error(err, "Mount healer could not delete pod", "pod", pod.Namespace+"/"+pod.Name)
				continue
			}
			deleted++
			h.record(healActionPodDeleted)
			log.Info("Mount healer deleted pod so its volume is staged again",
				"pod", pod.Namespace+"/"+pod.Name, "stagingPath", stagingPath)
		}
	}
	return deleted
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

// portalsFor returns the storage portals a staged volume depends on, each with a
// port: the ones in its iSCSI connector file when one matches its device, else the
// driver's configured portal for the device's protocol.
func (h *mountHealer) portalsFor(info mount.MountInfo) []string {
	if strings.HasPrefix(info.Source, devicePrefixNVMe) {
		if p := h.node.driver.nvmeofPortal; p != "" {
			return []string{withDefaultPort(p, strconv.Itoa(client.NVMeDefaultPort))}
		}
		return nil
	}
	if portals := iscsiPortalsForDevice(filepath.Base(info.Source)); len(portals) > 0 {
		out := make([]string, len(portals))
		for i, p := range portals {
			out[i] = withDefaultPort(p, iscsiDefaultPort)
		}
		return out
	}
	if p := h.node.driver.iscsiPortal; p != "" {
		return []string{withDefaultPort(p, iscsiDefaultPort)}
	}
	return nil
}

func withDefaultPort(portal, port string) string {
	if _, _, err := net.SplitHostPort(portal); err == nil {
		return portal
	}
	return net.JoinHostPort(strings.Trim(portal, "[]"), port)
}

// iscsiTargetIQN returns the IQN of the session behind a block device, or "" when
// sysfs does not say. Device names are reused across logins, so the name alone
// can point at another volume's connector file.
func iscsiTargetIQN(devName string) string {
	resolved, err := filepath.EvalSymlinks(filepath.Join(sysfsRoot, "block", devName))
	if err != nil {
		return ""
	}
	m := iscsiSessionInPath.FindStringSubmatch(filepath.ToSlash(resolved))
	if m == nil {
		return ""
	}
	data, err := os.ReadFile(filepath.Join(sysfsRoot, "class", "iscsi_session", "session"+m[1], "targetname"))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func iscsiPortalsForDevice(devName string) []string {
	entries, err := os.ReadDir(connectorDir)
	if err != nil {
		return nil
	}
	iqn := iscsiTargetIQN(devName)
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
		var owns bool
		if iqn != "" {
			owns = c.TargetIqn == iqn
		} else {
			owns = c.MountTargetDevice != nil && c.MountTargetDevice.Name == devName
			owns = owns || slices.ContainsFunc(c.Devices, func(d iscsilib.Device) bool { return d.Name == devName })
		}
		if owns {
			return c.TargetPortals
		}
	}
	return nil
}

// dialPortal reports whether a host:port accepts a TCP connection.
func dialPortal(portal string) bool {
	conn, err := net.DialTimeout("tcp", portal, portalDialTimeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func (h *mountHealer) record(action string) {
	h.node.driver.metrics.RecordMountHeal(action)
}
