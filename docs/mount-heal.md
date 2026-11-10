# Recovering from storage outages

When the network path between the nodes and TrueNAS goes down (a router reboot,
for example), what happens to the volumes depends on how long it lasts compared
with the iSCSI replacement timeout: how long the kernel keeps I/O queued for a
session whose connection dropped before failing it.

## Outage shorter than the replacement timeout

I/O stalls and resumes when the session logs back in. Nothing breaks and nothing
has to be done. open-iscsi defaults to 120 seconds. To ride out longer outages,
raise it for the driver's sessions:

```yaml
node:
  iscsiReplacementTimeout: 5m
```

or `TRUENAS_ISCSI_REPLACEMENT_TIMEOUT=5m` on the node plugin. It is set on each
target's node record before login, and on live sessions through sysfs, both when
the node plugin starts and when a volume is staged. Unset, the host's setting is
left alone.

The cost: while the connection is down, processes doing I/O on the volume block
for up to that long, and a pod stuck that way cannot be stopped before the
outage ends or the timeout passes.

## Outage longer than the replacement timeout

The block device fails its I/O. XFS shuts down and answers everything, even
`stat`, with `EIO`; ext4 aborts to read-only. The sessions come back with the
network, but the filesystems do not. Kubelet still considers the volumes staged
and published, never calls `NodeStageVolume` again, and the pods keep failing.
Remounting from the node plugin would not help them: every running container
holds the dead mount in its own mount namespace. The pods have to be restarted,
and the volume staged again in between.

The driver handles each step of that:

- **Unstage and Unpublish** unmount a mount that cannot even be stat'ed.
  Without that, unstaging a shut-down XFS fails at the mount check forever.
- **Stage** replaces a staging mount that is unreadable, read-only when it
  should be writable, or fails a directory read with an I/O error, instead of
  reporting it as staged.
- **Publish** stages the volume again when its staging mount is missing or dead
  while kubelet believes it staged. That covers a pod coming back to the same
  node before kubelet got to unstage the volume, which kubelet then would not
  do.
- **The mount healer** (opt-in, below) restarts the pods once storage answers
  again.

A directory read that only times out does not count as dead in Stage and
Publish. Kubelet calls Stage again for volumes in use whenever it restarts, and
a volume that is just slow under load must not lose its session to that.

## The mount healer

Off unless enabled. Every interval, the node plugin:

1. Reads the node's mount table and picks this driver's block staging mounts.
2. Skips a volume while none of its portals accept a TCP connection. It is not
   even probed then: a read would sit in the kernel until the replacement
   timeout, holding the mount busy. The portals come from the iSCSI connector
   file of the session behind the device (matched by target IQN), or the
   driver's configured iSCSI or NVMe-oF portal.
3. Checks the volume: `statfs`, the read-only flag when it was not mounted
   read-only on request, and a directory read bounded by 5 seconds. A probe
   still hanging from an earlier round counts as a failure, and no second one
   is started on the same mount.
4. Acts on an I/O error at once, but on a read that only timed out after 3 scans
   in a row.
5. Deletes the pods on this node that use the volume, with a UID precondition.
   Kubelet then unpublishes and unstages it, and the replacement pod gets a
   fresh mount. Any pod with a controller is deleted (StatefulSet, ReplicaSet,
   Job, DaemonSet, and operators such as Strimzi's StrimziPodSet or
   CloudNativePG's Cluster), except the mirror pod of a static pod, which the
   Node controls and deleting restarts nothing. A pod without a controller is
   left alone and logged.
6. Leaves the volume alone for the cooldown period. If it is still dead after 3
   rounds of restarts, it stops and logs an error: that points at a damaged
   filesystem, which restarting pods does not repair.

The healer never mounts, unmounts, or logs out of sessions itself; that stays
with the CSI calls above.

### Enabling it

With the Helm chart:

```yaml
node:
  mountHeal:
    enabled: true
    interval: 1m
    cooldown: 10m
```

This also adds `delete` on pods to the node plugin's ClusterRole.

With the plain manifest, set these on the `csi-node` container of the node
DaemonSet and add `delete` to the pods rule of `truenas-csi-node-role`:

| Environment variable | Default | Description |
|---|---|---|
| `TRUENAS_MOUNT_HEAL_INTERVAL` | unset (off) | How often to check staged volumes, for example `1m` |
| `TRUENAS_MOUNT_HEAL_COOLDOWN` | `10m` | Minimum time between two restarts for the same volume |
| `TRUENAS_KUBELET_DIR` | `/var/lib/kubelet` | Kubelet root directory |

### Observing it

Healer outcomes are counted in `truenas_csi_mount_heal_actions_total{action}`
when node metrics are enabled:

| Action | Meaning |
|---|---|
| `portal_unreachable` | Storage for a staged volume is unreachable; checks wait |
| `unusable_mount` | A staged volume failed its check |
| `no_consumers` | ...and no pod on the node used it |
| `pod_deleted` | A pod was deleted so its volume is staged again |
| `pod_skipped_unmanaged` | A pod without a controller uses a dead volume and needs a manual restart |
| `pod_delete_failed` | The API refused or failed the deletion |
| `list_pods_failed` | The pods on the node could not be listed, for example because the role lacks permission |
| `consumer_not_found` | A pod mounting the volume is not among the node's pods; check that the node ID is the Kubernetes node name |
| `gave_up` | The volume stayed dead after the maximum rounds of restarts and needs repair |

## Limits

- Pods are restarted, which is the disruption the outage already caused; the
  healer only shortens it.
- Pods are deleted, not evicted, so PodDisruptionBudgets do not hold the healer
  back. The pods are already failing, and a budget that blocked their restart
  would keep the outage going.
- The node plugin's ClusterRole is shared by every node, so with the healer
  enabled each node plugin can delete pods in any namespace. The plugin already
  runs privileged on the host; weigh that before enabling it.
- A filesystem damaged beyond what its journal replays fails to mount at stage
  and needs `xfs_repair` or `fsck` by hand.
- Raw block volumes are not checked: there is no filesystem to probe.
- NFS mounts are never probed or torn down.
