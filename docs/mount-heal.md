# Mount healer

When the network path between the nodes and TrueNAS goes down for longer than
the iSCSI replacement timeout (a router reboot, for example), the block device
under a staged volume fails its I/O. XFS shuts down and keeps returning `EIO`;
ext4 aborts to read-only. The sessions come back once the network does, but the
filesystems do not.

Kubelet does not notice. It still considers the volume staged and published, so
it never calls `NodeStageVolume` again, and the pods keep failing until someone
restarts them. Remounting from the node plugin would not help: every running
container holds the dead mount in its own mount namespace.

## What the driver does

Always on:

- `NodeStageVolume` replaces a staging mount that is unusable instead of
  reporting it as already staged.
- `NodePublishVolume` refuses to bind-mount from an unusable staging mount, and
  replaces an unusable existing target mount.

Opt-in, the mount healer in the node plugin. Every interval it:

1. Reads the node's mount table and picks this driver's block staging mounts.
2. Checks each one: `statfs`, the read-only flag when the volume was not
   mounted read-only on request, and a directory read bounded by a 5 second
   timeout. A probe that is still hanging from an earlier round counts as a
   failure, and no second probe is started on the same mount.
3. Skips the volume while none of its portals accept a TCP connection, since
   restarting pods then would only trade I/O errors for a failing stage. The
   portals come from the iSCSI connector file, or the driver's configured
   iSCSI or NVMe-oF portal.
4. Deletes the pods on this node that use the volume, so kubelet unpublishes
   and unstages it and the next `NodeStageVolume` mounts it fresh. Only pods
   with a controller (Deployment, StatefulSet, Job, ...) are deleted, with a UID
   precondition. A pod without a controller is left alone and logged, since
   nothing would recreate it.
5. Leaves the volume alone for the cooldown period, so kubelet has time to
   restage it before the next attempt.

The healer never mounts, unmounts, or logs out of sessions itself; that stays
with the regular CSI calls.

## Enabling it

With the Helm chart:

```yaml
node:
  mountHeal:
    enabled: true
    interval: 1m
    cooldown: 10m
```

This also adds `delete` on pods to the node plugin's ClusterRole.

With the plain manifest, set these on the `csi-node` container of the
node DaemonSet and add `delete` to the pods rule of `truenas-csi-node-role`:

| Environment variable | Default | Description |
|---|---|---|
| `TRUENAS_MOUNT_HEAL_INTERVAL` | unset (off) | How often to check staged volumes, for example `1m` |
| `TRUENAS_MOUNT_HEAL_COOLDOWN` | `10m` | Minimum time between two restarts for the same volume |
| `TRUENAS_KUBELET_DIR` | `/var/lib/kubelet` | Kubelet root directory |

## Observing it

Healer outcomes are counted in `truenas_csi_mount_heal_actions_total{action}`
when node metrics are enabled:

| Action | Meaning |
|---|---|
| `unusable_mount` | A staged volume failed its check |
| `portal_unreachable` | ...and storage was still unreachable, so nothing was done |
| `no_consumers` | ...and no pod on the node used it |
| `pod_deleted` | A pod was deleted so its volume is staged again |
| `pod_skipped_unmanaged` | A pod without a controller uses a dead volume and needs a manual restart |
| `pod_delete_failed` | The API refused or failed the deletion |

## Limits

- Pods are restarted, which is the disruption the outage already caused; the
  healer only shortens it.
- Short blips are better handled on the host by the open-iscsi
  `replacement_timeout`, which keeps I/O queued instead of failing it.
