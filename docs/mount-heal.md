# Mount healer (DoMaLo fork)

After a transient network outage between Kubernetes nodes and TrueNAS (for
example a router reboot), iSCSI/NVMe-oF sessions may recover while the staged
XFS/ext4 mount stays dead: kubelet still considers the volume staged/published,
so it never calls `NodeStageVolume` again and workloads keep seeing EIO/EROFS.

This fork adds a **node-side mount healer** that periodically:

1. Finds CSI `globalmount` staging paths under the kubelet CSI plugin directory
2. Detects unusable mounts (`statfs` RO/error + short write probe)
3. Waits until the iSCSI portal is reachable again
4. Unmounts pod bind mounts, unstages (logout), restages, and rebinds consumers

## Configuration

Set on the **node** DaemonSet (csi-node container):

| Environment variable | Default | Description |
|---|---|---|
| `TRUENAS_MOUNT_HEAL_INTERVAL` | `30s` | How often to scan. Set `0` to disable. |
| `TRUENAS_KUBELET_CSI_PATH` | `/var/lib/kubelet/plugins/kubernetes.io/csi` | Root used to discover staging mounts |

Example:

```yaml
env:
  - name: TRUENAS_MOUNT_HEAL_INTERVAL
    value: "30s"
```

## Notes

- Publish also refuses to bind-mount from an unusable staging path, and remounts
  an unusable existing publish target.
- NVMe-oF volumes are torn down when unusable; full restage is left to kubelet
  because the on-disk NVMe connector does not store portal publish context.
- Prefer also tuning host `open-iscsi` `replacement_timeout` for short blips;
  the healer covers outages long enough that the filesystem has already failed.
