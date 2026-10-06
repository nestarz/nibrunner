set -eu
scratch=$1
init=$2
scope=/sys/fs/cgroup$(sed -n 's/^0:://p' /proc/self/cgroup)
mkdir "$scope/supervisor"
printf '%s' "$$" > "$scope/supervisor/cgroup.procs"
printf '+memory' > "$scope/cgroup.subtree_control"
mkdir "$scope/tenant"
printf '67108864' > "$scope/tenant/memory.max"
printf '33554432' > "$scope/tenant/memory.swap.max"
printf '1' > "$scope/tenant/memory.oom.group"
printf '%s' "$scope" > "$scratch/cgroup"
exec unshare --mount --pid --fork --ipc --uts --net --mount-proc --propagation private \
    /bin/sh "$scratch/namespace.sh" "$scratch" "$scope" "$init"
