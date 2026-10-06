set -eu
scratch=$1
scope=$2
init=$3
mount -t tmpfs -o mode=755,size=8M tmpfs /mnt
mkdir -p /mnt/root /mnt/volume
mount --bind "$scratch/root" /mnt/root
mount --bind "$scratch/volume" /mnt/volume
for directory in /usr /bin /lib /lib64; do
    if [ -d "$directory" ]; then
        mkdir -p "/mnt/root$directory"
        mount --bind "$directory" "/mnt/root$directory"
        mount -o remount,bind,ro "/mnt/root$directory"
    fi
done
mount --bind /proc /mnt/root/proc
mount --bind /dev/null /mnt/root/dev/null
mount --bind "$scope" /sys/fs/cgroup
mount -t tmpfs -o mode=755,size=1M tmpfs /run
mkdir -p /run/config /run/nibrunner/channels
mount --bind "$scratch/channels" /run/nibrunner/channels
touch /run/config/instance.env
mount --bind "$scratch/instance.env" /run/config/instance.env
exec "$init" --prepared-root
