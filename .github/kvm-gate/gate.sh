#!/usr/bin/env bash
# Drives an installed, started nibrunnerd with desired-state documents and holds it to what it
# reports. Root, on a host `nibrunnerd install` laid out from its starter configuration.
#
#   gate.sh <tenant binary>
set -euo pipefail

state=/var/lib/nibrunner
tenant_digest=$(sha256sum "$1" | cut -d' ' -f1)
install -D -m 0644 "$1" "$state/artifact-store/gate-tenant"
max_apps=$(sed -n 's/^max_apps = //p' /etc/nibrunner/config.toml)
wanted_slots=$((max_apps + 8))

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
fail() {
    echo "FAILED: $*" >&2
    jq . "$state/reported.json" >&2 || true
    exit 1
}
now_ms() { date +%s%3N; }
expect() { [ "$1" = "$2" ] || fail "$3: expected $2, got $1"; }

# instance APP DESIRED_STATE MEMORY_MIB SCRATCH [ARGS] [MAX_RESTARTS]
instance() {
    jq -nc --arg app "$1" --arg desired "$2" --argjson memory "$3" --argjson scratch "$4" \
        --argjson args "${5:-[]}" --argjson restarts "${6:-3}" --arg digest "$tenant_digest" '
    {
      appId: $app, deploymentId: "dep-1", desiredState: $desired, scratch: $scratch,
      layers: [{ kind: "executable", destinationPath: "/app/tenant", digest: $digest, objectKey: "gate-tenant" }],
      config: {
        httpPort: 3000,
        command: { program: "/app/tenant", args: $args, workingDirectory: "/app", environment: {} },
        resources: { vcpuCount: 1, memoryMib: $memory },
        healthCheck: { kind: "http", path: "/", intervalMs: 500, timeoutMs: 2000, gracePeriodMs: 30000, healthyThreshold: 1, unhealthyThreshold: 3 },
        restartPolicy: { maxRestarts: $restarts, initialBackoffMs: 500, maxBackoffMs: 30000, backoffFactor: 2, resetAfterMs: 60000 }
      },
      hostnames: [{ hostname: "\($app).gate.test", kind: "platform" }]
    }
    + if $desired == "on-request" then { activation: { sleepWhen: { kind: "traffic-idle", timeoutMs: 60000 } } } else {} end'
}

# apply REVISION [INSTANCE...]: the whole document, waited on until the daemon has taken it up.
apply() {
    local revision=$1 digest
    shift
    printf '%s\n' "$@" | jq -s --arg revision "$revision" --argjson maxApps "$wanted_slots" \
        '{ hostId: "gate", revision: $revision, maxApps: $maxApps, volumes: [], instances: ., checkpoints: [], exports: [] }' \
        > "$state/desired.json.next"
    mv "$state/desired.json.next" "$state/desired.json"
    digest=$(sha256sum "$state/desired.json" | cut -d' ' -f1)
    for _ in $(seq 60); do
        [ "$(jq -r .acceptedDigest "$state/reported.json" 2>/dev/null)" = "$digest" ] && return
        sleep 1
    done
    fail "the document $revision was not taken up"
}

field() { jq -r --arg app "$1" ".instances[] | select(.appId == \$app) | .$2" "$state/reported.json"; }

# wait_for APP STATE SECONDS, printing how many milliseconds it took.
wait_for() {
    local started
    started=$(now_ms)
    for _ in $(seq "$3"); do
        if [ "$(field "$1" state)" = "$2" ]; then
            echo $(($(now_ms) - started))
            return
        fi
        sleep 1
    done
    fail "$1 did not become $2 within $3 s"
}

get() { curl -fsS --max-time 300 -H "Host: $1.gate.test" "http://127.0.0.1$2"; }

timed_get() {
    local started answer
    started=$(now_ms)
    answer=$(get "$1" "$2")
    echo "$answer $(($(now_ms) - started))"
}

say "(d) a desired maxApps of $wanted_slots, above config.toml's max_apps = $max_apps, is taken up"
apply d
slots=""
for _ in $(seq 30); do
    slots=$(curl -fsS http://127.0.0.1:9100/metrics | sed -n 's/^nibrunner_slots{of="total"} //p')
    [ "$slots" = "$wanted_slots" ] && break
    sleep 1
done
expect "$slots" "$wanted_slots" 'nibrunner_slots{of="total"}'
echo "PASS (d): $slots slots"

say "(a) a memory scratch survives sleep and wake, and is empty after stop and start"
memory='{"kind":"memory","mib":256}'
apply a-up "$(instance mem on-request 512 "$memory")"
boot_ms=$(wait_for mem running 180)
echo "cold boot with a memory scratch: $boot_ms ms"
expect "$(get mem /)" ok "/ on mem"
read -r written fill_ms < <(timed_get mem '/fill?mib=200')
expect "$written" 209715200 "bytes written to the memory scratch"
echo "200 MiB written in $fill_ms ms"
sleep_ms=$(wait_for mem idle 240)
echo "asleep after $sleep_ms ms"
read -r size wake_ms < <(timed_get mem /size)
expect "$size" 209715200 "the file after sleep and wake"
expect "$(get mem /)" ok "/ on mem after wake"
echo "woken, file intact, in $wake_ms ms"
apply a-stopped "$(instance mem stopped 512 "$memory")"
wait_for mem stopped 120 >/dev/null
apply a-started "$(instance mem on-request 512 "$memory")"
wait_for mem running 180 >/dev/null
expect "$(get mem /size)" absent "the file after stop and start"
echo "PASS (a): boot ${boot_ms} ms, wake ${wake_ms} ms, scratch empty after stop and start"

say "(b) a disk scratch of 8 GiB: its cold boot, a 1 GiB write, sleep and wake, and nothing left after stop"
disk='{"kind":"disk","mib":8192}'
scratch_file="$state/vm/disk/scratch.ext4"
apply b-up "$(instance disk on-request 256 "$disk")"
disk_boot_ms=$(wait_for disk running 300)
echo "cold boot with an 8 GiB disk scratch: $disk_boot_ms ms (with a memory scratch: $boot_ms ms)"
read -r written fill_ms < <(timed_get disk '/fill?mib=1024')
expect "$written" 1073741824 "bytes written to the disk scratch"
echo "1 GiB written in $fill_ms ms; on the host the scratch holds $(du -m "$scratch_file" | cut -f1) MiB of $(($(stat -c %s "$scratch_file") / 1048576))"
wait_for disk idle 240 >/dev/null
read -r size disk_wake_ms < <(timed_get disk /size)
expect "$size" 1073741824 "the file after sleep and wake"
echo "woken, file intact, in $disk_wake_ms ms"
apply b-stopped "$(instance disk stopped 256 "$disk")"
wait_for disk stopped 120 >/dev/null
[ ! -e "$scratch_file" ] || fail "$scratch_file is still there after stop"
leftover=$(find "$state" /run/nibrunner -name scratch.ext4 2>/dev/null)
[ -z "$leftover" ] || fail "scratch files left after stop: $leftover"
mke2fs_file="$state/mke2fs-probe.ext4"
truncate -s 8G "$mke2fs_file"
mke2fs_started=$(now_ms)
mke2fs -q -t ext4 -F "$mke2fs_file"
mke2fs_ms=$(($(now_ms) - mke2fs_started))
rm -f "$mke2fs_file"
echo "PASS (b): boot ${disk_boot_ms} ms (mke2fs of 8 GiB alone: ${mke2fs_ms} ms), 1 GiB in ${fill_ms} ms, wake ${disk_wake_ms} ms, no file after stop"

say "(c) a program run once that exits 7 is reported exited with lastExitCode 7"
apply c "$(instance once running 256 '{"kind":"memory","mib":64}' '["exit","7"]' 0)"
wait_for once exited 180 >/dev/null
expect "$(field once lastExitCode)" 7 lastExitCode
grep -q 'exiting with 7' "$state/logs/once.log" || fail "the program's last line is not in its log"
echo "PASS (c): exited, lastExitCode 7, its last line logged"

apply done
say "every check passed"
