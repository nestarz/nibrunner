set -eu
printf '%s\n' "$TEST_VALUE" >> /app/runs
printf 'ready\n'
printf 'error\n' >&2
while :; do
    /bin/sleep 1
done
