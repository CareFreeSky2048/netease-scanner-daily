#!/usr/bin/env bash
# Debian 12: inspect by default; explicitly create a dedicated 1-GiB swap file.
set -euo pipefail
mode="${1:---check}"
if [[ $# -gt 1 || ( "$mode" != '--check' && "$mode" != '--apply' && "$mode" != '--extend' ) ]]; then
  echo 'Usage: bash setup_swap.sh [--check|--apply|--extend]' >&2
  exit 2
fi
echo '=== Memory ==='
free -h
echo '=== Active swap ==='
swapon --show
echo '=== Root filesystem ==='
df -h /
if [[ "$mode" == '--check' ]]; then
  echo 'Read-only check finished. --apply creates initial 1 GiB; --extend adds a separate 1 GiB toward 2 GiB total.'
  exit 0
fi
if [[ "$EUID" != 0 ]]; then
  echo 'Run swap creation as root.' >&2
  exit 1
fi
exec 9>/run/lock/netease-swap.lock
flock -n 9 || { echo 'Another swap setup is running.' >&2; exit 1; }
swap_path=/swapfile-netease
required_free_kib=2097152
target_kib=1048576
if [[ "$mode" == '--extend' ]]; then
  swap_path=/swapfile-netease-extra
  target_kib=2097152
  required_free_kib=3145728
fi
swap_kib=$(awk '$1 == "SwapTotal:" { print $2 }' /proc/meminfo)
# Swap headers consume a few KiB, so a 1-GiB file reports slightly below 1 GiB.
if (( swap_kib >= target_kib - 1024 )); then
  echo 'Target swap capacity already active; existing swap kept unchanged.'
  exit 0
fi
if [[ "$mode" == '--extend' ]] && (( swap_kib < 1047552 )); then
  echo 'Enable initial 1 GiB swap using --apply before extending.' >&2
  exit 1
fi
if command -v docker >/dev/null; then
  scanner_state=$(docker inspect --format '{{.State.Running}}' netease-scanner-scanner-1 2>/dev/null || true)
  if [[ "$scanner_state" == 'true' ]]; then
    echo 'Stop the scanner first: docker stop -t 40 netease-scanner-scanner-1' >&2
    exit 1
  fi
fi
if [[ -e "$swap_path" || -L "$swap_path" ]]; then
    echo 'The dedicated swap file already exists. Refusing to overwrite it.' >&2
  exit 1
fi
fs_type=$(findmnt -n -o FSTYPE --target /)
case "$fs_type" in
  ext4|xfs) ;;
  *) echo "Filesystem $fs_type needs a separate swap-file procedure; no swap created." >&2; exit 1 ;;
esac
available_kib=$(df -Pk / | awk 'NR == 2 { print $4 }')
if (( available_kib < required_free_kib )); then
  echo "Insufficient free disk space (need $((required_free_kib / 1048576)) GiB). No swap created." >&2
  exit 1
fi
created=0
active=0
cleanup() {
  status=$?
  trap - EXIT
  if (( status != 0 && created == 1 && active == 0 )); then
    # Only remove the exact file created by this invocation, never existing swap.
    case "$swap_path" in
      /swapfile-netease|/swapfile-netease-extra) rm -f -- "$swap_path" ;;
      *) echo 'Unexpected cleanup path; refusing removal.' >&2 ;;
    esac
  elif (( status != 0 && active == 1 )); then
    echo 'Swap is active, but boot persistence did not finish. Report this error; do not reformat the file.' >&2
  fi
  exit "$status"
}
trap cleanup EXIT
umask 077
# noclobber prevents replacing a file created between the checks and this step.
(set -o noclobber; : > "$swap_path")
created=1
fallocate -l 1G "$swap_path"
chmod 600 "$swap_path"
mkswap "$swap_path"
swapon "$swap_path"
active=1
if ! awk -v swap_path="$swap_path" '$1 == swap_path { found=1 } END { exit !found }' /etc/fstab; then
  fstab_backup="/etc/fstab.before-netease-$(date +%Y%m%d-%H%M%S)-$$"
  cp -p -- /etc/fstab "$fstab_backup"
  printf '\n%s none swap sw 0 0\n' "$swap_path" >> /etc/fstab
  echo "Boot configuration backed up: $fstab_backup"
fi
echo '1 GiB swap enabled and configured for boot. Scanner remains stopped.'
free -h
swapon --show
