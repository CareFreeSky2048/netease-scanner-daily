#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
umask 077
compose_args=()
if [[ "${1:-}" == "--tunnel" ]]; then
  compose_args=(-f compose.tunnel.yaml)
  shift
fi
if [[ "${EUID}" != 0 ]]; then
  echo 'Run as root (or via sudo) to prepare volume permissions.' >&2
  exit 1
fi
command -v docker >/dev/null
docker compose version
if [[ ! -f .env || ! -f secrets/music_cookie.txt ]]; then
  echo 'Create .env and secrets/music_cookie.txt according to README.md first.' >&2
  exit 1
fi
mkdir -p data config
if [[ ! -f config/config.json ]]; then
  cp config.example.json config/config.json
fi
if [[ $# -gt 0 ]]; then
  if [[ -n "$(find data -mindepth 1 -print -quit)" ]]; then
    echo 'data/ is not empty. Refusing to overwrite existing records.' >&2
    exit 1
  fi
  docker run --rm --network none -v "$PWD:/deployment" -w /deployment \
    python:3.11.14-slim-bookworm python verify_migration.py "$1" --extract-to /deployment
fi
chown -R 10001:10001 data
chmod 700 data secrets
chown root:10001 secrets
chmod 750 secrets
chown root:10001 secrets/music_cookie.txt
chmod 440 secrets/music_cookie.txt
chmod 700 config
chown root:10001 config
chmod 750 config
chown root:10001 config/config.json
chmod 440 config/config.json
chmod 600 .env
docker compose "${compose_args[@]}" config --quiet
docker compose "${compose_args[@]}" build
docker compose "${compose_args[@]}" up -d
docker compose "${compose_args[@]}" ps
echo 'Scheduling remains disabled until you complete the API probe and a full manual scan.'
if [[ ${#compose_args[@]} -gt 0 ]]; then
  echo 'Run: docker compose -f compose.tunnel.yaml exec -T scanner python probe.py'
  echo 'View via SSH: -L 15432:127.0.0.1:18080 then open http://127.0.0.1:15432/'
else
  echo 'Run: docker compose exec -T scanner python probe.py'
fi
