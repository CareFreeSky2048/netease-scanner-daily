#!/usr/bin/env bash
# Keep the small-host limits in every management command. SSH tunnel deployment.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
exec docker compose -f compose.tunnel.yaml -f compose.low-resource.yaml "$@"
