#!/usr/bin/env bash
# Wait until Trino has started and the lakehouse catalog answers a query.
set -euo pipefail
url="${1:-http://localhost:8080}"
for _ in $(seq 1 120); do
  if curl -fs "$url/v1/info" | grep -q '"starting":false'; then
    if curl -fs -X POST -H "X-Trino-User: wait" -d "SHOW SCHEMAS FROM lakehouse" "$url/v1/statement" >/dev/null; then
      echo "Trino is ready"
      exit 0
    fi
  fi
  sleep 2
done
echo "Trino did not become ready" >&2
exit 1
