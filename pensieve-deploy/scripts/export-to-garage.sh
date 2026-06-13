#!/bin/bash
#
# Pre-generate bulk event exports and upload them to Garage for researcher
# distribution.
#
# For each configured range it calls the local export API, gzips the stream, and
# uploads the result to <ARCHIVE_REMOTE>:<bucket>/exports/. The interactive
# Grafana download buttons hit the live API directly; this job offloads the big,
# recurring ranges (e.g. last_3_months) to Garage so researchers can pull a
# stable, compressed object over S3/rclone without touching the indexer.
#
# Environment variables (from .env):
#   PENSIEVE_API_TOKENS     - API tokens (the first one is used)
#   ARCHIVE_REMOTE          - rclone remote name (default: garage)
#   STORAGE_BOX_PATH        - bucket / remote path (default: pensieve-archive)
#   EXPORT_GARAGE_RANGES    - space-separated ranges (default: "last_3_months")
#   EXPORT_API_URL          - API base URL (default: http://127.0.0.1:8080)
#   EXPORT_TMP_DIR          - scratch dir for the compressed file (default: /data/exports-tmp)

set -euo pipefail
cd "$(dirname "$0")/.."

# Load .env for manual runs (systemd already injects it via EnvironmentFile).
# Parse line-by-line rather than `source`: values may contain spaces
# (e.g. PREVIEW_SITE_NAME=Nostr Preview), which `source` would mis-execute.
if [ -f .env ]; then
    while IFS='=' read -r key val; do
        case "$key" in '' | \#*) continue ;; esac
        export "$key=$val"
    done < .env
fi

API_TOKEN="${PENSIEVE_API_TOKENS%%,*}"
REMOTE="${ARCHIVE_REMOTE:-garage}"
BUCKET="${STORAGE_BOX_PATH:-pensieve-archive}"
API_URL="${EXPORT_API_URL:-http://127.0.0.1:8080}"
RANGES="${EXPORT_GARAGE_RANGES:-last_3_months}"
TMP_DIR="${EXPORT_TMP_DIR:-/data/exports-tmp}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

if [ -z "${API_TOKEN:-}" ]; then
    echo "ERROR: PENSIEVE_API_TOKENS is empty (need it to call the export API)" >&2
    exit 1
fi

mkdir -p "$TMP_DIR"
log "Pre-generating exports -> $REMOTE:$BUCKET/exports/  (ranges: $RANGES)"

for range in $RANGES; do
    tmp="$TMP_DIR/pensieve-$range.jsonl.gz"

    # --fail aborts on a non-2xx status; pipefail + set -e abort on a mid-stream
    # error, so we never upload a truncated export.
    log "Exporting '$range' from API ..."
    curl --fail --silent --show-error \
        -H "Authorization: Bearer $API_TOKEN" \
        "$API_URL/api/v1/export?range=$range&format=jsonl" \
        | gzip > "$tmp"

    size=$(stat -c %s "$tmp")
    log "  $(numfmt --to=iec --suffix=B "$size" 2>/dev/null || echo "$size bytes") gzipped; uploading ..."
    rclone copyto "$tmp" "$REMOTE:$BUCKET/exports/pensieve-$range.jsonl.gz" \
        --stats-one-line --log-level INFO
    rm -f "$tmp"
    log "  '$range' done."
done

log "All exports uploaded."
