#!/bin/bash
# Configure and start the TAP service.
#
#   PUBLIC_URL      base URL clients use, no trailing slash  (http://host)
#   CATALOG_PREFIX  where the Parquet catalogs live          (s3://bucket/catalogs/)
#   TABLES          space-separated  name=relative/glob      (allwise=allwise_k0_8/*.parquet ...)
#                   Optional when DISCOVER is on; use it to give a folder a different table name.
#   DISCOVER        "true" (default) serves every folder of Parquet files under
#                   CATALOG_PREFIX as a table named after the folder, and keeps
#                   looking, so a folder uploaded later appears without a restart
#   DISCOVER_INTERVAL  seconds between looks (default 300)
#   AWS_REGION      region of the bucket
#   DUCKDB_MEMORY, DUCKDB_THREADS   per-query limits.  Every request in progress may
#                   use DUCKDB_MEMORY, so the container needs about that times
#                   the number of heavy queries expected at once
#   DUCKDB_TEMP     disk one query may use for work that does not fit in memory (default 4GB)
#   TAP_PORT        port to listen on (default 80)
#   HTTP_HOSTS      other names the service answers under, e.g. localhost
#   MAX_REQUESTS    requests handled at once (default 12)
#   QUERY_TIMEOUT   seconds before a query is cancelled (default 300; 0 = never)
#   MAXREC_LIMIT    most rows one query may return (default 2000000; 0 = no limit)
#   CLIENT_LIMIT    requests one client may have in progress at once (default 0 = no limit)
#   CLIENT_HEADER   header in which the proxy in front names the client; without it
#                   the client is the address the connection comes from
#   ASYNC_LIMIT, ASYNC_CLIENT_LIMIT   async jobs running at once, in all and
#                   for one client (default 0 = no limit)
#   UNLIMITED_CLIENTS  comma-separated clients the per-client limits skip
#   SHARED_CLIENTS  comma-separated clients that stand for many users (a portal in the
#                   same task);  SHARED_CLIENT_LIMIT and SHARED_ASYNC_LIMIT are their limits
#   UPLOAD_URLS     hosts an upload by URL may name:  none (default), any, or a list
#   VOLUME_LIMIT    what one client may be sent in a day, written like 20GB (default 0 = no
#                   limit);  SHARED_VOLUME_LIMIT is the same for the shared clients together
#   WORK_RETENTION_DAYS  days results and uploads are kept in /work (default 7; 0 = for ever)
set -euo pipefail

: "${PUBLIC_URL:?}" "${CATALOG_PREFIX:?}"
TABLES="${TABLES:-}"
DISCOVER="${DISCOVER:-true}"
DISCOVER_INTERVAL="${DISCOVER_INTERVAL:-300}"
if [ -z "$TABLES" ] && [ "$DISCOVER" != "true" ]; then
    echo "Set TABLES, or leave DISCOVER on." >&2; exit 1
fi
AWS_REGION="${AWS_REGION:-us-west-2}"
DUCKDB_MEMORY="${DUCKDB_MEMORY:-2GB}"
DUCKDB_THREADS="${DUCKDB_THREADS:-2}"

sed -i -e "s|__DUCKDB_MEMORY__|${DUCKDB_MEMORY}|" -e "s|__DUCKDB_TEMP__|${DUCKDB_TEMP:-4GB}|" \
       -e "s|__DUCKDB_THREADS__|${DUCKDB_THREADS}|" /opt/tap/s3_init.sql

sed -e "s|__PUBLIC_URL__|${PUBLIC_URL}|" -e "s|__CATALOG_PREFIX__|${CATALOG_PREFIX}|" \
    -e "s|__HTTP_HOSTS__|${HTTP_HOSTS:-}|" \
    -e "s|__QUERY_TIMEOUT__|${QUERY_TIMEOUT:-300}|" -e "s|__MAXREC_LIMIT__|${MAXREC_LIMIT:-2000000}|" \
    -e "s|__CLIENT_HEADER__|${CLIENT_HEADER:-}|" -e "s|__CLIENT_LIMIT__|${CLIENT_LIMIT:-0}|" \
    -e "s|__ASYNC_LIMIT__|${ASYNC_LIMIT:-0}|" -e "s|__ASYNC_CLIENT_LIMIT__|${ASYNC_CLIENT_LIMIT:-0}|" \
    -e "s|__UNLIMITED_CLIENTS__|${UNLIMITED_CLIENTS:-}|" \
    -e "s|__SHARED_CLIENTS__|${SHARED_CLIENTS:-}|" -e "s|__SHARED_CLIENT_LIMIT__|${SHARED_CLIENT_LIMIT:-0}|" \
    -e "s|__SHARED_ASYNC_LIMIT__|${SHARED_ASYNC_LIMIT:-0}|" -e "s|__UPLOAD_URLS__|${UPLOAD_URLS:-none}|" \
    -e "s|__VOLUME_LIMIT__|${VOLUME_LIMIT:-0}|" -e "s|__SHARED_VOLUME_LIMIT__|${SHARED_VOLUME_LIMIT:-0}|" \
    /opt/tap/TAP.conf.template > /opt/tap/TAP.conf

args=()
for t in ${TABLES}; do
    args+=(--table "${t%%=*}=${CATALOG_PREFIX}${t#*=}")
done

export HOME=/var/lib/tap TAP_HOME=/var/lib/tap

# The S3 credentials are resolved here, not in each request (see
# refresh_secret.py), and again every few minutes since they are temporary.
as_tap() { setpriv --reuid www-data --regid www-data --init-groups "$@"; }
mkdir -p /var/lib/tap/tmp
chown -R www-data:www-data /var/lib/tap
as_tap python /opt/tap/refresh_secret.py /var/lib/tap/secrets "${AWS_REGION}"
( while sleep 300; do
      as_tap python /opt/tap/refresh_secret.py /var/lib/tap/secrets "${AWS_REGION}" \
          || echo "WARNING: could not refresh the S3 credentials" >&2
  done ) &

if [ "$DISCOVER" = "true" ]; then
    args+=(--discover "${CATALOG_PREFIX}" --require-column hpx29)
fi

build_catalog() {
    as_tap python /opt/nexsciTAP/tools/make_duckdb_catalog.py --db /var/lib/tap/catalog.duckdb \
           --init-sql-file /opt/tap/s3_init.sql "${args[@]}" "$@"
}

# The first build must succeed.  After that the catalog is rebuilt whenever
# the folders or their files change; each request opens the catalog file
# afresh, and the file is replaced in one step, so no restart is involved.
build_catalog
if [ "$DISCOVER" = "true" ]; then
    ( while sleep "$DISCOVER_INTERVAL"; do
          build_catalog --if-changed || echo "WARNING: could not refresh the table list" >&2
      done ) &
fi

# /work may be a network file system shared with other copies of this
# container, already holding other jobs: own the top of it, not every file.
mkdir -p /work/TAP
chown www-data:www-data /work /work/TAP 2>/dev/null || true

# Jobs older than the retention period are removed, here because nothing
# else would: results in a shared work area outlive the container.  Every
# copy does this; removing what another already removed is harmless.
WORK_RETENTION_DAYS="${WORK_RETENTION_DAYS:-7}"
if [ "$WORK_RETENTION_DAYS" -gt 0 ]; then
    ( while true; do
          as_tap find /work/TAP -mindepth 1 -maxdepth 1 -type d -name 'tap_*' \
                 -mtime "+${WORK_RETENTION_DAYS}" -exec rm -rf {} + 2>/dev/null || true
          sleep 3600
      done ) &
fi

# One pre-loaded process that forks per request (TAP/forkserver.py).
cd /opt/nexsciTAP
exec python -m TAP.forkserver --conf /opt/tap/TAP.conf --host 0.0.0.0 --port "${TAP_PORT:-80}" \
     --user www-data --max-children "${MAX_REQUESTS:-12}"
