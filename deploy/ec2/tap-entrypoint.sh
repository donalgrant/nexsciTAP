#!/bin/bash
# Configure and start the TAP service.
#
#   PUBLIC_URL      base URL clients use, no trailing slash  (http://host)
#   CATALOG_PREFIX  where the Parquet catalogs live          (s3://bucket/catalogs/)
#   TABLES          space-separated  name=relative/glob      (allwise=allwise_k0_8/*.parquet ...)
#   AWS_REGION      region of the bucket
#   DUCKDB_MEMORY, DUCKDB_THREADS   per-query limits
#   TAP_PORT        port to listen on (default 80)
#   HTTP_HOSTS      other names the service answers under, e.g. localhost
#   MAX_REQUESTS    requests handled at once (default 12)
#   QUERY_TIMEOUT   seconds before a query is cancelled (default 300; 0 = never)
#   MAXREC_LIMIT    most rows one query may return (default 2000000; 0 = no limit)
set -euo pipefail

: "${PUBLIC_URL:?}" "${CATALOG_PREFIX:?}" "${TABLES:?}"
AWS_REGION="${AWS_REGION:-us-west-2}"
DUCKDB_MEMORY="${DUCKDB_MEMORY:-2GB}"
DUCKDB_THREADS="${DUCKDB_THREADS:-2}"

sed -i -e "s|__DUCKDB_MEMORY__|${DUCKDB_MEMORY}|" \
       -e "s|__DUCKDB_THREADS__|${DUCKDB_THREADS}|" /opt/tap/s3_init.sql

sed -e "s|__PUBLIC_URL__|${PUBLIC_URL}|" -e "s|__CATALOG_PREFIX__|${CATALOG_PREFIX}|" \
    -e "s|__HTTP_HOSTS__|${HTTP_HOSTS:-}|" \
    -e "s|__QUERY_TIMEOUT__|${QUERY_TIMEOUT:-300}|" -e "s|__MAXREC_LIMIT__|${MAXREC_LIMIT:-2000000}|" \
    /opt/tap/TAP.conf.template > /opt/tap/TAP.conf

args=()
for t in ${TABLES}; do
    args+=(--table "${t%%=*}=${CATALOG_PREFIX}${t#*=}")
done

export HOME=/var/lib/tap TAP_HOME=/var/lib/tap

# The S3 credentials are resolved here, not in each request (see
# refresh_secret.py), and again every few minutes since they are temporary.
as_tap() { setpriv --reuid www-data --regid www-data --init-groups "$@"; }
chown -R www-data:www-data /var/lib/tap
as_tap python /opt/tap/refresh_secret.py /var/lib/tap/secrets "${AWS_REGION}"
( while sleep 300; do
      as_tap python /opt/tap/refresh_secret.py /var/lib/tap/secrets "${AWS_REGION}" \
          || echo "WARNING: could not refresh the S3 credentials" >&2
  done ) &

as_tap python /opt/nexsciTAP/tools/make_duckdb_catalog.py --db /var/lib/tap/catalog.duckdb \
       --init-sql-file /opt/tap/s3_init.sql "${args[@]}"

mkdir -p /work/TAP
chown -R www-data:www-data /work

# One pre-loaded process that forks per request (TAP/forkserver.py).
cd /opt/nexsciTAP
exec python -m TAP.forkserver --conf /opt/tap/TAP.conf --host 0.0.0.0 --port "${TAP_PORT:-80}" \
     --user www-data --max-children "${MAX_REQUESTS:-12}"
