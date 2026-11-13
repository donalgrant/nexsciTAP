#!/bin/bash
# Configure and start the TAP service.
#
#   PUBLIC_URL      base URL clients use, no trailing slash  (http://host)
#   CATALOG_PREFIX  where the Parquet catalogs live          (s3://bucket/catalogs/)
#   TABLES          space-separated  name=relative/glob      (allwise=allwise_k0_8/*.parquet ...)
#   AWS_REGION      region of the bucket
#   DUCKDB_MEMORY, DUCKDB_THREADS   per-query limits
set -euo pipefail

: "${PUBLIC_URL:?}" "${CATALOG_PREFIX:?}" "${TABLES:?}"
AWS_REGION="${AWS_REGION:-us-west-2}"
DUCKDB_MEMORY="${DUCKDB_MEMORY:-2GB}"
DUCKDB_THREADS="${DUCKDB_THREADS:-2}"

sed -i -e "s|__AWS_REGION__|${AWS_REGION}|" -e "s|__DUCKDB_MEMORY__|${DUCKDB_MEMORY}|" \
       -e "s|__DUCKDB_THREADS__|${DUCKDB_THREADS}|" /opt/tap/s3_init.sql

sed -e "s|__PUBLIC_URL__|${PUBLIC_URL}|" -e "s|__CATALOG_PREFIX__|${CATALOG_PREFIX}|" \
    /opt/tap/TAP.conf.template > /opt/tap/TAP.conf

args=()
for t in ${TABLES}; do
    args+=(--table "${t%%=*}=${CATALOG_PREFIX}${t#*=}")
done

export HOME=/var/lib/tap
python /opt/nexsciTAP/tools/make_duckdb_catalog.py --db /opt/tap/catalog.duckdb \
       --init-sql-file /opt/tap/s3_init.sql "${args[@]}"

mkdir -p /work/TAP
chown -R www-data:www-data /work /var/lib/tap /opt/tap/catalog.duckdb

exec apache2ctl -D FOREGROUND
