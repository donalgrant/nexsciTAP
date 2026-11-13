#!/bin/bash
# Build and (re)start the pilot stack on one Docker host.
#
#   run.sh <build context dir> <public base URL>
set -euo pipefail

CTX="$1"; PUBLIC_URL="$2"
HERE="$CTX/nexsciTAP/deploy/ec2"
FIREFLY_IMAGE="${FIREFLY_IMAGE:-ipac/firefly:2026.4.0}"
HIPS_URL="${HIPS_URL:-https://irsa.ipac.caltech.edu/data/hips/CDS/DSS2/color}"

docker build -t aiab-tap -f "$HERE/Dockerfile.tap" "$CTX"

docker network inspect aiab >/dev/null 2>&1 || docker network create aiab
docker volume inspect aiab-work >/dev/null 2>&1 || docker volume create aiab-work
docker rm -f tap firefly proxy >/dev/null 2>&1 || true

docker run -d --name tap --init --network aiab --restart unless-stopped --memory 3g \
    -v aiab-work:/work \
    -e PUBLIC_URL="$PUBLIC_URL" \
    -e CATALOG_PREFIX="s3://ipac-archive-in-a-box-pilot/catalogs/" \
    -e TABLES="allwise=allwise_k0_8/*.parquet euclid_q1=euclid_q1/*.parquet" \
    -e AWS_REGION=us-west-2 -e DUCKDB_MEMORY=2GB -e DUCKDB_THREADS=8 \
    aiab-tap

docker run -d --name firefly --network aiab --restart unless-stopped --memory 3g \
    -e ADMIN_PASSWORD="$(head -c 18 /dev/urandom | base64)" \
    "$FIREFLY_IMAGE"

# The nginx image fills ${PUBLIC_HOST} into the template at start-up; it only
# substitutes variables that are set, so nginx's own $variables are untouched.
docker run -d --name proxy --network aiab --restart unless-stopped -p 80:80 \
    -e PUBLIC_HOST="${PUBLIC_URL#*://}" \
    -v "$HERE/nginx.conf:/etc/nginx/templates/default.conf.template:ro" \
    nginx:1.27-alpine

# CDS's sky-image servers are unreachable (November 2026), so point Firefly's
# default coverage image at IRSA's copy.  Wait for the web app to unpack first.
for i in $(seq 1 60); do
    f=$(docker exec firefly sh -c 'find / -name firefly.html -path "*webapps*" 2>/dev/null | head -1' || true)
    [ -n "$f" ] && break
    sleep 5
done
if [ -n "${f:-}" ]; then
    docker exec firefly sed -i "s|hipsSourceURL: '[^']*'|hipsSourceURL: '${HIPS_URL}'|" "$f"
    echo "patched $f"
else
    echo "WARNING: firefly.html not found; coverage image left at its default"
fi

docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
