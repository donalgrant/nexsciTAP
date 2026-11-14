#!/bin/bash
# Create or update the Fargate stack.
#
#   deploy.sh <stack name> <tap image> <proxy image> <firefly image> [DesiredCount]
#
# Reads the deployment's choices from the environment:
#   VPC_ID, SUBNET_IDS (comma-separated), ALLOWED_CIDR_1, ALLOWED_CIDR_2,
#   CATALOG_BUCKET, and optionally TABLES and PROJECT_TAG.
# Uses whatever AWS credentials and region the aws CLI is configured with.
set -euo pipefail

STACK="$1"; TAP_IMAGE="$2"; PROXY_IMAGE="$3"; FIREFLY_IMAGE="$4"; COUNT="${5:-1}"
HERE="$(cd "$(dirname "$0")" && pwd)"

# A new stack needs a Firefly admin password; an existing one keeps its own,
# so that updating the stack does not restart the task for nothing.
PASSWORD=()
if ! aws cloudformation describe-stacks --stack-name "$STACK" >/dev/null 2>&1; then
    PASSWORD=("FireflyAdminPassword=$(head -c 18 /dev/urandom | base64 | tr -d '/+=')")
fi

aws cloudformation deploy \
    --stack-name "$STACK" \
    --template-file "$HERE/stack.yaml" \
    --capabilities CAPABILITY_IAM --no-fail-on-empty-changeset \
    --tags "Project=${PROJECT_TAG:-archive-in-a-box-pilot}" \
    --parameter-overrides \
        "VpcId=${VPC_ID:?}" "SubnetIds=${SUBNET_IDS:?}" \
        "AllowedCidr1=${ALLOWED_CIDR_1:?}" "AllowedCidr2=${ALLOWED_CIDR_2:?}" \
        "TapImage=$TAP_IMAGE" "ProxyImage=$PROXY_IMAGE" "FireflyImage=$FIREFLY_IMAGE" \
        "CatalogBucket=${CATALOG_BUCKET:?}" "Tables=${TABLES:-}" \
        "DesiredCount=$COUNT" ${PASSWORD[@]+"${PASSWORD[@]}"}

aws cloudformation describe-stacks --stack-name "$STACK" \
    --query 'Stacks[0].Outputs[].[OutputKey,OutputValue]' --output text
