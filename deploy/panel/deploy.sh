#!/bin/bash
# Create or update the control panel's stack, and upload its page.
#
#   deploy.sh ADMIN_CIDR [PANEL_SECRET]
#
#   ADMIN_CIDR     the one address range admitted to the panel, e.g. 203.0.113.7/32
#   PANEL_SECRET   the service stack's PanelSecret; leave out to keep the value
#                  the panel's stack already has (empty on a first deployment)
#
# Environment:  STACK (default aiab-panel), SERVICE_STACK (aiab-fargate),
# CATALOG_BUCKET (ipac-archive-in-a-box-pilot), which is also where the
# function's code is uploaded (under deploy/panel/), AWS_PROFILE, AWS_REGION.

set -euo pipefail
cd "$(dirname "$0")"

ADMIN_CIDR=${1:?usage: deploy.sh ADMIN_CIDR [PANEL_SECRET]}
STACK=${STACK:-aiab-panel}
SERVICE_STACK=${SERVICE_STACK:-aiab-fargate}
CATALOG_BUCKET=${CATALOG_BUCKET:-ipac-archive-in-a-box-pilot}

SERVICE_URL=$(aws cloudformation describe-stacks --stack-name "$SERVICE_STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='TapUrl'].OutputValue" --output text | sed 's#/TAP/*$##')

OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT

aws cloudformation package --template-file stack.yaml --s3-bucket "$CATALOG_BUCKET" \
    --s3-prefix deploy/panel --output-template-file "$OUT/packaged.yaml" > /dev/null

PARAMS=("ServiceStack=$SERVICE_STACK" "ServiceUrl=$SERVICE_URL" "AdminCidr=$ADMIN_CIDR" "CatalogBucket=$CATALOG_BUCKET")
if [ $# -ge 2 ]; then PARAMS+=("PanelSecret=$2"); fi

aws cloudformation deploy --stack-name "$STACK" --template-file "$OUT/packaged.yaml" \
    --capabilities CAPABILITY_IAM --no-fail-on-empty-changeset \
    --tags Project=archive-in-a-box-pilot --parameter-overrides "${PARAMS[@]}"

BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='Bucket'].OutputValue" --output text)
aws s3 sync site "s3://$BUCKET/site/" --delete --cache-control no-cache

aws cloudformation describe-stacks --stack-name "$STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='PanelUrl'].OutputValue" --output text
