#!/bin/bash
# Create or update the control panel's stack, and upload its page.
#
#   deploy.sh ADMIN_CIDR [PANEL_SECRET]
#
#   ADMIN_CIDR     the address ranges admitted to the panel, comma-separated,
#                  e.g. 203.0.113.7/32 or 134.4.0.0/16,131.215.0.0/16
#   PANEL_SECRET   the service stack's PanelSecret; leave out to keep the value
#                  the panel's stack already has (empty on a first deployment)
#
# Environment:  STACK (default aiab-panel), SERVICE_STACK (aiab-fargate),
# ALERT_EMAIL (where alerts go; default: keep what the stack has),
# SIGN_IN (required | off; default: keep what the stack has, required at first),
# DOMAIN_NAME and CERTIFICATE_ARN (the panel's own name and its certificate in
# us-east-1; default: keep what the stack has, the Amazon name at first),
# CATALOG_BUCKET (ipac-archive-in-a-box-pilot), which is also where the
# function's code is uploaded (under deploy/panel/), AWS_PROFILE, AWS_REGION.

set -euo pipefail
cd "$(dirname "$0")"

ADMIN_CIDR=${1:?usage: deploy.sh ADMIN_CIDR [PANEL_SECRET]}
STACK=${STACK:-aiab-panel}
SERVICE_STACK=${SERVICE_STACK:-aiab-fargate}
CATALOG_BUCKET=${CATALOG_BUCKET:-ipac-archive-in-a-box-pilot}

# Deploy only what is on GitHub.  The panel has been replaced by older code
# once already, from a second machine whose clone was behind; and code that is
# deployed but not pushed is code the next machine cannot have.
# FORCE=1 deploys anyway.
if [ "${FORCE:-}" != 1 ] && git rev-parse --git-dir >/dev/null 2>&1; then
    branch=$(git rev-parse --abbrev-ref HEAD)
    git fetch --quiet origin "$branch" 2>/dev/null || { echo "Cannot reach GitHub to compare this clone with it;  FORCE=1 to deploy anyway." >&2; exit 1; }
    behind=$(git rev-list --count "HEAD..origin/$branch")
    ahead=$(git rev-list --count "origin/$branch..HEAD")
    if [ "$behind" != 0 ]; then
        echo "This clone is $behind commit(s) behind GitHub;  deploying it would put older code on the panel.  Run:  git pull --ff-only" >&2; exit 1
    fi
    if [ "$ahead" != 0 ] || [ -n "$(git status --porcelain --untracked-files=no -- .)" ]; then
        echo "This clone has changes under deploy/panel that are not on GitHub.  Commit and push them first, so every machine deploys the same code." >&2; exit 1
    fi
fi
COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)

SERVICE_URL=$(aws cloudformation describe-stacks --stack-name "$SERVICE_STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='TapUrl'].OutputValue" --output text | sed 's#/TAP/*$##')

OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT

# A function that cannot be loaded takes the whole panel down, so try loading
# it here first, with stand-ins for the AWS library it will find in Lambda.
mkdir -p "$OUT/stubs/botocore"
printf 'def client(*a, **k):\n    return None\n' > "$OUT/stubs/boto3.py"
: > "$OUT/stubs/botocore/__init__.py"
printf 'class ClientError(Exception):\n    pass\n' > "$OUT/stubs/botocore/exceptions.py"
(cd functions && PYTHONPATH="$OUT/stubs" python3 -c "import panel; panel.handler; panel.watch_handler") \
    || { echo "functions/panel.py does not load; nothing was deployed" >&2; exit 1; }

aws cloudformation package --template-file stack.yaml --s3-bucket "$CATALOG_BUCKET" \
    --s3-prefix deploy/panel --output-template-file "$OUT/packaged.yaml" > /dev/null

# The conversion task runs in the service's network.
net() { aws cloudformation describe-stacks --stack-name "$SERVICE_STACK" \
    --query "Stacks[0].Parameters[?ParameterKey=='$1'].ParameterValue" --output text; }

PARAMS=("ServiceStack=$SERVICE_STACK" "ServiceUrl=$SERVICE_URL" "AdminCidr=$ADMIN_CIDR" "CatalogBucket=$CATALOG_BUCKET"
        "VpcId=$(net VpcId)" "SubnetIds=$(net SubnetIds)")

# Sign-in returns to the panel's own address, which exists once the stack does.
# A first deployment therefore needs running twice.
CALLBACK=$(aws cloudformation describe-stacks --stack-name "$STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='PanelUrl'].OutputValue" --output text 2>/dev/null || true)
if [ -n "$CALLBACK" ] && [ "$CALLBACK" != "None" ]; then PARAMS+=("CallbackUrl=$CALLBACK"); fi
if [ -n "${SIGN_IN:-}" ]; then PARAMS+=("SignIn=$SIGN_IN"); fi
if [ -n "${ALERT_EMAIL:-}" ]; then PARAMS+=("AlertEmail=$ALERT_EMAIL"); fi
if [ -n "${DOMAIN_NAME+x}" ]; then PARAMS+=("DomainName=$DOMAIN_NAME" "CertificateArn=${CERTIFICATE_ARN:-}"); fi
if [ $# -ge 2 ]; then PARAMS+=("PanelSecret=$2"); fi

aws cloudformation deploy --stack-name "$STACK" --template-file "$OUT/packaged.yaml" \
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM --no-fail-on-empty-changeset \
    --tags Project=archive-in-a-box-pilot --parameter-overrides "${PARAMS[@]}"

BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='Bucket'].OutputValue" --output text)
aws s3 sync site "s3://$BUCKET/site/" --delete --cache-control no-cache --exclude commit.txt
# Which code the panel is running, readable at <panel address>/commit.txt.
printf '%s\n' "$COMMIT" | aws s3 cp - "s3://$BUCKET/site/commit.txt" --cache-control no-cache --content-type text/plain

aws cloudformation describe-stacks --stack-name "$STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='PanelUrl'].OutputValue" --output text
