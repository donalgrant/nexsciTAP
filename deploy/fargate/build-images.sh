#!/bin/bash
# Build the pilot's container images, push them to ECR, and roll the service.
#
# Runs inside the CodeBuild project that deploy/fargate/stack.yaml defines,
# from a directory that already holds this repository as ./nexsciTAP at the
# commit to build.  It needs Docker, the aws CLI and these variables:
#
#   REGISTRY          <account>.dkr.ecr.<region>.amazonaws.com
#   IMAGE_PREFIX      repository name prefix (aiab -> aiab-tap, aiab-proxy, aiab-firefly)
#   SPATIAL_REPO, SPATIAL_REF   the spatial index library and the commit to build
#   FIREFLY_SOURCE    public Firefly image to serve (ipac/firefly:<version>)
#   ECS_CLUSTER, ECS_SERVICE    service to restart on the new images
#
# Each image is pushed under the commit it was built from and under "live",
# the tag the task definition names.  Restarting the service therefore runs
# what was just built, and the commit tag records what that was.
set -euo pipefail

: "${REGISTRY:?}" "${IMAGE_PREFIX:?}" "${SPATIAL_REPO:?}" "${SPATIAL_REF:?}" "${FIREFLY_SOURCE:?}"

COMMIT="$(git -C nexsciTAP rev-parse --short HEAD)"
echo "Building nexsciTAP ${COMMIT} with SpatialIndex ${SPATIAL_REF}"

git clone --quiet "$SPATIAL_REPO" SpatialIndex
git -C SpatialIndex checkout --quiet "$SPATIAL_REF"

aws ecr get-login-password | docker login --username AWS --password-stdin "$REGISTRY" >/dev/null

TAP="$REGISTRY/$IMAGE_PREFIX-tap"
PROXY="$REGISTRY/$IMAGE_PREFIX-proxy"
FIREFLY="$REGISTRY/$IMAGE_PREFIX-firefly"

# The TAP image's build context is the directory holding both source trees.
docker build --quiet -t "$TAP:$COMMIT" -t "$TAP:live" -f nexsciTAP/deploy/ec2/Dockerfile.tap .
docker build --quiet -t "$PROXY:$COMMIT" -t "$PROXY:live" -f nexsciTAP/deploy/fargate/Dockerfile.proxy nexsciTAP/deploy/fargate

for image in "$TAP:$COMMIT" "$TAP:live" "$PROXY:$COMMIT" "$PROXY:live"; do
    docker push --quiet "$image"
done

# Firefly is served unmodified; keep a copy of the requested version in ECR
# so that starting a task does not depend on Docker Hub.
FIREFLY_TAG="${FIREFLY_SOURCE##*:}"
if aws ecr describe-images --repository-name "$IMAGE_PREFIX-firefly" \
       --image-ids "imageTag=$FIREFLY_TAG" >/dev/null 2>&1; then
    echo "Firefly $FIREFLY_TAG is already in ECR"
else
    docker pull --quiet "$FIREFLY_SOURCE"
    docker tag "$FIREFLY_SOURCE" "$FIREFLY:$FIREFLY_TAG"
    docker push --quiet "$FIREFLY:$FIREFLY_TAG"
fi

if [ -n "${ECS_CLUSTER:-}" ] && [ -n "${ECS_SERVICE:-}" ]; then
    echo "Restarting $ECS_SERVICE on the new images"
    aws ecs update-service --cluster "$ECS_CLUSTER" --service "$ECS_SERVICE" \
        --force-new-deployment --query 'service.deployments[0].id' --output text
    aws ecs wait services-stable --cluster "$ECS_CLUSTER" --services "$ECS_SERVICE"
    echo "Service is stable on nexsciTAP ${COMMIT}"
fi
