#!/bin/bash

# Check if a version tag was provided
if [ -z "$1" ]; then
  echo "Usage: $0 <version-tag>"
  exit 1
fi

TAG=$1
REPO="public.ecr.aws/eodh/airbus-optical-adaptor"

# Build the Docker image
docker build -t $TAG -f airbus_optical_adaptor/Dockerfile .

# Tag the Docker image
docker tag $TAG $REPO:$TAG

# Push the Docker image
docker push $REPO:$TAG
