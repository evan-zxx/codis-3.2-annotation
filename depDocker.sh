#!/bin/bash
#docker build -f Dockerfile  -t codis-image .

./scripts/docker.sh cleanup

./scripts/docker.sh zookeeper
./scripts/docker.sh dashboard
./scripts/docker.sh proxy
./scripts/docker.sh server
./scripts/docker.sh fe
