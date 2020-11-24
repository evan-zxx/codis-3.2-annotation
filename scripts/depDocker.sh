#!/bin/bash
#docker build -f Dockerfile  -t codis-image .

docker.sh cleanup

docker.sh zookeeper
docker.sh dashboard
docker.sh proxy
docker.sh server
#docker.sh sen
docker.sh fe
