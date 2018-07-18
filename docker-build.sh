#!/bin/bash

docker build -t distiller-base -f docker/base/Dockerfile .
docker build -t distiller-daemon -f docker/daemon/Dockerfile .
docker build -t distiller-worker -f docker/worker/Dockerfile .