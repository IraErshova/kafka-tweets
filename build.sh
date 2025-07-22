#!/bin/bash

echo "Build Kafka tweet stream"
echo "Build docker-compose"

docker-compose build

if [ $? -eq 0 ]; then
    echo "All services built successfully"
    echo "To start the system, run: ./run.sh"
else
    echo "Docker-compose build failed!"
    exit 1
fi