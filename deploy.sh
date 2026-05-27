#!/bin/bash

set -e

CLUSTER_NAME="weather-monitoring"

if [ -d /usr/lib/jvm/java-21-openjdk-amd64 ]; then
    export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
    export PATH="$JAVA_HOME/bin:$PATH"
fi

mvn clean package -DskipTests

kubectl apply -f k8s/configs/
kubectl apply -f k8s/infrastructure/
kubectl apply -f k8s/apps/

kubectl rollout restart deployment
kubectl rollout restart statefulset 2>/dev/null || true