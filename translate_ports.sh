#!/usr/bin/env bash
set -e

kubectl port-forward service/http-cluster-service 8080:8080 & kubectl port-forward service/websocket-cluster-service 8081:8080 &