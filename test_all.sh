#!/bin/bash
set -e

export MSYS_NO_PATHCONV=1

# Укажите имя вашего k3d кластера
CLUSTER_NAME="mycluster"
# Имя сервиса NATS внутри Kubernetes 
NATS_URL="nats://nats-service.default.svc.cluster.local:4222"

echo "=== 1. Запуск Unit-тестов (Boundary Events) ==="
docker run --rm \
  -v "$(pwd)":/usr/src/app \
  -w /usr/src/app/bpmn-engine \
  -e LIBRARY_PATH=/usr/lib/gcc/x86_64-alpine-linux-musl/14.2.0:/usr/lib \
  rust:1.88-alpine \
  sh -c "apk add --no-cache build-base musl-dev linux-headers && cargo test"
echo "✅ Unit-тесты пройдены!"

echo "=== 2. Пересборка Docker-образа ==="
docker build -t bpmn-engine-service:latest -f bpmn-engine/Dockerfile bpmn-engine/
echo "✅ Образ bpmn-engine-service:latest собран!"

echo "=== 3. Загрузка образа в кластер k3d ==="
k3d image import bpmn-engine-service:latest -c $CLUSTER_NAME
echo "✅ Образ загружен во внутреннее хранилище кластера!"

echo "=== 4. Перезапуск сервиса ==="
kubectl rollout restart deployment/bpmn-engine-deployment
kubectl rollout status deployment/bpmn-engine-deployment
echo "✅ Сервис перезапущен!"

sleep 5

echo "=== 5. Интеграционное тестирование (NATS) ==="

echo "▶ Сценарий А: Сетевой сбой (Exponential Backoff)"
kubectl run nats-test-a --rm -i --restart='Never' --image=natsio/nats-box:latest -- \
  sh -c "sleep 2 && nats pub mes.in.v1.erp.inst-123 '{\"process_instance_id\": \"inst-123\", \"status\": \"NETWORK_ERROR\", \"error_message\": \"Connection Timeout to ERP\", \"payload\": {}}' -s $NATS_URL"

sleep 2

echo "▶ Сценарий Б: Бизнес-ошибка (Saga / Compensation)"
kubectl run nats-test-b --rm -i --restart='Never' --image=natsio/nats-box:latest -- \
  sh -c "sleep 2 && nats pub mes.in.v1.erp.inst-456 '{\"process_instance_id\": \"inst-456\", \"status\": \"BUSINESS_ERROR\", \"error_message\": \"Invalid Material ID in payload\", \"payload\": {}}' -s $NATS_URL"

echo "▶ Сценарий В: Нарушение SLA (Таймаут)"
echo "Движок автоматически обработает токен. (Примечание: в main.rs задан таймаут 30 минут. Для быстрого теста в консоли измените chrono::Duration::minutes(30) на seconds(15) перед сборкой)."

echo "=== 6. Логи сервиса ==="
kubectl logs -f deployment/bpmn-engine-deployment --tail=50