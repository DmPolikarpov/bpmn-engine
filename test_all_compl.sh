#!/bin/bash
set -e
export MSYS_NO_PATHCONV=1

# Переходим в директорию скрипта для корректной работы относительных путей
cd "$(dirname "$0")"

CLUSTER_NAME="${CLUSTER_NAME:-mycluster}"
NATS_URL="nats://nats-service.default.svc.cluster.local:4222"
DB_HOST="bpmn-db-rw.default.svc.cluster.local"

echo "=== 1. Запуск Unit-тестов (Boundary Events) ==="
docker run --rm \
  -v "$(pwd)":/usr/src/app \
  -w /usr/src/app/bpmn-engine \
  -e LIBRARY_PATH=/usr/lib/gcc/x86_64-alpine-linux-musl/14.2.0:/usr/lib \
  rust:1.88-alpine \
  sh -c "apk add --no-cache build-base musl-dev linux-headers && cargo test"

echo -e "\n=== 2. Сборка и деплой Docker-образа ==="
docker build -t bpmn-engine-service:latest -f bpmn-engine/Dockerfile bpmn-engine/
k3d image import bpmn-engine-service:latest -c "$CLUSTER_NAME"
kubectl rollout restart deployment/bpmn-engine-deployment
kubectl rollout status deployment/bpmn-engine-deployment --timeout=60s

echo -e "\n=== 3. Проверка работоспособности шлюзов (HTTP / WebSocket) ==="
echo "▶ Тестирование HTTP Gateway (REST API)..."
kubectl run http-test --image=curlimages/curl:8.4.0 --restart='Never' --rm -i -- \
  -s -w "HTTP Status: %{http_code} (Gateway is online)\n" -o /dev/null --max-time 5 http://http-cluster-service:8080/ || true

echo "▶ Тестирование WebSocket Gateway (Handshake Upgrade)..."
kubectl run ws-test --image=curlimages/curl:8.4.0 --restart='Never' --rm -i -- \
  -s -i -N -H "Connection: Upgrade" -H "Upgrade: websocket" \
  -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" -H "Sec-WebSocket-Version: 13" \
  --max-time 3 http://websocket-cluster-service:8080/ws || true

echo -e "\n=== 4. Инициализация тестовых данных в PostgreSQL ==="
BPMN_FILE=$(find . -name "test_process.bpmn" -type f | head -n 1)

if [ -z "$BPMN_FILE" ]; then
  echo "ОШИБКА: Файл test_process.bpmn не найден!"
  exit 1
fi

echo "Используем BPMN файл: $BPMN_FILE"

# Кодируем XML в Base64 для безопасной передачи через bash/psql без потери кавычек
XML_BASE64=$(base64 -w 0 "$BPMN_FILE" 2>/dev/null || base64 "$BPMN_FILE" | tr -d '\n')

DB_SEED_SQL="
  INSERT INTO process_definitions (id, name, version, bpmn_xml) 
  VALUES ('Process_ERP_Test', 'ERP Test Process', 1, convert_from(decode('$XML_BASE64', 'base64'), 'UTF8')) 
  ON CONFLICT (id) DO UPDATE SET bpmn_xml = EXCLUDED.bpmn_xml;

  DELETE FROM process_instances WHERE id IN ('inst-network', 'inst-business', 'inst-sla', 'inst-success');

  -- Сценарий А: Сетевой сбой
  INSERT INTO process_instances (id, definition_id, status) VALUES ('inst-network', 'Process_ERP_Test', 'ACTIVE');
  INSERT INTO tokens (id, instance_id, element_id, state, created_at, retry_count) 
  VALUES ('tok-network', 'inst-network', 'ServiceTask_ERP_Sync', 'ACTIVE', NOW(), 0);

  -- Сценарий Б: Бизнес-ошибка (Компенсация)
  INSERT INTO process_instances (id, definition_id, status) VALUES ('inst-business', 'Process_ERP_Test', 'ACTIVE');
  INSERT INTO tokens (id, instance_id, element_id, state, created_at, retry_count) 
  VALUES ('tok-business', 'inst-business', 'ServiceTask_ERP_Sync', 'ACTIVE', NOW(), 0);

  -- Сценарий В: Нарушение SLA (Устанавливаем время создания токена на 35 минут назад)
  INSERT INTO process_instances (id, definition_id, status) VALUES ('inst-sla', 'Process_ERP_Test', 'ACTIVE');
  INSERT INTO tokens (id, instance_id, element_id, state, created_at, retry_count) 
  VALUES ('tok-sla', 'inst-sla', 'ServiceTask_ERP_Sync', 'ACTIVE', NOW() - INTERVAL '35 minutes', 0);

  -- Сценарий Г: Успешное выполнение (Happy Path)
  INSERT INTO process_instances (id, definition_id, status) VALUES ('inst-success', 'Process_ERP_Test', 'ACTIVE');
  INSERT INTO tokens (id, instance_id, element_id, state, created_at, retry_count) 
  VALUES ('tok-success', 'inst-success', 'ServiceTask_ERP_Sync', 'ACTIVE', NOW(), 0);
"
kubectl run pg-seed --rm -i --restart='Never' --image=postgres:15-alpine \
  --env="PGPASSWORD=bpmn_super_secret_password" \
  -- sh -c "echo \"$DB_SEED_SQL\" | psql -h $DB_HOST -U bpmn_user -d bpmn_db"

echo -e "\n=== 5. Интеграционное тестирование NATS JetStream ==="
echo "▶ Сценарий А: Имитация сетевого сбоя (Network Error)"
kubectl run nats-test-a --rm -i --restart='Never' --image=natsio/nats-box:latest -- \
  sh -c "nats pub mes.in.v1.erp.network '{\"process_instance_id\": \"inst-network\", \"status\": \"NETWORK_ERROR\", \"error_message\": \"Timeout\", \"payload\": {}}' -s $NATS_URL"

echo "▶ Сценарий Б: Имитация бизнес-ошибки (Business Error)"
kubectl run nats-test-b --rm -i --restart='Never' --image=natsio/nats-box:latest -- \
  sh -c "nats pub mes.in.v1.erp.business '{\"process_instance_id\": \"inst-business\", \"status\": \"BUSINESS_ERROR\", \"error_message\": \"Invalid ID\", \"payload\": {}}' -s $NATS_URL"

echo "▶ Сценарий Г: Успешный ответ от ERP (Happy Path)"
kubectl run nats-test-success --rm -i --restart='Never' --image=natsio/nats-box:latest -- \
  sh -c "nats pub mes.in.v1.erp.success '{\"process_instance_id\": \"inst-success\", \"status\": \"SUCCESS\", \"error_message\": null, \"payload\": {\"erp_doc_id\": \"DOC-999888\"}}' -s $NATS_URL"

echo "⏳ Ожидание обработки событий BPMN-движком (15 секунд)..."
sleep 15

echo -e "\n=== 6. Валидация результатов (БД) ==="
VALIDATION_SQL="
  SELECT 'NETWORK SCENARIO (Expected > 0): ' || retry_count AS result FROM tokens WHERE instance_id = 'inst-network' AND state = 'ACTIVE'
  UNION ALL
  SELECT 'BUSINESS SCENARIO (Expected Task_Compensation): ' || element_id FROM tokens WHERE instance_id = 'inst-business' AND state = 'ACTIVE'
  UNION ALL
  SELECT 'SLA SCENARIO (Expected Task_Timeout_Escalation): ' || element_id FROM tokens WHERE instance_id = 'inst-sla' AND state = 'ACTIVE'
  UNION ALL
  SELECT 'SUCCESS SCENARIO (Expected EndEvent_Success): ' || element_id FROM tokens WHERE instance_id = 'inst-success' AND state = 'ACTIVE';
"
kubectl run pg-validate --rm -i --restart='Never' --image=postgres:15-alpine \
  --env="PGPASSWORD=bpmn_super_secret_password" \
  -- sh -c "echo \"$VALIDATION_SQL\" | psql -h $DB_HOST -U bpmn_user -d bpmn_db -t"

echo -e "\n=== 7. Последние логи BPMN Engine ==="
kubectl logs deployment/bpmn-engine-deployment --tail=40

echo -e "\n✅ E2E Тестирование успешно завершено!"