#!/usr/bin/env bash
set -e

# Переходим в директорию расположения скрипта
cd "$(dirname "$0")"

CLUSTER_NAME="${CLUSTER_NAME:-mycluster}"
TESTER_IMAGE="mes-tester:latest"
JOB_NAME="mes-integration-test"

# Определяем директорию тестов
if [ -d "./tests" ]; then
    TESTS_DIR="./tests"
else
    TESTS_DIR="."
fi

# Гарантированная очистка при любом завершении скрипта
trap 'echo "🧹 Финальная очистка окружения..."; kubectl delete job "${JOB_NAME}" --ignore-not-found=true >/dev/null 2>&1 || true; docker rmi "${TESTER_IMAGE}" >/dev/null 2>&1 || true' EXIT

echo "=== 1. Сборка Docker-образа тестера ==="
docker build -t "${TESTER_IMAGE}" "${TESTS_DIR}"

echo "=== 2. Импорт образа в кластер k3d ==="
if command -v k3d &> /dev/null && k3d cluster list | grep -q "${CLUSTER_NAME}"; then
    k3d image import "${TESTER_IMAGE}" -c "${CLUSTER_NAME}"
fi

echo "=== 3. Очистка старых запусков Job ==="
kubectl delete job "${JOB_NAME}" --ignore-not-found=true

echo "=== 4. Запуск интеграционного теста в кластере ==="
kubectl apply -f "${TESTS_DIR}/test-job.yaml"

echo "=== 5. Ожидание завершения теста ==="
kubectl wait --for=condition=complete "job/${JOB_NAME}" --timeout=90s 2>/dev/null || \
kubectl wait --for=condition=failed "job/${JOB_NAME}" --timeout=5s 2>/dev/null || true

# Надежная проверка: проверяем количество успешных завершений пода в Job
SUCCEEDED_COUNT=$(kubectl get job "${JOB_NAME}" -o jsonpath='{.status.succeeded}' 2>/dev/null || echo "0")

if [ "${SUCCEEDED_COUNT}" -lt 1 ]; then
    echo ""
    echo "❌ Тест завершился с ошибкой или превысил таймаут!"
    echo "=== Состояние пода тестера ==="
    kubectl get pods -l "job-name=${JOB_NAME}" || true
    echo ""
    echo "=== Логи тестового контейнера ==="
    kubectl logs -l "job-name=${JOB_NAME}" --tail=200 || echo "⚠️ Не удалось извлечь логи тестера."
    echo ""
    echo "=== Логи BPMN Engine ==="
    kubectl logs deployment/bpmn-engine-deployment --tail=50 || echo "⚠️ Не удалось извлечь логи bpmn-engine."
    exit 1
fi

echo ""
echo "=== 6. Результаты выполнения теста ==="
kubectl logs -l "job-name=${JOB_NAME}" --tail=200

echo ""
echo "=== Интеграционный тест успешно выполнен! ==="