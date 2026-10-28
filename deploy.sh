#!/usr/bin/env bash
set -e

# Переходим в директорию скрипта (корень проекта)
cd "$(dirname "$0")"

CLUSTER_NAME="${CLUSTER_NAME:-erp}"
ENGINE_IMAGE="bpmn-engine-service:latest"
HTTP_IMAGE="http-cluster-service:latest"
WS_IMAGE="websocket-cluster-service:latest"

echo "Checking Kubernetes cluster connection..."
if ! kubectl cluster-info &> /dev/null; then
    echo "ERROR: Kubernetes cluster is not reachable."
    echo "Please ensure your local cluster (kind/k3d/Docker/Minikube) is running."
    echo "  - kind:     kind create cluster --name ${CLUSTER_NAME}"
    echo "  - k3d:      k3d cluster create ${CLUSTER_NAME}"
    echo "  - minikube: minikube start"
    exit 1
fi
echo "Cluster is reachable. Proceeding..."

echo "=== Starting MES BPMN Orchestrator Pipeline ==="
echo ""

# 1. Применение базовых манифестов NATS
echo "[1/8] Deploying NATS JetStream Infrastructure..."
kubectl apply -f nats-configmap.yaml
kubectl apply -f nats-deployment.yaml

# ПРИНУДИТЕЛЬНЫЙ РЕСТАРТ: Гарантирует, что NATS 100% подхватит конфигурацию JetStream
kubectl rollout restart deployment/nats-deployment

echo "Waiting for NATS deployment to be ready..."
# 180s: при первом запуске образу nats:2.11-alpine может понадобиться скачаться,
# а Recreate-стратегия сначала останавливает старый под.
if ! kubectl rollout status deployment/nats-deployment --timeout=180s; then
    echo "ERROR: NATS deployment did not become ready. Current state:"
    kubectl get pods -l app=nats -o wide || true
    kubectl describe pods -l app=nats || true
    kubectl get events --sort-by=.lastTimestamp | tail -n 20 || true
    exit 1
fi

# 2. Инициализация JetStream стримов
echo "[2/8] Running NATS Streams Initialization Job..."
kubectl delete job nats-init-streams --ignore-not-found=true
kubectl apply -f nats-init-streams-job.yaml

echo "Waiting for NATS streams initialization to complete..."
kubectl wait --for=condition=complete job/nats-init-streams --timeout=60s

# 3. Сборка всех Docker-образов контура
echo "[3/8] Building Docker Images..."
echo "  -> Building BPMN Engine (Rust)..."
# Создаем папку миграций внутри модуля bpmn-engine, если её нет
if [ ! -d "bpmn-engine/migrations" ]; then
  echo "📁 Папка migrations не найдена. Создаем пустую директорию..."
  mkdir -p bpmn-engine/migrations
fi
docker build -t "${ENGINE_IMAGE}" ./bpmn-engine
echo "  -> Building HTTP API Gateway (Node.js)..."
docker build -t "${HTTP_IMAGE}" ./bpmn-gateways/http-cluster
echo "  -> Building WebSocket Cluster (Node.js)..."
docker build -t "${WS_IMAGE}" ./bpmn-gateways/websocket-cluster

# 4. Импорт образов в локальное хранилище кластера
echo "[4/8] Importing Images into Cluster..."
if command -v kind &> /dev/null && kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    echo "  -> Loading images into kind cluster '${CLUSTER_NAME}'..."
    kind load docker-image "${ENGINE_IMAGE}" "${HTTP_IMAGE}" "${WS_IMAGE}" --name "${CLUSTER_NAME}"
elif command -v k3d &> /dev/null && k3d cluster list | grep -q "${CLUSTER_NAME}"; then
    k3d image import "${ENGINE_IMAGE}" "${HTTP_IMAGE}" "${WS_IMAGE}" -c "${CLUSTER_NAME}"
elif command -v minikube &> /dev/null; then
    minikube image load "${ENGINE_IMAGE}" "${HTTP_IMAGE}" "${WS_IMAGE}"
else
    echo "WARN: no kind/k3d/minikube cluster detected - images were NOT loaded into the cluster."
fi

# 5. Применение остальных корневых YAML
# В этот шаг автоматически попадают http-cluster-deployment.yaml, websocket-cluster-deployment.yaml, ingress и базы данных
echo "[5/8] Applying root Kubernetes Manifests (Gateways, DB, Ingress)..."
shopt -s nullglob
for manifest in *.yaml *.yml; do
    # Пропускаем манифесты NATS, так как они уже применены на шаге 1 и 2
    if [[ "$manifest" == "nats-configmap.yaml" || "$manifest" == "nats-deployment.yaml" || "$manifest" == "nats-init-streams-job.yaml" ]]; then
        continue
    fi
    echo "Applying ${manifest}..."
    kubectl apply -f "${manifest}"
done

# 5b. Владение legacy-таблицами (M6).
# initdb создаёт структуру от суперпользователя, а миграции движка выполняются
# от bpmn_user. `ALTER TABLE execution_history ADD COLUMN actor` (0005) без
# владения падает с SQLSTATE 42501, и движок уходит в CrashLoopBackOff.
# На новом кластере владение передаёт init.sql; здесь — для уже развёрнутой БД.
DB_POD="${DB_POD:-$(kubectl get pod -l cnpg.io/cluster=bpmn-db,cnpg.io/instanceRole=primary -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)}"
if [ -n "${DB_POD}" ]; then
    echo "[5b/8] Ensuring schema ownership for migrations (${DB_POD})..."
    kubectl exec -i "${DB_POD}" -c postgres -- \
        psql -U postgres -d bpmn_db -v ON_ERROR_STOP=1 -q \
        -c "ALTER TABLE IF EXISTS execution_history OWNER TO bpmn_user;" \
        || echo "WARN: не удалось передать владение execution_history — миграция 0005 может упасть"
else
    echo "[5b/8] WARN: primary pod bpmn-db не найден — владение execution_history не проверено"
fi

# 6. Специфичный деплой манифестов, лежащих во вложенных папках
echo "[6/8] Deploying BPMN Engine Service..."
kubectl apply -f ./bpmn-engine/deployment.yaml

# 7. Принудительный рестарт подов для подхвата свежих образов (из-за IfNotPresent)
echo "[7/8] Triggering Rolling Updates for Engine and Gateways..."
kubectl rollout restart deployment/bpmn-engine-deployment
kubectl rollout restart deployment/http-cluster-deployment
kubectl rollout restart deployment/websocket-cluster-deployment

# 8. Финальный контроль готовности всех узлов
echo "[8/8] Checking Deployment Rollout Statuses..."
kubectl rollout status deployment/bpmn-engine-deployment --timeout=120s
kubectl rollout status deployment/http-cluster-deployment --timeout=120s
kubectl rollout status deployment/websocket-cluster-deployment --timeout=120s

echo ""
echo "=== Deployment Pipeline Finished Successfully ==="