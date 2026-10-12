#!/usr/bin/env bash
set -e

# Локальная разработка: проброс портов кластера на localhost.
#   HTTP-шлюз -> http://localhost:8080 (package.json: proxy / REACT_APP_GATEWAY_URL)
#   WS-шлюз   -> ws://localhost:8081   (REACT_APP_WS_URL)
#
# Важно: `kubectl port-forward` завершается, как только под перезапустился
# или связь прервалась. Если отваливается HTTP-проброс, CRA dev-server
# отвечает 500 «Error occurred while trying to proxy ... (ECONNREFUSED)»
# на все запросы к /api/... — в UI это выглядит как сломанная кнопка
# «Задеплоить BPMN в Оркестратор»/любое действие. Поэтому держим оба
# проброса в цикле с автоперезапуском.
NAMESPACE="${NAMESPACE:-default}"
HTTP_SERVICE="${HTTP_SERVICE:-http-cluster-service}"
WS_SERVICE="${WS_SERVICE:-websocket-cluster-service}"
HTTP_PORT="${HTTP_PORT:-8080}"
WS_PORT="${WS_PORT:-8081}"
ADDRESS="${ADDRESS:-127.0.0.1}"

forward_forever() {
    local name="$1" service="$2" local_port="$3"

    while true; do
        echo "[${name}] kubectl port-forward service/${service} ${local_port}:8080 (namespace ${NAMESPACE})"
        kubectl -n "${NAMESPACE}" port-forward --address "${ADDRESS}" \
            "service/${service}" "${local_port}:8080" || true
        echo "[${name}] Проброс прерван, перезапуск через 2 с..."
        sleep 2
    done
}

forward_forever "HTTP" "${HTTP_SERVICE}" "${HTTP_PORT}" &
HTTP_FORWARD_PID=$!
forward_forever "WS" "${WS_SERVICE}" "${WS_PORT}" &
WS_FORWARD_PID=$!

trap 'kill "${HTTP_FORWARD_PID}" "${WS_FORWARD_PID}" 2>/dev/null || true' EXIT INT TERM

echo "Пробросы активны: http://localhost:${HTTP_PORT} и ws://localhost:${WS_PORT}/ws/status (Ctrl+C для остановки)"
wait