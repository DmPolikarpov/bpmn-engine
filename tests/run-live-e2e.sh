#!/usr/bin/env bash
set -e

# Живые E2E без кластерных Job'ов (M4/M6/M7). В отличие от `run-test.sh`, ничего не
# собирает: проверяет то, что уже развёрнуто. Port-forward до шлюза скрипты
# поднимают сами, БД читают через `kubectl exec ... psql`, событие «в обход шлюза»
# публикуют из пода шлюза.
#
#   tests/run-live-e2e.sh                  # все наборы
#   tests/run-live-e2e.sh metrics          # только API метрик (M7)
#   tests/run-live-e2e.sh ws-filter        # только фильтры WS-подписки (M7)
#   tests/run-live-e2e.sh roles            # только роли (M6)
#   tests/run-live-e2e.sh orders           # только заказы (M4)
#   GATEWAY_URL=http://127.0.0.1:8080 tests/run-live-e2e.sh   # шлюз уже проброшен
#
# Нужен рабочий kubectl-контекст кластера с загруженными образами (`./deploy.sh`).

cd "$(dirname "$0")/.."

# Node запускает сами сценарии (глобальный fetch требует Node 18+), kubectl — туннель
# и psql. Оба проверяем сразу, чтобы падение было понятным, а не на середине набора.
NODE_BIN="${NODE_BIN:-node}"
command -v "${NODE_BIN}" > /dev/null 2>&1 || {
    echo "❌ ${NODE_BIN} не найден в PATH (нужен Node 18+); переопределить: NODE_BIN=/путь/к/node"
    exit 2
}
command -v kubectl > /dev/null 2>&1 || {
    echo "❌ kubectl не найден в PATH — E2E ходит в кластер через port-forward и psql"
    exit 2
}

SUITES=("$@")
if [ ${#SUITES[@]} -eq 0 ]; then
    SUITES=(orders roles metrics ws-filter)
fi

for suite in "${SUITES[@]}"; do
    case "${suite}" in
        orders|orders-api) SCRIPT="tests/orders-api.e2e.cjs" ;;
        roles|roles-auth) SCRIPT="tests/roles-auth.e2e.cjs" ;;
        metrics|metrics-api) SCRIPT="tests/metrics-api.e2e.cjs" ;;
        ws|ws-filter|filters) SCRIPT="tests/ws-filter.e2e.cjs" ;;
        *)
            echo "❌ Неизвестный набор: ${suite} (ожидается orders, roles, metrics или ws-filter)"
            exit 2
            ;;
    esac

    echo "=== ${SCRIPT} ==="
    "${NODE_BIN}" "${SCRIPT}"
done

echo "✅ Живые E2E пройдены: ${SUITES[*]}"
