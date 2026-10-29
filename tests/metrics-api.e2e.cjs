/**
 * Живой E2E контура M7: HTTP-шлюз → NATS `mes.in.v1.api.metrics` → движок → Postgres.
 *
 * Проверяется контракт, который читает кабинет: коды 200/503, camelCase-поля,
 * цикл запуска, SLA по открытому шагу, разрезы по шагу и роли, доля брака и
 * предупреждения (схемы нет — финиш только по проекции).
 *
 * Сид — только legacy-таблицы (`process_instances`, `tokens`,
 * `execution_history`, `process_definitions`): тогда время шага берётся в
 * миллисекундах без вендорной истории, а финиш — из статуса проекции
 * (`finishedBy: "legacy"`). Следы удаляются в конце.
 *
 * Запуск (кластер поднят, образы M7 загружены — `deploy.sh`):
 *   node tests/metrics-api.e2e.cjs
 *
 * Переменные: `GATEWAY_URL` — шлюз уже доступен; `GATEWAY_PORT`/`GATEWAY_SERVICE`
 * — параметры туннеля; `DB_POD`/`DB_USER`/`DB_NAME`/`DB_PASSWORD` — psql.
 */
const { execFileSync, spawn, spawnSync } = require('child_process');

const GATEWAY_PORT = process.env.GATEWAY_PORT || '8098';
const BASE = process.env.GATEWAY_URL || `http://127.0.0.1:${GATEWAY_PORT}`;
const PORT_FORWARD = [
    'port-forward',
    process.env.GATEWAY_SERVICE || 'svc/http-cluster-service',
    `${GATEWAY_PORT}:8080`
];
const REQUEST_TIMEOUT_MS = 15000;
const TUNNEL_TIMEOUT_MS = 20000;
const PSQL = [
    'exec',
    '-i',
    process.env.DB_POD || 'bpmn-db-1',
    '-c',
    'postgres',
    '--',
    'env',
    `PGPASSWORD=${process.env.DB_PASSWORD || 'bpmn_super_secret_password'}`,
    'psql',
    '-h',
    '127.0.0.1',
    '-U',
    process.env.DB_USER || 'bpmn_user',
    '-d',
    process.env.DB_NAME || 'bpmn_db',
    '-v',
    'ON_ERROR_STOP=1',
    '-A',
    '-t',
    '-f',
    '-'
];

const MINUTE = 60 * 1000;
const PROCESS_ID = 'e2e-metrics-proc';
const INSTANCE_DONE = 'e2e-metrics-done';
const INSTANCE_OPEN = 'e2e-metrics-open';
/** Повторно активированный узел (M8): проверка `state_changed_at`. */
const REOPEN_PROCESS_ID = 'e2e-metrics-reopen';
const INSTANCE_REOPEN = 'e2e-metrics-rework';
/** Заказ, привязанный к завершённому запуску (M4/M8). */
const ORDER_NUM = '№E2E-METRICS';

const results = [];

function check(name, condition, actual) {
    results.push({ name, ok: Boolean(condition), actual });
    console.log(
        `${condition ? '✅' : '❌'} ${name}${condition ? '' : ` → ${JSON.stringify(actual)}`}`
    );
}

async function call(method, path) {
    const response = await fetch(`${BASE}${path}`, {
        method,
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS)
    });
    const text = await response.text();

    let body;
    try {
        body = JSON.parse(text);
    } catch (error) {
        body = { raw: text };
    }

    return { status: response.status, body };
}

function sql(statements, label) {
    try {
        execFileSync('kubectl', PSQL, { input: statements, encoding: 'utf8', stdio: 'pipe' });
    } catch (error) {
        console.error(`❌ SQL (${label}):`, error.stderr || error.message);
        throw error;
    }
}

async function isHealthy() {
    try {
        const response = await fetch(`${BASE}/health`, { signal: AbortSignal.timeout(2000) });
        return response.ok;
    } catch (error) {
        return false;
    }
}

function closeTunnel(tunnel) {
    if (!tunnel || tunnel.exitCode !== null) return;
    spawnSync('taskkill', ['/pid', String(tunnel.pid), '/t', '/f'], {
        stdio: 'ignore',
        windowsHide: true
    });
}

async function openTunnel() {
    if (await isHealthy()) return null;

    const tunnel = spawn('kubectl', PORT_FORWARD, { stdio: 'ignore', windowsHide: true });
    let exitCode = null;
    tunnel.on('exit', (code) => {
        exitCode = code;
    });

    const deadline = Date.now() + TUNNEL_TIMEOUT_MS;
    while (Date.now() < deadline) {
        if (exitCode !== null) throw new Error(`kubectl port-forward завершился с кодом ${exitCode}`);
        if (await isHealthy()) return tunnel;
        await new Promise((resolve) => setTimeout(resolve, 500));
    }

    closeTunnel(tunnel);
    throw new Error(`шлюз не ответил на ${BASE}/health за ${TUNNEL_TIMEOUT_MS / 1000} с`);
}

/** ±2 минуты: `NOW()` в БД и часы пода не совпадают до миллисекунды. */
const near = (actual, expected) =>
    typeof actual === 'number' && Math.abs(actual - expected) <= 2 * MINUTE;

const seed = `
DELETE FROM execution_history WHERE instance_id LIKE 'e2e-metrics-%';
DELETE FROM tokens WHERE instance_id LIKE 'e2e-metrics-%';
DELETE FROM process_instances WHERE id LIKE 'e2e-metrics-%';
DELETE FROM process_definitions WHERE id IN ('${PROCESS_ID}', '${REOPEN_PROCESS_ID}');
DELETE FROM orders WHERE order_num = '${ORDER_NUM}';

INSERT INTO process_definitions (id, name, version, bpmn_xml)
VALUES ('${PROCESS_ID}', 'E2E метрики', 1, '<definitions />');

-- Завершённый запуск: цикл 8 минут, шаг ждал 7 минут под ролью operator.
INSERT INTO process_instances (id, definition_id, status, created_at, updated_at)
VALUES ('${INSTANCE_DONE}', '${PROCESS_ID}', 'COMPLETED', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '2 minutes');
INSERT INTO tokens (id, instance_id, element_id, state, created_at)
VALUES ('${INSTANCE_DONE}:Task_Check', '${INSTANCE_DONE}', 'Task_Check', 'COMPLETED', NOW() - INTERVAL '9 minutes');
INSERT INTO execution_history (instance_id, event_type, element_id, actor, created_at)
VALUES ('${INSTANCE_DONE}', 'USER_TASK_COMPLETED', 'Task_Check', 'operator', NOW() - INTERVAL '2 minutes');

-- Заказ (M4): по нему движок подписывает запуск номером заказа (M8).
INSERT INTO orders (order_num, nomenclature, volume_liters, fat_content, packaging, due_date, counterparty, customer_instance_id)
VALUES ('${ORDER_NUM}', 'E2E молоко', 100, 3.2, 'ПЭТ 1 л', CURRENT_DATE + 7, 'E2E', '${INSTANCE_DONE}');

-- Активный запуск: шаг висит 30 минут — при пороге 15 минут это нарушение SLA.
INSERT INTO process_instances (id, definition_id, status, created_at, updated_at)
VALUES ('${INSTANCE_OPEN}', '${PROCESS_ID}', 'ACTIVE', NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '30 minutes');
INSERT INTO tokens (id, instance_id, element_id, state, created_at)
VALUES ('${INSTANCE_OPEN}:Task_Wait', '${INSTANCE_OPEN}', 'Task_Wait', 'ACTIVE', NOW() - INTERVAL '30 minutes');

-- Повторно активированный узел (M8): строка токена старая, но ждёт он 2 минуты.
INSERT INTO process_definitions (id, name, version, bpmn_xml)
VALUES ('${REOPEN_PROCESS_ID}', 'E2E повторная активация', 1, '<definitions />');
INSERT INTO process_instances (id, definition_id, status, created_at, updated_at)
VALUES ('${INSTANCE_REOPEN}', '${REOPEN_PROCESS_ID}', 'ACTIVE', NOW() - INTERVAL '3 hours', NOW() - INTERVAL '2 minutes');
INSERT INTO tokens (id, instance_id, element_id, state, created_at, state_changed_at)
VALUES ('${INSTANCE_REOPEN}:Task_Rework', '${INSTANCE_REOPEN}', 'Task_Rework', 'ACTIVE', NOW() - INTERVAL '3 hours', NOW() - INTERVAL '2 minutes');
`;
async function runChecks() {
    sql(seed, 'сид');

    // Порог 15 минут: шаг завершённого запуска (7 минут) в норме, ждущий шаг
    // активного (30 минут) — нарушение. Иначе оба шага считались бы браком по SLA.
    const response = await call(
        'GET',
        `/api/v1/metrics?processId=${PROCESS_ID}&slaMinutes=15&limit=5`
    );
    const body = response.body || {};

    check('GET /api/v1/metrics → 200', response.status === 200, response);
    check('ответ ok', body.ok === true, body.ok);
    check('generatedAt — ISO-время', /^\d{4}-\d{2}-\d{2}T/.test(body.generatedAt || ''), body.generatedAt);
    check('порог SLA взят из запроса', body.slaMinutes === 15, body.slaMinutes);
    check('точность источника времени — 1 с', body.precisionMs === 1000, body.precisionMs);
    check('код статуса не уходит в тело', body.status === undefined, body.status);

    const process = (body.processes || [])[0] || {};
    check('разрез по процессу один', (body.processes || []).length === 1, body.processes && body.processes.length);
    check('processId из запроса', process.processId === PROCESS_ID, process.processId);
    check('имя процесса из process_definitions', process.processName === 'E2E метрики', process.processName);
    check('запусков два', process.runs === 2, process.runs);
    check('завершён один', process.completed === 1, process.completed);
    check('активен один', process.active === 1, process.active);
    check('цикл ≈ 8 минут', near(process.averageCycleMs, 8 * MINUTE), process.averageCycleMs);

    const runs = process.lastRuns || [];
    check('limit=5 вернул оба запуска', runs.length === 2, runs.length);
    check(
        'новые сверху',
        runs[0] && runs[0].instanceId === INSTANCE_DONE,
        runs.map((run) => run.instanceId)
    );

    const done = runs.find((run) => run.instanceId === INSTANCE_DONE) || {};
    check('финиш из проекции', done.finishedBy === 'legacy', done.finishedBy);
    check('статус завершённого', done.status === 'COMPLETED', done.status);
    check('время финиша — ISO', /^\d{4}-\d{2}-\d{2}T/.test(done.finishedAt || ''), done.finishedAt);
    check('номер заказа в запуске (M8)', done.orderNum === ORDER_NUM, done.orderNum);

    const open = runs.find((run) => run.instanceId === INSTANCE_OPEN) || {};
    check('активный запуск без финиша', open.finishedAt === null, open.finishedAt);
    check('нарушение SLA видно', open.slaBreached === true, open.slaBreached);
    check(
        'назван шаг, который ждал',
        open.breachedSteps && open.breachedSteps[0] && open.breachedSteps[0].elementId === 'Task_Wait',
        open.breachedSteps
    );
    check(
        'ожидание ≈ 30 минут',
        open.breachedSteps && near(open.breachedSteps[0].waitedMs, 30 * MINUTE),
        open.breachedSteps && open.breachedSteps[0].waitedMs
    );
    check('шаг ещё открыт', open.breachedSteps && open.breachedSteps[0].open === true, open.breachedSteps);

    check('SLA: запуск с нарушением', process.slaRuns === 1, process.slaRuns);
    check('SLA: шаг с нарушением', process.slaSteps === 1, process.slaSteps);

    const step = (body.steps || []).find((item) => item.elementId === 'Task_Check') || {};
    check('разрез по шагу', step.samples === 1, step.samples);
    check('ожидание шага ≈ 7 минут', near(step.averageMs, 7 * MINUTE), step.averageMs);
    check('роль шага из аудита', step.role === 'operator', step.role);

    const role = (body.roles || []).find((item) => item.role === 'operator') || {};
    check('разрез по роли', role.steps === 1, role.steps);
    check('среднее ожидание роли ≈ 7 минут', near(role.averageMs, 7 * MINUTE), role.averageMs);

    check('схемы нет — брак не определён', process.defectShare === null, process.defectShare);
    check(
        'предупреждение о схеме',
        (body.warnings || []).some((warning) => warning.includes('нет схемы')),
        body.warnings
    );

    const limited = await call('GET', `/api/v1/metrics?processId=${PROCESS_ID}&limit=1`);
    check(
        'limit=1 обрезает список запусков',
        limited.body.processes && limited.body.processes[0].lastRuns.length === 1,
        limited.body.processes && limited.body.processes[0].lastRuns.length
    );

    // M8: ожидание считается от последней активации (`state_changed_at`).
    const reopen = await call(
        'GET',
        `/api/v1/metrics?processId=${REOPEN_PROCESS_ID}&slaMinutes=15&limit=5`
    );
    const reopenProcess = (reopen.body.processes || [])[0] || {};
    const reopenRun = (reopenProcess.lastRuns || [])[0] || {};
    check(
        'повторная активация: шаг ждёт от последней записи состояния',
        reopenRun.slaBreached === false,
        { breached: reopenRun.slaBreached, steps: reopenRun.breachedSteps }
    );
    check('повторная активация не даёт ни одного нарушения', reopenProcess.slaSteps === 0, reopenProcess.slaSteps);
    const reopenStep = (reopen.body.steps || [])[0] || {};
    check(
        'ожидание повторно активированного шага ≈ 2 минуты',
        near(reopenStep.averageMs, 2 * MINUTE),
        reopenStep.averageMs
    );

    const clamped = await call('GET', `/api/v1/metrics?processId=${PROCESS_ID}&slaMinutes=0`);
    check('порог SLA прижат к 1 минуте', clamped.body.slaMinutes === 1, clamped.body.slaMinutes);

    const unknown = await call('GET', '/api/v1/metrics?processId=e2e-metrics-none');
    check('неизвестный процесс — пусто', (unknown.body.processes || []).length === 0, unknown.body.processes);
    check(
        'предупреждение о неизвестном процессе',
        (unknown.body.warnings || []).some((warning) => warning.includes('e2e-metrics-none')),
        unknown.body.warnings
    );

    sql(
        `
        DELETE FROM execution_history WHERE instance_id LIKE 'e2e-metrics-%';
        DELETE FROM tokens WHERE instance_id LIKE 'e2e-metrics-%';
        DELETE FROM process_instances WHERE id LIKE 'e2e-metrics-%';
        DELETE FROM process_definitions WHERE id IN ('${PROCESS_ID}', '${REOPEN_PROCESS_ID}');
        DELETE FROM orders WHERE order_num = '${ORDER_NUM}';
        `,
        'очистка'
    );

    const after = await call('GET', `/api/v1/metrics?processId=${PROCESS_ID}`);
    check(
        'после очистки запусков нет',
        (after.body.processes || []).length === 0,
        after.body.processes
    );

    const failed = results.filter((result) => !result.ok);
    console.log(
        `\n${failed.length === 0 ? '✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ' : `❌ ПРОВАЛЕНО: ${failed.length}`} (${results.length} проверок)`
    );
    return failed.length;
}

async function main() {
    const tunnel = await openTunnel();
    try {
        return await runChecks();
    } finally {
        closeTunnel(tunnel);
    }
}

// `process.exit` здесь обрезал бы буфер stdout при перенаправлении в файл:
// скрипт гоняется и вживую, и через `wip/*.cmd` с логом.
main()
    .then((failed) => {
        process.exitCode = failed === 0 ? 0 : 1;
    })
    .catch((error) => {
        console.error('❌ E2E упал:', error);
        process.exitCode = 1;
    });

