/**
 * Живой E2E фильтрации WS-подписки (M7).
 *
 * Проверяется то, чего не видит юнит-тест `filters.js`: шлюз в кластере
 * действительно отсекает чужие события **до** отправки. Клиенты — встроенный в
 * Node `WebSocket` (без зависимостей), события публикуются в NATS из пода
 * `http-cluster` (там есть модуль `nats`).
 *
 * Сценарий:
 * 1. `filtered` подключается с `?instanceId=<A>&role=operator` — ждёт только
 *    экземпляр A и только события без чужой роли;
 * 2. `control` подключается без фильтров и объявляет экземпляр B сообщением
 *    `{"type":"subscribe","instanceIds":["<B>"]}`;
 * 3. `all` подключается без фильтров — получает всё (поведение до M7);
 * 4. публикуются события A (без роли), B (role: operator), A с чужой ролью;
 * 5. `control` снимает фильтры `{"type":"unsubscribe"}` — и получает событие C.
 *
 * Запуск (кластер поднят, образы M7 загружены — `deploy.sh`):
 *   node tests/ws-filter.e2e.cjs
 *
 * Переменные: `WS_URL` — шлюз уже доступен; `WS_PORT`/`WS_SERVICE` — туннель;
 * `PUBLISH_POD` — под с модулем `nats` (по умолчанию `deploy/http-cluster-deployment`).
 */
const { execFileSync, spawn, spawnSync } = require('child_process');

const WS_PORT = process.env.WS_PORT || '8097';
const HOST = `127.0.0.1:${WS_PORT}`;
const BASE = `http://${HOST}`;
const WS_BASE = process.env.WS_URL || `ws://${HOST}/ws/status`;
const PORT_FORWARD = ['port-forward', process.env.WS_SERVICE || 'svc/websocket-cluster-service', `${WS_PORT}:8080`];
const PUBLISH_POD = process.env.PUBLISH_POD || 'deploy/http-cluster-deployment';
const SUBJECT_PREFIX = 'mes.bpmn.v1.event.';
const TUNNEL_TIMEOUT_MS = 20000;

const RUN = Date.now().toString().slice(-6);
const INST_A = `e2e-ws-a-${RUN}`;
const INST_B = `e2e-ws-b-${RUN}`;
const INST_C = `e2e-ws-c-${RUN}`;

const results = [];

function check(name, condition, actual) {
    results.push({ name, ok: Boolean(condition), actual });
    console.log(`${condition ? '✅' : '❌'} ${name}${condition ? '' : ` → ${JSON.stringify(actual)}`}`);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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
    spawnSync('taskkill', ['/pid', String(tunnel.pid), '/t', '/f'], { stdio: 'ignore', windowsHide: true });
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
        await sleep(500);
    }

    closeTunnel(tunnel);
    throw new Error(`шлюз не ответил на ${BASE}/health за ${TUNNEL_TIMEOUT_MS / 1000} с`);
}

/** Публикует события в NATS из пода шлюза: на хосте нет `nats`, в поде есть. */
function publish(events) {
    const body = events
        .map(
            (event) =>
                `  await nc.publish(${JSON.stringify(SUBJECT_PREFIX + event.instanceId)}, ` +
                `sc.encode(${JSON.stringify(JSON.stringify(event.message))}));\n  await sleep(400);`
        )
        .join('\n');

    const script = `
const { connect, StringCodec } = require('nats');
const sc = StringCodec();
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
(async () => {
  const nc = await connect({ servers: process.env.NATS_URL || 'nats://nats-service:4222' });
${body}
  await nc.flush();
  await nc.close();
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
`;

    try {
        execFileSync('kubectl', ['exec', PUBLISH_POD, '--', 'node', '-e', script], {
            encoding: 'utf8',
            stdio: 'pipe'
        });
    } catch (error) {
        console.error('❌ публикация в NATS:', error.stderr || error.message);
        throw error;
    }
}

/** Клиент WS: копит всё, что пришло, и умеет отправлять управляющие сообщения. */
function openClient(name, query = '') {
    const client = { name, events: [], control: [], socket: null };

    client.ready = new Promise((resolve, reject) => {
        const socket = new WebSocket(`${WS_BASE}${query}`);
        client.socket = socket;

        socket.onopen = () => resolve(client);
        socket.onerror = (error) => reject(new Error(`WS ${name}: ${error.message || 'ошибка сокета'}`));
        socket.onmessage = (event) => {
            let message;
            try {
                message = JSON.parse(event.data);
            } catch (error) {
                message = { raw: event.data };
            }

            if (message && (message.type === 'subscribed' || message.type === 'unsubscribed')) {
                client.control.push(message);
            } else {
                client.events.push(message);
            }
        };
    });

    return client;
}

/** Отправка управляющего сообщения с ожиданием ответа шлюза. */
async function sendControl(client, payload) {
    const before = client.control.length;
    client.socket.send(JSON.stringify(payload));

    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
        if (client.control.length > before) return client.control[client.control.length - 1];
        await sleep(50);
    }

    return null;
}

/**
 * События только наших экземпляров.
 *
 * В потоке `mes.bpmn.v1.event.>` бывают и чужие события: SLA-воркер движка
 * публикует `TIMED_OUT` по просроченным токенам (в том числе по тем, что сеет
 * соседний `metrics-api.e2e.cjs`). Клиент без фильтров их получает — поэтому
 * считаем только свои id, иначе набор зависит от того, что ещё идёт в кластере.
 */
const ownEvents = (client) =>
    client.events.filter((event) => [INST_A, INST_B, INST_C].includes(event.instance_id));

const elementIds = (events) => events.map((event) => event.element_id || event.elementId || null);

function closeClient(client) {
    if (client.socket && client.socket.readyState <= 1) client.socket.close();
}

async function runChecks() {
    const filtered = openClient('filtered', `?instanceId=${INST_A}&role=operator`);
    const control = openClient('control');
    const all = openClient('all');
    await Promise.all([filtered.ready, control.ready, all.ready]);

    check('три WS-клиента подключены', [filtered, control, all].every((client) => client.socket.readyState === 1), {
        filtered: filtered.socket.readyState,
        control: control.socket.readyState,
        all: all.socket.readyState
    });

    const ack = await sendControl(control, { type: 'subscribe', instanceIds: [INST_B] });
    check('шлюз подтвердил подписку', ack && ack.type === 'subscribed', ack);
    check(
        'в подтверждении экземпляр из сообщения',
        ack && Array.isArray(ack.instanceIds) && ack.instanceIds.length === 1 && ack.instanceIds[0] === INST_B,
        ack && ack.instanceIds
    );

    publish([
        // 1. Свой экземпляр без роли — подсветка схемы не должна гаснуть.
        {
            instanceId: INST_A,
            message: { type: 'token_updated', instance_id: INST_A, element_id: 'Task_A', state: 'ACTIVE' }
        },
        // 2. Чужой экземпляр с объявленной ролью — фильтр `control` его ждёт.
        {
            instanceId: INST_B,
            message: { type: 'process_completed', instance_id: INST_B, role: 'operator' }
        },
        // 3. Свой экземпляр, но чужая роль — `filtered` не должен его увидеть.
        {
            instanceId: INST_A,
            message: { type: 'token_updated', instance_id: INST_A, element_id: 'Task_Role', role: 'storekeeper' }
        }
    ]);
    await sleep(2000);

    check(
        'filtered: своё событие без роли дошло',
        elementIds(ownEvents(filtered)).join(',') === 'Task_A',
        elementIds(ownEvents(filtered))
    );
    check(
        'filtered: чужой экземпляр отсечён',
        !filtered.events.some((event) => event.instance_id === INST_B),
        filtered.events
    );
    check(
        'filtered: чужая роль отсечена',
        !filtered.events.some((event) => (event.element_id || '') === 'Task_Role'),
        elementIds(ownEvents(filtered))
    );
    check(
        'control: событие своего экземпляра дошло',
        ownEvents(control).length === 1 && ownEvents(control)[0].instance_id === INST_B,
        ownEvents(control)
    );
    check('all: без фильтров дошло всё', ownEvents(all).length === 3, elementIds(ownEvents(all)));
    check(
        'каждому клиенту ушло своё',
        ownEvents(filtered).length === 1 && ownEvents(all).length === 3,
        {
            filtered: ownEvents(filtered).length,
            control: ownEvents(control).length,
            all: ownEvents(all).length
        }
    );

    const reset = await sendControl(control, { type: 'unsubscribe' });
    check(
        'unsubscribe снимает фильтры',
        reset && Array.isArray(reset.instanceIds) && reset.instanceIds.length === 0,
        reset
    );

    publish([
        {
            instanceId: INST_C,
            message: { type: 'token_updated', instance_id: INST_C, element_id: 'Task_C', state: 'ACTIVE' }
        }
    ]);
    await sleep(1500);

    check(
        'после unsubscribe приходит любой экземпляр',
        control.events.some((event) => event.instance_id === INST_C),
        control.events
    );
    check(
        'filtered не получил чужой экземпляр после сброса у соседа',
        !filtered.events.some((event) => event.instance_id === INST_C),
        filtered.events
    );

    closeClient(filtered);
    closeClient(control);
    closeClient(all);

    const failed = results.filter((result) => !result.ok);
    console.log(
        `\n${failed.length === 0 ? '✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ' : `❌ ПРОВАЛЕНО: ${failed.length}`} (${results.length} проверок)`
    );
    return failed.length;
}

// `process.exit` обрезал бы буфер stdout при перенаправлении в файл.
async function main() {
    const tunnel = await openTunnel();
    try {
        return await runChecks();
    } finally {
        closeTunnel(tunnel);
    }
}

main()
    .then((failed) => {
        process.exitCode = failed === 0 ? 0 : 1;
    })
    .catch((error) => {
        console.error('❌ E2E упал:', error);
        process.exitCode = 1;
    });
