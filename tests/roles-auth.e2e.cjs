/**
 * Живой E2E контура M6: роль сессии как авторизация события.
 *
 * Проверяется то, что обещают README и `docs/dairy-plant-roadmap.md`:
 * `sendEvent` чужой роли не двигает шаг (403 с причиной), событие без роли
 * (воркер, `nats pub`, E2E-скрипт) проходит, а `execution_history` хранит
 * исполнителя шага (`actor`).
 *
 * Путь события: HTTP-шлюз → проверка прав движка (`mes.in.v1.api.auth`) →
 * JetStream → рантайм графа → проекция в legacy `tokens`/`process_instances`
 * и аудит `execution_history`.
 *
 * Схема поднимается скриптом: у стартовой кнопки роль `SALES`
 * (`act:userAction/@role`), у задачи — `bpmn:potentialOwner name="SALES"`.
 *
 * Запуск (кластер поднят, образы загружены — см. `deploy.sh`):
 *   node tests/roles-auth.e2e.cjs
 *
 * Переменные окружения: `GATEWAY_URL` — шлюз уже доступен, туннель не нужен;
 * `GATEWAY_PORT`/`GATEWAY_SERVICE` — параметры туннеля; `DB_POD`/`DB_USER`/
 * `DB_NAME`/`DB_PASSWORD` — проверки и очистка через `kubectl exec ... psql`.
 */
const { execFileSync, spawn, spawnSync } = require('child_process');
const fs = require('fs');

const GATEWAY_PORT = process.env.GATEWAY_PORT || '8098';
const BASE = process.env.GATEWAY_URL || `http://127.0.0.1:${GATEWAY_PORT}`;
const PORT_FORWARD = [
  'port-forward',
  process.env.GATEWAY_SERVICE || 'svc/http-cluster-service',
  `${GATEWAY_PORT}:8080`
];
const REQUEST_TIMEOUT_MS = 15000;
const TUNNEL_TIMEOUT_MS = 20000;
const WAIT_TIMEOUT_MS = 15000;
/**
 * Хелпер публикации в NATS. Лежит рядом и копируется в под шлюза: только там
 * есть зависимость `nats` и `NATS_URL`, без них прямой `nats pub` не изобразить.
 */
const NATS_PUBLISHER = 'tests-nats-publish.cjs';
const NATS_PUBLISHER_PATH = require('path').join(__dirname, 'nats-publish.cjs');
/** Куда копируем хелпер в поде: каталог приложения принадлежит root. */
const PUBLISHER_IN_POD = `/tmp/${NATS_PUBLISHER}`;
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
  // `-t -A`: только значения без рамок и заголовков — их сравниваем как есть.
  '-t',
  '-A',
  '-f',
  '-'
];

/** Роль кнопки старта и `potentialOwner` задачи — одна и та же (`SALES`). */
const START_ROLE = 'SALES';
/** Роль, которой шаг не принадлежит: её события должны получать 403. */
const FOREIGN_ROLE = 'WAREHOUSE';
const PROCESS_ID = 'Process_RoleSmoke';
const START_EVENT = 'Event_Start';
const TASK_ID = 'Task_Approve';

const stamp = Date.now().toString().slice(-6);
const allowedInstance = `e2e-role-ok-${stamp}`;
const deniedInstance = `e2e-role-deny-${stamp}`;
const anonymousInstance = `e2e-role-anon-${stamp}`;

const BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:act="http://erp.mes/schema/bpmn/action"
                  id="Definitions_RoleSmoke">
  <bpmn:process id="${PROCESS_ID}" isExecutable="true">
    <bpmn:startEvent id="${START_EVENT}">
      <bpmn:extensionElements>
        <act:userAction id="start" role="${START_ROLE}" elementId="${TASK_ID}" />
      </bpmn:extensionElements>
      <bpmn:outgoing>Flow_Start</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="Flow_Start" sourceRef="${START_EVENT}" targetRef="${TASK_ID}" />
    <bpmn:userTask id="${TASK_ID}" name="Role smoke task">
      <bpmn:potentialOwner name="${START_ROLE}" />
      <bpmn:incoming>Flow_Start</bpmn:incoming>
      <bpmn:outgoing>Flow_End</bpmn:outgoing>
    </bpmn:userTask>
    <bpmn:sequenceFlow id="Flow_End" sourceRef="${TASK_ID}" targetRef="Event_End" />
    <bpmn:endEvent id="Event_End" />
  </bpmn:process>
</bpmn:definitions>`;

const results = [];
/** Туннель до шлюза; `main` кладёт его сюда, чтобы `call` мог переоткрыть. */
let tunnel = null;

function check(name, condition, actual) {
  results.push({ name, ok: Boolean(condition), actual });
  console.log(`${condition ? '✅' : '❌'} ${name}${condition ? '' : ` → ${JSON.stringify(actual)}`}`);
}

/** `kubectl port-forward` иногда рвёт соединение — это не отказ шлюза. */
const isConnectionError = (error) =>
  error instanceof TypeError && (error.message === 'fetch failed' || Boolean(error.cause));

/** Повтор запроса после переоткрытия туннеля (тело и `correlation_key` не меняются). */
async function withTunnelRetry(run) {
  try {
    return await run();
  } catch (error) {
    if (!isConnectionError(error)) throw error;

    console.log('🔌 туннель до шлюза закрылся — переоткрываю и повторяю запрос');
    closeTunnel(tunnel);
    tunnel = await openTunnel();
    return await run();
  }
}

/**
 * Запрос к шлюзу. `actor` уходит заголовком `X-Actor-Role` — так его отправляет
 * кабинет; пустая строка означает «событие без роли».
 */
async function request(method, path, body, actor) {
  const headers = {};

  if (body) headers['Content-Type'] = 'application/json';
  if (actor) headers['X-Actor-Role'] = actor;

  const response = await fetch(`${BASE}${path}`, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS)
  });
  const text = await response.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = { raw: text };
  }
  return { status: response.status, body: json };
}

const call = (method, path, body, actor) => withTunnelRetry(() => request(method, path, body, actor));

/** Событие экземпляра: `POST /api/v1/process/instances/:id/events`. */
function sendEvent(instanceId, eventName, elementId, actor, outputData) {
  return call(
    'POST',
    `/api/v1/process/instances/${encodeURIComponent(instanceId)}/events`,
    {
      event_name: eventName,
      // Уникальный ключ на каждое событие: повтором считается тот же ключ.
      correlation_key: `${instanceId}-${eventName}-${elementId}-${Date.now()}-${Math.random()
        .toString(16)
        .slice(2, 8)}`,
      element_id: elementId || '',
      output_data: outputData || {}
    },
    actor
  );
}

function psql(statements, label) {
  try {
    return execFileSync('kubectl', PSQL, { input: statements, encoding: 'utf8', stdio: 'pipe' }).trim();
  } catch (error) {
    console.error(`❌ SQL (${label}):`, error.stderr || error.message);
    throw error;
  }
}

/** Одно значение из БД (`''`, если строк нет). */
function scalar(query, label) {
  return psql(query, label).split(/\r?\n/)[0].trim();
}

const sqlString = (value) => `'${String(value).replace(/'/g, "''")}'`;

/** Ждёт, пока проекция рантайма доедет до БД (события обрабатываются асинхронно). */
async function waitFor(description, read, expected) {
  const deadline = Date.now() + WAIT_TIMEOUT_MS;
  let actual = null;

  while (Date.now() < deadline) {
    actual = read();
    if (actual === expected) return actual;
    await new Promise((resolve) => setTimeout(resolve, 300));
  }

  console.log(
    `⏳ ожидание «${description}»: получено ${JSON.stringify(actual)}, ждали ${JSON.stringify(expected)}`
  );
  return actual;
}

const tokenState = (instanceId, elementId) =>
  scalar(
    `SELECT state FROM tokens WHERE instance_id = ${sqlString(instanceId)} AND element_id = ${sqlString(
      elementId
    )} ORDER BY created_at DESC LIMIT 1;`,
    `токен ${elementId}`
  );

const instanceStatus = (instanceId) =>
  scalar(`SELECT status FROM process_instances WHERE id = ${sqlString(instanceId)};`, 'статус');

/** Последняя запись аудита шага: `actor|event_type|element_id` (`NULL|...` без роли). */
const lastAudit = (instanceId, eventType, elementId) =>
  scalar(
    `SELECT coalesce(actor, 'NULL') || '|' || event_type || '|' || coalesce(element_id, '')
       FROM execution_history
       WHERE instance_id = ${sqlString(instanceId)} AND event_type = ${sqlString(eventType)}
         AND coalesce(element_id, '') = ${sqlString(elementId || '')}
       ORDER BY id DESC LIMIT 1;`,
    `аудит ${eventType}`
  );

async function isHealthy() {
  try {
    const response = await fetch(`${BASE}/health`, { signal: AbortSignal.timeout(2000) });
    return response.ok;
  } catch {
    return false;
  }
}

/** Гасит дерево процессов: `child.kill()` на Windows оставляет внуков. */
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
    if (exitCode !== null) {
      throw new Error(`kubectl port-forward завершился с кодом ${exitCode}`);
    }
    if (await isHealthy()) return tunnel;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }

  closeTunnel(tunnel);
  throw new Error(`шлюз не ответил на ${BASE}/health за ${TUNNEL_TIMEOUT_MS / 1000} с`);
}

function cleanup() {
  psql(
    `
    DELETE FROM execution_history WHERE instance_id LIKE 'e2e-role-%';
    DELETE FROM tokens WHERE instance_id LIKE 'e2e-role-%';
    DELETE FROM process_instances WHERE id LIKE 'e2e-role-%';
    DELETE FROM process_definitions WHERE id = '${PROCESS_ID}';
    `,
    'очистка'
  );
}

/**
 * Событие в обход шлюза: publish в JetStream из пода шлюза.
 *
 * Так проверяется страховка рантайма — шлюз чужую роль отсекает до публикации,
 * и запись `USER_TASK_DENIED` в аудите иначе не увидеть. Ровно так же в NATS
 * попадает событие от `nats pub` или стороннего воркера.
 */
function publishDirect(subject, payload) {
  const pod = execFileSync(
    'kubectl',
    ['get', 'pod', '-l', 'app=http-cluster', '-o', 'jsonpath={.items[0].metadata.name}'],
    { encoding: 'utf8' }
  ).trim();

  if (!pod) {
    throw new Error('под шлюза не найден: публиковать событие в NATS нечем');
  }

  // Копируем через stdin (`sh -c cat > файл`): `kubectl cp` на Windows
  // принимает путь `D:\...` за удалённую спецификацию и падает. Каталог
  // приложения в образе принадлежит root, поэтому пишем в `/tmp`, а модуль
  // `nats` ищем через `NODE_PATH` (там `node_modules` шлюза).
  execFileSync('kubectl', ['exec', '-i', pod, '--', 'sh', '-c', `cat > ${PUBLISHER_IN_POD}`], {
    input: fs.readFileSync(NATS_PUBLISHER_PATH),
    stdio: ['pipe', 'pipe', 'pipe']
  });

  return execFileSync(
    'kubectl',
    [
      'exec',
      '-i',
      pod,
      '--',
      'env',
      'NODE_PATH=/usr/src/app/node_modules',
      'node',
      PUBLISHER_IN_POD,
      subject
    ],
    { input: JSON.stringify(payload), encoding: 'utf8', stdio: 'pipe' }
  ).trim();
}

async function runChecks() {
  // Прошлый прогон мог упасть до очистки: убираем свои следы заранее.
  cleanup();

  const health = await call('GET', '/health');
  check('GET /health → 200', health.status === 200, health.body);
  check('subject проверки прав в /health', health.body.auth_subject === 'mes.in.v1.api.auth', health.body);
  check('заголовок роли X-Actor-Role', health.body.actor_header === 'X-Actor-Role', health.body);

  /* ---------------------------------------------------------------- *
   * 1. Старт чужой ролью: отказ ещё до публикации события
   * ---------------------------------------------------------------- */

  const deniedStart = await sendEvent(deniedInstance, 'StartProcess', TASK_ID, FOREIGN_ROLE, {
    bpmn_xml: BPMN
  });
  check('старт чужой ролью → 403', deniedStart.status === 403, deniedStart.body);
  check(
    'причина отказа называет роль',
    typeof deniedStart.body.error === 'string' && deniedStart.body.error.includes(START_ROLE),
    deniedStart.body.error
  );
  check(
    'в отказе требуемая роль SALES',
    Array.isArray(deniedStart.body.details && deniedStart.body.details.required) &&
      deniedStart.body.details.required.includes(START_ROLE),
    deniedStart.body.details
  );
  check(
    'в отказе роль сессии',
    deniedStart.body.details && deniedStart.body.details.actor === FOREIGN_ROLE,
    deniedStart.body.details
  );
  check(
    'экземпляр не создан',
    scalar(
      `SELECT count(*) FROM process_instances WHERE id = ${sqlString(deniedInstance)};`,
      'нет экземпляра'
    ) === '0',
    deniedInstance
  );

  /* ---------------------------------------------------------------- *
   * 2. Старт своей ролью: экземпляр создан, токен на задаче
   * ---------------------------------------------------------------- */

  const allowedStart = await sendEvent(allowedInstance, 'StartProcess', TASK_ID, START_ROLE, {
    bpmn_xml: BPMN
  });
  check('старт ролью SALES → 202', allowedStart.status === 202, allowedStart.body);
  check(
    'токен на задаче ACTIVE',
    (await waitFor('ACTIVE на Task_Approve', () => tokenState(allowedInstance, TASK_ID), 'ACTIVE')) ===
      'ACTIVE',
    tokenState(allowedInstance, TASK_ID)
  );

  /* ---------------------------------------------------------------- *
   * 3. Завершение чужой ролью: 403, токен не сдвинулся, отказ в аудите
   * ---------------------------------------------------------------- */

  const deniedStep = await sendEvent(allowedInstance, 'CompleteTask', TASK_ID, FOREIGN_ROLE);
  check('шаг чужой ролью → 403', deniedStep.status === 403, deniedStep.body);
  check(
    'отказ объясняет роль шага',
    typeof deniedStep.body.error === 'string' && deniedStep.body.error.includes(START_ROLE),
    deniedStep.body.error
  );
  check(
    'событие не дошло до движка (шлюз не публиковал)',
    scalar(
      `SELECT count(*) FROM execution_history
       WHERE instance_id = ${sqlString(allowedInstance)} AND event_type = 'USER_TASK_DENIED';`,
      'нет USER_TASK_DENIED'
    ) === '0',
    lastAudit(allowedInstance, 'USER_TASK_DENIED', TASK_ID)
  );
  check(
    'токен остался на задаче',
    tokenState(allowedInstance, TASK_ID) === 'ACTIVE',
    tokenState(allowedInstance, TASK_ID)
  );
  check(
    'шаг не закрыт в legacy-проекции',
    scalar(
      `SELECT count(*) FROM tokens WHERE instance_id = ${sqlString(
        allowedInstance
      )} AND element_id = ${sqlString(TASK_ID)} AND state = 'COMPLETED';`,
      'нет COMPLETED'
    ) === '0',
    allowedInstance
  );

  /* ---------------------------------------------------------------- *
   * 4. Страховка рантайма: событие в обход шлюза всё равно не двигает шаг
   * ---------------------------------------------------------------- */

  const direct = publishDirect('mes.in.v1.event.erp.CompleteTask', {
    event_id: `direct-${stamp}-${Date.now()}`,
    instance_id: allowedInstance,
    element_id: TASK_ID,
    payload: {},
    actor: FOREIGN_ROLE
  });
  check('событие опубликовано в JetStream', direct.startsWith('PUBLISHED'), direct);
  check(
    'движок записал отказ с actor',
    (await waitFor(
      'USER_TASK_DENIED',
      () => lastAudit(allowedInstance, 'USER_TASK_DENIED', TASK_ID),
      `${FOREIGN_ROLE}|USER_TASK_DENIED|${TASK_ID}`
    )) === `${FOREIGN_ROLE}|USER_TASK_DENIED|${TASK_ID}`,
    lastAudit(allowedInstance, 'USER_TASK_DENIED', TASK_ID)
  );
  check(
    'токен по-прежнему на задаче',
    tokenState(allowedInstance, TASK_ID) === 'ACTIVE',
    tokenState(allowedInstance, TASK_ID)
  );

  /* ---------------------------------------------------------------- *
   * 5. Завершение своей ролью: токен сдвинулся, аудит с actor
   * ---------------------------------------------------------------- */

  const allowedStep = await sendEvent(allowedInstance, 'CompleteTask', TASK_ID, START_ROLE);
  check('шаг ролью SALES → 202', allowedStep.status === 202, allowedStep.body);
  check(
    'токен шага COMPLETED',
    (await waitFor('COMPLETED на Task_Approve', () => tokenState(allowedInstance, TASK_ID), 'COMPLETED')) ===
      'COMPLETED',
    tokenState(allowedInstance, TASK_ID)
  );
  check(
    'завершение записано в execution_history с actor',
    (await waitFor(
      'USER_TASK_COMPLETED',
      () => lastAudit(allowedInstance, 'USER_TASK_COMPLETED', TASK_ID),
      `${START_ROLE}|USER_TASK_COMPLETED|${TASK_ID}`
    )) === `${START_ROLE}|USER_TASK_COMPLETED|${TASK_ID}`,
    lastAudit(allowedInstance, 'USER_TASK_COMPLETED', TASK_ID)
  );
  check(
    'экземпляр завершён',
    (await waitFor('статус COMPLETED', () => instanceStatus(allowedInstance), 'COMPLETED')) === 'COMPLETED',
    instanceStatus(allowedInstance)
  );

  /* ---------------------------------------------------------------- *
   * 6. Обратная совместимость: событие без роли проходит (воркер, E2E)
   * ---------------------------------------------------------------- */

  const anonymousStart = await sendEvent(anonymousInstance, 'StartProcess', TASK_ID, '', {
    bpmn_xml: BPMN
  });
  check('старт без роли → 202', anonymousStart.status === 202, anonymousStart.body);
  check(
    'токен без роли на задаче ACTIVE',
    (await waitFor('ACTIVE без роли', () => tokenState(anonymousInstance, TASK_ID), 'ACTIVE')) === 'ACTIVE',
    tokenState(anonymousInstance, TASK_ID)
  );

  const anonymousStep = await sendEvent(anonymousInstance, 'CompleteTask', TASK_ID, '');
  check('шаг без роли → 202', anonymousStep.status === 202, anonymousStep.body);
  check(
    'токен без роли COMPLETED',
    (await waitFor('COMPLETED без роли', () => tokenState(anonymousInstance, TASK_ID), 'COMPLETED')) ===
      'COMPLETED',
    tokenState(anonymousInstance, TASK_ID)
  );
  check(
    'аудит без роли: actor NULL',
    (await waitFor(
      'USER_TASK_COMPLETED без роли',
      () => lastAudit(anonymousInstance, 'USER_TASK_COMPLETED', TASK_ID),
      `NULL|USER_TASK_COMPLETED|${TASK_ID}`
    )) === `NULL|USER_TASK_COMPLETED|${TASK_ID}`,
    lastAudit(anonymousInstance, 'USER_TASK_COMPLETED', TASK_ID)
  );
  check(
    'экземпляр без роли завершён',
    (await waitFor('статус COMPLETED', () => instanceStatus(anonymousInstance), 'COMPLETED')) === 'COMPLETED',
    instanceStatus(anonymousInstance)
  );

  // Оставляем БД в том же состоянии, в котором нашли: строки `e2e-role-*`
  // нужны только этому прогону, и кабинет не должен их показывать.
  cleanup();

  const failed = results.filter((result) => !result.ok);
  console.log(
    `\n${failed.length === 0 ? '✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ' : `❌ ПРОВАЛЕНО: ${failed.length}`} (${
      results.length
    } проверок)`
  );
  return failed.length;
}

/** Открывает туннель, гоняет проверки и всегда гасит туннель. */
async function main() {
  tunnel = await openTunnel();
  try {
    return await runChecks();
  } finally {
    closeTunnel(tunnel);
  }
}

main()
  .then((failed) => process.exit(failed === 0 ? 0 : 1))
  .catch((error) => {
    console.error('❌ E2E упал:', error);
    process.exit(1);
  });
