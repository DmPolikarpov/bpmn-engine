/**
 * Живой E2E контура M4: HTTP-шлюз → NATS request/reply → API заказов движка → Postgres.
 *
 * Проверяется тот же контракт, который использует кабинет (`erp-simulator/src/ordersApi.js`):
 * коды 200/400/404/409, поля в camelCase, состояние контуров и текущий шаг из
 * legacy read-model (`tokens`, `process_instances`).
 *
 * Туннель до шлюза скрипт открывает сам (`kubectl port-forward`) и гасит его в
 * конце: запущенный из оболочки `port-forward` живёт до первого таймаута
 * команды, и прогон падал на закрытом порту вместо проверок.
 *
 * Запуск (кластер поднят, образы загружены — см. `deploy/`):
 *   node tests/orders-api.e2e.cjs
 *
 * Переменные окружения: `GATEWAY_URL` — шлюз уже доступен, туннель не нужен;
 * `GATEWAY_PORT`/`GATEWAY_SERVICE` — параметры туннеля; `DB_POD`/`DB_USER`/
 * `DB_NAME`/`DB_PASSWORD` — сид и очистка через `kubectl exec ... psql`.
 */
const { execFileSync, spawn, spawnSync } = require('child_process');

const GATEWAY_PORT = process.env.GATEWAY_PORT || '8099';
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
  '-f',
  '-'
];

const results = [];
const orderNum = `№E2E${Date.now().toString().slice(-6)}`;
const customerInstance = 'e2e-inst-customer';
const supplierInstance = 'e2e-inst-supplier';
const activeElement = 'Task_E2E_Customer';

function check(name, condition, actual) {
  results.push({ name, ok: Boolean(condition), actual });
  console.log(`${condition ? '✅' : '❌'} ${name}${condition ? '' : ` → ${JSON.stringify(actual)}`}`);
}

async function call(method, path, body) {
  const response = await fetch(`${BASE}${path}`, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
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

function sql(statements, label) {
  try {
    execFileSync('kubectl', PSQL, { input: statements, encoding: 'utf8', stdio: 'pipe' });
  } catch (error) {
    console.error(`❌ SQL (${label}):`, error.stderr || error.message);
    throw error;
  }
}

const isoDate = (days) => {
  const date = new Date();
  date.setDate(date.getDate() + days);
  return date.toISOString().slice(0, 10);
};

/** Тело NewOrder — ровно те поля, что собирает `toOrderPayload` в кабинете. */
const newOrder = {
  orderNum,
  nomenclature: 'E2E молоко 3,2%',
  volumeLiters: 250,
  fatContent: 3.2,
  packaging: 'ПЭТ 1 л',
  dueDate: isoDate(7),
  counterparty: 'E2E контрагент'
};

/** Пробный `/health` с таймаутом: `fetch` без `signal` вешал прогон. */
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

/**
 * Туннель до шлюза на время прогона.
 *
 * Если шлюз уже доступен (свой `port-forward` или прямой адрес в `BASE_URL`),
 * ничего не запускается. Возвращает процесс либо `null`.
 */
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

async function runChecks() {
  // Прошлый прогон мог упасть до очистки: убираем свои следы заранее, иначе
  // POST вернёт 409 и проверки поедут.
  sql(
    `
    DELETE FROM tokens WHERE instance_id LIKE 'e2e-inst-%';
    DELETE FROM process_instances WHERE id LIKE 'e2e-inst-%';
    DELETE FROM process_definitions WHERE id = 'Process_E2E';
    DELETE FROM orders WHERE order_num LIKE '№E2E%';
    `,
    'подготовка'
  );

  const health = await call('GET', '/health');
  check('GET /health → 200', health.status === 200, health.body);
  check('шлюз видит NATS', health.body.nats_connected === true, health.body);
  check('subject заказов в /health', health.body.orders_subject === 'mes.in.v1.api.orders', health.body);

  const created = await call('POST', '/api/v1/orders', newOrder);
  check('POST /api/v1/orders → 200', created.status === 200, created.body);
  check('ответ ok:true', created.body.ok === true, created.body);
  check('orderNum сохранён', created.body.order && created.body.order.orderNum === orderNum, created.body.order);
  check('статус нового заказа NEW', created.body.order && created.body.order.status === 'NEW', created.body.order);
  check(
    'контуры без процессов (IDLE)',
    created.body.order &&
      created.body.order.customerProcessState === 'IDLE' &&
      created.body.order.supplierProcessState === 'IDLE',
    created.body.order
  );
  check(
    'dueDate в ISO-формате',
    created.body.order && created.body.order.dueDate === newOrder.dueDate,
    created.body.order
  );
  check('createdAt присутствует', Boolean(created.body.order && created.body.order.createdAt), created.body.order);

  const duplicate = await call('POST', '/api/v1/orders', newOrder);
  check('повторный POST → 409', duplicate.status === 409, duplicate);
  check('409 объясняет причину', /уже существует/.test(duplicate.body.error || ''), duplicate.body);

  const invalid = await call('POST', '/api/v1/orders', {
    ...newOrder,
    orderNum: `${orderNum}-bad`,
    nomenclature: '   ',
    volumeLiters: 0,
    fatContent: 120,
    dueDate: isoDate(-1)
  });
  const fields = (invalid.body.details || []).map((detail) => detail.field).sort();
  check('невалидный заказ → 400', invalid.status === 400, invalid.body);
  check(
    'details перечисляют поля',
    ['dueDate', 'fatContent', 'nomenclature', 'volumeLiters'].every((field) => fields.includes(field)),
    fields
  );

  const list = await call('GET', '/api/v1/orders');
  check('GET /api/v1/orders → 200', list.status === 200, list.body);
  check(
    'заказ есть в списке',
    Array.isArray(list.body.orders) && list.body.orders.some((order) => order.orderNum === orderNum),
    list.body.orders && list.body.orders.map((order) => order.orderNum)
  );

  const card = await call('GET', `/api/v1/orders/${encodeURIComponent(orderNum)}`);
  check('GET карточки → 200', card.status === 200, card.body);
  check('карточка того же заказа', card.body.order && card.body.order.orderNum === orderNum, card.body.order);

  const missing = await call('GET', `/api/v1/orders/${encodeURIComponent('№нет-такого')}`);
  check('неизвестный заказ → 404', missing.status === 404, missing);

  const badOrg = await call('POST', `/api/v1/orders/${encodeURIComponent(orderNum)}/instances`, {
    org: 'CLIENT',
    instanceId: customerInstance
  });
  check('org вне CUSTOMER/SUPPLIER → 400', badOrg.status === 400, badOrg);

  const badOrder = await call('POST', `/api/v1/orders/${encodeURIComponent('№нет-такого')}/instances`, {
    org: 'CUSTOMER',
    instanceId: customerInstance
  });
  check('привязка к неизвестному заказу → 404', badOrder.status === 404, badOrder);

  const attached = await call('POST', `/api/v1/orders/${encodeURIComponent(orderNum)}/instances`, {
    org: 'CUSTOMER',
    instanceId: customerInstance
  });
  check('привязка экземпляра → 200', attached.status === 200, attached.body);
  check(
    'статус заказа IN_PROGRESS',
    attached.body.order && attached.body.order.status === 'IN_PROGRESS',
    attached.body.order
  );
  check(
    'экземпляр записан',
    attached.body.order && attached.body.order.customerInstanceId === customerInstance,
    attached.body.order
  );

  // Текущий шаг API считает по tokens: имитируем legacy read-model, которую
  // пишет рантайм, и проверяем, что карточка показывает шаг, а не копию в orders.
  // Определение заводим сами: на пустой БД `SELECT ... LIMIT 1` вернул бы NULL
  // и `process_instances.definition_id` не прошёл бы NOT NULL.
  sql(
    `
    INSERT INTO process_definitions (id, name, version, bpmn_xml)
    VALUES ('Process_E2E', 'E2E заказ', 1, '<bpmn:definitions/>')
    ON CONFLICT DO NOTHING;
    INSERT INTO process_instances (id, definition_id, status, business_key)
    VALUES ('${customerInstance}', 'Process_E2E', 'ACTIVE', '${orderNum}');
    INSERT INTO tokens (id, instance_id, element_id, state)
    VALUES ('tok-e2e-customer', '${customerInstance}', '${activeElement}', 'ACTIVE');
    `,
    'активный шаг'
  );

  const withStep = await call('GET', `/api/v1/orders/${encodeURIComponent(orderNum)}`);
  check(
    'карточка показывает текущий шаг',
    withStep.body.order && withStep.body.order.customerActiveElementId === activeElement,
    withStep.body.order
  );
  check(
    'состояние контура ACTIVE',
    withStep.body.order && withStep.body.order.customerProcessState === 'ACTIVE',
    withStep.body.order
  );

  const supplier = await call('POST', `/api/v1/orders/${encodeURIComponent(orderNum)}/instances`, {
    org: 'SUPPLIER',
    instanceId: supplierInstance
  });
  check(
    'привязка второй стороны',
    supplier.status === 200 &&
      supplier.body.order &&
      supplier.body.order.supplierInstanceId === supplierInstance &&
      supplier.body.order.status === 'IN_PROGRESS',
    supplier.body.order
  );
  check(
    'шаг заказчика не потерян',
    supplier.body.order && supplier.body.order.customerActiveElementId === activeElement,
    supplier.body.order
  );

  sql(
    `
    DELETE FROM tokens WHERE instance_id LIKE 'e2e-inst-%';
    DELETE FROM process_instances WHERE id LIKE 'e2e-inst-%';
    DELETE FROM process_definitions WHERE id = 'Process_E2E';
    DELETE FROM orders WHERE order_num LIKE '№E2E%';
    `,
    'очистка'
  );

  const after = await call('GET', '/api/v1/orders');
  check(
    'заказ удалён из списка',
    Array.isArray(after.body.orders) && !after.body.orders.some((order) => order.orderNum === orderNum),
    after.body.orders && after.body.orders.map((order) => order.orderNum)
  );

  const failed = results.filter((result) => !result.ok);
  console.log(
    `\n${failed.length === 0 ? '✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ' : `❌ ПРОВАЛЕНО: ${failed.length}`} (${results.length} проверок)`
  );
  return failed.length;
}

/** Открывает туннель, гоняет проверки и всегда гасит туннель. */
async function main() {
  const tunnel = await openTunnel();
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
