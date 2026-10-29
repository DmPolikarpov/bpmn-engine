/**
 * Живой E2E демо-контура M8: демо-схема «Молочный завод» целиком, без браузера.
 *
 * Проверяется то, что обещают roadmap (раздел 8) и README, но не проверял ни
 * один автотест: события кабинета двигают токены по **настоящей** демо-схеме -
 * три пула, развилки по переменным, сообщения между процессами (M3) и
 * `serviceTask` в мок-ERP.
 *
 * Путь события тот же, что у кабинета: HTTP-шлюз → проверка прав → JetStream →
 * рантайм графа → проекция в legacy `tokens`/`process_instances` и аудит
 * `execution_history`.
 *
 * Сценарии:
 *  1. `ok`     — заказ → закупка сырья → поставщик отгрузил (сообщение) →
 *                производство → отгрузка ГП (`End_Main`);
 *  2. `reject` — лаборатория забраковала партию (`labPassed=false`) →
 *                `Task_ReturnMilk` → `End_Reject`;
 *  3. `crash`  — авария пастеризации (`pasteurizeOk=false`) → `serviceTask`
 *                `Task_ERP_CriticalAlert` (мок-воркер) → `End_ERP_Aborted`;
 *  4. `degraded` — деградация линии (`packOutcome=degraded`) → `Tech_LogDegradation`
 *                → `serviceTask` `Task_ERP_HandleDeviation` → `End_ERP_DeviationLogged`.
 *
 * Схемы берутся из демо-клиента (`erp-simulator/src/bpmn-sdk/defaults/dairyPlant.js`),
 * чтобы тест не разошёлся с тем, что показывает кабинет: скрипт вырезает
 * шаблонные строки `DAIRY_PLANT_PROCESS_XML` и `DAIRY_SUPPLIER_PROCESS_XML` и
 * раскрывает в них `\${` обратно в `${` (в исходнике это экранирование
 * шаблонной строки, а не часть XML).
 *
 * Запуск (кластер поднят, образы загружены — `deploy.sh`):
 *   node tests/dairy-plant.e2e.cjs
 *
 * Переменные окружения: `GATEWAY_URL` — шлюз уже доступен, туннель не нужен;
 * `GATEWAY_PORT`/`GATEWAY_SERVICE` — параметры туннеля; `DB_POD`/`DB_USER`/
 * `DB_NAME`/`DB_PASSWORD` — проверки и очистка через `kubectl exec ... psql`.
 */
const { execFileSync, spawn, spawnSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const GATEWAY_PORT = process.env.GATEWAY_PORT || '8097';
const BASE = process.env.GATEWAY_URL || `http://127.0.0.1:${GATEWAY_PORT}`;
const PORT_FORWARD = [
  'port-forward',
  process.env.GATEWAY_SERVICE || 'svc/http-cluster-service',
  `${GATEWAY_PORT}:8080`
];
const REQUEST_TIMEOUT_MS = 15000;
const TUNNEL_TIMEOUT_MS = 20000;
const WAIT_TIMEOUT_MS = 20000;
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
  '-t',
  '-A',
  '-f',
  '-'
];

/** Имена событий движка (`erp-simulator/src/bpmn-sdk/constants.js`). */
const EVENT = {
  DEPLOY: 'DeploySchema',
  START: 'StartProcess',
  COMPLETE: 'CompleteBlock'
};
/** Служебный адрес деплоя (`ENGINE_ELEMENTS.PROCESS_DEPLOYMENT`). */
const DEPLOY_ELEMENT = 'Process_Deployment';
const START_NODE = 'Task_ReceiveOrder';

const DEMO_SOURCE = path.join(
  __dirname,
  '..',
  'erp-simulator',
  'src',
  'bpmn-sdk',
  'defaults',
  'dairyPlant.js'
);

/** Отметка прогона: по ней ищутся экземпляры, созданные движком (поставщик). */
const STAMP = Date.now().toString().slice(-6);
const STARTED_AT = new Date().toISOString();
const ORDER_OK = `№E2E-D${STAMP}`;
const ORDER_REJECT = `№E2E-R${STAMP}`;
const ORDER_CRASH = `№E2E-C${STAMP}`;
const ORDER_DEGRADED = `№E2E-G${STAMP}`;
const INSTANCE_OK = `e2e-dairy-ok-${STAMP}`;
const INSTANCE_REJECT = `e2e-dairy-reject-${STAMP}`;
const INSTANCE_CRASH = `e2e-dairy-crash-${STAMP}`;
const INSTANCE_DEGRADED = `e2e-dairy-degraded-${STAMP}`;

const results = [];
/** Туннель до шлюза; `main` кладёт его сюда, чтобы `call` мог переоткрыть. */
let tunnel = null;

function check(name, condition, actual) {
  results.push({ name, ok: Boolean(condition), actual });
  console.log(`${condition ? '✅' : '❌'} ${name}${condition ? '' : ` → ${JSON.stringify(actual)}`}`);
}

/**
 * Вырезает XML процесса из исходника демо-клиента.
 *
 * `defaults/dairyPlant.js` — ES-модуль (шаблонные строки), поэтому `require`
 * его не берёт. Вырезаем строку по имени константы и раскрываем `\${` обратно в
 * `${`: в шаблоне это экранирование, а в XML — условие шлюза.
 *
 * @param {string} constant имя экспорта (`DAIRY_PLANT_PROCESS_XML`).
 * @returns {string} XML процесса.
 */
function readSchema(constant) {
  const source = fs.readFileSync(DEMO_SOURCE, 'utf8');
  const head = `export const ${constant} = \``;
  const from = source.indexOf(head);

  if (from < 0) throw new Error(`в ${DEMO_SOURCE} нет константы ${constant}`);

  const start = from + head.length;
  const end = source.indexOf('`;', start);

  if (end < 0) throw new Error(`не найден конец шаблонной строки ${constant}`);

  return source.slice(start, end).replace(/\\\$\{/g, '${').replace(/\\\\/g, '\\');
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
 * Запрос к шлюзу. `actor` уходит заголовком `X-Actor-Role` (так его отправляет
 * кабинет): в этом сценарии роли не проверяются, но путь должен быть тем же.
 */
async function request(method, path_, body, actor) {
  const headers = {};

  if (body) headers['Content-Type'] = 'application/json';
  if (actor) headers['X-Actor-Role'] = actor;

  const response = await fetch(`${BASE}${path_}`, {
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

const call = (method, path_, body, actor) => withTunnelRetry(() => request(method, path_, body, actor));

/** Событие экземпляра: `POST /api/v1/process/instances/:id/events`. */
function sendEvent(instanceId, eventName, elementId, outputData, actor) {
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

/** Деплой схемы: служебный экземпляр `inst-deploy-*` и `element_id` деплоя. */
async function deploySchema(xml, label) {
  const response = await sendEvent(
    `inst-deploy-e2e-dairy-${STAMP}`,
    EVENT.DEPLOY,
    DEPLOY_ELEMENT,
    { bpmn_xml: xml },
    'DIRECTOR'
  );

  check(`деплой схемы (${label}) → 202`, response.status === 202, response.body);
  return response;
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

/**
 * Ждёт, пока проекция рантайма доедет до БД (события обрабатываются асинхронно).
 *
 * @param {string} description что ждём (для сообщения).
 * @param {() => any} read чтение фактического значения.
 * @param {any|((actual: any) => boolean)} expected ожидаемое значение или предикат.
 * @returns {any} фактическое значение на момент совпадения (или последнее).
 */
async function waitFor(description, read, expected) {
  const matches = typeof expected === 'function' ? expected : (actual) => actual === expected;
  const deadline = Date.now() + WAIT_TIMEOUT_MS;
  let actual = null;

  while (Date.now() < deadline) {
    actual = read();
    if (matches(actual)) return actual;
    await new Promise((resolve) => setTimeout(resolve, 300));
  }

  console.log(`⏳ ожидание «${description}»: получено ${JSON.stringify(actual)}`);
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

const tokenCount = (instanceId, elementId) =>
  scalar(
    `SELECT count(*) FROM tokens WHERE instance_id = ${sqlString(instanceId)} AND element_id = ${sqlString(
      elementId
    )};`,
    `след ${elementId}`
  );

/** Ждёт `ACTIVE` на узле: так проверяется «шаг встал туда, куда ведёт ветка». */
const waitForStep = (instanceId, elementId) =>
  waitFor(`токен ${elementId}=ACTIVE`, () => tokenState(instanceId, elementId), 'ACTIVE');

/**
 * Ждёт любой след узла в проекции.
 *
 * У `serviceTask` состояния `ACTIVE` почти не видно: мок-воркер ERP отвечает за
 * миллисекунды, и к первому опросу токен уже `COMPLETED`. Поэтому проверяется
 * сам факт прохождения узла, а не момент «ждёт».
 */
async function waitForTrace(instanceId, elementId) {
  const count = await waitFor(
    `след ${elementId}`,
    () => tokenCount(instanceId, elementId),
    (value) => value !== '0'
  );
  check(`${instanceId}: узел ${elementId} пройден`, count !== '0', count);
  return count;
}

async function isHealthy() {
  try {
    const response = await fetch(`${BASE}/health`, { signal: AbortSignal.timeout(2000) });
    return response.ok;
  } catch {
    return false;
  }
}

/** Гасит дерево процессов: `child.kill()` на Windows оставляет внуков. */
function closeTunnel(handle) {
  if (!handle || handle.exitCode !== null) return;
  spawnSync('taskkill', ['/pid', String(handle.pid), '/t', '/f'], {
    stdio: 'ignore',
    windowsHide: true
  });
}

async function openTunnel() {
  if (await isHealthy()) return null;

  const handle = spawn('kubectl', PORT_FORWARD, { stdio: 'ignore', windowsHide: true });
  let exitCode = null;
  handle.on('exit', (code) => {
    exitCode = code;
  });

  const deadline = Date.now() + TUNNEL_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (exitCode !== null) {
      throw new Error(`kubectl port-forward завершился с кодом ${exitCode}`);
    }
    if (await isHealthy()) return handle;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }

  closeTunnel(handle);
  throw new Error(`шлюз не ответил на ${BASE}/health за ${TUNNEL_TIMEOUT_MS / 1000} с`);
}

/**
 * Старт экземпляра завода.
 *
 * `element_id` — узел, который должен стать `ACTIVE` (кнопка стартового события
 * ведёт в `Task_ReceiveOrder`), `bpmn_xml` — определение процесса, `orderNum` —
 * бизнес-ключ корреляции сообщений (M3).
 */
async function startFactory(instanceId, xml, orderNum, variables) {
  const response = await sendEvent(instanceId, EVENT.START, START_NODE, {
    bpmn_xml: xml,
    orderNum,
    ...variables
  });

  check(`старт ${instanceId} → 202`, response.status === 202, response.body);

  const state = await waitForStep(instanceId, START_NODE);
  check(`${orderNum}: токен на ${START_NODE}`, state === 'ACTIVE', state);
}

/**
 * Закрывает шаг и ждёт следующий узел.
 *
 * `variables` уходят в `output_data`: развилки демо-схемы сравнивают их как
 * строки (`${stockSufficient == 'true'}`), поэтому значения передаются строками.
 *
 * `to === null` — ждать нечего: следующим узлом идёт catch/throw-событие, а
 * проекция пишет в `tokens` только задачи (`bpmn:userTask`, `bpmn:serviceTask`).
 */
async function step(instanceId, from, to, variables) {
  const response = await sendEvent(instanceId, EVENT.COMPLETE, from, variables);

  if (response.status !== 202) {
    check(`${from} → ${to || 'следующее событие'}: событие принято`, false, response.body);
    return;
  }

  if (!to) return;

  const state = await waitForStep(instanceId, to);
  check(`${from} → ${to}`, state === 'ACTIVE', state);
}

/** Закрывает последний шаг ветки и ждёт финиша экземпляра. */
async function finish(instanceId, from, variables) {
  const response = await sendEvent(instanceId, EVENT.COMPLETE, from, variables);

  if (response.status !== 202) {
    check(`${from}: событие принято`, false, response.body);
    return;
  }

  const status = await waitFor(`${instanceId} завершён`, () => instanceStatus(instanceId), 'COMPLETED');
  check(`${instanceId}: статус COMPLETED`, status === 'COMPLETED', status);
}

/**
 * Экземпляр поставщика, созданный движком по `trg:processTrigger`.
 *
 * Префикс задан в схеме (`instancePrefix="inst-supp-"`), бизнес-ключ в БД не
 * пишется (`business_key` остаётся `NULL`), поэтому ищем самый свежий экземпляр,
 * который встал на `Task_ConfirmPO` после старта прогона.
 */
async function findSupplierInstance() {
  const query = `
    SELECT t.instance_id
      FROM tokens t
      JOIN process_instances i ON i.id = t.instance_id
     WHERE t.element_id = 'Task_ConfirmPO' AND t.state = 'ACTIVE'
       AND i.created_at >= ${sqlString(STARTED_AT)}
     ORDER BY i.created_at DESC
     LIMIT 1;`;

  return waitFor(
    'экземпляр поставщика на Task_ConfirmPO',
    () => scalar(query, 'поставщик'),
    (value) => value !== ''
  );
}

/**
 * Доводит завод до приёмки молока: закупка, экземпляр поставщика, отгрузка.
 *
 * Возвращает идентификатор экземпляра поставщика ('' — если движок его не создал).
 * Ветка закупки нужна всем сценариям: производство начинается только после
 * сообщения `MilkShipped`, а оно приходит из процесса поставщика.
 */
async function purchaseAndSupply(instanceId, xml, orderNum) {
  await step(instanceId, 'Task_ReceiveOrder', 'Task_ProcessOrder');
  await step(instanceId, 'Task_ProcessOrder', 'Task_CheckStock');
  await step(instanceId, 'Task_CheckStock', 'Task_CreatePO', { stockSufficient: 'false' });
  await step(instanceId, 'Task_CreatePO', 'Task_ApprovePO');
  await step(instanceId, 'Task_ApprovePO', 'Task_SendPO');
  // Дальше `Event_SendPO`: он запускает процесс поставщика, а завод встаёт на
  // catch-событие `Task_WaitPOConfirmed` — в `tokens` его нет, поэтому не ждём.
  await step(instanceId, 'Task_SendPO', null);

  const supplier = await findSupplierInstance();
  check(`${orderNum}: движок создал экземпляр поставщика`, supplier.startsWith('inst-supp-'), supplier);
  if (!supplier) return '';

  await step(supplier, 'Task_ConfirmPO', 'Task_ShipMilk');
  await finish(supplier, 'Task_ShipMilk');

  // Сообщение MilkShipped ищет экземпляр завода по orderNum, а не по instance_id.
  const received = await waitForStep(instanceId, 'Task_ReceiveMilk');
  check(`${orderNum}: MilkShipped разбудил приёмку завода`, received === 'ACTIVE', received);

  return supplier;
}

/** Приёмка и лаборатория: до развилки ГОСТ. */
async function acceptAndAnalyze(instanceId) {
  await step(instanceId, 'Task_ReceiveMilk', 'Task_AcceptMilk');
  await step(instanceId, 'Task_AcceptMilk', 'Task_AnalyzeMilk');
}

/**
 * Технологический маршрут до упаковки.
 *
 * `variables.pasteurizeOk === 'false'` обрывает маршрут на пастеризации: дальше
 * движок уходит в аварийную ветку (`Task_ERP_CriticalAlert`), и упаковки нет.
 */
async function productionUntilPack(instanceId, variables) {
  await step(instanceId, 'Task_StoreMilk', 'Tech_CleanCool');
  await step(instanceId, 'Tech_CleanCool', 'Tech_Normalize');
  await step(instanceId, 'Tech_Normalize', 'Tech_Homogenize');
  await step(instanceId, 'Tech_Homogenize', 'Tech_Pasteurize');

  // Негативная ветка пастеризации: после шлюза идёт `serviceTask` ERP, а не
  // задача технологической цепочки — ждём его след, а не `Tech_Ferment`.
  if (variables.pasteurizeOk === 'false') {
    await step(instanceId, 'Tech_Pasteurize', null, { pasteurizeOk: 'false' });
    await waitForTrace(instanceId, 'Task_ERP_CriticalAlert');
    return;
  }

  await step(instanceId, 'Tech_Pasteurize', 'Tech_Ferment', { pasteurizeOk: 'true' });
  await step(instanceId, 'Tech_Ferment', 'Tech_CoolCheck');
  await step(instanceId, 'Tech_CoolCheck', 'Tech_Pack', {
    techQcPassed: variables.techQcPassed || 'true'
  });
}


/** Сценарий 1: заказ → закупка → поставщик → производство → отгрузка ГП. */
async function scenarioHappy(factoryXml, supplierXml) {
  console.log('\n── Сценарий 1: заказ → закупка → производство → отгрузка ──');

  await deploySchema(supplierXml, 'поставщик');
  await startFactory(INSTANCE_OK, factoryXml, ORDER_OK, { stockSufficient: 'false' });

  const supplier = await purchaseAndSupply(INSTANCE_OK, factoryXml, ORDER_OK);
  if (!supplier) return;

  check(
    'поставщик закрыл свой процесс',
    instanceStatus(supplier) === 'COMPLETED',
    instanceStatus(supplier)
  );

  await acceptAndAnalyze(INSTANCE_OK);
  await step(INSTANCE_OK, 'Task_AnalyzeMilk', 'Task_StoreMilk', { labPassed: 'true' });
  await productionUntilPack(INSTANCE_OK, {});
  await step(INSTANCE_OK, 'Tech_Pack', 'Task_StoreFG', { packOutcome: 'good' });
  await step(INSTANCE_OK, 'Task_StoreFG', 'Task_Ship');
  await finish(INSTANCE_OK, 'Task_Ship');

  // `End_Main` в проекцию не попадает: конечные события в `tokens` не пишутся.
  // Признак успеха — статус `COMPLETED` (проверен в `finish`) и закрытая отгрузка.
  check(
    'отгрузка ГП закрыта (Task_Ship COMPLETED)',
    tokenState(INSTANCE_OK, 'Task_Ship') === 'COMPLETED',
    tokenState(INSTANCE_OK, 'Task_Ship')
  );
}

/** Сценарий 2: лаборатория забраковала партию → возврат поставщику. */
async function scenarioReject(factoryXml) {
  console.log('\n── Сценарий 2: брак по ГОСТ → End_Reject ──');

  await startFactory(INSTANCE_REJECT, factoryXml, ORDER_REJECT, { stockSufficient: 'false' });
  if (!(await purchaseAndSupply(INSTANCE_REJECT, factoryXml, ORDER_REJECT))) return;

  await acceptAndAnalyze(INSTANCE_REJECT);
  await step(INSTANCE_REJECT, 'Task_AnalyzeMilk', 'Task_ReturnMilk', { labPassed: 'false' });
  await finish(INSTANCE_REJECT, 'Task_ReturnMilk');

  check(
    'возврат оформлен (Task_ReturnMilk COMPLETED)',
    tokenState(INSTANCE_REJECT, 'Task_ReturnMilk') === 'COMPLETED',
    tokenState(INSTANCE_REJECT, 'Task_ReturnMilk')
  );
  check(
    'склад сырья не активировался',
    tokenCount(INSTANCE_REJECT, 'Task_StoreMilk') === '0',
    tokenCount(INSTANCE_REJECT, 'Task_StoreMilk')
  );
}

/** Сценарий 3: авария пастеризации → serviceTask мок-ERP → End_ERP_Aborted. */
async function scenarioCrash(factoryXml) {
  console.log('\n── Сценарий 3: авария ПОУ → ERP → End_ERP_Aborted ──');

  await startFactory(INSTANCE_CRASH, factoryXml, ORDER_CRASH, { stockSufficient: 'false' });
  if (!(await purchaseAndSupply(INSTANCE_CRASH, factoryXml, ORDER_CRASH))) return;

  await acceptAndAnalyze(INSTANCE_CRASH);
  await step(INSTANCE_CRASH, 'Task_AnalyzeMilk', 'Task_StoreMilk', { labPassed: 'true' });
  await productionUntilPack(INSTANCE_CRASH, { pasteurizeOk: 'false' });

  // `Task_ERP_CriticalAlert` — serviceTask: его закрывает мок-воркер, человека нет.
  const crashStatus = await waitFor(
    `${INSTANCE_CRASH}: экземпляр закрыт воркером ERP`,
    () => instanceStatus(INSTANCE_CRASH),
    'COMPLETED'
  );

  check(
    'авария ушла в ERP (Task_ERP_CriticalAlert)',
    tokenCount(INSTANCE_CRASH, 'Task_ERP_CriticalAlert') !== '0',
    'Task_ERP_CriticalAlert'
  );
  check(
    'производство остановлено (статус COMPLETED после End_ERP_Aborted)',
    crashStatus === 'COMPLETED',
    crashStatus
  );
  check(
    'пастеризация не продолжена',
    tokenCount(INSTANCE_CRASH, 'Tech_Ferment') === '0',
    'Tech_Ferment'
  );
}

/** Сценарий 4: деградация линии → регистрация в ERP → End_ERP_DeviationLogged. */
async function scenarioDegraded(factoryXml) {
  console.log('\n── Сценарий 4: деградация линии → ERP → End_ERP_DeviationLogged ──');

  await startFactory(INSTANCE_DEGRADED, factoryXml, ORDER_DEGRADED, { stockSufficient: 'false' });
  if (!(await purchaseAndSupply(INSTANCE_DEGRADED, factoryXml, ORDER_DEGRADED))) return;

  await acceptAndAnalyze(INSTANCE_DEGRADED);
  await step(INSTANCE_DEGRADED, 'Task_AnalyzeMilk', 'Task_StoreMilk', { labPassed: 'true' });
  await productionUntilPack(INSTANCE_DEGRADED, {});
  await step(INSTANCE_DEGRADED, 'Tech_Pack', 'Tech_LogDegradation', { packOutcome: 'degraded' });
  await finish(INSTANCE_DEGRADED, 'Tech_LogDegradation');

  // `Task_ERP_HandleDeviation` — serviceTask: след появляется, когда мок-ERP
  // закрывает отклонение; конечное событие в `tokens` не пишется.
  await waitForTrace(INSTANCE_DEGRADED, 'Task_ERP_HandleDeviation');

  check(
    'деградация зафиксирована (Tech_LogDegradation COMPLETED)',
    tokenState(INSTANCE_DEGRADED, 'Tech_LogDegradation') === 'COMPLETED',
    tokenState(INSTANCE_DEGRADED, 'Tech_LogDegradation')
  );
  check(
    'ГП на склад не поступила',
    tokenCount(INSTANCE_DEGRADED, 'Task_StoreFG') === '0',
    tokenCount(INSTANCE_DEGRADED, 'Task_StoreFG')
  );
}

/**
 * Уборка следов прогона.
 *
 * Экземпляры поставщика создаёт движок (`inst-supp-*`), поэтому удаляются все
 * такие экземпляры, появившиеся после старта прогона: чужие демо-данные за тот
 * же период не затрагиваются. Определения (`process_definitions`) остаются —
 * демо-схема должна быть задеплоена и после теста.
 */
function cleanup() {
  psql(
    `
    DELETE FROM execution_history
     WHERE instance_id LIKE 'e2e-dairy-%'
        OR instance_id IN (
             SELECT id FROM process_instances
              WHERE id LIKE 'inst-supp-%' AND created_at >= ${sqlString(STARTED_AT)}
           );
    DELETE FROM tokens
     WHERE instance_id LIKE 'e2e-dairy-%'
        OR instance_id IN (
             SELECT id FROM process_instances
              WHERE id LIKE 'inst-supp-%' AND created_at >= ${sqlString(STARTED_AT)}
           );
    DELETE FROM process_instances
     WHERE id LIKE 'e2e-dairy-%'
        OR (id LIKE 'inst-supp-%' AND created_at >= ${sqlString(STARTED_AT)});
    `,
    'очистка'
  );
}

/** Проверки контура: шлюз жив, схемы демо читаются, сценарии проходят. */
async function runChecks() {
  const health = await call('GET', '/health');
  check('шлюз отвечает на /health', health.status === 200, health.body);

  const factoryXml = readSchema('DAIRY_PLANT_PROCESS_XML');
  const supplierXml = readSchema('DAIRY_SUPPLIER_PROCESS_XML');

  check('схема завода прочитана из демо-клиента', factoryXml.includes('Process_Dairy'), factoryXml.length);
  check(
    'схема поставщика прочитана из демо-клиента',
    supplierXml.includes('Process_Supplier'),
    supplierXml.length
  );
  check(
    'условия развилок раскрыты (`${stockSufficient == \'true\'}`)',
    factoryXml.includes("${stockSufficient == 'true'}"),
    'условие не найдено — проверьте readSchema'
  );

  // Определение завода нужно и для старта (в payload), и для того, чтобы
  // поставщик нашёл уже задеплоенную схему завода при корреляции сообщений.
  await deploySchema(factoryXml, 'завод');

  await scenarioHappy(factoryXml, supplierXml);
  await scenarioReject(factoryXml);
  await scenarioCrash(factoryXml);
  await scenarioDegraded(factoryXml);

  const failed = results.filter((result) => !result.ok);
  console.log(
    `\n${failed.length === 0 ? '✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ' : `❌ ПРОВАЛЕНО: ${failed.length}`} (${results.length} проверок)`
  );
  return failed.length;
}

/** Открывает туннель, гоняет сценарии, гасит туннель и убирает следы. */
async function main() {
  tunnel = await openTunnel();
  let failed = 1;

  try {
    failed = await runChecks();
  } finally {
    try {
      cleanup();
    } catch (error) {
      console.error('⚠️ уборка не выполнена:', error.message);
    }
    closeTunnel(tunnel);
  }

  return failed;
}

main()
  .then((failed) => process.exit(failed === 0 ? 0 : 1))
  .catch((error) => {
    console.error('❌ E2E упал:', error);
    process.exit(1);
  });

