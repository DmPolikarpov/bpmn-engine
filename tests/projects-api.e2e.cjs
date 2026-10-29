/**
 * Живой E2E контура M6: HTTP-шлюз → NATS request/reply → API проектов движка → Postgres.
 *
 * Проверяется тот же контракт, который использует кабинет
 * (`erp-simulator/src/projectsApi.js`): список, схема, деплой с версией,
 * история версий, коды 200/400/404 и формат ошибок полей.
 *
 * Туннель до шлюза скрипт открывает сам (`kubectl port-forward`) и гасит его в
 * конце: запущенный из оболочки `port-forward` живёт до первого таймаута
 * команды, и прогон падал на закрытом порту вместо проверок.
 *
 * Запуск (кластер поднят, образы загружены — см. `deploy/`):
 *   node tests/projects-api.e2e.cjs
 *
 * Переменные окружения: `GATEWAY_URL` — шлюз уже доступен, туннель не нужен;
 * `GATEWAY_PORT`/`GATEWAY_SERVICE` — параметры туннеля; `DB_POD`/`DB_USER`/
 * `DB_NAME`/`DB_PASSWORD` — очистка через `kubectl exec ... psql`.
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
const suffix = Date.now().toString().slice(-6);
const projectId = `project-e2e-${suffix}`;

/** Минимальная BPMN-схема: сервер проверяет только «это действительно BPMN». */
const bpmn = (tag) =>
  `<?xml version="1.0" encoding="UTF-8"?><bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"><bpmn:process id="Process_${tag}"/></bpmn:definitions>`;

const firstXml = bpmn('E2E');
const secondXml = bpmn('E2E_v2');

function check(name, condition, actual) {
  results.push({ name, ok: Boolean(condition), actual });
  console.log(`${condition ? '✅' : '❌'} ${name}${condition ? '' : ` → ${JSON.stringify(actual)}`}`);
}

async function call(method, path, body, headers) {
  const response = await fetch(`${BASE}${path}`, {
    method,
    headers: {
      ...(body ? { 'Content-Type': 'application/json' } : {}),
      ...(headers || {})
    },
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

const deploy = (xml, name = 'E2E завод') =>
  call('PUT', `/api/v1/projects/${projectId}`, { name, bpmnXml: xml }, { 'X-Actor-Role': 'DIRECTOR' });

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
 * Если шлюз уже доступен (свой `port-forward` или прямой адрес в `BASE`),
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

/** Убирает строки прогона: своя схема не должна попадать в демо-список. */
function cleanup(label) {
  sql(
    `
    DELETE FROM project_versions WHERE project_id LIKE 'project-e2e-%';
    DELETE FROM projects WHERE project_id LIKE 'project-e2e-%';
    `,
    label
  );
}

async function runChecks() {
  // Прошлый прогон мог упасть до очистки: убираем свои следы заранее, иначе
  // первый деплой стал бы v3 и проверки версий поехали.
  cleanup('подготовка');

  const health = await call('GET', '/health');
  check('GET /health → 200', health.status === 200, health.body);
  check('шлюз видит NATS', health.body.nats_connected === true, health.body);
  check(
    'subject проектов в /health',
    health.body.projects_subject === 'mes.in.v1.api.projects',
    health.body
  );

  const empty = await call('GET', `/api/v1/projects/${projectId}`);
  check('неизвестный проект → 404', empty.status === 404, empty);

  const created = await deploy(firstXml);
  check('PUT /api/v1/projects/:id → 200', created.status === 200, created.body);
  check('ответ ok:true', created.body.ok === true, created.body);
  check(
    'первая версия проекта v1',
    created.body.project && created.body.project.version === 1,
    created.body.project
  );
  check(
    'projectId и имя сохранены',
    created.body.project &&
      created.body.project.projectId === projectId &&
      created.body.project.name === 'E2E завод',
    created.body.project
  );
  check(
    'роль деплоя из X-Actor-Role',
    created.body.project && created.body.project.updatedBy === 'DIRECTOR',
    created.body.project
  );
  check(
    'схема вернулась целиком',
    created.body.project && created.body.project.bpmnXml === firstXml,
    created.body.project && created.body.project.bpmnXml
  );

  const updated = await deploy(secondXml, 'E2E завод v2');
  check(
    'повторный деплой поднимает версию',
    updated.status === 200 && updated.body.project && updated.body.project.version === 2,
    updated.body.project
  );
  check(
    'имя проекта обновилось',
    updated.body.project && updated.body.project.name === 'E2E завод v2',
    updated.body.project
  );


  const latest = await call('GET', `/api/v1/projects/${projectId}`);
  check(
    'GET отдаёт последнюю версию',
    latest.status === 200 && latest.body.project && latest.body.project.version === 2,
    latest.body.project
  );

  const old = await call('GET', `/api/v1/projects/${projectId}?version=1`);
  check(
    'GET ?version=1 отдаёт историю',
    old.status === 200 && old.body.project && old.body.project.version === 1,
    old.body.project
  );
  check(
    'история не перезаписана',
    old.body.project && old.body.project.bpmnXml === firstXml,
    old.body.project && old.body.project.bpmnXml
  );

  const missingVersion = await call('GET', `/api/v1/projects/${projectId}?version=7`);
  check('версии нет → 404', missingVersion.status === 404, missingVersion);

  const versions = await call('GET', `/api/v1/projects/${projectId}/versions`);
  const versionNumbers = (versions.body.versions || []).map((item) => item.version);
  check('GET …/versions → 200', versions.status === 200, versions.body);
  check(
    'история: [2, 1]',
    versionNumbers.length === 2 && versionNumbers[0] === 2 && versionNumbers[1] === 1,
    versionNumbers
  );
  check(
    'история знает автора',
    (versions.body.versions || []).every((item) => item.updatedBy === 'DIRECTOR'),
    versions.body.versions
  );

  const list = await call('GET', '/api/v1/projects');
  const summary = (list.body.projects || []).find((item) => item.projectId === projectId);
  check('GET /api/v1/projects → 200', list.status === 200, list.body);
  check('проект есть в списке', Boolean(summary), list.body.projects);
  check(
    'список отдаёт версию и размер схемы',
    summary && summary.version === 2 && summary.xmlBytes > 0,
    summary
  );

  const invalidXml = await deploy('это не BPMN');
  const invalidFields = (invalidXml.body.details || []).map((detail) => detail.field);
  check('схема не BPMN → 400', invalidXml.status === 400, invalidXml.body);
  check('details называют bpmnXml', invalidFields.includes('bpmnXml'), invalidFields);

  const invalidId = await call(
    'PUT',
    `/api/v1/projects/${encodeURIComponent('project e2e/bad')}`,
    { name: 'Плохой id', bpmnXml: firstXml },
    { 'X-Actor-Role': 'DIRECTOR' }
  );
  const idFields = (invalidId.body.details || []).map((detail) => detail.field);
  check('небезопасный projectId → 400', invalidId.status === 400, invalidId.body);
  check('details называют projectId', idFields.includes('projectId'), idFields);

  const emptyName = await deploy(firstXml, '   ');
  check('пустое имя → 400', emptyName.status === 400, emptyName.body);

  cleanup('очистка');

  const after = await call('GET', `/api/v1/projects/${projectId}`);
  check('проект удалён из БД', after.status === 404, after);

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

