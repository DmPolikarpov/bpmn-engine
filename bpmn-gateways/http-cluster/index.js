const express = require('express');
const { connect, StringCodec } = require('nats');
const Ajv = require('ajv');

const app = express();
/**
 * Лимит тела запроса. BPMN-схема приезжает в `output_data.bpmn_xml`,
 * поэтому дефолтных 100 КБ из body-parser недостаточно: реальные
 * диаграммы bpmn-js (вместе с BPMN DI) легко их превышают. Раньше
 * gateway отвечал HTML-страницей 413 «request entity too large»,
 * которую клиент не может разобрать и показывает только код статуса.
 */
const JSON_BODY_LIMIT = process.env.JSON_BODY_LIMIT || '10mb';

/**
 * CORS. Клиент (`erp-simulator`) может обращаться к gateway напрямую
 * (`REACT_APP_GATEWAY_URL=http://localhost:8080/...`), а браузер перед
 * POST с `Content-Type: application/json` отправляет preflight OPTIONS.
 * Без этих заголовков запрос вообще не доходит до шлюза, и в UI видно
 * только «Ошибка сети» — хотя проблема именно в preflight.
 *
 * `CORS_ORIGIN` — либо `*`, либо список origin'ов через запятую.
 */
const CORS_ORIGIN = process.env.CORS_ORIGIN || '*';

function applyCors(req, res) {
    const origin = req.headers.origin;
    const allowOrigin = CORS_ORIGIN === '*' ? origin || '*' : CORS_ORIGIN;

    res.setHeader('Access-Control-Allow-Origin', allowOrigin);
    res.setHeader('Vary', 'Origin');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    res.setHeader('Access-Control-Max-Age', '600');
}

app.use((req, res, next) => {
    applyCors(req, res);

    if (req.method === 'OPTIONS') {
        res.status(204).end();
        return;
    }

    next();
});

// Тело парсим только после CORS: иначе ответ 413/400 от body-parser
// уходит без CORS-заголовков, и браузер при прямом обращении к шлюзу
// показывает «Ошибка сети» вместо причины отказа.
app.use(express.json({ limit: JSON_BODY_LIMIT }));

const NATS_URL = process.env.NATS_URL || 'nats://nats-service:4222';
const PORT = process.env.PORT || 8080;
const sc = StringCodec();

/**
 * API заказов (M4).
 *
 * Заказы живут в БД движка, поэтому шлюз не ходит в Postgres сам: он
 * перекладывает REST в NATS-запрос `request/reply` и отдаёт ответ как есть.
 * Subject и таймаут настраиваются, чтобы тот же образ работал в кластере и
 * на локальном стенде.
 */
const ORDERS_SUBJECT = process.env.ORDERS_API_SUBJECT || 'mes.in.v1.api.orders';
const ORDERS_TIMEOUT_MS = Number(process.env.ORDERS_TIMEOUT_MS || 5000);

let nc;
let js;

// JSON Schema for event validation
const eventSchema = {
    type: "object",
    properties: {
        event_name: { type: "string", minLength: 1 },
        correlation_key: { type: "string", minLength: 1 },
        element_id: { type: "string" },
        output_data: {
            type: "object",
            properties: {
                bpmn_xml: { type: "string" }
            }
        }
    },
    required: ["event_name", "correlation_key"],
    additionalProperties: true
};

const ajv = new Ajv({ allErrors: true });
const validateEvent = ajv.compile(eventSchema);

async function initNats() {
    try {
        nc = await connect({ servers: NATS_URL });
        js = nc.jetstream();
        console.log(`Connected to NATS at ${NATS_URL}`);
    } catch (err) {
        console.error('Error connecting to NATS:', err);
        process.exit(1);
    }
}

app.post('/api/v1/process/instances/:id/events', async (req, res) => {
    const instanceId = req.params.id;
    console.log(`📩 [Gateway] Incoming HTTP request for instance_id: ${instanceId}`);

    const isValid = validateEvent(req.body);

    if (!isValid) {
        console.warn(`⚠️ [Gateway] Schema validation failed for ${instanceId}`);
        return res.status(400).json({
            error: "Validation Failed: Request body does not match expected JSON schema",
            details: validateEvent.errors.map(err => ({
                field: err.instancePath || 'root',
                message: err.message,
                params: err.params
            })),
            expected_schema: eventSchema
        });
    }

    const { event_name, correlation_key, element_id, output_data } = req.body;

    // JetStream инициализируется асинхронно (initNats), а соединение может
    // отвалиться в рантайме. Без явной проверки `js.publish` падал с
    // невнятным 500 — отдаём 503 с подсказкой, что именно недоступно.
    if (!js) {
        console.error('❌ [Gateway] JetStream недоступен — событие не опубликовано');
        return res.status(503).json({
            error: 'NATS JetStream is not connected',
            details: 'Шлюз не подключён к NATS: проверьте под nats-deployment и стрим INBOUND_EVENTS.'
        });
    }

    const natsPayload = JSON.stringify({
        event_id: correlation_key,
        instance_id: instanceId,
        element_id: element_id || '',
        payload: output_data || {}
    });

    const subject = `mes.in.v1.event.erp.${event_name}`;

    try {
        await js.publish(subject, sc.encode(natsPayload));
        console.log(`✅ [Gateway] Event published to NATS on subject: ${subject}`);
        res.status(202).json({ 
            status: 'event_accepted', 
            instance_id: instanceId,
            subject 
        });
    } catch (err) {
        console.error('❌ [Gateway] Failed to publish event to NATS:', err);
        res.status(500).json({
            error: 'Failed to publish event to NATS',
            details: err && err.message ? err.message : String(err)
        });
    }
});

/**
 * Запрос к API заказов движка через NATS `request/reply`.
 *
 * Движок отвечает на `mes.in.v1.api.orders` (очередь `orders-api` — отвечает
 * одна реплика): тело запроса и ответа описано в
 * `bpmn-engine/src/orders.rs`.
 */
async function requestOrders(payload) {
    if (!nc) {
        const error = new Error('NATS не подключён: заказы недоступны');
        error.status = 503;
        throw error;
    }

    const message = await nc.request(
        ORDERS_SUBJECT,
        sc.encode(JSON.stringify(payload)),
        { timeout: ORDERS_TIMEOUT_MS }
    );

    return JSON.parse(sc.decode(message.data));
}

/**
 * Переносит ответ движка в HTTP: `status` из тела — это код (400/404/409/500),
 * остальные поля уходят клиенту как есть (`order`, `orders`, `details`).
 */
function sendOrdersReply(res, body) {
    const status =
        typeof body.status === 'number' ? body.status : body.ok === false ? 500 : 200;
    const { status: _status, ...rest } = body;

    res.status(status).json(rest);
}

/** Общая обёртка маршрутов заказов: таймаут и обрыв NATS — это 503. */
async function proxyOrders(res, payload, label) {
    try {
        sendOrdersReply(res, await requestOrders(payload));
    } catch (error) {
        const timedOut =
            error && (error.code === 'TIMEOUT' || /timeout/i.test(String(error.message)));
        const status = error && error.status ? error.status : 503;

        console.error(`❌ [Gateway] ${label}: ${error && error.message ? error.message : error}`);
        res.status(status).json({
            error: timedOut
                ? 'Оркестратор не ответил на запрос заказов'
                : 'Не удалось получить заказы из оркестратора',
            details: error && error.message ? String(error.message) : String(error)
        });
    }
}

/** `GET /api/v1/orders` — список заказов (последние 200, новые сверху). */
app.get('/api/v1/orders', async (req, res) => {
    await proxyOrders(res, { action: 'list' }, 'список заказов');
});

/** `GET /api/v1/orders/:orderNum` — карточка заказа. */
app.get('/api/v1/orders/:orderNum', async (req, res) => {
    await proxyOrders(res, { action: 'get', orderNum: req.params.orderNum }, 'карточка заказа');
});

/** `POST /api/v1/orders` — создание заказа: тело — поля `NewOrder`. */
app.post('/api/v1/orders', async (req, res) => {
    await proxyOrders(res, { action: 'create', order: req.body }, 'создание заказа');
});

/**
 * `POST /api/v1/orders/:orderNum/instances` — привязка экземпляра процесса к
 * стороне заказа: `{ org: 'CUSTOMER' | 'SUPPLIER', instanceId }`.
 */
app.post('/api/v1/orders/:orderNum/instances', async (req, res) => {
    const body = req.body || {};

    await proxyOrders(
        res,
        {
            action: 'attach',
            orderNum: req.params.orderNum,
            org: body.org,
            instanceId: body.instanceId
        },
        'привязка экземпляра к заказу'
    );
});

/**
 * Диагностика: проверка port-forward и базовое здоровье шлюза.
 * `curl -s localhost:8080/health`
 */
app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        nats_connected: Boolean(nc),
        jetstream: Boolean(js),
        body_limit: JSON_BODY_LIMIT,
        orders_subject: ORDERS_SUBJECT
    });
});

/**
 * Корень шлюза. `translate_ports.sh` печатает ссылку
 * `http://localhost:8080`, и её открывают в браузере, а Express на
 * неизвестный путь отвечает HTML-страницей «Cannot GET /». Здесь отдаём
 * JSON-карту маршрутов: сразу видно, что проброс жив и что 8080 — это
 * REST API, а не UI (клиент `erp-simulator` поднимается отдельно на 3000).
 */
app.get('/', (req, res) => {
    res.json({
        service: 'http-cluster-gateway',
        description: 'REST API шлюза BPMN-оркестратора. UI (erp-simulator) запускается отдельно: npm start -> http://localhost:3000',
        routes: [
            {
                method: 'POST',
                path: '/api/v1/process/instances/:id/events',
                description: 'Публикация события процесса в NATS (mes.in.v1.event.erp.<event_name>)'
            },
            {
                method: 'GET',
                path: '/api/v1/orders',
                description: 'Список заказов из БД движка (NATS request/reply mes.in.v1.api.orders)'
            },
            {
                method: 'GET',
                path: '/api/v1/orders/:orderNum',
                description: 'Карточка заказа: предметные поля и текущие шаги обоих контуров'
            },
            {
                method: 'POST',
                path: '/api/v1/orders',
                description: 'Создание заказа: тело — NewOrder (номенклатура, объём, жирность, упаковка, срок, контрагент)'
            },
            {
                method: 'POST',
                path: '/api/v1/orders/:orderNum/instances',
                description: 'Привязка экземпляра процесса к стороне заказа: { org, instanceId }'
            },
            {
                method: 'GET',
                path: '/health',
                description: 'Проверка проброса и состояния подключения к NATS/JetStream'
            }
        ]
    });
});

/**
 * Ошибки разбора тела запроса (битый JSON, превышен лимит) и прочие
 * необработанные исключения — всегда JSON: SDK читает причину отказа из
 * `error`/`details`, а HTML-страница body-parser'а даёт лишь код статуса.
 */
app.use((err, req, res, next) => {
    // Error-handler выполняется вне обычной цепочки middleware,
    // поэтому CORS-заголовки выставляем здесь ещё раз — иначе браузер
    // не покажет причину отказа (даже когда она уже в JSON).
    applyCors(req, res);

    const status = err.status || err.statusCode || 500;
    let error = 'Request failed';

    if (err.type === 'entity.too.large') {
        error = `Request body is too large (limit: ${JSON_BODY_LIMIT})`;
    } else if (err.type === 'entity.parse.failed') {
        error = 'Invalid JSON body';
    }

    console.error(`❌ [Gateway] Ошибка обработки запроса (${status}): ${err.message}`);
    res.status(status).json({ error, details: err.message });
});

initNats().then(() => {
    app.listen(PORT, () => {
        console.log(`HTTP Cluster Gateway listening on port ${PORT}`);
    });
});

process.on('SIGINT', async () => {
    if (nc) await nc.close();
    process.exit(0);
});