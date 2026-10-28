const { WebSocketServer } = require('ws');
const http = require('http');
const { connect, StringCodec } = require('nats');

const filters = require('./filters');

const NATS_URL = process.env.NATS_URL || 'nats://nats-cluster:4222';
const PORT = process.env.PORT || 8080;
/**
 * Что слушаем. Поток токенов движка: `mes.bpmn.v1.event.<instance>`.
 * Subject вынесен в env, чтобы стенд мог слушать отдельный префикс.
 */
const BPMN_EVENTS_SUBJECT = process.env.BPMN_EVENTS_SUBJECT || 'mes.bpmn.v1.event.>';
const sc = StringCodec();

let nc;

const server = http.createServer((req, res) => {
    // `/health` — единственный HTTP-ответ шлюза: видно, что под жив, что NATS
    // подключён и сколько клиентов ждут событий (WS — на `/ws/status`).
    if (req.url && req.url.split('?')[0] === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(
            JSON.stringify({
                status: 'ok',
                nats_connected: Boolean(nc),
                clients: wss.clients.size,
                nats_subject: BPMN_EVENTS_SUBJECT,
                ws_path: '/ws/status',
                filter: 'instanceId/role: query-параметры или {"type":"subscribe"}'
            })
        );
        return;
    }

    res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ error: 'not found', ws: '/ws/status', health: '/health' }));
});

const wss = new WebSocketServer({
    server,
    path: '/ws/status' // Путь соответствует настройкам Ingress
});

/**
 * Раскладывает событие по клиентам с учётом их фильтров (M7).
 *
 * JSON разбирается один раз на событие, а не на клиента: событие одно, а
 * подписчиков много. Клиент без фильтров получает всё — так работал шлюз до
 * M7, и старые вкладки не ломаются.
 */
function broadcastMessage(data, subject) {
    let message = null;
    try {
        message = JSON.parse(data);
    } catch (error) {
        // Не JSON — отдаём как есть: формат события не должен теряться из-за
        // невозможности его разобрать здесь.
    }

    wss.clients.forEach((client) => {
        if (client.readyState !== 1 /* WebSocket.OPEN */) return;
        if (!filters.matches(client.filters, message, subject)) return;

        client.send(data);
    });
}

async function initWS() {
    try {
        nc = await connect({ servers: NATS_URL });
        console.log(`Подключено к NATS по адресу ${NATS_URL}`);

        // Поток токенов и завершений процессов от BPMN-движка.
        const sub = nc.subscribe(BPMN_EVENTS_SUBJECT);

        console.log(`Подписка на ${BPMN_EVENTS_SUBJECT} активна`);

        for await (const m of sub) {
            const messageData = sc.decode(m.data);
            console.log(`Получено событие состояния: ${m.subject}`);
            // Трансляция изменения статуса клиентским панелям — по фильтрам
            // подписчика (M7): чужие экземпляры до него не доходят.
            broadcastMessage(messageData, m.subject);
        }
    } catch (err) {
        console.error('Ошибка инициализации NATS подписки:', err);
        process.exit(1);
    }
}

wss.on('connection', (ws, req) => {
    // Фильтры можно объявить сразу в адресе: `?instanceId=...&role=...`.
    // Пустые фильтры — «всё», как было до M7.
    ws.filters = filters.parseQuery(req && req.url);

    console.log(
        `Новый WS клиент подключен (экземпляры: ${
            ws.filters.instanceIds.join(', ') || 'все'
        }, роль: ${ws.filters.role || 'любая'})`
    );

    ws.on('message', (raw) => {
        const applied = filters.applyControl(ws.filters, raw.toString());
        if (!applied.reply) return;

        ws.filters = applied.filters;
        ws.send(applied.reply);
        console.log(
            `WS клиент подписан: экземпляры ${ws.filters.instanceIds.join(', ') || 'все'}, роль ${
                ws.filters.role || 'любая'
            }`
        );
    });

    ws.on('close', () => {
        console.log('WS клиент отключился');
    });

    ws.on('error', console.error);
});

initWS();

server.listen(PORT, () => {
    console.log(`WebSocket Cluster запущен на порту ${PORT}`);
});

process.on('SIGINT', async () => {
    if (nc) await nc.close();
    server.close();
    process.exit(0);
});