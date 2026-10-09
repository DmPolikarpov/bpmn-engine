const { WebSocketServer } = require('ws');
const http = require('http');
const { connect, StringCodec } = require('nats');

const NATS_URL = process.env.NATS_URL || 'nats://nats-cluster:4222'; //[cite: 3]
const PORT = process.env.PORT || 8080; //[cite: 3]
const sc = StringCodec();

const server = http.createServer();
const wss = new WebSocketServer({ 
    server, 
    path: '/ws/status' // Путь соответствует настройкам Ingress[cite: 5]
});

let nc;

// Функция для широковещательной рассылки всем активным клиентам
function broadcastMessage(data) {
    wss.clients.forEach((client) => {
        if (client.readyState === 1 /* WebSocket.OPEN */) {
            client.send(data);
        }
    });
}

async function initWS() {
    try {
        nc = await connect({ servers: NATS_URL });
        console.log(`Подключено к NATS по адресу ${NATS_URL}`);

        // Асинхронная подписка на стрим аудита и состояний от BPMN-движка[cite: 2]
        const sub = nc.subscribe('mes.bpmn.v1.event.>');
        
        console.log('Подписка на mes.bpmn.v1.event.> активна');

        for await (const m of sub) {
            const messageData = sc.decode(m.data);
            console.log(`Получено событие состояния: ${m.subject}`);
            // Трансляция изменения статуса внешним клиентским панелям и MES-системам[cite: 1]
            broadcastMessage(messageData);
        }
    } catch (err) {
        console.error('Ошибка инициализации NATS подписки:', err);
        process.exit(1);
    }
}

wss.on('connection', (ws) => {
    console.log('Новый WS клиент подключен');
    
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