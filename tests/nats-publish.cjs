/**
 * Публикация события напрямую в NATS/JetStream — минуя HTTP-шлюз (M6).
 *
 * Скрипт живёт **внутри пода шлюза**: там есть зависимость `nats` и
 * `NATS_URL`, а на хосте E2E-прогона их нет. Нужен он ровно для одного:
 * проверить страховку движка. Шлюз отсекает чужую роль до публикации, поэтому
 * `USER_TASK_DENIED` в `execution_history` появляется только на событии,
 * которое пришло в NATS в обход шлюза (как это делает `nats pub`).
 *
 * Использование (тело события — в stdin, subject — аргументом):
 *   node tests-nats-publish.cjs mes.in.v1.event.erp.CompleteTask < payload.json
 *
 * Печатает `PUBLISHED stream=<имя> seq=<номер>`: JetStream подтвердил запись,
 * значит событие точно увидит рантайм.
 */
const { connect, StringCodec } = require('nats');

const subject = process.argv[2];

if (!subject) {
  console.error('Использование: node tests-nats-publish.cjs <subject> < payload.json');
  process.exit(2);
}

const sc = StringCodec();
let input = '';

process.stdin.setEncoding('utf8');
process.stdin.on('data', (chunk) => {
  input += chunk;
});
process.stdin.on('end', async () => {
  const payload = input.trim() || '{}';

  const nc = await connect({ servers: process.env.NATS_URL || 'nats://nats-service:4222' });

  try {
    const ack = await nc.jetstream().publish(subject, sc.encode(payload));
    console.log(`PUBLISHED stream=${ack.stream} seq=${ack.seq}`);
  } finally {
    await nc.close();
  }
});
