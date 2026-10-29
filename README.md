# MES BPMN Orchestrator

Оркестратор бизнес-процессов для MES/ERP-контура: Rust-движок, NATS JetStream
как транспорт, HTTP- и WebSocket-шлюзы, React-клиент с редактором BPMN-схем.

Состав:

| Компонент | Что это | Где лежит |
| --- | --- | --- |
| Движок | Rust-бинарник: приём событий, токены шагов, SLA-воркер, воркер ответов ERP | [`bpmn-engine/`](bpmn-engine) |
| HTTP-шлюз | Node/Express: единственная точка приёма событий процесса по HTTP | [`bpmn-gateways/http-cluster`](bpmn-gateways/http-cluster) |
| WS-шлюз | Node/`ws`: раздача изменений токенов всем клиентам | [`bpmn-gateways/websocket-cluster`](bpmn-gateways/websocket-cluster) |
| Клиент | React + BPMN Client SDK (редактор/просмотр схем, подсветка токенов) | [`erp-simulator/`](erp-simulator) (git submodule) |
| Инфраструктура | NATS + JetStream, PostgreSQL, манифесты и скрипт деплоя | корень репозитория |

> **Коротко.** Каждый блок схемы «привязывается» к сервису одним ключом —
> `element_id` (атрибут `id` элемента BPMN). Клиент отправляет событие с этим
> `element_id`, движок переводит узел в `ACTIVE` и публикует изменение токена
> всем подписчикам; сервис либо отвечает в `mes.in.v1.erp.response`, либо шаг
> двигает следующий UI-обработчик. Никаких дополнительных extension-атрибутов в
> схеме для этого не нужно. А вот роль исполнителя шага живёт в схеме штатным
> `bpmn:potentialOwner`, а запуск второго процесса из узла — расширением
> `trg:processTrigger`: см. [«Роль исполнителя
> узла»](#роль-исполнителя-узла-bpmnpotentialowner) и [«Кросс-процессный запуск
> из узла»](#кросс-процессный-запуск-из-узла-trgprocesstrigger).
> Как добавить новую связку — см.
> [чек-лист](#как-привязать-новый-блок-к-сервису).

## Содержание

- [Архитектура и поток данных](#архитектура-и-поток-данных)
- [Главное правило: блок привязывается к сервису через `element_id`](#главное-правило-блок-привязывается-к-сервису-через-element_id)
- [Контракты транспорта](#контракты-транспорта)
- [Какие привязки уже настроены](#какие-привязки-уже-настроены)
- [Роль исполнителя узла (`bpmn:potentialOwner`)](#роль-исполнителя-узла-bpmnpotentialowner)
- [Кросс-процессный запуск из узла (`trg:processTrigger`)](#кросс-процессный-запуск-из-узла-trgprocesstrigger)
- [Кнопки участников узла (`act:userAction`)](#кнопки-участников-узла-actuseraction)
- [Локальный кэш схемы (`localStorage`)](#локальный-кэш-схемы-localstorage)
- [Метрики выполненных процессов](#метрики-выполненных-процессов)
- [Как привязать новый блок к сервису](#как-привязать-новый-блок-к-сервису)
- [Правила и ограничения идентификаторов](#правила-и-ограничения-идентификаторов)
- [Диагностика](#диагностика)
- [Развёртывание и разработка](#развёртывание-и-разработка)
- [Известные ограничения](#известные-ограничения)
- [Карта исходников](#карта-исходников)

## Архитектура и поток данных

```
                 POST /api/v1/process/instances/:id/events
  erp-simulator  ───────────────►  http-cluster (8080)
  (React + SDK)                          │
        ▲                                │ NATS: mes.in.v1.event.erp.<event_name>
        │ ws://localhost:8081/ws/status  ▼
        │                        ┌───────────────────────────┐
        │  websocket-cluster ◄───┤   bpmn-engine (Rust)      │
        │   mes.bpmn.v1.event.>  │  • токены шагов (PostgreSQL)
        │                        │  • SLA-воркер / таймауты  │
        │                        └───────┬───────────────────┘
        │                                │ mes.out.v1.cmd.erp  (команда внешней системе)
        │                                ▼
        │                        внешние системы (ERP, MES)
        │                                │
        └────────────────────────────────┘ NATS: mes.in.v1.erp.response (ответ сервиса)
```

Один BPMN-узел = один `element_id`. Движок ничего не знает о «сервисах» как о
сущностях: он лишь двигает **токены** (состояния узлов) и рассылает их
изменения. Привязка «этот блок обслуживается вот этим сервисом» существует
ровно там, где совпадает `element_id` — в схеме, в коде клиента и в коде
внешней системы.

## Главное правило: блок привязывается к сервису через `element_id`

`element_id` — это атрибут `id` элемента BPMN (`<bpmn:userTask id="Task_SecurityCheck">`).
Он один и тот же в четырёх местах, и все четыре обязаны совпадать:

| Где | Что задаёт | Источник |
| --- | --- | --- |
| BPMN XML | `id` узла в схеме | редактор схемы в клиенте или XML вручную |
| Клиент | какой шаг активировать событием | `client.sendEvent(instanceId, eventName, elementId, payload)` — [`client/httpClient.js`](erp-simulator/src/bpmn-sdk/client/httpClient.js) |
| HTTP-шлюз | `element_id` → тело сообщения в NATS | [`http-cluster/index.js`](bpmn-gateways/http-cluster/index.js) |
| Движок | запись токена `ACTIVE`, продвижение и завершение | [`bpmn-engine/src/main.rs`](bpmn-engine/src/main.rs) |

Что делает движок при входящем событии (`main.rs` → `runtime::GraphRuntime`):

1. если в `output_data` есть `bpmn_xml` — деплоит определение схемы
   (`process_definitions.id` = `id` из `<bpmn:process ...>`) и стартует экземпляр
   в вендорных сторах;
2. проводит токен по графу: завершённый узел закрывается, следующие узлы
   активируются по `sequenceFlow`, условия и `default` шлюзов считает движок;
3. на каждое изменение публикует `token_updated` (`ACTIVE`/`COMPLETED`) вместе с
   `payload` в `mes.bpmn.v1.event.<instance_id>`;
4. повторяет шаги в legacy read-model (`tokens`, `process_instances`), чтобы
   кабинет и внешние системы читали привычные таблицы.

`element_id` события — **адрес кнопки**, а не маршрут: движок берёт `Waiting`-токен
этого узла, а следующий узел определяет схема (M1, дизайн 5.2). Набор активных
токенов хранит движок, поэтому несколько `ACTIVE` в `tokens` — норма для
параллельных ветвей; «текущий шаг» на клиенте — **последнее `ACTIVE`-сообщение**,
а пройденные узлы видны как `COMPLETED` (зелёная заливка без пульсации).

Имя события (`event_name`) **не участвует** в привязке: движок его не читает
(оно попадает только в subject `mes.in.v1.event.erp.<event_name>` и в логи),
поэтому переименование события ничего не ломает, а переименование
`element_id` — ломает всё.

## Контракты транспорта

### 1. HTTP-шлюз: приём событий процесса

```
POST {GATEWAY}/api/v1/process/instances/{instanceId}/events
Content-Type: application/json
```

| Поле | Обяз. | Назначение |
| --- | --- | --- |
| `event_name` | да | Имя события; становится последним сегментом subject |
| `correlation_key` | да | Идемпотентность/трассировка (`event_id` в NATS) |
| `element_id` | нет | **Ключ привязки блок↔сервис**: узел, который станет `ACTIVE` |
| `output_data` | нет | Полезная нагрузка; `bpmn_xml` внутри сохраняет определение |
| `actor` (или заголовок `X-Actor-Role`) | нет | Роль сессии для проверки прав (M6); пусто — событие не проверяется |

Ответы: `202 {status: 'event_accepted', instance_id, subject}`, `400` — не прошла
JSON-схема, `403` — роль сессии не вправе двигать шаг (`{error, details}` с
причиной), `413` — тело больше `JSON_BODY_LIMIT` (по умолчанию `10mb`),
`503` — шлюз не подключён к JetStream или не дозвонился до проверки прав,
`500` — публикация в NATS не удалась.

```bash
curl -X POST http://localhost:8080/api/v1/process/instances/inst-cust-abc123/events \
  -H 'Content-Type: application/json' \
  -d '{
        "event_name": "StartProcess",
        "correlation_key": "corr-abc123",
        "element_id": "Task_CreatePurchaseOrder",
        "output_data": { "bpmn_xml": "<bpmn:definitions ...>", "orderNum": "№101" }
      }'
```

`element_id` у старта — **первый шаг** процесса, а не стартовое событие: движок
сам проводит токен от `startEvent` до этого узла (маршрут по графу схемы, M1).

### 2. NATS-предметы (subjects) и потоки JetStream

| Stream | Subject | Кто публикует | Кто читает |
| --- | --- | --- | --- |
| `INBOUND_EVENTS` | `mes.in.v1.event.>` | HTTP-шлюз (`...event.erp.<event_name>`) | движок, consumer `engine-inbound-consumer` |
| `ERP_RESPONSES` | `mes.in.v1.erp.>` | внешние системы (ответы сервисов) | движок, consumer `engine-erp-response-consumer` |
| `OUTBOUND_DISPATCH` | `mes.out.v1.cmd.<target_system>` | движок (`serviceTask` и SLA-команды), `target_system` в нижнем регистре | воркеры внешних систем |
| `BPMN_EVENTS` | `mes.bpmn.v1.event.>` | движок (токены, завершение процесса) | WS-шлюз |
| `WS_NOTIFICATIONS` | `mes.ws.v1.event.>` | (резерв) | — |

Служебные `request/reply`-subject'ы (это не стримы, а прямой обмен с движком):
`mes.in.v1.api.orders` — REST заказов (очередь `orders-api`, M4),
`mes.in.v1.api.auth` — проверка прав на событие (очередь `auth-api`, M6),
`mes.in.v1.api.metrics` — серверные метрики запусков (очередь `metrics-api`, M7).

Стримы создаёт Job [`nats-init-streams-job.yaml`](nats-init-streams-job.yaml);
если стрима нет — публикация падает, а шлюз отвечает `503`.

### 3. Сообщения

**Входящее событие (движок), `InboundEvent`** — [`events.rs`](bpmn-engine/src/events.rs):

```json
{ "event_id": "corr-abc123", "instance_id": "inst-cust-abc123",
  "element_id": "StartEvent_Customer", "payload": { "orderNum": "№101" },
  "actor": "CLIENT" }
```

`actor` необязателен: без него событие (воркер, `nats pub`, E2E-скрипт) проходит
без проверки прав — см. [«Проверка прав на событие»](#проверка-прав-на-событие-m6).

**Изменение токена (WS-клиентам)**, публикует движок в `mes.bpmn.v1.event.<instance_id>`:

```json
{ "event": "token_updated", "instance_id": "inst-cust-abc123",
  "element_id": "Task_SecurityCheck", "state": "ACTIVE",
  "timestamp": "2026-01-01T10:00:00+00:00", "payload": { "orderNum": "№101" } }
```

Состояния: `ACTIVE`, `COMPLETED`, `FAILED`, `TIMED_OUT`
([`TOKEN_STATES`](erp-simulator/src/bpmn-sdk/constants.js)). Клиент подсвечивает
на схеме только сообщения с `event === 'token_updated'`.

Подсветка навешивается **двумя** классами: базовым `bpmn-sdk-token` (он же
применяет `fill`/`stroke` к фигуре) и модификатором состояния
(`bpmn-sdk-token--active`, `--completed`, `--failed`, `--timed-out`).
`canvas.addMarker` в diagram-js принимает один класс за вызов, поэтому пару
собирает `tokenMarkerClasses`
([`tokens.js`](erp-simulator/src/bpmn-sdk/tokens.js)): без базового класса
заливка и обводка не применяются, и токен на схеме не виден.

«Текущий шаг» заказа клиент двигает только по `ACTIVE`: `COMPLETED` прежнего узла
приходит в момент активации следующего, поэтому на него нельзя ни откатывать шаг
назад, ни закрывать карточку заказа ([`App.jsx`](erp-simulator/src/App.jsx)).

Старт процесса — одно событие на действие: `StartProcess` (в его `payload` уходит
`bpmn_xml`), а `element_id` — **первый шаг** схемы (`startProcess` в `App.jsx`,
таблица действий ниже). Движок сам проводит токен от `startEvent` до этого узла,
поэтому первый блок сразу получает токен (`ACTIVE`) и после выполнения уходит в
`COMPLETED`. Второго события нет: лишний `CompleteBlock` закрыл бы шаг, который
движок только что активировал.

**Команда воркеру `serviceTask`, `ServiceCommand`** → `mes.out.v1.cmd.<target_system>`:

Движок публикует её сам, как только активировал узел `bpmn:serviceTask`:

```json
{ "instance_id": "inst-cust-abc123", "element_id": "Task_ERP_CriticalAlert",
  "target_system": "erp",
  "payload": { "orderNum": "№101", "pasteurizeOk": "false" },
  "idempotency_key": "inst-cust-abc123:Task_ERP_CriticalAlert:0190a1b2-…",
  "deadline": "2026-10-22T10:01:00+00:00" }
```

| Поле | Обяз. | Назначение |
| --- | --- | --- |
| `instance_id` | да | Экземпляр процесса |
| `element_id` | да | Узел `serviceTask` — его же нужно вернуть в ответе |
| `target_system` | да | Тема воркера из `camunda:topic` узла; дублирует subject |
| `payload` | да | Переменные экземпляра на момент активации; транспортных полей (`bpmn_xml`, `token_id`) в них нет |
| `idempotency_key` | да | `<instance>:<element>:<token>`; стабилен для одной активации, новый — при повторном входе в узел |
| `deadline` | да | RFC 3339, `now + timeout_secs` узла (вендор: 60 с) |

Тема воркера задаётся в схеме: `<bpmn:serviceTask camunda:topic="erp">`
(пространство имён `http://camunda.org/schema/1.0/bpmn`). Без атрибута команда
уходит в `mes.out.v1.cmd.default`.

**Команда внешней системе (SLA-воркер), `ActionRequiredEvent`** → `mes.out.v1.cmd.<target_system>`:

```json
{ "process_instance_id": "inst-cust-abc123", "current_step": "Task_SecurityCheck",
  "target_system": "ERP",
  "payload": { "status": "CHECK_TIMEOUT", "reason": "SLA_EXPIRED", "action": "CANCEL_OR_RETRY" } }
```

**Ответ сервиса (ERP), `ErpResponseEvent`** → `mes.in.v1.erp.response`:

| Поле | Обяз. | Назначение |
| --- | --- | --- |
| `process_instance_id` | да | Экземпляр процесса |
| `element_id` | нет | Адрес шага: если указан, он обязан совпасть с ожидающим `serviceTask`, иначе ответ игнорируется (`erp_target_mismatch`) — так устаревший ответ не закроет соседний узел. Пусто → узел ищется по `token_id` из `payload`, иначе берётся единственный ожидающий `serviceTask` |
| `status` | да | `SUCCESS` (регистр не важен) — шаг завершается, граф идёт дальше; любой другой — ошибка: с `retryable` уходит в ретрай, иначе (или когда `camunda:retries` исчерпаны) — компенсация |
| `error_message` | нет | Текст ошибки: попадает в лог, `erp_retry.last_error` и `ActionRequiredEvent.payload.error` |
| `retryable` | нет | `true` — ошибка временная, движок переотправит команду с backoff (пока не кончились `camunda:retries`). По умолчанию `false` — сразу компенсация |
| `payload` | да | Данные сервиса; уезжают клиентам вместе с токеном. Поле у `ErpResponseEvent` объявлено без `#[serde(default)]`, поэтому без него ответ не десериализуется и теряется (в логах движка — `❌ [ERP Consumer] Ошибка десериализации`) |
| `idempotency_key` | нет | Ключ из `ServiceCommand`: успех или компенсация с тем же ключом не двигают токен повторно. Без него ответ гасится отсутствием ожидающего токена, как в M1 |

Неуспешный ответ не двигает граф. Порядок решения:

1. `retryable = true` и `attempts < camunda:retries` → команда с тем же
   `idempotency_key` уходит воркеру повторно через `base·2^attempts` секунд
   (`ERP_RETRY_BASE_SECS` = 5 с, потолок `ERP_RETRY_MAX_SECS` = 300 с);
2. иначе — компенсация: движок публикует `ActionRequiredEvent` в
   `mes.out.v1.cmd.<system>`, токен остаётся на шаге, решение за человеком.

Повтор неуспешного ответа, пока ретрай ещё не отправлен, считается дублем и не
сжигает вторую попытку. Состояние попыток — таблица `erp_retry` (миграция
`0002_erp_retry.sql`).


```bash
nats pub mes.in.v1.erp.response \
  '{"process_instance_id":"inst-complex-1234","element_id":"ServiceTask_ValidateOrder",
    "status":"SUCCESS","payload":{"validation_status":"APPROVED"}}' \
  --server=nats-service:4222
```

### 4. Клиентская сторона (BPMN Client SDK)

```js
const { client } = useBpmnEngine();                       // HTTP-клиент шлюза

// 1) деплой схемы: inst-deploy-<org> + DeploySchema + Process_Deployment
await client.deploySchema('CUSTOMER', bpmnXml);

// 2) привязка «блок → действие сервиса»: какой узел активируем и чем.
//    В демо параметры вызова (событие и узел) берутся из кнопки схемы —
//    `act:userAction`, см. «Кнопки участников узла».
await client.sendEvent(instanceId, 'SecurityPass', 'Task_QualityControl', {
  entryTime: '10:15:00', orderNum: '№101',
});
```

`sendEvent(instanceId, eventName, elementId, outputData)` сам формирует URL, тело
и `correlation_key` (UUID v4). Смысл третьего аргумента в этом проекте —
«следующий узел, который должен стать `ACTIVE`».

## Какие привязки уже настроены

### A. Демо-приложение клиента (роль → блок → сервис)

Кнопки кабинета объявлены в самих блоках схемы (`act:userAction`): надпись,
иконка и роль участника — это данные, а не код, а `event_name` и `element_id`
(`event`/`elementId`) схема выводит сама, если в кнопке их не записали
(см. [«Кнопки участников узла»](#кнопки-участников-узла-actuseraction)). В
[`erp-simulator/src/App.jsx`](erp-simulator/src/App.jsx) остаются обработчики —
«сервисы»: по нажатию кнопки они собирают **вычисляемый** payload (номера
УПД/ТТН, метки времени, `instanceId` новой стороны) и вызывают
`client.sendEvent(...)` параметрами из схемы: `resolveAction` доводит пару
«событие + узел» до значений, которые понимает движок. Колонка `element_id` —
узел, который после события станет `ACTIVE` (и подсветится на схеме).

Колонка «Роль (вход)» — роль, под которой пользователь входит в демо; **какие шаги
этой роли принадлежат, решает схема**: у задачи — `bpmn:potentialOwner`, у кнопки
на событии — её собственное поле `role` (у событий нет `resourceRole`, поэтому
роль инициатора старта записана в кнопке `createOrder` на `StartEvent_Customer`).
Кабинет строится по ролям, поэтому и надпись кнопки, и её получателя можно
поменять в редакторе схемы
(см. [«Роль исполнителя узла»](#роль-исполнителя-узла-bpmnpotentialowner) и
[«Кнопки участников узла»](#кнопки-участников-узла-actuseraction)).

| Роль (вход) | Кнопка в UI (обработчик) | `event_name` | `element_id` | Экземпляр |
| --- | --- | --- | --- | --- |
| Руководитель (EXECUTIVE) | Деплой схемы (`deploySchema('CUSTOMER', xml)`) | `DeploySchema` | `Process_Deployment` | `inst-deploy-customer` |
| Руководитель Поставщика | Деплой схемы (`deploySchema('SUPPLIER', xml)`) | `DeploySchema` | `Process_Deployment` | `inst-deploy-supplier` |
| Мастер производства (`handleProductionCreateOrder`) | «Принять заказ клиента»: форма заказа (номенклатура, объём, жирность, упаковка, срок, контрагент) | `StartProcess` | `Task_CreatePurchaseOrder` | `inst-cust-*` |
| Менеджер закупок (`handleCustomerSendOrderToSupplier`) | Передать заказ поставщику | `CompleteBlock` | `Task_WaitForSupplierShipment` | `inst-cust-*` |
| Менеджер закупок (там же, шаг 2 — `startProcess`) | Запуск процесса поставщика | `StartProcess` | `Task_SupplierCreateDocs` | `inst-supp-*` |
| Менеджер поставщика (`handleSupplierCreateDocs`) | Сформировать УПД/ТТН | `CompleteBlock` | `Task_SupplierShipment` | `inst-supp-*` |
| Менеджер поставщика (`handleSupplierShipment`) | Отгрузить цистерну | `CompleteBlock` | `EndEvent_Supplier` | `inst-supp-*` |
| Менеджер поставщика (там же, шаг 2) | Сигнал заказчику | `SupplierShippedSignal` | `Task_ReceiveDocsAndIssuePass` | `inst-cust-*` |
| Менеджер закупок (`handleCustomerIssuePass`) | Выписать пропуск | `CompleteBlock` | `Task_SecurityCheck` | `inst-cust-*` |
| Охрана КПП (`handleSecurityCheckAndPass`) | Пропустить транспорт | `CompleteBlock` | `Task_QualityControl` | `inst-cust-*` |
| Специалист ОТК (`handleQualityApprove`) | Принять качество | `CompleteBlock` | `EndEvent_Customer` | `inst-cust-*` |

`CompleteBlock` в колонке `event_name` — умолчание: `event_name` это метка
операции (она уходит в subject NATS `mes.in.v1.event.erp.*` и в журнал), а токен
двигает `element_id`, поэтому в кнопках демо-схем событие и узел не записаны, и
их выводит схема (`DEFAULT_ACTION_EVENT` и цель единственной исходящей связи,
см. [«Кнопки участников узла»](#кнопки-участников-узла-actuseraction)). Явные
имена остались там, где их пишут данные: `DeploySchema`, `StartProcess` и
`SupplierShippedSignal` (сигнал второй стороне — поле `counterpartEvent`).

Строки с `startProcess` — про запуск процесса: **старт это одно событие.** Клиент
называет им не стартовое событие, а **первый шаг** процесса: движок сам проводит
токен от `startEvent` до этого узла (M1). Второй `CompleteBlock` (так было до M1)
закрыл бы шаг, который движок только что активировал, — подсветка уехала бы на
следующий узел. Узел первого шага схема
выводит из кнопки старта: `act:userAction/@elementId`, а если поле пусто — по
единственной исходящей связи стартового события (`resolveAction`).

У старта есть следствие для списка заказов: токен `StartProcess` движок публикует
**раньше**, чем обработчик кнопки дождётся ответа HTTP. Заказ к этому моменту уже
заводит ветка «заказ по токену» в подписке на токены, поэтому
`handleProductionCreateOrder` не добавляет свой `pendingOrder` вслепую, а сливает
его с найденным по `custInstId` (`{ ...pendingOrder, ...order }` — состояние из WS
свежее, о состоянии движка ответ HTTP ничего не знает). Иначе в списке оказывались
два заказа с одним `№`: React ругался на неуникальный `key={o.id}` в списке и
таблицах заказов, а «текущий шаг» мог уехать на чужой заказ. Регрессия закрыта
тестом «токен старта, обогнавший ответ HTTP, не удваивает заказ».

#### Заказы переживают F5 (M4)

Заказ — запись в БД движка (`orders`), а не `useState` кабинета:
`orderNum` — бизнес-ключ и ключ корреляции сообщений M3. Клиент ходит в REST
шлюза через [`ordersApi.js`](erp-simulator/src/ordersApi.js):

| Метод клиента | HTTP | Что делает |
| --- | --- | --- |
| `ordersClient.listOrders()` | `GET /api/v1/orders` | список последних заказов при входе в кабинет |
| `ordersClient.getOrder(orderNum)` | `GET /api/v1/orders/:orderNum` | карточка: предметные поля и текущие шаги всех трёх контуров |
| `ordersClient.createOrder(payload)` | `POST /api/v1/orders` | заказ **до** `startProcess`: движку нужно, к чему привязать экземпляр |
| `ordersClient.attachInstance(orderNum, org, instanceId)` | `POST /api/v1/orders/:orderNum/instances` | какой экземпляр относится к стороне заказа |

Порядок в `handleProductionCreateOrder`: `nextOrderNum(orders)` → `createOrder` →
`startProcess` → `attachInstance('CUSTOMER')`. Экземпляр поставщика создаёт движок
(M3), поэтому к заказу его привязывает подписка на токены — по первому
`ACTIVE`-кадру `inst-supp-*`, один раз (`attachedInstances`). Без `attachInstance`
после F5 карточка осталась бы без текущего шага: шаг сервер считает по `tokens`,
а не по `useState` браузера.

Список перечитывается при **входе** в кабинет (эффект на `currentUser`), а не на
монтировании: после «Выхода» локальный список очищается, и без повторного чтения
он остался бы пустым, хотя заказы на сервере есть. Прочитанное сливается с
локальным (`mergeOrders`): серверные поля главнее, локальные
«документы/пропуск/время» не теряются, а заказы, о которых сервер ещё не знает,
остаются в списке. API может быть недоступен — тогда
`listOrders`/`createOrder` возвращают `{ok:false}`, и кабинет работает как до M4:
заказы живут в этой сессии, а `409` на создание считается нормой (заказ уже есть).

Тот же приём используется и в интеграционном тесте:
[`App.test.js`](erp-simulator/src/App.test.js) проверяет **один** вызов кнопки
производства — `sendEvent(inst-cust-*, 'StartProcess', 'Task_CreatePurchaseOrder',
{ orderNum, volume, nomenclature, fatContent, packaging, dueDate, counterparty,
initiator, bpmn_xml })` — и то, что второго события на первый шаг нет: узел первого
шага (умолчание `CompleteBlock` не отправляется вовсе) схема выводит по исходящей
связи стартового события.

Живой прогон всего контура (шлюз → NATS `request/reply` → движок → Postgres) —
[`tests/orders-api.e2e.cjs`](tests/orders-api.e2e.cjs): коды `200/400/404/409`,
camelCase, `IDLE`/`ACTIVE` контуров и текущий шаг из `tokens`. Скрипт сам
открывает `kubectl port-forward`, заводит экземпляр и токен через
`kubectl exec … psql` и убирает за собой:

```bash
node tests/orders-api.e2e.cjs   # 29 проверок; GATEWAY_URL — если шлюз уже доступен
```

### B. Сервисные задачи в движке (ответ ERP)

Привязки «блок → следующий блок» в коде больше нет: успешный ответ завершает
внешнюю задачу узла, чей токен ждёт ответа, и дальше маршрут ведёт
`GraphRuntime` по `sequenceFlow` задеплоенной схемы (M1, шаг 3):

| Что пришло | Что делает движок |
| --- | --- |
| `element_id` узла `serviceTask`, `status: SUCCESS` | внешняя задача завершается, токен идёт по исходящим связям; условия шлюзов вычисляются по переменным из `payload` |
| `element_id` пуст | узел ищется по `token_id` из `payload`, иначе берётся единственный ожидающий `serviceTask`; если ждут несколько — событие отбрасывается (`no_waiting_token` в логе) |
| узел уже не ждёт ответа (другой шаг/инстанс) | событие логируется и игнорируется, очередь не блокируется |
| `status != SUCCESS`, `retryable: true` | `erp_retry_scheduled`: команда с тем же ключом уходит повторно после backoff (`camunda:retries`), шаг остаётся `ACTIVE` |
| `status != SUCCESS` без `retryable` (или ретраи исчерпаны) | `erp_compensate`: `ActionRequiredEvent` в `mes.out.v1.cmd.<system>`, токен ждёт человека |
| после закрытия узла нет исходящих связей | токен уходит в `EndEvent` → экземпляр `COMPLETED` |

Полный E2E-сценарий (схема с `ServiceTask_ValidateOrder` →
`ServiceTask_ReserveStock`, публикация ответов ERP и проверка токенов в БД) —
[`tests/test_workflow.py`](tests/test_workflow.py).

## Роль исполнителя узла (`bpmn:potentialOwner`)

Роль, которая ведёт шаг, хранится **в самой схеме** — в стандартном элементе
BPMN 2.0 `bpmn:potentialOwner` (свойство `Activity#resources`):

```xml
<bpmn:userTask id="Task_QualityControl" name="Контроль качества ОТК">
  <bpmn:incoming>Flow_C5</bpmn:incoming>
  <bpmn:outgoing>Flow_C6</bpmn:outgoing>
  <bpmn:potentialOwner name="QUALITY" />
</bpmn:userTask>
```

Почему так:

- это тип из спецификации BPMN 2.0, уже описанный в `bpmn-moddle` внутри
  `bpmn-js`, поэтому собственный `moddleExtension` не нужен: редактор создаёт
  `Modeler` без расширений, и `camunda:*`-подобные атрибуты молча теряются, а
  `potentialOwner` — нет;
- роль переживает импорт и экспорт схемы: `saveXML` редактора возвращает ту же
  строку `<bpmn:potentialOwner name="..." />` (проверено тестом
  [`bpmnModdleRole.test.js`](erp-simulator/src/vendor/bpmnModdleRole.test.js) на
  том самом `bpmn-moddle`, который упакован в бандл);
- «каноническая» запись по спецификации (`resourceAssignmentExpression` →
  `formalExpression`) читается, но при экспорте нормализуется моддлом в
  `bpmn:expression`, поэтому SDK пишет короткую форму с атрибутом `name`.

Роль — **и маршрутизация в интерфейсе, и авторизация на сервере**: с M6 движок
сверяет роль сессии с `bpmn:potentialOwner` узла, а шлюз отвечает `403`, если
роль чужая (см. [«Проверка прав на событие»](#проверка-прав-на-событие-m6)).
WS-шлюз при этом по-прежнему вещает токены всем клиентам — фильтрация
подписок на экземпляры остаётся открытой частью M6
([«Известные ограничения»](#известные-ограничения)).

### Проверка прав на событие (M6)

Раньше шаг можно было провести прямым `sendEvent` мимо роли: движок исполнял
маршрут по графу и не смотрел, кто нажал кнопку. Теперь роль проверяется дважды
— в шлюзе (синхронно, чтобы кабинет увидел причину) и в движке (страховка от
`nats pub` мимо шлюза).

| Слой | Что делает | Где |
| --- | --- | --- |
| клиент | шлёт роль сессии заголовком `X-Actor-Role` и полем `actor` | [`httpClient.js`](erp-simulator/src/bpmn-sdk/client/httpClient.js) |
| HTTP-шлюз | до публикации спрашивает движок (`nc.request`), при отказе — `403` с текстом | [`http-cluster/index.js`](bpmn-gateways/http-cluster/index.js) |
| движок | отвечает на `mes.in.v1.api.auth` и **повторно** проверяет роль в `handle_inbound` | [`src/auth.rs`](bpmn-engine/src/auth.rs), [`src/runtime/mod.rs`](bpmn-engine/src/runtime/mod.rs) |
| движок | разбирает роли схемы: `bpmn:potentialOwner/@name`, `act:userAction/@role` | [`src/runtime/roles.rs`](bpmn-engine/src/runtime/roles.rs) |
| движок | пишет аудит шага: `execution_history.event_type` / `actor` | [`src/db.rs`](bpmn-engine/src/db.rs), миграция `0005` |

Откуда берётся узел:

- **старт** — из `bpmn_xml` самого события (роль кнопки старта лежит в
  `act:userAction`, потому что у `bpmn:startEvent` нет `resourceRole`);
- **завершение шага** — из ожидающего токена экземпляра: `potentialOwner`
  задачи, а если его нет — роль её кнопки.

Ответ шлюза при отказе:

```http
POST /api/v1/process/instances/inst-cust-102/events
X-Actor-Role: WAREHOUSE

403 Forbidden
{
  "error": "Роль WAREHOUSE не может завершить шаг Task_ReceiveOrder: нужна роль SALES.",
  "details": { "actor": "WAREHOUSE", "required": ["SALES"], "elementId": "Task_ReceiveOrder" }
}
```

Кабинет показывает эту причину вместо общего «попробуйте ещё раз»:
`httpClient` кладёт текст `error` из тела ответа в `details` результата
`sendEvent`, а [`App.jsx`](erp-simulator/src/App.jsx) берёт его в
`eventFailure`.

Живой прогон контура (кабинет → шлюз → NATS → движок → Postgres) —
[`tests/roles-auth.e2e.cjs`](tests/roles-auth.e2e.cjs). Скрипт сам поднимает схему
с кнопкой `SALES` (`act:userAction/@role` + `bpmn:potentialOwner`), открывает
`kubectl port-forward` и сверяет `tokens`/`execution_history`:

| Сценарий | Ожидание |
| --- | --- |
| старт чужой ролью | `403` с причиной и `details.required`, экземпляр не создан |
| шаг чужой ролью через шлюз | `403`, событие не опубликовано, токен на месте |
| шаг чужой ролью в обход шлюза | токен на месте, `USER_TASK_DENIED` с `actor` |
| шаг своей ролью / без роли | `202`, токен `COMPLETED`, `actor` = `SALES` / `NULL` |

```bash
node tests/roles-auth.e2e.cjs   # 28 проверок; GATEWAY_URL — если шлюз уже доступен
```

Оба живых набора (заказы M4 и роли M6) идут одной командой — она ничего не
собирает и рассчитана на уже развёрнутый кластер:
`tests/run-live-e2e.sh` (или `tests/run-live-e2e.sh roles` / `orders` — по одному).
В Windows, где `bash` оказывается WSL без Node, наборы запускаются напрямую
(`node tests/roles-auth.e2e.cjs`) или через переменную `NODE_BIN` с путём к `node.exe`.

К ним добавлены живые наборы M7 — контракт метрик и фильтрация WS-подписки — и
демо-сценарий M8:

```bash
node tests/metrics-api.e2e.cjs   # 37 проверок: сид legacy-таблиц → GET /api/v1/metrics
node tests/ws-filter.e2e.cjs     # 12 проверок: три WS-клиента, события через NATS из пода
node tests/dairy-plant.e2e.cjs   # 97 проверок: демо-схема завода целиком (см. «Демо-сценарий»)
```

Событие «в обход шлюза» публикует [`tests/nats-publish.cjs`](tests/nats-publish.cjs):
E2E копирует хелпер в под шлюза (только там есть зависимость `nats`) и пишет в
JetStream напрямую. Иначе страховку рантайма не увидеть — шлюз отсекает чужую
роль ещё до публикации, и записи `USER_TASK_DENIED` в аудите не появляется.

### Демо-сценарий завода (M8)

[`tests/dairy-plant.e2e.cjs`](tests/dairy-plant.e2e.cjs) гоняет **демо-схему
кабинета**, а не синтетическую: XML вырезается из
[`erp-simulator/src/bpmn-sdk/defaults/dairyPlant.js`](erp-simulator/src/bpmn-sdk/defaults/dairyPlant.js)
(`DAIRY_PLANT_PROCESS_XML`, `DAIRY_SUPPLIER_PROCESS_XML`), поэтому расходятся не
только движок и клиент — ловится и расхождение схемы с движком.

| Сценарий | Маршрут | Что подтверждает |
| --- | --- | --- |
| `ok` | заказ → развилка ГП (`stockSufficient=false`) → закупка → поставщик → `MilkShipped` → приёмка → лаборатория → техцепочка → отгрузка ГП | корреляция сообщений M3 по `orderNum`, ветка закупки, весь технологический маршрут |
| `reject` | `labPassed=false` → `Task_ReturnMilk` | брак по ГОСТ: возврат партии, склад сырья не активируется |
| `crash` | `pasteurizeOk=false` → `Task_ERP_CriticalAlert` | `serviceTask` мок-ERP закрывает аварию, производство не продолжается |
| `degraded` | `packOutcome=degraded` → `Tech_LogDegradation` → `Task_ERP_HandleDeviation` | деградация линии уходит в ERP, ГП на склад не поступает |

Что видно только на живой проекции (и потому проверяется здесь, а не в Jest):

- в `tokens` попадают только задачи (`bpmn:userTask`, `bpmn:serviceTask`):
  catch-, throw- и end-события проекция не пишет. Поэтому сценарий проверяет
  узлы-задачи и итоговый статус экземпляра, а не «финиш на `End_Main`»;
- экземпляр поставщика создаёт движок (`trg:processTrigger`, префикс
  `inst-supp-`), а корреляция ответа идёт по `orderNum` (`business_key` в БД
  остаётся `NULL`) — тест находит поставщика запросом, а не по своему id;
- `serviceTask` живёт в `ACTIVE` миллисекунды (мок-воркер отвечает сразу),
  поэтому проверяется след узла, а не состояние ожидания;
- уборка удаляет только следы прогона (`e2e-dairy-*` и `inst-supp-*`,
  созданные после старта), определения остаются задеплоенными.

Запуск: `tests/run-live-e2e.sh dairy` или `node tests/dairy-plant.e2e.cjs`
(97 проверок, около полутора минут на живом кластере).

Границы M6:

- **роль заявленная, а не удостоверенная**: IdP в демо нет, `X-Actor-Role` —
  такой же вход, как выбор роли на экране входа. Проверка отвечает на вопрос
  «можно ли этой роли», а не «кто ты»;
- **событие без роли не проверяется**: так шлют NATS-воркеры, `nats pub` и
  E2E-скрипты (`tests/*.e2e.cjs`), и это же поведение было до M6. Кабинет роль
  заполняет всегда, поэтому из интерфейса мимо проверки шаг не провести;
- **узел без роли — общая очередь**: схема без `potentialOwner` и кнопок с
  ролью отыгрывается любой сессией (как и до M6);
- `role:catalog` в проверке не участвует: это титулы для интерфейса, а не права.

Настройки: `AUTH_API_SUBJECT` (по умолчанию `mes.in.v1.api.auth`) и
`AUTH_TIMEOUT_MS` (3000) — у HTTP-шлюза; subject и очередь `auth-api` заданы в
[`src/auth.rs`](bpmn-engine/src/auth.rs). Сбой проверки — `503`, а не «пропустить»:
иначе недоступный движок открывал бы шаг любой роли.

Роли узлов в демо-схемах (`DEFAULT_CUSTOMER_BPMN_XML` / `DEFAULT_SUPPLIER_BPMN_XML`):

| Узел | Роль | Кто ведёт шаг |
| --- | --- | --- |
| `Task_CreatePurchaseOrder` | `MANAGER` | Менеджер закупок Заказчика |
| `Task_WaitForSupplierShipment` | `MANAGER` | Менеджер закупок Заказчика (ожидание отгрузки) |
| `Task_ReceiveDocsAndIssuePass` | `MANAGER` | Менеджер закупок Заказчика |
| `Task_SecurityCheck` | `SECURITY` | Охрана КПП |
| `Task_QualityControl` | `QUALITY` | Специалист ОТК |
| `Task_SupplierCreateDocs` | `SUPPLIER_MANAGER` | Менеджер Поставщика |
| `Task_SupplierShipment` | `SUPPLIER_MANAGER` | Менеджер Поставщика |

Кабинет в [`App.jsx`](erp-simulator/src/App.jsx) показывает блок шага, если роль
узла из схемы равна роли вошедшего пользователя (`canWork(org, [element_id, ...])`),
а не по захардкоженному списку; роли для входа и выпадающего списка инспектора
собираются из проекта — каталог `role:catalog` и назначения
`bpmn:potentialOwner` ([`erp-simulator/src/projectRoles.js`](erp-simulator/src/projectRoles.js)).

### Как назначить роль

1. **В редакторе схемы** — инспектор узлов
   ([`EnginePanel.jsx`](erp-simulator/src/bpmn-sdk/panels/EnginePanel.jsx)):
   выбрать узел → поле «Роль исполнителя» (набор ролей приходит пропом
   `roleOptions`). Поле активно только для задач и подпроцессов: у событий и
   шлюзов `resourceRole` в BPMN 2.0 нет, и моддл потерял бы такой элемент при
   сохранении.
2. **В XML** — той же строкой `bpmn:potentialOwner` (пример выше).
3. **Из кода** (SDK, [`xml/bpmnXml.js`](erp-simulator/src/bpmn-sdk/xml/bpmnXml.js)):

```js
import { getElementRole, getRoleAssignments, setElementRole } from './vendor/bpmn-sdk';

getRoleAssignments(xml);                                // {Task_QualityControl: 'QUALITY', ...}
getElementRole(xml, 'Task_QualityControl');             // 'QUALITY'
setElementRole(xml, 'Task_QualityControl', 'SECURITY'); // новая схема (строка)
setElementRole(xml, 'Task_QualityControl', '');         // снять роль
```

Инварианты, которые проверяются тестами:

- правка идёт текстом (как `renameElementId`), поэтому остальная схема, включая
  `bpmndi:BPMNShape`, не переформатируется;
- роль пишется **без `id`**: иначе `renameElementId` переименовал бы её вместе с
  узлом, и роль «уехала» бы вместе с шагом;
- роль нельзя записать в событие, шлюз или `sequenceFlow` — вместо молчаливой
  потери элемента SDK бросает ошибку;
- роль, для которой нет каталога, не теряется: её название — сам код
  (`potentialOwner name="Кладовщик"` → титул «Кладовщик»), поэтому загруженный
  BPMN играется без правок кода, а инспектор показывает такую роль в списке.

Проверки в коде:
[`bpmnXml.test.js`](erp-simulator/src/bpmn-sdk/xml/bpmnXml.test.js) (текстовые
правки), [`EnginePanel.test.jsx`](erp-simulator/src/bpmn-sdk/panels/EnginePanel.test.jsx)
(поле и откаты), [`bpmnModdleRole.test.js`](erp-simulator/src/vendor/bpmnModdleRole.test.js)
(импорт/экспорт моддлом), [`App.test.js`](erp-simulator/src/App.test.js)
(перераспределение роли в схеме переключает кабинет).

## Кросс-процессный запуск из узла (`trg:processTrigger`)

Активация узла может запускать экземпляр **другого** процесса: шаг заказчика
«передать заказ поставщику» создаёт экземпляр процесса поставщика. В BPMN 2.0
такого понятия нет, поэтому привязка живёт расширением в собственном namespace
`trg` — внутри расширений узла:

```xml
<bpmn:serviceTask id="Task_WaitForSupplierShipment" name="Ожидание отгрузки">
  <bpmn:extensionElements>
    <trg:processTrigger targetProcessId="Process_SupplierSupply_v1"
                        instancePrefix="inst-supp-"
                        startElementId="Task_WaitForSupplierShipment"
                        firstElementId="Task_SupplierCreateDocs"
                        payloadKeys="orderNum,volume" />
  </bpmn:extensionElements>
</bpmn:serviceTask>
```

| Поле | Смысл |
| --- | --- |
| `targetProcessId` | `bpmn:process/@id` целевого процесса, экземпляр которого запускается. **Обязательное**: без него триггера нет |
| `instancePrefix` | префикс идентификатора нового экземпляра (`inst-supp-`): по нему сторона отличает чужие экземпляры в списке и сопоставляет их со стороной обмена (`INSTANCE_PREFIX` в [`constants.js`](erp-simulator/src/bpmn-sdk/constants.js)) |
| `startElementId` | узел **текущего** процесса, активация которого запускает целевой. Обычно совпадает с узлом-владельцем триггера: поле позволяет проверить, что триггер стоит на ожидаемом узле |
| `firstElementId` | узел **целевого** процесса, который первым станет `ACTIVE` (точка входа в чужой маршрут). Пусто — штатный маршрут от `startEvent` целевого процесса |
| `payloadKeys` | ключи payload текущего экземпляра, которые передаются новому (через запятую). Передаётся только перечисленное: контекст целиком между сторонами не «переливается» |

Почему собственный namespace:

- кросс-процессного запуска в BPMN 2.0 нет, а писать это в `camunda:*` — значит
  зависеть от чужого вендорского namespace;
- тип требует регистрации в моделлере
  (`new Modeler({ container, moddleExtensions: TRIGGER_MODELE_EXTENSIONS })`),
  поэтому описание лежит отдельно —
  [`xml/triggerModdle.js`](erp-simulator/src/bpmn-sdk/xml/triggerModdle.js).
  **Без регистрации правка холста молча теряет триггер:** `bpmn-js` читает
  элемент по имени тега (`$type` — `trg:processTrigger`, а не
  `trg:ProcessTrigger`), `$instanceOf('trg:ProcessTrigger')` даёт `false`, а
  `moddle.create` падает с `unknown type` — та же ловушка, что и у `camunda:*`.
  Поведение зафиксировано тестом
  [`bpmnModdleTrigger.test.js`](erp-simulator/src/vendor/bpmnModdleTrigger.test.js):
  без расширения парсер не выдаёт **ни одного** предупреждения, элемент
  остаётся в XML, но прочитать и записать его через модель нельзя;
- запись и снятие идут через `modeling.updateProperties`
  ([`BpmnEditor.jsx`](erp-simulator/src/bpmn-sdk/components/BpmnEditor.jsx)),
  поэтому попадают в `commandStack` (Ctrl+Z) и сохраняют чужие расширения узла
  (`camunda:*`, другие `trg`-элементы).

### Как назначить триггер

1. **В редакторе схемы** — инспектор узлов
   ([`EnginePanel.jsx`](erp-simulator/src/bpmn-sdk/panels/EnginePanel.jsx)):
   выбрать узел → блок «Запуск другого процесса при активации узла» → поля
   `targetProcessId`, `instancePrefix`, `firstElementId`, `startElementId`,
   `payloadKeys`. Поле «Запускаемый процесс» ходит по подсказкам-`datalist` из
   пропа `triggerTargets` — это список `bpmn:process/@id` **схемы второй
   стороны** (у Заказчика — процессы Поставщика), который
   [`App.jsx`](erp-simulator/src/App.jsx) считает через `getProcessIds`:
   переименование процесса в схеме соседа сразу видно в подсказках.
2. **В XML** — той же строкой `trg:processTrigger` (пример выше).
3. **На витрине `<script>`-подключения** — `npm run sdk:demo && npm start` →
   [http://localhost:3000/sdk-demo.html](http://localhost:3000/sdk-demo.html):
   инспектор на странице пишет роль и триггер прямо в схему демо
   (`setElementRole`/`setElementTrigger`), а подсказки «Запускаемый процесс»
   берёт из демо-схемы поставщика функцией `getProcessIds`.
4. **Из кода** (SDK, [`xml/bpmnXml.js`](erp-simulator/src/bpmn-sdk/xml/bpmnXml.js)):

```js
import { getElementTrigger, getTriggerAssignments, listTriggers, setElementTrigger } from './vendor/bpmn-sdk';

getTriggerAssignments(xml);   // {Task_WaitForSupplierShipment: {targetProcessId: 'Process_SupplierSupply_v1', ...}}
getElementTrigger(xml, 'Task_WaitForSupplierShipment'); // триггер узла либо null
listTriggers(xml);            // [{elementId, trigger}, ...] — в порядке схемы

setElementTrigger(xml, 'Task_WaitForSupplierShipment', {
  targetProcessId: 'Process_SupplierSupply_v1',
  instancePrefix: 'inst-supp-',
  startElementId: 'Task_WaitForSupplierShipment',
  firstElementId: 'Task_SupplierCreateDocs',
  payloadKeys: 'orderNum, volume',
});
setElementTrigger(xml, 'Task_WaitForSupplierShipment', null); // снять триггер
```

Инварианты, которые проверяются тестами:

- **пустой `targetProcessId` = «триггера нет»**: `setElementTrigger(xml, id,
  { targetProcessId: '' })` удаляет элемент, а не оставляет пустой атрибут;
- **хостом может быть только узел потока или активность** (`isTriggerHostType`):
  на `sequenceFlow` SDK бросает ошибку — движок активирует узлы токеном, а
  последовательный поток активирован не бывает (панель в этом случае показывает
  подсказку и блокирует поля);
- **значения валидируются до записи**: `instancePrefix` — по
  `INSTANCE_PREFIX_PATTERN`, `startElementId` и `firstElementId` — как обычный
  `element_id`, `payloadKeys` — по `TRIGGER_PAYLOAD_KEY_PATTERN`
  (`/^[A-Za-z_][A-Za-z0-9_.-]*$/`). Недопустимое значение в схему не уходит:
  поле подсвечивается, а введённый текст остаётся в черновике;
- **`payloadKeys` канонизируется**: `' volume , orderNum ,, volume '` →
  `'volume,orderNum'` (пробелы и пустые элементы списка убираются, повторы
  схлопываются), поэтому повторное применение того же триггера не переписывает
  схему;
- **правка идёт текстом**, как `renameElementId` и `setElementRole`: остальная
  схема, включая `bpmndi:BPMNShape`, не переформатируется, а namespace `trg`
  добавляется в `bpmn:definitions`, если его там не было;
- **неизвестные поля отбрасываются**: `normalizeTrigger` собирает триггер ровно
  из `TRIGGER_FIELDS`, а не-объект вместо триггера — ошибка.

Проверки в коде:
[`bpmnXml.test.js`](erp-simulator/src/bpmn-sdk/xml/bpmnXml.test.js) (запись,
дописывание в существующие `extensionElements` с сохранением `camunda:*`,
снятие, идемпотентность канонизации, отказы),
[`EnginePanel.test.jsx`](erp-simulator/src/bpmn-sdk/panels/EnginePanel.test.jsx)
(поля формы, подсказки, откат при отказе владельца схемы),
[`bpmnModdleTrigger.test.js`](erp-simulator/src/vendor/bpmnModdleTrigger.test.js)
(чтение тем самым `bpmn-moddle`, который упакован в бандл),
[`App.test.js`](erp-simulator/src/App.test.js) (подсказки берутся из схемы
второй стороны).

> **Триггер читает движок (M3).** Активация узла, на котором объявлен
> `trg:processTrigger`, создаёт экземпляр целевого процесса: id —
> `<instancePrefix><orderNum>`, переменные — только из `payloadKeys`. Демо-проект
> «Молочный завод» больше не запускает поставщика из кабинета: сообщение
> `PurchaseOrder` уходит throw-событием и несёт триггер. Механика сообщений и
> корреляция по бизнес-ключу — в разделе
> [«Межпроцессные сообщения»](#межпроцессные-сообщения-m3).

## Межпроцессные сообщения (M3)

Процессы общаются сообщениями BPMN 2.0, а не вызовами кабинета:

```xml
<bpmn:definitions ... xmlns:trg="http://erp.mes/schema/bpmn/trigger">
  <bpmn:message id="Message_PurchaseOrder" name="PurchaseOrder" />

  <bpmn:intermediateThrowEvent id="Event_SendPO">
    <bpmn:extensionElements>
      <trg:processTrigger targetProcessId="Process_Supplier"
                          instancePrefix="inst-supp-"
                          firstElementId="Task_ConfirmPO"
                          payloadKeys="orderNum" />
    </bpmn:extensionElements>
    <bpmn:messageEventDefinition messageRef="Message_PurchaseOrder" />
  </bpmn:intermediateThrowEvent>

  <bpmn:intermediateCatchEvent id="Task_WaitPOConfirmed">
    <bpmn:messageEventDefinition messageRef="Message_PurchaseOrderConfirmed" />
  </bpmn:intermediateCatchEvent>
</bpmn:definitions>
```

Что делает движок:

| Событие схемы | Поведение |
| --- | --- |
| токен вошёл в `intermediateThrowEvent` | отправляется `MessageSent`; перехватчик в [`pump.rs`](bpmn-engine/src/runtime/pump.rs) запоминает узел и имя |
| у узла есть `trg:processTrigger` | создаётся экземпляр `targetProcessId`: id `<instancePrefix><orderNum>`, переменные — только `payloadKeys`; повтор по тому же заказу второй экземпляр не создаёт |
| сообщение ждёт `intermediateCatchEvent` другого экземпляра | доставка **только** экземпляру с тем же `orderNum`; токен сдвигается, переменные отправителя мержатся |
| сообщение никто не ждёт | в лог уходит `message_no_waiter`, состояние не меняется |

Корреляция — по бизнес-ключу `orderNum`
([`runtime/messages.rs`](bpmn-engine/src/runtime/messages.rs),
`CORRELATION_KEY`), а не по `instance_id`: завод и поставщик — разные
экземпляры, но номер заказа у них один. Поэтому триггер обязан передать
`orderNum` в `payloadKeys`; без него сообщение не доставляется и в лог уходит
`message_without_correlation`.

Клиент:

- «Отправить заказ поставщику» закрывает `Task_SendPO` и **не** вызывает
  `startProcess`: схема Поставщика деплоится заранее (upsert), а экземпляр
  создаёт движок по триггеру `Event_SendPO`;
- подтверждение заказа и отгрузка — throw-события поставщика
  (`PurchaseOrderConfirmed`, `MilkShipped`), завод ждёт их на catch-событиях;
- `startsProcess` и `counterpartEvent` в кнопках остались для **старых схем**:
  если они объявлены, кабинет работает по-прежнему.

Третий пул «Молочного завода» (клиент, M5) говорит с заводом теми же тремя шагами:

| Направление | Узел | Сообщение | Что делает движок |
| --- | --- | --- | --- |
| клиент → завод (MF1) | `Event_SendOrder` | `ClientOrder` | триггер создаёт `Process_Dairy` (`inst-cust-<orderNum>`), вход в `Task_ReceiveOrder` |
| завод → клиент (MF2) | `Event_ConfirmOrder` после `Task_ProcessOrder` | `OrderConfirmed` | будит `Event_OrderConfirmed` клиента; на маршрут завода не влияет — токен идёт дальше в `Task_CheckStock` |
| завод → клиент (MF6) | `Event_ShipGoods` после `Task_Ship` | `GoodsShipped` | будит `Event_GoodsShipped`; клиент переходит к приёмке по УПД |

Заказ клиента приходит в завод **до** проверки остатков: подтверждение объёма и
срока не зависит от того, хватает ли готовой продукции, поэтому ветка закупки
сырья остаётся нетронутой. Схема завода должна быть задеплоена до подтверждения —
иначе триггеру некуда создать экземпляр (`message_target_not_deployed` в логе
движка).

Проверки: [`runtime/messages.rs`](bpmn-engine/src/runtime/messages.rs) (разбор
триггера и связей, payload, корреляция),
`vendor_patches.rs::message_delivery_respects_correlation` (чужой заказ не
получает сообщение), [`dairyPlant.test.js`](erp-simulator/src/bpmn-sdk/defaults/dairyPlant.test.js)
(схема), [`projectXml.test.js`](erp-simulator/src/bpmn-sdk/xml/projectXml.test.js)
(`bpmn:message` переживает нарезку процесса),
[`App.test.js`](erp-simulator/src/App.test.js) (ответы завода двигают шаг клиента
до приёмки, кнопка приёмки закрывает заказ на `End_Client`).

Ограничения M3:

- **один шаг**: сообщение, отправленное стартовавшим экземпляром, движок не
  разбирает каскадом (вложенные триггеры — M3.2);
- **`firstElementId`** должен совпадать со штатным первым шагом целевого
  процесса; расхождение логируется `message_entry_mismatch`;
- **catch-событие — не шаг кабинета**: пока процесс ждёт сообщение, подсветка
  стоит на последнем человеческом шаге;
- **payload — только скаляры** (`HashMap<String, String>` вендора): вложенные
  объекты (`documents`) между процессами не передаются.

## Кнопки участников узла (`act:userAction`)

Кнопка кабинета — тоже привязка, и она тоже живёт в схеме: в BPMN 2.0 такого
понятия нет, поэтому кнопка объявляется расширением в namespace `act` (тип
объявляет `ACTION_MODELE_EXTENSIONS` — `moddleExtensions` редактора):

```xml
<bpmn:userTask id="Task_CreatePurchaseOrder" name="Сформировать заказ у поставщика">
  <bpmn:extensionElements>
    <act:userAction id="sendToSupplier" label="Сформировать и отправить заказ Поставщику"
                    icon="Send"
                    startsProcess="Process_SupplierSupply_v1" />
  </bpmn:extensionElements>
  <bpmn:potentialOwner name="MANAGER" />
</bpmn:userTask>
```

| Поле | Смысл |
| --- | --- |
| `id` | ключ кнопки: по нему клиент (`ACTION_ID` в `App.jsx`) сопоставляет её со своим обработчиком. Незнакомый ключ ошибкой не считается: такую кнопку кабинет показывает общим шагом (см. ниже) |
| `label`, `icon` | надпись и имя иконки в кабинете |
| `role` | роль участника; пусто — берётся у узла-хоста (`bpmn:potentialOwner`) |
| `event`, `elementId` | `event_name` и `element_id` события нажатия. **Оба необязательны**: пустое `event` — «шаг выполнен» (`DEFAULT_ACTION_EVENT` = `CompleteBlock`), пустой `elementId` — следующий блок по единственной исходящей связи узла (`getNextElementId`). Записанное в схеме значение всегда важнее умолчания |
| `counterpartEvent`, `counterpartElementId` | необязательный сигнал в экземпляр второй стороны по тому же заказу |
| `startsProcess` | `bpmn:process/@id` процесса второй стороны, который запускает нажатие |
| `variables` | переменные процесса, которые выставляет нажатие: пары `ключ=значение` через `;` (`stockSufficient=true`). Клиент добавляет их в `output_data` события; по ним `conditionExpression` шлюза выбирает ветку (M1, см. [дизайн рантайма графа](docs/runtime-m1-design.md#81-расширение-actuseractionvariables)) |

Почему так: движок имя события не разбирает — оно уходит только в subject NATS
(`mes.in.v1.event.erp.${event_name}`) и в журнал, а токен двигает `element_id`
(см. «B. Сервисные задачи»). Поэтому кнопке достаточно знать, **куда** шаг
переводит токен, и демо-схемы записаны коротко: «нажатие завершает блок и
переводит токен на следующий узел». Обработчик кабинета разрешает пару по схеме
(`resolveAction`), а явные `event`/`elementId` — это способ описать нестандартный
переход (прыжок через блок, возврат, активация события).

Что это меняет:

- **надпись, роль и адрес кнопки правятся в схеме**, а не в коде: переименовали
  `label` — кабинет показывает новую надпись, поменяли `role` — кнопку видит
  другая роль (проверяется тестами
  [`App.test.js`](erp-simulator/src/App.test.js));
- **кросс-процессные эффекты тоже данные**: `startsProcess` заменил жёстко
  прописанный запуск процесса поставщика, `counterpartEvent` — сигнал заказчику
  (`SupplierShippedSignal` на `Task_ReceiveDocsAndIssuePass`);
- **кнопка запуска процесса** — та, что объявлена на стартовом событии
  (`getStartAction`): её `role` — роль инициатора, а `event`/`elementId` (если они
  записаны) — первый шаг; у старта связь одна, поэтому пару обычно выводит схема
  (движок переводит токен двумя событиями, см. «A.» выше);
- **обработчики в `App.jsx` остались**: они собирают payload, которого в схеме нет
  (номера УПД/ТТН, метки времени, идентификатор нового экземпляра), и пишут в
  журнал события процесса. Поимённо кабинет разбирает только эти шаги — их ключи
  перечислены в `ACTION_ID`;
- **новый блок работает без правок кода**: кнопку с незнакомым ключом кабинет
  показывает общим шагом (`schemaStepActions`) — роль берётся у кнопки либо у
  узла, событие и узел — из схемы, а payload состоит из номера заказа и метки
  времени. Поэтому добавленный в редакторе блок с кнопкой видит его роль сразу
  после сохранения/деплоя схемы;
- **шаг, которому некуда идти**, кабинет кнопкой не рисует: если кнопка не назвала
  узел, а у блока нет единственной исходящей связи, показывается подсказка
  «заполните „Узел после нажатия“» — движок токен двигает узлом события, и нажатие
  «в никуда» ничего бы не сдвинуло.

API SDK: `listActions`, `getElementActions`, `getElementAction`,
`getActionAssignments`, `getStartAction`, `setElementActions`, `normalizeAction`,
`isValidAction`, `isActionHostType`, `parseActionVariables`, `ACTION_FIELDS`,
`EMPTY_ACTION`, `ACTION_TYPE`, `ACTION_NAMESPACE`, `ACTION_MODELE_EXTENSIONS`, а
для вывода адреса кнопки из связей — `resolveAction`, `resolveActionEvent`,
`resolveActionTarget`, `getNextElementId`, `getOutgoingTargetIds`,
`DEFAULT_ACTION_EVENT` — подробности и правила записи в
[README SDK](erp-simulator/src/bpmn-sdk/README.md#кнопки-участников-узла).

Как назначить кнопку:

1. **В редакторе схемы** — инспектор узлов, блок «Кнопки участников на узле»:
   выбрать узел → заполнить поля (подсказки — роли пользователей, узлы схемы и
   процессы второй стороны) → «Добавить кнопку». Обязательны ключ и надпись:
   кнопка попадает в схему сразу, а в поле «Узел после нажатия» подсказывается
   блок, который схема выведет по связи. Список пишется целиком, а
   «Удалить кнопку» убирает её из схемы;
2. **В XML** — той же строкой `act:userAction` (пример выше), объявив
   `xmlns:act="http://erp.mes/schema/bpmn/action"` в `bpmn:definitions` (инспектор
   делает это сам вместе с первой кнопкой);
3. **Из кода** — `setElementActions(xml, elementId, actions)` (список заменяется
   целиком, `[]` снимает все) и чтение `getElementActions`/`listActions`.

> Схема без `act:userAction` — схема без кнопок: кабинет покажет только ожидание.
> Демо-схемы SDK кнопки несут; кнопка «Демо-схема» в панели схемы возвращает их,
> если в браузере осталась копия от прежней версии.

## Проект в браузере (`localStorage`)

Правки редактора живут в состоянии React, поэтому без сохранения F5 возвращал
демо-проект SDK. Кнопка «Сохранить схему» пишет проект целиком в `localStorage`
браузера ([`erp-simulator/src/projectStore.js`](erp-simulator/src/projectStore.js)),
а приложение при следующем открытии стартует не с демо-проекта, а с этой копией:

| Что | Функция | Ключ в хранилище |
| --- | --- | --- |
| Создать | `createProject({name, xml})` | `bpmn-projects` (список) |
| Изменить | `updateProject(id, {xml, name})` | тот же |
| Удалить | `deleteProject(id)` | тот же |
| Активный | `ensureDefaultProject()` → запись; `setActiveProjectId(id)` | `bpmn-projects:active` |

Проект — одна схема с процессами обеих сторон (`bpmn:collaboration`), поэтому
правка роли у Заказчика сразу видна и процессу Поставщика: двух независимых
копий (`bpmn-schema:CUSTOMER`/`:SUPPLIER`) больше нет. Старый кэш мигрирует
`migratedProjectXml` — одноразово, при первом открытии приложения.

Детали:

- **кэш локальный и на браузер**, а не серверный: у каждого пользователя своя
  копия, и деплой он не заменяет — схема уходит в кластер только кнопкой
  «Задеплоить BPMN в Оркестратор» (либо в `output_data.bpmn_xml` при старте
  процесса);
- **деплой сохраняет проект тоже**, иначе после F5 кабинет показывал бы схему,
  которой в кластере уже нет;
- **«Демо-схема» меняет только процесс своего контура**: остальные остаются в
  проекте, иначе сброс одной стороны ломал бы связи `startsProcess`/
  `counterpartEvent` другой;
- **подпись в панели схемы** говорит, что происходит: «✅ Сохранено в 12:34:56»
  или «⚠ Есть несохранённые изменения» (XML разошёлся с сохранённой копией);
- **хранилище может быть недоступно** (приватный режим, переполненная квота):
  `createProject`/`updateProject` вернут `null`, приложение продолжит работать на
  проекте в памяти (`MEMORY_PROJECT_ID`), а в ленте появится «Схема не сохранена»;
- **битая запись приложение не роняет**: нечитаемый JSON, запись без `xml` или
  чужой формат считаются отсутствием проекта — подхватится демо-проект;
- **валидность XML проверяет редактор**, а не кэш: хранилище принимает любую
  непустую строку и намеренно не дублирует проверку схемы.

Проверки: [`projectStore.test.js`](erp-simulator/src/projectStore.test.js)
(CRUD, активный проект, миграция, отказы хранилища) и
[`App.test.js`](erp-simulator/src/App.test.js) (подхват при старте процесса,
кнопки сохранения/сброса, «перезагрузка» повторным монтированием).

## Метрики выполненных процессов

Кабинет руководителя показывает инфографику по процессам
([`MetricsPanel.jsx`](erp-simulator/src/MetricsPanel.jsx)): сколько запусков
завершено, среднее/минимальное/максимальное время выполнения и столбики
последних запусков. Основной источник — **серверные метрики**
(`GET /api/v1/metrics`, см. [«Серверные метрики»](#серверные-метрики-m7)): их
считает движок по Postgres, поэтому панель не пуста у второго клиента и не
меняется после F5. Метрика браузера
([`erp-simulator/src/metricsStore.js`](erp-simulator/src/metricsStore.js))
осталась запасным источником: пока API не ответил (или шлюз недоступен), панель
показывает её, а подпись в шапке говорит, откуда цифры.

Отдельного события «процесс завершён» движок не публикует: токен просто встаёт
на `bpmn:endEvent` и остаётся `ACTIVE`. Поэтому финиш определяет клиент по схеме
(у конечного узла нет исходящих связей — `getOutgoingTargetIds`), а метрику этого
браузера хранит в `localStorage`
([`erp-simulator/src/metricsStore.js`](erp-simulator/src/metricsStore.js),
ключ `bpmn-process-metrics`):

| Что | Функция | Когда вызывается |
| --- | --- | --- |
| Старт | `beginRun({id, org, processId, processName, orderNumber})` | `startProcess` в [`App.jsx`](erp-simulator/src/App.jsx) — экземпляр создан |
| Финиш | `completeRun(instanceId, {finishedAt, lastElementId, orderNumber})` | токен `ACTIVE` встал на конечный узел схемы контура |
| Чтение | `loadRuns()` | при монтировании кабинета и после каждой записи |
| Забыть | `clearRuns()` | кнопка «Очистить» в панели инфографики (режим метрик браузера) |

Детали:

- **метрика браузера — запасной источник и мгновенный отклик**: её видит тот
  клиент, который запускал процессы; основной источник — серверный API (M7), а
  эта копия нужна, когда шлюз недоступен, и чтобы столбик появлялся сразу после
  финиша, не дожидаясь ответа NATS; журнал `execution_history` в кластере она не
  заменяет;
- **финиш определяет схема контура**, а не движок: `org` берётся из префикса
  экземпляра (`inst-cust-*` / `inst-supp-*`), конечный узел — из текущей схемы
  этой стороны. Правка схемы (например, переименование `endEvent`) сразу меняет
  условие финиша;
- **экземпляр, который клиент не начинал** (страница открыта посреди процесса),
  метрики не получает: время выполнения оказалось бы выдуманным — `completeRun`
  для незнакомого экземпляра возвращает `null`;
- **повторный финиш идемпотентен**: уже закрытая запись не переписывается
  (движок может отдать кадр дважды);
- **записей не больше `MAX_RUNS` (60)**: метрика — не журнал, ей достаточно
  последних запусков, а квота `localStorage` общая со схемой;
- **имя процесса и номер заказа берутся из схемы и payload**, а не из кода:
  `processName` — это `bpmn:process/@name`;
- **агрегаты считаются по записям** (`summarizeRuns`, `groupRunsByProcess`): по
  карточке на процесс, контур пользователя первым — процессы второй стороны
  остаются рядом, потому что обмен межорганизационный;
- **недоступное хранилище** (приватный режим, квота) метрику не ломает: функции
  возвращают `null`/`false`/`[]`, а заказ обрабатывается дальше.

Проверки: [`metricsStore.test.js`](erp-simulator/src/metricsStore.test.js)
(старт/финиш, идемпотентность, кривые часы, отказы хранилища, агрегаты),
[`metricsApi.test.js`](erp-simulator/src/metricsApi.test.js) (строка запроса,
разбор ответа движка в записи панели, кэш ответа, отказы шлюза),
[`MetricsPanel.test.jsx`](erp-simulator/src/MetricsPanel.test.jsx) (карточки
процессов, плитки времени и серверные разрезы, столбики, подпись источника) и
[`App.test.js`](erp-simulator/src/App.test.js) (метрика переживает смену
пользователя, промежуточный узел финишем не считается, панель берёт цифры
сервера и кэш после F5).

### Серверные метрики (M7)

`GET /api/v1/metrics` — те же цифры, но посчитанные движком по Postgres, а не
браузером: у руководителя панель больше не пуста, а F5 и второй браузер видят
одно и то же.

```bash
curl -s 'http://localhost:8080/api/v1/metrics?limit=5&slaMinutes=30'
curl -s 'http://localhost:8080/api/v1/metrics?processId=zakaz&defectNodes=End_Defect'
```

Шлюз перекладывает запрос в NATS (`mes.in.v1.api.metrics`), отвечает движок
([`metrics.rs`](bpmn-engine/src/metrics.rs)). Параметры необязательны:

| Параметр | По умолчанию | Смысл |
| --- | --- | --- |
| `processId` | все процессы | разрез по `def_key` определения (завод / поставщик / клиент) |
| `limit` | `20` (максимум `50`) | сколько последних запусков вернуть |
| `slaMinutes` | `SLA_TIMEOUT_MINUTES` (`30`) | порог ожидания шага; больше `1440` не бывает |
| `defectNodes` | env `METRICS_DEFECT_END_EVENTS` → эвристика по имени `endEvent` | какие конечные узлы считать браком |

Ответ (сокращён):

```json
{ "ok": true, "generatedAt": "2026-10-26T11:00:00.000Z",
  "slaMinutes": 30, "precisionMs": 1000, "defectEndEvents": ["End_Defect"],
  "totals": { "instances": 12, "runs": 12, "steps": 34, "openSteps": 2 },
  "processes": [{ "processId": "zakaz", "processName": "Заказ молока",
                  "runs": 12, "completed": 9, "active": 3, "terminated": 0,
                  "averageCycleMs": 540000, "medianCycleMs": 480000,
                  "averageStepMs": 61000, "slaRuns": 1, "slaSteps": 2,
                  "defectRuns": 1, "defectShare": 0.111,
                  "outcomes": [{ "nodeId": "End_Ok", "runs": 8 }],
                  "lastRuns": [{ "instanceId": "inst-cust-1", "status": "COMPLETED",
                                 "finishedBy": "graph", "slaBreached": false }] }],
  "steps": [{ "elementId": "Task_Accept", "role": "operator", "samples": 9, "averageMs": 61000 }],
  "roles": [{ "role": "operator", "steps": 9, "averageMs": 61000 }],
  "warnings": [] }
```

Откуда берутся цифры:

- **запуск** — legacy-проекция `process_instances`; **шаг** — активация
  `tokens.created_at` и завершение `execution_history.created_at` (миллисекунды),
  а если пары нет — `TokenArrived`/`TokenCompleted` вендорной истории (секунды,
  отсюда `precisionMs: 1000`);
- **финиш** — `ProcessCompleted`/`ProcessTerminated`, иначе последний узел
  сверяется с `endEvent` **определения** (`bpmn_xml`), а не с «узлом без
  исходящих связей схемы»; видно источник в `finishedBy` (`event` / `graph` /
  `legacy`);
- **роль шага** — `execution_history.actor` (M6), откуда разрезы `steps` и
  `roles`;
- **брак** — запуски, закончившиеся на узле брака (`defectEndEvents` показывает,
  какие узлы приняты за брак: цифра не «магическая»);
- **SLA** — шаг (включая ещё открытый) ждал дольше `slaMinutes`;
  `breachedSteps` перечисляет, **что** именно ждало, — видно без консоли.

Контракт проверяется без Postgres: `aggregate` — чистая функция над снимком
таблиц, 11 юнит-тестов в [`metrics.rs`](bpmn-engine/src/metrics.rs)
(`cargo test --bin bpmn-engine metrics::`). Живой прогон всего пути (шлюз → NATS →
движок → Postgres) — [`tests/metrics-api.e2e.cjs`](tests/metrics-api.e2e.cjs),
37 проверок на развёрнутом кластере.

**Панель руководителя читает этот API** (M7):
[`erp-simulator/src/metricsApi.js`](erp-simulator/src/metricsApi.js) —
`metricsClient.load({limit, slaMinutes})` → `toPanelRuns` (ответ движка
перекладывается в записи запусков панели) → `MetricsPanel`. Числа поэтому
одинаковы у двух браузеров и после F5, а не только по `curl`. Ответ кэшируется в
`localStorage` (`bpmn-process-metrics-cache`): после перезагрузки панель
рисуется сразу, подпись в шапке честно говорит, откуда цифры — `с сервера`,
`кэш сервера` или `этот браузер` (локальный запасной источник). Кнопка
«Обновить» перечитывает API; кнопки «Очистить» в режиме сервера нет — цифры
принадлежат Postgres, а не браузеру. Плитки «Ожидание шага», `SLA`
(`slaRuns / slaSteps`) и «Брак» — разрезы движка, поэтому превышение SLA видно
без консоли. Данные браузера держатся отдельно (`bpmn-process-metrics`) и
подмешиваются только когда API не ответил ни разу.

### Фильтрация WS-подписки (M7)

WS-шлюз вещает `mes.bpmn.v1.event.>` и умеет отсекать чужие экземпляры **до**
отправки. Клиент объявляет фильтры либо в адресе, либо сообщением:

```js
// в адресе: ws://localhost:8081/ws/status?instanceId=inst-cust-1&role=OPERATOR
const socket = new StatusSocket('ws://localhost:8081/ws/status');
socket.connect();
socket.subscribe({ instanceIds: ['inst-cust-1'], role: 'OPERATOR' });
// → {"type":"subscribe","instanceIds":["inst-cust-1"],"role":"OPERATOR"}
// ← {"type":"subscribed","instanceIds":["inst-cust-1"],"role":"OPERATOR"}
socket.unsubscribe(); // снова все экземпляры
```

Правила:

- **ничего не объявлено — приходит всё** (поведение до M7: старые вкладки и
  демо не ломаются);
- `instanceIds` — событие проходит, если id экземпляра (поле сообщения,
  `payload.instance_id` или суффикс subject) есть в списке;
- `role` — отсекаются только события с **объявленной** чужой ролью
  (`role`/`roles`/`actor` в сообщении или `payload`); `token_updated` роли не
  несёт, поэтому фильтр по роли — задел под события с ролью, а не текущий
  ограничитель;
- фильтры восстанавливаются после переконнекта (`StatusSocket.subscribe`
  запоминает их и повторяет при `open`);
- кабинет по умолчанию подписан на все экземпляры: он **обнаруживает** новые
  экземпляры по событиям (движок создаёт их сам, например завод по
  `processTrigger`), и сужение списка сломало бы это.

Проверки: `npm test` в
[`bpmn-gateways/websocket-cluster`](bpmn-gateways/websocket-cluster/filters.test.js)
(фильтры — 8 тестов на `node --test`),
[`wsClient.test.js`](erp-simulator/src/bpmn-sdk/client/wsClient.test.js)
(объявление фильтров, повтор после реконнекта) и живой
[`tests/ws-filter.e2e.cjs`](tests/ws-filter.e2e.cjs) — 12 проверок на кластере:
три WS-клиента (query-фильтр, `subscribe` сообщением, без фильтров), события
публикуются в NATS из пода шлюза.

`filters.js` — отдельный модуль, поэтому он копируется в образ явно
(`COPY index.js filters.js ./` в Dockerfile шлюза). Пропущенный `COPY` виден
сразу: под падает в `CrashLoopBackOff` с `Cannot find module './filters'`.

## Как привязать новый блок к сервису

Ниже — полный чек-лист. Порядок важен: сначала узел в схеме, потом его `id`
в коде, который реагирует на этот шаг.

### Шаг 1. Задать узлу схему и `id`

Откройте редактор схемы и через инспектор узлов («Настройки узла»,
[`EnginePanel.jsx`](erp-simulator/src/bpmn-sdk/panels/EnginePanel.jsx)) задайте
`id`, `name` и роль исполнителя шага. Этот же `id` можно править прямо в XML:

```xml
<userTask id="Task_FlashPasteurization" name="Пастеризация" />
```

Ограничения — [ниже](#правила-и-ограничения-идентификаторов). Правило
простое: **`id` — это адрес блока, по нему сервис его находит.**

### Шаг 2. Сохранить схему: локально или в кластер

В панели схемы две кнопки, которые легко перепутать, поэтому по порядку:

| Кнопка | Что делает | Куда попадает схема |
| --- | --- | --- |
| «Сохранить схему» | `saveSchema(org, xml)` — копия в `localStorage` | в кэш браузера: правки переживают F5, но кластер о них не знает |
| «Задеплоить BPMN в Оркестратор» | `client.deploySchema(orgType, xml)`: событие `DeploySchema` для служебного экземпляра `inst-deploy-<org>` | `process_definitions` в БД, то есть в кластер |

Деплой обновляет и браузерную копию — определение в кластере и схема, которую
показывает кабинет, не расходятся после перезагрузки. Обратное неверно:
локальное сохранение в кластер не уходит (подробнее — [«Локальный кэш
схемы»](#локальный-кэш-схемы-localstorage)).

#### Совместимость схемы с движком

Схему компилирует движок, и он разбирает не весь BPMN: парсер обходит прямых
детей `<bpmn:process>` и знает только `startEvent`, `endEvent`, `serviceTask`,
`userTask`, `exclusiveGateway`, `parallelGateway` и `sequenceFlow`. Узел другого
типа — например, базовый `<bpmn:task>`, который создаёт кнопка «Task» в палитре
редактора, — движок **пропускает молча**; связи, которые вели в него или из него,
становятся «висячими», и компиляция отвечает `SequenceFlowSourceNotFound` /
`SequenceFlowTargetNotFound` (тот же отказ дают узел без входящей или исходящей
связи, `endEvent`, до которого нельзя дойти, и форма шлюза — полный список
проверок в компиляторе).

Узнать об отказе неоткуда: шлюз публикует событие в NATS и отвечает
`202 Accepted` независимо от результата, а движок при ошибке компиляции не
сохраняет ни определение, ни экземпляр процесса — `tokens` потом не проходит по
внешнему ключу к `process_instances`, поэтому WS-кадров о токенах тоже не будет.
Снаружи это выглядит как «ничего не произошло»: схема задеплоена, событие
принято, а процесса нет.

Поэтому проверку повторяет клиент (`validateEngineSchema` из SDK) и делает отказ
видимым: панель схемы показывает причину до деплоя, деплой несовместимой схемы не
отправляется, а «Принять заказ клиента» не заводит заказ — иначе в списке
остался бы «заказ-призрак» без экземпляра процесса. Проверка зеркалит правила
компилятора и лежит в SDK рядом с остальной логикой XML (см.
[README SDK](erp-simulator/src/bpmn-sdk/README.md#совместимость-схемы-с-движком)).

Альтернатива для одноразового запуска — передать схему в `output_data.bpmn_xml`
вместе со стартом процесса (так делает тест):

```js
// `element_id` — первый шаг процесса: движок сам дойдёт от стартового события
// до того же узла (M1, дизайн 10.2 п. 1).
await client.sendEvent('inst-cust-abc123', 'StartProcess', 'Task_CreatePurchaseOrder', {
  bpmn_xml: bpmnXml,
  orderNum: '№101',
});
```

### Шаг 3. Привязать блок в клиенте (кто и чем двигает шаг)

У привязки две части: `event_name` (человекочитаемое имя операции) и
`element_id` (сам блок). Канонический вид вызова:

```js
const res = await client.sendEvent(
  order.customerInstanceId,      // в каком экземпляре процесса
  'FlashPasteurization',         // event_name
  'Task_FlashPasteurization',    // element_id — ключ привязки
  { temperature: 72, orderNum: order.id } // выходные данные шага
);
if (!res.ok) throw new Error(res.error);
```

В демо-приложении такие вызовы разложены по обработчикам ролей
(`handleProductionCreateOrder`, `handleSupplierCreateDocs`, ...), но **сама
привязка** — надпись, роль и, если нужен «нестандартный» переход,
`event_name`/`element_id` — объявлена в схеме (`act:userAction`, см.
[«Кнопки участников узла»](#кнопки-участников-узла-actuseraction)). Обычный шаг
пары не требует: у блока одна исходящая связь, поэтому «завершить блок» и
«следующий узел» схема выводит сама, а обработчик разрешает их `resolveAction`.
Дальше показан вариант с явной парой — так пишут прыжок через блок или активацию
события. Обработчик
находит её по ключу кнопки и добавляет только вычисляемые данные:

```js
// App.jsx: кнопка из схемы + payload, который данными не выразить.
// `actionOf` разрешает пару «событие + узел» по схеме (`resolveAction`), поэтому
// незаполненные поля кнопки читаются как «шаг выполнен» и «следующий блок».
const action = actionOf('CUSTOMER', ACTION_ID.SEND_TO_SUPPLIER); // { elementId, action }
await client.sendEvent(
  order.customerInstanceId,
  action.action.event,       // 'CompleteBlock' (если в кнопке нет своего event)
  action.action.elementId,   // 'Task_WaitForSupplierShipment' (цель связи)
  { orderNum: order.id, supplierInstanceId: instId } // вычисляемые данные
);
```

Если привязку задают из кода, её пишут в схему тем же API, что и инспектор:

```js
import { listActions, setElementActions } from './vendor/bpmn-sdk';

listActions(xml);   // [{elementId, action}, ...] — все кнопки схемы

setElementActions(xml, 'Task_SecurityCheck', [
  {
    id: 'securityPass',
    label: 'Проверить пропуск и открыть ворота',
    icon: 'ShieldCheck',
    // `event`/`elementId` можно не писать: пустые поля означают «шаг выполнен»
    // (`CompleteBlock`) и следующий блок по единственной исходящей связи узла
  },
]);
```

### Шаг 4. Привязать сторону сервиса

**Вариант A — «человеческий» шаг.** Сервис = действие в UI (как в демо): кнопка
роли вызывает `sendEvent` из шага 3, больше ничего не нужно — движок переведёт
указанный узел в `ACTIVE` и разошлёт это по WS.

Исключение — **первый шаг процесса**: у него нет предшественника, поэтому его
называет сам старт. `StartProcess` несёт `bpmn_xml` и `element_id` **первого
блока**: движок сам доходит от `startEvent` до того же узла (M1). Второго события
не нужно: оно закрыло бы шаг, который движок только что активировал (в демо старт
делает `startProcess` в [`App.jsx`](erp-simulator/src/App.jsx)).

**Вариант B — внешняя система (ERP/MES).** Узел `bpmn:serviceTask` не ждёт
человека: движок сам публикует команду в `mes.out.v1.cmd.<camunda:topic>`
(контракт — раздел 3), а система отвечает:

```bash
nats pub mes.in.v1.erp.response \
  '{"process_instance_id":"inst-cust-abc123","element_id":"ServiceTask_FlashPasteurization",
    "status":"SUCCESS","payload":{"batch":"B-42"},
    "idempotency_key":"inst-cust-abc123:ServiceTask_FlashPasteurization:<token>"}' \
  --server=nats-service:4222
```

Ответ закрывает `serviceTask` в движке: тот завершает внешнюю задачу и сам ведёт
токен по графу задеплоенной схемы — правок в коде не нужно, маршрут берётся из
`sequenceFlow`. Ошибка (`status != SUCCESS`) не роняет шаг: с `retryable = true`
команда переотправляется с backoff, иначе (или когда `camunda:retries` узла
исчерпаны) движок публикует `ActionRequiredEvent` и ждёт человека — подробности
в разделе про контракт (§3) и в
[дизайне](docs/runtime-m1-design.md#105-ретраи-backoff-и-компенсация-ответа-m2).

**Вариант D — демо без 1С: мок-воркер.** В репозитории есть NATS-воркер, который
играет роль ERP: читает `mes.out.v1.cmd.erp`, отвечает в
`mes.in.v1.erp.response` тем же `idempotency_key` и помнит уже обработанные
ключи. Он собирается вместе с движком (второй бинарь того же образа) и
запускается отдельным подом:

```bash
docker build -t bpmn-engine-service:latest bpmn-engine/bpmn-engine
kind load docker-image bpmn-engine-service:latest --name erp
kubectl apply -f bpmn-engine/bpmn-engine/erp-mock-worker.yaml

# демо ретраев: первая попытка на каждый ключ падает с retryable = true
kubectl set env deploy/erp-mock-worker ERP_MOCK_FAIL_FIRST=1
```

| Переменная | По умолчанию | Смысл |
| --- | --- | --- |
| `ERP_MOCK_TOPIC` | `erp` | тема воркера (`camunda:topic` демо-схемы) |
| `ERP_MOCK_DELAY_MS` | `500` | имитация работы ERP |
| `ERP_MOCK_FAIL_FIRST` | `0` | сколько первых попыток на ключ провалить (`retryable = true`) |
| `ERP_MOCK_FAIL_ELEMENT` | — | узел, который всегда отвечает `FAILED` (проверка компенсации) |


**Вариант C — адресация конкретной системе.** Работа уходит системе сразу по
активации узла: `ServiceCommand` в `mes.out.v1.cmd.<system>`
(`instance_id`, `element_id`, `payload`, `idempotency_key`, `deadline`), ответ —
тот же `mes.in.v1.erp.response` с `idempotency_key` из команды. Тема задаётся в
схеме: `<bpmn:serviceTask camunda:topic="erp">`. SLA-воркер шлёт по этому же
subject свой `ActionRequiredEvent`.

### Шаг 5. Проверить привязку

```bash
# 1) событие принято
curl -i -X POST http://localhost:8080/api/v1/process/instances/inst-cust-abc123/events \
  -H 'Content-Type: application/json' \
  -d '{"event_name":"FlashPasteurization","correlation_key":"c-1","element_id":"Task_FlashPasteurization","output_data":{}}'

# 2) токен появился
kubectl exec -it deploy/bpmn-db -- psql -U bpmn_user -d bpmn_db \
  -c "SELECT id, element_id, state FROM tokens WHERE instance_id = 'inst-cust-abc123';"

# 3) пришло в WS (подсветка на схеме)
kubectl logs deploy/websocket-cluster-deployment -f
```

Если подсветка расходится с ожиданием (токен `ACTIVE`, а на схеме пусто — или
выполненный шаг не уходит в зелёный), проверьте три условия:

1. **`element_id` есть в схеме.** Он должен совпадать с `id` узла в
   **задеплоенной** схеме. Подсветка чужого `element_id` молча пропускается
   ([`BpmnViewer.jsx`](erp-simulator/src/bpmn-sdk/components/BpmnViewer.jsx):
   «diagram-js бросает TypeError для неизвестного идентификатора»), хотя токен
   в БД уже есть. Именно так было с `Task_WaitForSupplierShipment`: клиент
   (`handleCustomerSendOrderToSupplier`) его активировал, а в демо-схеме
   заказчика такого узла не было — после первого шага заказчика подсветка
   пропадала совсем. Сейчас узел есть, а состав узлов демо-схем зафиксирован
   тестом [`src/vendor/bpmnSdkBundle.test.js`](erp-simulator/src/vendor/bpmnSdkBundle.test.js)
   («демо-схема заказчика содержит узлы, которые активирует клиент»).
2. **Подсветка узла видна.** `BpmnViewer` навешивает базовый класс
   `bpmn-sdk-token` (он же рисует заливку и обводку) вместе с модификатором
   состояния — см. контракт токенов выше.
3. **Узел был `ACTIVE` раньше.** `COMPLETED` получает только тот узел, на котором
   стоял токен: движок закрывает **прежний `ACTIVE`**, а не «блок перед текущим».
   Если шаг живёт только в интерфейсе (клиент показал его «текущим», а события
   движку не отправил), подсветки у него не будет ни в каком состоянии. Так было
   до M1: интерфейс сразу считал текущим `Task_CreatePurchaseOrder`
   (`Task_SupplierCreateDocs` у поставщика), а токен стоял на `StartEvent_Customer`
   (`StartEvent_Supplier`) — после выполнения шага движок закрывал `COMPLETED`
   стартовое событие, а сам блок не подсвечивался вообще. Сейчас `StartProcess`
   называет первый шаг (см. `startProcess` в [`App.jsx`](erp-simulator/src/App.jsx),
   таблица действий выше); перед проверкой состояния посмотрите
   `SELECT element_id, state FROM tokens`, чтобы убедиться, что токен вообще был на
   этом узле.

## Правила и ограничения идентификаторов

`element_id` — это строка, которая едет через HTTP, NATS и PostgreSQL, поэтому
на неё накладываются ограничения всех трёх слоёв:

| Правило | Значение | Где проверяется |
| --- | --- | --- |
| Формат | `/^[A-Za-z_][A-Za-z0-9_.-]*$/` — латиница, цифры, `_`, `.`, `-`; не с цифры | `isValidElementId()` в [`bpmnXml.js`](erp-simulator/src/bpmn-sdk/xml/bpmnXml.js) — используется в `EnginePanel` и `handleChangeId` в `BpmnEditor`; `renameElementId()` тоже бросает ошибку |
| Уникальность | `id` не должен повторяться в пределах схемы | `isIdAvailableAmong()` (`EnginePanel`), `elementRegistry` в `BpmnEditor.handleChangeId`, `renameElementId()` |
| `tokens.element_id` | до 255 символов | `db-init-configmap.yaml` |
| `tokens.id` | `"<instance_id>:<element_id>"` — **≤ 64 символов суммарно** | `save_token_state` в [`db.rs`](bpmn-engine/src/db.rs) |
| `process_instances.id` | до 64 символов | `db-init-configmap.yaml` |
| `process_definitions.id` | до 64 символов, равен `id` из `<bpmn:process>` | `save_process_definition` |

Следствия, которые ломают привязки на практике:

- **Инстанс создаётся только вместе со схемой.** `process_instances` пишется
  лишь в ветке, где в событии есть `bpmn_xml`, и ссылается на
  `process_definitions(id)`. Если шаг отправлен в незадеплоенный процесс,
  вставка падает по внешнему ключу, а движок логирует ошибку и продолжает
  работу (`let _ = ...`) — токен сохранится, а экземпляра процесса в БД не
  будет.
- **Повторная активация того же узла** — это upsert (сначала `UPDATE`, потом
  `INSERT`), поэтому цикл в схеме переиспользует одну строку токена, но
  историю шага не хранит (`created_at` не перезаписывается).
- **Кириллица/пробелы в `id` недопустимы** — редактор их не пропустит, а
  сообщение `event_name` при этом может быть любым (оно ни на что не влияет).

## Диагностика

| Симптом | Причина | Что делать |
| --- | --- | --- |
| `400 Validation Failed` | нет `event_name`/`correlation_key` | дописать поля (см. схему в ответе — шлюз возвращает `expected_schema`) |
| `403` «Роль … не может завершить шаг …» | роль сессии не совпала с `bpmn:potentialOwner` (или ролью кнопки в `act:userAction`) | войти ролью, которой назначен шаг; кто и когда двигал шаг — `SELECT created_at, event_type, element_id, actor FROM execution_history ORDER BY id DESC` |
| `503` «Проверка прав недоступна» | движок не ответил на `mes.in.v1.api.auth` (нет подписки `auth-api` или таймаут) | логи `bpmn-engine` с префиксом `[Auth]`; проверить `NATS_URL` и что движок жив |
| `CrashLoopBackOff` движка, в логе `must be owner of table execution_history` (SQLSTATE 42501) | `execution_history` создал initdb от `postgres`, а миграции идут от `bpmn_user`: `ALTER TABLE` требует владения, `GRANT` даёт только DML | `./deploy.sh` (шаг 5b передаёт владение) или разово: `kubectl exec -i bpmn-db-1 -c postgres -- psql -U postgres -d bpmn_db -c 'ALTER TABLE execution_history OWNER TO bpmn_user;'`, затем `kubectl rollout restart deployment/bpmn-engine-deployment` |
| Шаг двигается любой ролью, `403` не приходит | образ `http-cluster` старше M6: в `/health` нет `auth_subject`, роль не проверяется до публикации | пересобрать шлюз (`./deploy.sh` целиком) или вручную: `docker build -t http-cluster-service:latest ./bpmn-gateways/http-cluster && kind load docker-image http-cluster-service:latest --name erp && kubectl rollout restart deployment/http-cluster-deployment` |

| `413 request entity too large` | `bpmn_xml` больше лимита | поднять `JSON_BODY_LIMIT` у http-cluster |
| `503` «JetStream is not connected» | нет стрима `INBOUND_EVENTS` или NATS недоступен | `kubectl apply -f nats-init-streams-job.yaml`, проверить `nats stream ls` |
| `500` / «Ошибка сети» в UI | нет проброса портов (CRA-прокси не может достучаться до 8080) | `./translate_ports.sh` (перезапускает `kubectl port-forward`) |
| Токен `ACTIVE`, но шаг не подсвечен | `element_id` отсутствует в задеплоенной схеме | сверить `id` узла и `element_id`, передеплоить схему |
| Первый блок схемы не подсвечен / не уходит в зелёный | токен стоит не на нём: `StartProcess` назвал другое `element_id` (у кнопки старта `act:userAction/@elementId`, иначе — цель единственной исходящей связи) | `SELECT element_id, state FROM tokens`; передеплоить схему с кнопкой старта и запустить процесс заново |
| Токен висит `ACTIVE` | шаг никто не завершил | внешний сервис должен ответить в `mes.in.v1.erp.response`; иначе через `SLA_TIMEOUT_MINUTES` (по умолчанию 30) прилетит `TIMED_OUT` |
| Процесс не доходит до `COMPLETED` | у узла нет исходящего `sequenceFlow` (или сработал не тот `default`), либо ответ ERP не пришёл | проверить связи/условия схемы и ответ в `mes.in.v1.erp.response`; см. `runtime`-логи пода |
| WS-подсветки нет вообще | не подключён WS-шлюз (`ws://localhost:8081/ws/status`) или движок не публикует `mes.bpmn.v1.event.>` | проверить логи `websocket-cluster` и `bpmn-engine` |
| В UI «Заказ» появился, но действий нет | WS-событие не сопоставилось с заказом (`instanceId`/`orderNum`) | проверить формат `payload.orderNum` в событии |

## Развёртывание и разработка

```bash
# 1. Кластер: NATS + JetStream-стримы + образы движка/шлюзов + манифесты + rollout
./deploy.sh

# 2. Проброс портов для локального клиента (с автоперезапуском)
./translate_ports.sh        # http://localhost:8080 и ws://localhost:8081/ws/status

# 3. Клиент (CRA-прокси из package.json ведёт на localhost:8080)
cd erp-simulator && npm install && npm start
```

Проверки:

```bash
cd erp-simulator && npm run test:ci    # Jest: App + SDK (в т.ч. привязки sendEvent)
cd erp-simulator && npm run sdk:refresh # пересборка вендорного бандла SDK (src/vendor)
./run-test.sh                          # E2E в кластере: tests/test_workflow.py в Job
./tests/run-live-e2e.sh                # живые E2E: заказы, роли, метрики, WS-фильтры, демо-сценарий завода
./test_all.sh                          # cargo test + пересборка движка + NATS-сценарии ошибок/SLA
```

Миграции и владение таблицами проверяются **без чистого кластера** — на временной БД
того же пода: `init.sql` проигрывается от суперпользователя (как `postInitApplicationSQLRefs`
в CNPG), затем `migrations/*.sql` — от `bpmn_user`:

```bash
DB_POD=bpmn-db-1   # pod примари, см. `kubectl get pod -l cnpg.io/instanceRole=primary`

# 1. Временная БД: владелец — прикладной пользователь, как у bpmn_db
kubectl exec -i $DB_POD -c postgres -- psql -U postgres -d postgres \
  -c "CREATE DATABASE m6_init_check OWNER bpmn_user;"

# 2. init.sql — блок `init.sql: |` из db-init-configmap.yaml без 4-пробельного отступа
kubectl exec -i $DB_POD -c postgres -- psql -U postgres -d m6_init_check -v ON_ERROR_STOP=1 -f - < /tmp/init.sql

# 3. Миграция от прикладного пользователя: тут и вылезает 42501, если таблица чужая
kubectl exec -i $DB_POD -c postgres -- env PGPASSWORD=bpmn_super_secret_password \
  psql -h 127.0.0.1 -U bpmn_user -d m6_init_check -v ON_ERROR_STOP=1 -f - < bpmn-engine/migrations/0005_execution_history_actor.sql
```

Регресс-контроль того же сценария: убрать из `init.sql` строку
`ALTER TABLE execution_history OWNER TO bpmn_user;` — шаг 3 обязан упасть с
`must be owner of table execution_history` (SQLSTATE 42501). Так проверено, что
исправление держится на передаче владения, а не на побочном эффекте. Временную БД
убирает `DROP DATABASE m6_init_check WITH (FORCE);`.

Переменные окружения, влияющие на привязки:

| Переменная | Компонент | По умолчанию | Смысл |
| --- | --- | --- | --- |
| `DATABASE_URL` | движок | `postgres://bpmn_user:...@bpmn-db-rw:5432/bpmn_db` | БД токенов и определений |
| `NATS_URL` | движок, шлюзы | `nats://nats-service:4222` | брокер |
| `SLA_CHECK_INTERVAL_SECS` | движок | `3` | период сканирования «зависших» токенов |
| `SLA_TIMEOUT_MINUTES` | движок | `30` | после какого простоя токен уходит в `TIMED_OUT` + команда в `mes.out.v1.cmd.erp` |
| `PORT` | шлюзы | `8080` | порт шлюза внутри пода |
| `JSON_BODY_LIMIT` | http-cluster | `10mb` | лимит тела (BPMN-схема приезжает в `output_data.bpmn_xml`) |
| `AUTH_API_SUBJECT` | http-cluster | `mes.in.v1.api.auth` | subject проверки прав (M6); должен совпадать с подпиской движка |
| `AUTH_TIMEOUT_MS` | http-cluster | `3000` | сколько ждать ответ движка; таймаут — `503`, а не «пропустить» |
| `METRICS_API_SUBJECT` | http-cluster | `mes.in.v1.api.metrics` | subject метрик (M7); должен совпадать с подпиской движка |
| `METRICS_TIMEOUT_MS` | http-cluster | `5000` | сколько ждать ответ движка по метрикам; таймаут — `503` |
| `METRICS_DEFECT_END_EVENTS` | движок | — | id конечных узлов «брак» через запятую; если пусто — по имени `endEvent` |
| `BPMN_EVENTS_SUBJECT` | websocket-cluster | `mes.bpmn.v1.event.>` | что слушает WS-шлюз (фильтры — у подписчика) |
| `CORS_ORIGIN` | http-cluster | `*` | источник(и) для браузера |
| `REACT_APP_GATEWAY_URL` | клиент | `/api/v1/process/instances` | адрес HTTP-шлюза |
| `REACT_APP_WS_URL` | клиент | `ws://localhost:8081/ws/status` | адрес WS-шлюза |
| `REACT_APP_METRICS_API_URL` | клиент | `/api/v1/metrics` | адрес API метрик (M7); в dev — через CRA-прокси, в прод-сборке — адрес шлюза |

## Известные ограничения

1. **`serviceTask` исполняет мок-воркер (M2).** Движок сам активирует
   `Task_ERP_*`, публикует команду в `mes.out.v1.cmd.erp` и двигает токен по
   ответу; в демо роль 1С играет
   [`erp-mock-worker`](bpmn-engine/src/bin/erp_mock_worker.rs). Настоящего
   коннектора ERP/MES в репозитории нет: контракт описан в §3 и M2
   [дорожной карты](erp-simulator/docs/dairy-plant-roadmap.md).
2. **Проходные токены (`startEvent`, шлюзы) остаются в вендорной таблице
   `token`.** В `Waiting` они копятся и не проецируются в шаги кабинета;
   уборка/GC пока не сделана — таблица растёт вместе с числом прогонов.
3. **`event_name` не участвует в маршрутизации** (движок его не читает), а
   литерал `erp` в subject HTTP-шлюза (`mes.in.v1.event.erp.<event_name>`)
   зашит в код шлюза.
4. **Ретраи и компенсация — на уровне шага, не процесса (M2).** Ответ воркера
   с `retryable = true` уходит в `erp_retry` с экспоненциальным backoff, а
   исчерпание `camunda:retries` или `retryable = false` даёт
   `ActionRequiredEvent`; токен при этом остаётся на шаге. Компенсации
   пройденных шагов (saga) и таймеры/эскалации не реализованы и в дорожную
   карту не входят: SLA-воркер по-прежнему смотрит legacy `tokens`, помечает
   `TIMED_OUT` и граф не двигает.
5. **`migrations/` — пустой каталог:** в git пустые каталоги не хранятся, его
   создаёт `deploy.sh`. Каталог нужен макросу `sqlx::migrate!("./migrations")` на
   этапе сборки, а структура таблиц описана в [`schema.sql`](schema.sql) и
   [`db-init-configmap.yaml`](db-init-configmap.yaml).
6. **Корневой `Dockerfile` собирает `--bin bpmn-engine-inst`**, которого в
   [`Cargo.toml`](bpmn-engine/Cargo.toml) нет (объявлен единственный `src/main.rs`
   пакета `bpmn-engine`). `deploy.sh` этот файл не использует: он собирает
   [`bpmn-engine/Dockerfile`](bpmn-engine/Dockerfile) с контекстом `./bpmn-engine`,
   где пакет собирается по имени.
7. **Панель свойств bpmn-js не подключена** к редактору: привязку нельзя задать
   «в свойствах элемента» — только `id` узла плюс код клиента/движка
   (`bpmn-js-properties-panel` есть в зависимостях, но в `BpmnEditor` не
   добавляется через `additionalModules`).
8. **WS-шлюз отдаёт всё, пока клиент не объявил фильтры.** С M7 подписку можно
   сузить до экземпляров и роли (`?instanceId=…&role=…` в адресе или
   `{"type":"subscribe"}`; см. [«Фильтрация WS-подписки»](#фильтрация-ws-подписки-m7)).
   Кабинет фильтры не объявляет намеренно: он **обнаруживает** новые
   экземпляры по событиям (движок создаёт вторую сторону сам), поэтому
   служебные `inst-deploy-*`/`inst-def-*` по-прежнему отбрасывает клиент.
9. **SLA считает таймаут от `created_at`, а не от последней активации.** В
    `save_token_state` ветка `UPDATE` меняет только `state`, поэтому повторно
    активированный узел (цикл/возврат) может уйти в `TIMED_OUT` сразу после
    активации — если строке токена уже больше `SLA_TIMEOUT_MINUTES`.
10. **Роль узла — авторизация на сервере, но роль сессии не удостоверена.** С M6
    движок разбирает `bpmn:potentialOwner`/`act:userAction/@role` и отклоняет
    событие с чужой ролью (`403` от шлюза, аудит в `execution_history`), но
    роль приходит **заявленным** заголовком `X-Actor-Role`: IdP в демо нет,
    поэтому проверка отвечает на вопрос «можно ли этой роли», а не «кто ты».
    Событие без роли (воркер, `nats pub`, E2E-скрипт) не проверяется, а узел
    без роли — общая очередь. WS-шлюз вещает токены всем клиентам, а схемы
    живут в копии у каждой сессии: переназначение роли руководителем не
    «приезжает» другим клиентам — им нужна та же схема (деплой/перезагрузка).
    Роль нельзя задать событию или шлюзу: `resourceRole`
    по спецификации BPMN 2.0 есть только у Activity — SDK и редактор это
    блокируют вместо молчаливой потери элемента при сохранении.
11. **Кабинет подсвечивает один «текущий шаг».** Движок хранит набор активных
    токенов, `parallelGateway`/`inclusiveGateway` исполняются, и несколько
    `ACTIVE` в `tokens` — норма для параллельных ветвей. Но клиент берёт последнее
    `ACTIVE`-сообщение и не рисует набор одновременно активных узлов: подсветка
    набора запланирована в M1.1 (`token_id` уже приходит в `token_updated`).
12. **Межпроцессные сообщения работают одним шагом (M3).** Движок читает
    `trg:processTrigger` и `bpmn:message`, создаёт экземпляр второй стороны и
    доставляет catch-событию по `orderNum` (раздел
    [«Межпроцессные сообщения»](#межпроцессные-сообщения-m3)). Ограничения:
    нет каскада (сообщение стартовавшего экземпляра не разбирается в том же
    проходе), `firstElementId` должен совпадать со штатным первым шагом,
    payload — только скалярные переменные, `bpmn:messageFlow` используется как
    диагностика (пара «источник → цель» берётся из триггера узла). Триггер
    срабатывает только по **задеплоенному** целевому процессу: `ClientOrder`
    создаёт завод, если его схема уже в кластере, иначе в лог уходит
    `message_target_not_deployed`.
13. **Инфографика кабинета читает серверный API, а не локальную копию.** С M7
    движок отдаёт метрики из Postgres (`GET /api/v1/metrics`, см.
    [«Серверные метрики»](#серверные-метрики-m7)) с разрезами по шагам, ролям и
    SLA, и панель руководителя берёт цифры оттуда: два браузера и F5 дают одно и
    то же, а истёкший SLA виден в плитках `SLA`/`Ожидание шага`. Метрика браузера
    (`bpmn-process-metrics`) осталась запасным источником, когда шлюз недоступен;
    ответ сервера кэшируется (`bpmn-process-metrics-cache`), чтобы панель не
    пустела. Ограничение осталось одно: номер заказа на карточке — локальный
    (`orderNum` в payload), в ответе движка заказ не участвует, поэтому подписью
    серверного столбика служит id экземпляра.

## Карта исходников

| Файл | Отвечает за |
| --- | --- |
| [`bpmn-engine/src/main.rs`](bpmn-engine/src/main.rs) | Приём событий, SLA-воркер, ретраи; публикация `Outcome` и экземпляров второй стороны (`peers`, M3) |
| [`bpmn-engine/src/runtime/mod.rs`](bpmn-engine/src/runtime/mod.rs) | `GraphRuntime`: деплой схемы, старт, завершение шага, ответ ERP, запуск второй стороны по триггеру и доставка сообщений; проверка роли на событии и аудит шага (M6) |
| [`bpmn-engine/src/runtime/messages.rs`](bpmn-engine/src/runtime/messages.rs) | Разбор `trg:processTrigger` / `bpmn:messageFlow`, `payloadKeys`, корреляция по `orderNum` |
| [`bpmn-engine/src/runtime/roles.rs`](bpmn-engine/src/runtime/roles.rs) | Роли узла из схемы (`bpmn:potentialOwner`, `act:userAction/@role`) и решение по событию (M6) |
| [`bpmn-engine/src/auth.rs`](bpmn-engine/src/auth.rs) | NATS-API проверки прав `mes.in.v1.api.auth`: узел из ожидающего токена, ответ шлюзу (M6) |
| [`bpmn-engine/src/metrics.rs`](bpmn-engine/src/metrics.rs) | NATS-API метрик `mes.in.v1.api.metrics` (M7): снимок таблиц, чистая агрегация (`aggregate`), цикл/шаги/роли/SLA/брак, финиш по `endEvent` определения |
| [`bpmn-engine/src/runtime/pump.rs`](bpmn-engine/src/runtime/pump.rs) | Снимки движка, перехват `MessageSent`, движок доставки сообщений |
| [`bpmn-engine/src/db.rs`](bpmn-engine/src/db.rs) | `process_definitions`, `process_instances`, `tokens` (id = `instance:element`), аудит шага в `execution_history` (M6) |
| [`bpmn-engine/src/nats_client.rs`](bpmn-engine/src/nats_client.rs) | Subject'ы: `mes.bpmn.v1.event.<instance>`, `mes.out.v1.cmd.<system>` |
| [`bpmn-engine/src/events.rs`](bpmn-engine/src/events.rs) | Контракты `InboundEvent`, `ErpResponseEvent`, `ActionRequiredEvent` |
| [`bpmn-gateways/http-cluster/index.js`](bpmn-gateways/http-cluster/index.js) | Валидация, subject `mes.in.v1.event.erp.<event_name>`, CORS, лимиты, прокси `GET /api/v1/metrics` в NATS (M7) |
| [`bpmn-gateways/websocket-cluster/index.js`](bpmn-gateways/websocket-cluster/index.js) | Вещание `mes.bpmn.v1.event.>` в `ws://<host>:8081/ws/status` с фильтрами подписчика (M7) |
| [`bpmn-gateways/websocket-cluster/filters.js`](bpmn-gateways/websocket-cluster/filters.js) | Чистые фильтры WS-подписки: `parseQuery`, `matches`, `applyControl`, `instanceIdOf`, `rolesOf` (+ 8 тестов на `node --test`) |
| [`erp-simulator/src/bpmn-sdk/client/httpClient.js`](erp-simulator/src/bpmn-sdk/client/httpClient.js) | `sendEvent(instanceId, eventName, elementId, outputData)`, `deploySchema(org, xml)` |
| [`erp-simulator/src/bpmn-sdk/constants.js`](erp-simulator/src/bpmn-sdk/constants.js) | `ENGINE_EVENTS`, `ENGINE_ELEMENTS`, `INSTANCE_PREFIX`, `TOKEN_STATES`, `ELEMENT_ID_PATTERN` |
| [`erp-simulator/src/bpmn-sdk/xml/bpmnXml.js`](erp-simulator/src/bpmn-sdk/xml/bpmnXml.js) | Поиск/переименование узлов (`id`), роли (`bpmn:potentialOwner`), триггер (`trg:processTrigger`) и кнопки участников (`act:userAction`) в XML, вывод адреса кнопки из связей (`resolveAction`, `getNextElementId`), проверка совместимости схемы с движком (`validateEngineSchema`) |
| [`erp-simulator/src/bpmn-sdk/xml/triggerModdle.js`](erp-simulator/src/bpmn-sdk/xml/triggerModdle.js) | Moddle-пакет `trg`: тип `trg:ProcessTrigger`, поля триггера и `TRIGGER_MODELE_EXTENSIONS` для редактора |
| [`erp-simulator/src/bpmn-sdk/xml/actionModdle.js`](erp-simulator/src/bpmn-sdk/xml/actionModdle.js) | Moddle-пакет `act`: тип `act:UserAction`, поля кнопки и `ACTION_MODELE_EXTENSIONS` для редактора |
| [`erp-simulator/src/bpmn-sdk/panels/EnginePanel.jsx`](erp-simulator/src/bpmn-sdk/panels/EnginePanel.jsx) | Инспектор узла: `id`, название, роль исполнителя, кнопки участников, триггер запуска другого процесса |
| [`erp-simulator/src/bpmn-sdk/xml/rolesModdle.js`](erp-simulator/src/bpmn-sdk/xml/rolesModdle.js) | Moddle-пакет `role`: каталог ролей процесса (`role:catalog`, `role:businessRole`) и `ROLE_MODELE_EXTENSIONS` для редактора |
| [`erp-simulator/src/projectRoles.js`](erp-simulator/src/projectRoles.js) | Роли проекта из схемы: `analyzeProject` (процессы, роли, `roleCodes`, `problems`), `findUser`, `loginOf`, `iconNameOf` |
| [`erp-simulator/src/projectStore.js`](erp-simulator/src/projectStore.js) | Проекты в браузере: `createProject`/`updateProject`/`deleteProject`/`listProjects`, активный проект (`ensureDefaultProject`), миграция старого кэша схем |
| [`erp-simulator/src/schemaStore.js`](erp-simulator/src/schemaStore.js) | Старый кэш схем по организациям: `saveSchema`/`loadSchema`/`clearSchema`, ключ `bpmn-schema:<ORG>`; читается только для миграции в проект |
| [`erp-simulator/src/metricsStore.js`](erp-simulator/src/metricsStore.js) | Метрики запусков в браузере (запасной источник, когда API недоступен): `beginRun`/`completeRun`/`loadRuns`/`clearRuns`, ключ `bpmn-process-metrics`, агрегаты инфографики (`summarizeRuns`, `groupRunsByProcess`) |
| [`erp-simulator/src/metricsApi.js`](erp-simulator/src/metricsApi.js) | Клиент серверных метрик (M7): `metricsClient.load` → `GET /api/v1/metrics`, `toPanelRuns` (ответ движка → записи панели), кэш ответа `bpmn-process-metrics-cache` (`loadMetricsCache`/`saveMetricsCache`/`clearMetricsCache`) |
| [`erp-simulator/src/MetricsPanel.jsx`](erp-simulator/src/MetricsPanel.jsx) | Инфографика руководителя: карточка на процесс, плитки времени и серверные разрезы (ожидание шага, SLA, брак), столбики последних запусков, подпись источника и кнопка «Обновить» |
| [`erp-simulator/src/ordersApi.js`](erp-simulator/src/ordersApi.js) | Заказы в БД движка (M4): `ordersClient.listOrders`/`getOrder`/`createOrder`/`attachInstance`, нормализация серверной карточки (`normalizeOrder`, `mergeOrders`), продолжение нумерации (`nextOrderNum`) и форма заказа (`defaultOrderDraft`, `toOrderPayload`, `formatDueDate`) |
| [`erp-simulator/src/App.jsx`](erp-simulator/src/App.jsx) | Демо-привязки: роль → кнопка из схемы (`ACTION_ID`) → `sendEvent`; кнопки вне `ACTION_ID` — общим шагом (`schemaStepActions`); сохранение/сброс схемы; подсказки триггера — `getProcessIds` из схемы второй стороны; метрики — `beginRun` на старте, `completeRun` по токену на конечном узле и `refreshMetrics` (`GET /api/v1/metrics`) для панели |
| [`tests/test_workflow.py`](tests/test_workflow.py) | E2E-проверка привязок через HTTP, БД, NATS и WS |
| [`tests/orders-api.e2e.cjs`](tests/orders-api.e2e.cjs) | Живой E2E API заказов (M4): HTTP-шлюз → NATS `request/reply` → движок → Postgres, контракт кабинета и уборка следов через `psql` |
| [`tests/roles-auth.e2e.cjs`](tests/roles-auth.e2e.cjs) | Живой E2E ролей (M6): `403` чужой роли с причиной, отказ движка на событии в обход шлюза, `actor` в `execution_history`; уборка следов через `psql` |
| [`tests/nats-publish.cjs`](tests/nats-publish.cjs) | Хелпер E2E: publish события прямо в JetStream из пода шлюза — проверяет страховку рантайма, которую шлюз закрывает собой |
| [`tests/metrics-api.e2e.cjs`](tests/metrics-api.e2e.cjs) | Живой E2E API метрик (M7): сид legacy-таблиц → `GET /api/v1/metrics` через шлюз, цикл запуска, SLA открытого шага, разрезы шага/роли, предупреждение «нет схемы»; уборка следов через `psql` |
| [`tests/ws-filter.e2e.cjs`](tests/ws-filter.e2e.cjs) | Живой E2E фильтров WS (M7): три клиента (query-фильтр, `subscribe` сообщением, без фильтров), события публикуются в NATS из пода шлюза, проверяется отсечение чужих экземпляров и ролей и сброс `unsubscribe` |
| [`tests/run-live-e2e.sh`](tests/run-live-e2e.sh) | Раннер живых E2E (заказы M4 + роли M6 + метрики и WS-фильтры M7 + демо-сценарий завода M8) по уже развёрнутому кластеру: без сборки образов и без Job'ов |
| [`tests/dairy-plant.e2e.cjs`](tests/dairy-plant.e2e.cjs) | Живой E2E демо-сценария (M8): схема берётся из демо-клиента, проход заказа по трём пулам (закупка → сообщение поставщику → `MilkShipped` → техцепочка → отгрузка) и три негативные ветки (брак, авария ПОУ, деградация линии) с `serviceTask` мок-ERP |

| [`schema.sql`](schema.sql), [`db-init-configmap.yaml`](db-init-configmap.yaml) | Таблицы и ограничения по длинам идентификаторов |
| [`erp-simulator/docs/dairy-plant-roadmap.md`](erp-simulator/docs/dairy-plant-roadmap.md) | Дорожная карта демо-проекта «Молочный завод»: что уже играется в кабинете, какие разрывы (граф, воркеры, межпроцессные сообщения, роли, метрики) и в каком порядке закрываются (M1–M8) |
