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

Что делает движок при входящем событии с `element_id` (`Main Loop`):

1. сохраняет токен `tokens(id = "<instance_id>:<element_id>", state = 'ACTIVE')`;
2. закрывает прежние `ACTIVE`-токены этого экземпляра (`COMPLETED`) и публикует по
   ним `token_updated COMPLETED` — иначе клиент подсветит несколько узлов сразу;
3. публикует `token_updated ACTIVE` вместе с `payload` в `mes.bpmn.v1.event.<instance_id>`;
4. всё остальное (какой сервис что делает) — дело внешнего кода.

Инвариант: **на экземпляр ровно один `ACTIVE`-токен**
([`complete_active_tokens`](bpmn-engine/src/db.rs)). Порядок публикации важен —
сначала `COMPLETED` прежних узлов, затем `ACTIVE` нового, поэтому «текущий шаг»
процесса на клиенте — **последнее `ACTIVE`-сообщение**, а пройденные узлы видны
как `COMPLETED` (зелёная заливка без пульсации).

Дополнительно: если в `output_data` есть `bpmn_xml`, движок компилирует схему,
сохраняет определение процесса (`process_definitions.id` = `id` из
`<bpmn:process ...>`) и создаёт экземпляр `process_instances` со статусом
`ACTIVE`.

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

Ответы: `202 {status: 'event_accepted', instance_id, subject}`, `400` — не прошла
JSON-схема, `413` — тело больше `JSON_BODY_LIMIT` (по умолчанию `10mb`),
`503` — шлюз не подключён к JetStream, `500` — публикация в NATS не удалась.

```bash
curl -X POST http://localhost:8080/api/v1/process/instances/inst-cust-abc123/events \
  -H 'Content-Type: application/json' \
  -d '{
        "event_name": "StartProcess",
        "correlation_key": "corr-abc123",
        "element_id": "StartEvent_Customer",
        "output_data": { "bpmn_xml": "<bpmn:definitions ...>", "orderNum": "№101" }
      }'
```

### 2. NATS-предметы (subjects) и потоки JetStream

| Stream | Subject | Кто публикует | Кто читает |
| --- | --- | --- | --- |
| `INBOUND_EVENTS` | `mes.in.v1.event.>` | HTTP-шлюз (`...event.erp.<event_name>`) | движок, consumer `engine-inbound-consumer` |
| `ERP_RESPONSES` | `mes.in.v1.erp.>` | внешние системы (ответы сервисов) | движок, consumer `engine-erp-response-consumer` |
| `OUTBOUND_DISPATCH` | `mes.out.v1.cmd.<target_system>` | движок (SLA-команды), `target_system` в нижнем регистре | внешние системы |
| `BPMN_EVENTS` | `mes.bpmn.v1.event.>` | движок (токены и аудит) | WS-шлюз |
| `WS_NOTIFICATIONS` | `mes.ws.v1.event.>` | (резерв) | — |

Стримы создаёт Job [`nats-init-streams-job.yaml`](nats-init-streams-job.yaml);
если стрима нет — публикация падает, а шлюз отвечает `503`.

### 3. Сообщения

**Входящее событие (движок), `InboundEvent`** — [`events.rs`](bpmn-engine/src/events.rs):

```json
{ "event_id": "corr-abc123", "instance_id": "inst-cust-abc123",
  "element_id": "StartEvent_Customer", "payload": { "orderNum": "№101" } }
```

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

Старт процесса — единственное место, где клиент отправляет два события на одно
действие: `StartProcess` (в его `payload` уходит `bpmn_xml`) и сразу второе
событие, которое ставит токен на первый шаг схемы (`startProcess` в `App.jsx`,
таблица действий ниже). Так первый блок проходит через оба состояния —
`ACTIVE` при старте и `COMPLETED` после выполнения — и подсвечивается целиком.

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
| `element_id` | нет | Какой шаг закрываем; пусто → `ServiceTask_ERP_Sync` |
| `status` | да | `SUCCESS` — шаг `COMPLETED`; любое другое — `FAILED` |
| `error_message` | нет | Текст ошибки для логов/аудита |
| `payload` | да | Данные сервиса; уезжают клиентам вместе с токеном. Поле у `ErpResponseEvent` объявлено без `#[serde(default)]`, поэтому без него ответ не десериализуется и теряется (в логах движка — `❌ [ERP Consumer] Ошибка десериализации`) |

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
| Мастер производства (`handleProductionCreateOrder`) | «Создать внутренний заказ на 100 л» | `StartProcess` | `StartEvent_Customer` | `inst-cust-*` |
| Мастер производства (там же, шаг 2 — `startProcess`) | Первый шаг процесса | `CompleteBlock` | `Task_CreatePurchaseOrder` | `inst-cust-*` |
| Менеджер закупок (`handleCustomerSendOrderToSupplier`) | Передать заказ поставщику | `CompleteBlock` | `Task_WaitForSupplierShipment` | `inst-cust-*` |
| Менеджер закупок (там же, шаг 2 — `startProcess`) | Запуск процесса поставщика | `StartProcess` | `StartEvent_Supplier` | `inst-supp-*` |
| Менеджер закупок (там же, шаг 3 — `startProcess`) | Первый шаг поставщика | `CompleteBlock` | `Task_SupplierCreateDocs` | `inst-supp-*` |
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

Строки с `startProcess` — про запуск процесса: **старт это два события.** Движок
не делает переходов сам (событие называет узел, который станет `ACTIVE`), поэтому
`StartProcess` оставляет токен на стартовом событии, а второе событие переносит
его на первый шаг — тот самый блок, который интерфейс сразу показывает «текущим
шагом» (`customerActiveElementId`/`supplierActiveElementId`). Без второго события
движок закрыл бы `COMPLETED` стартовое событие, а первый блок в его состоянии не
появлялся бы вовсе: после выполнения он не подсветился бы (ни зелёным, ни как-то
иначе) — именно так первый прямоугольник «не красился» и у заказчика, и у
поставщика.

У «старт — два события» есть следствие для списка заказов: раз событий два, токен
`StartProcess` движок публикует **раньше**, чем обработчик кнопки дождётся обоих
ответов HTTP (кадр WS успевает между двумя `sendEvent`). Заказ к этому моменту уже
заводит ветка «заказ по токену» в подписке на токены, поэтому
`handleProductionCreateOrder` не добавляет свой `pendingOrder` вслепую, а сливает
его с найденным по `custInstId` (`{ ...pendingOrder, ...order }` — состояние из WS
свежее, о состоянии движка ответ HTTP ничего не знает). Иначе в списке оказывались
два заказа с одним `№`: React ругался на неуникальный `key={o.id}` в списке и
таблицах заказов, а «текущий шаг» мог уехать на чужой заказ. Регрессия закрыта
тестом «токен старта, обогнавший ответ HTTP, не удваивает заказ».

Тот же приём используется и в интеграционном тесте:
[`App.test.js`](erp-simulator/src/App.test.js) проверяет оба вызова кнопки
производства — `sendEvent(inst-cust-*, 'StartProcess', 'StartEvent_Customer', {...})`
и `sendEvent(inst-cust-*, 'CompleteBlock', 'Task_CreatePurchaseOrder', {...})`.
Второе имя события — умолчание схемы (в кнопке старта записана только роль
инициатора), узел первого шага схема выводит по исходящей связи.

### B. Сервисные задачи в движке (ответ ERP)

Здесь привязка живёт целиком в `match` внутри
[`main.rs`](bpmn-engine/src/main.rs) — это «серверная» часть связи
блок↔сервис (продвижение процесса после ответа внешней системы):

| `element_id` из ответа | Что делает движок при `status: SUCCESS` |
| --- | --- |
| `ServiceTask_ValidateOrder` | токен → `COMPLETED`, затем активирует `ServiceTask_ReserveStock` |
| `ServiceTask_ReserveStock` | токен → `COMPLETED`, экземпляр → `COMPLETED` |
| `ServiceTask_ERP_Sync` (и пустой `element_id`) | токен → `COMPLETED`, экземпляр → `COMPLETED` |
| любой другой | только `COMPLETED`; продвижения/завершения нет — нужно добавить ветку |
| `status != SUCCESS` | токен → `FAILED`, экземпляр → `FAILED`, событие `FAILED` в WS |

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

Роль — это **маршрутизация в интерфейсе, а не авторизация**: движок её не читает
(проверено на живом движке — процесс со ролями проходит маршрут целиком), а
WS-шлюз вещает токены всем клиентам. Подробности —
[«Известные ограничения»](#известные-ограничения).

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
а не по захардкоженному списку; словарь ролей для выпадающего списка инспектора —
`ROLE_CODES` из [`erp-simulator/src/roles.js`](erp-simulator/src/roles.js).

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
- роль, для которой нет пользователя, не теряется: инспектор показывает её в
  списке, даже если такой роли нет среди `ROLE_CODES`.

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

> **Триггер пока декларация.** Движок его не читает: запуск второго процесса
> делает клиент отдельным `sendEvent` (так работает демо-сценарий «передать
> заказ поставщику»), а `trg:processTrigger` фиксирует пару «источник → цель»,
> точку входа и состав передаваемого payload. См.
> [«Известные ограничения»](#известные-ограничения).

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
`isValidAction`, `isActionHostType`, `ACTION_FIELDS`, `EMPTY_ACTION`,
`ACTION_TYPE`, `ACTION_NAMESPACE`, `ACTION_MODELE_EXTENSIONS`, а для вывода адреса
кнопки из связей — `resolveAction`, `resolveActionEvent`, `resolveActionTarget`,
`getNextElementId`, `getOutgoingTargetIds`, `DEFAULT_ACTION_EVENT` — подробности и
правила записи в
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

## Локальный кэш схемы (`localStorage`)

Правки редактора живут в состоянии React, поэтому без сохранения F5 возвращал
демо-схему SDK. Кнопка «Сохранить схему» пишет копию в `localStorage` браузера
([`erp-simulator/src/schemaStore.js`](erp-simulator/src/schemaStore.js)), а
приложение при следующем открытии стартует не с демо-схемы, а с этой копией:

| Что | Функция | Ключ в хранилище |
| --- | --- | --- |
| Сохранить | `saveSchema(org, xml)` — запись `{org, xml, savedAt}` | `bpmn-schema:<ORG>` |
| Подхватить | `loadSchema(org)` → `xml` либо `null` | тот же |
| Забыть | кнопка «Демо-схема» → `clearSchema(org)` | запись удаляется |

Детали:

- **кэш локальный и на браузер**, а не серверный: у каждого пользователя своя
  копия, и деплой он не заменяет — схема уходит в кластер только кнопкой
  «Задеплоить BPMN в Оркестратор» (либо в `output_data.bpmn_xml` при старте
  процесса);
- **деплой сохраняет копию тоже**, иначе после F5 кабинет показывал бы схему,
  которой в кластере уже нет;
- **копия может быть старше демо-схемы SDK**: если демо-схему дополнили (новый
  узел, переименование), старт всё равно возьмёт копию из браузера — вернуть
  актуальную схему SDK можно кнопкой «Демо-схема». Так, после появления в
  демо-схеме заказчика узла `Task_WaitForSupplierShipment` подсветка этого шага
  появится только после сброса старой копии;
- **подпись в панели схемы** говорит, что происходит: «Сохранено в 12:34:56»,
  «Есть несохранённые изменения» (XML разошёлся с копией) или «Схема по
  умолчанию» (копии нет — работает демо-схема);
- **хранилище может быть недоступно** (приватный режим, переполненная квота):
  `saveSchema` вернёт `null`, приложение продолжит работать в памяти, а в ленте
  появится «Схема не сохранена»;
- **битая запись приложение не роняет**: нечитаемый JSON или чужой формат
  считаются отсутствием копии — подхватится демо-схема;
- **валидность XML проверяет редактор**, а не кэш: `schemaStore` принимает любую
  непустую строку и намеренно не дублирует проверку схемы.

Проверки: [`schemaStore.test.js`](erp-simulator/src/schemaStore.test.js)
(сохранение, битые записи, отказы хранилища) и
[`App.test.js`](erp-simulator/src/App.test.js) (подхват при старте процесса,
кнопки сохранения/сброса, «перезагрузка» повторным монтированием).

## Метрики выполненных процессов

Кабинет руководителя показывает инфографику по процессам
([`MetricsPanel.jsx`](erp-simulator/src/MetricsPanel.jsx)): сколько запусков
завершено, среднее/минимальное/максимальное время выполнения и столбики
последних запусков. Пока измеряется одно — **время выполнения**: от старта
экземпляра до токена на конечном узле.

Отдельного события «процесс завершён» движок не публикует: токен просто встаёт
на `bpmn:endEvent` и остаётся `ACTIVE`. Поэтому финиш определяет клиент по схеме
(у конечного узла нет исходящих связей — `getOutgoingTargetIds`), а саму метрику
хранит в браузере ([`erp-simulator/src/metricsStore.js`](erp-simulator/src/metricsStore.js),
ключ `bpmn-process-metrics`):

| Что | Функция | Когда вызывается |
| --- | --- | --- |
| Старт | `beginRun({id, org, processId, processName, orderNumber})` | `startProcess` в [`App.jsx`](erp-simulator/src/App.jsx) — экземпляр создан |
| Финиш | `completeRun(instanceId, {finishedAt, lastElementId, orderNumber})` | токен `ACTIVE` встал на конечный узел схемы контура |
| Чтение | `loadRuns()` | при монтировании кабинета и после каждой записи |
| Забыть | `clearRuns()` | кнопка «Очистить» в панели инфографики |

Детали:

- **метрика локальная и на браузер**, как и кэш схемы: её видит тот клиент,
  который запускал процессы (журнал `execution_history` в кластере она не
  заменяет);
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
[`MetricsPanel.test.jsx`](erp-simulator/src/MetricsPanel.test.jsx) (карточки
процессов, плитки времени, столбики) и
[`App.test.js`](erp-simulator/src/App.test.js) (метрика переживает смену
пользователя, промежуточный узел финишем не считается).

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
отправляется, а «Создать внутренний заказ» не заводит заказ — иначе в списке
остался бы «заказ-призрак» без экземпляра процесса. Проверка зеркалит правила
компилятора и лежит в SDK рядом с остальной логикой XML (см.
[README SDK](erp-simulator/src/bpmn-sdk/README.md#совместимость-схемы-с-движком)).

Альтернатива для одноразового запуска — передать схему в `output_data.bpmn_xml`
вместе со стартом процесса (так делает тест):

```js
await client.sendEvent('inst-cust-abc123', 'StartProcess', 'StartEvent_Customer', {
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

Исключение — **первый шаг процесса**: он не «двигается», его нужно активировать
отдельным событием. `StartProcess` называет стартовое событие (и несёт
`bpmn_xml`), поэтому сразу за ним отправьте второе событие с `element_id` первого
блока — тогда шаг попадёт в состояние движка и после выполнения уйдёт в `COMPLETED`
(в демо это делает `startProcess` в [`App.jsx`](erp-simulator/src/App.jsx)).

**Вариант B — внешняя система (ERP/MES).** Система подписывается на
`mes.bpmn.v1.event.>` (или читает таблицу `tokens`), находит свой узел по
`element_id` и, выполнив работу, отвечает:

```bash
nats pub mes.in.v1.erp.response \
  '{"process_instance_id":"inst-cust-abc123","element_id":"Task_FlashPasteurization",
    "status":"SUCCESS","payload":{"batch":"B-42"}}' --server=nats-service:4222
```

Движок пометит блок `COMPLETED` и разошлёт событие. Чтобы процесс после этого
**пошёл дальше** (активировать следующий блок или завершить экземпляр),
добавьте ветку в `match element_id` в `main.rs`:

```rust
match element_id {
    "Task_FlashPasteurization" => {
        let next_element = "Task_Packing";
        db::save_token_state(&pool_erp, inst_id, next_element, "ACTIVE").await?;
        nats_erp.publish_token_event(inst_id, next_element, "ACTIVE", Some(&response.payload)).await?;
    }
    "ServiceTask_ReserveStock" | "ServiceTask_ERP_Sync" => {
        db::update_process_instance_status(&pool_erp, inst_id, "COMPLETED").await?;
    }
    _ => {}
}
```

**Вариант C — адресация конкретной системе.** SLA-воркер уже умеет публиковать
команду в `mes.out.v1.cmd.<target_system>`. Если нужно, чтобы по активации блока
движок сразу отправлял работу конкретному сервису, добавьте публикацию
`ActionRequiredEvent` в обработчик входящего события (сейчас там только запись
токена и публикация в WS) — и система будет слушать свой subject, а не общий
поток токенов.

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
   с первым прямоугольником обеих демо-схем: интерфейс сразу считал текущим
   `Task_CreatePurchaseOrder` (`Task_SupplierCreateDocs` у поставщика), а токен
   стоял на `StartEvent_Customer` (`StartEvent_Supplier`) — после выполнения шага
   движок закрывал `COMPLETED` стартовое событие, а сам блок не подсвечивался
   вообще. Лечится вторым событием старта (`startProcess` в
   [`App.jsx`](erp-simulator/src/App.jsx), таблица действий выше): перед проверкой
   состояния посмотрите `SELECT element_id, state FROM tokens`, чтобы убедиться,
   что токен вообще был на этом узле.

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
| `413 request entity too large` | `bpmn_xml` больше лимита | поднять `JSON_BODY_LIMIT` у http-cluster |
| `503` «JetStream is not connected» | нет стрима `INBOUND_EVENTS` или NATS недоступен | `kubectl apply -f nats-init-streams-job.yaml`, проверить `nats stream ls` |
| `500` / «Ошибка сети» в UI | нет проброса портов (CRA-прокси не может достучаться до 8080) | `./translate_ports.sh` (перезапускает `kubectl port-forward`) |
| Токен `ACTIVE`, но шаг не подсвечен | `element_id` отсутствует в задеплоенной схеме | сверить `id` узла и `element_id`, передеплоить схему |
| Первый блок схемы не подсвечен / не уходит в зелёный | токен стоит на стартовом событии: клиент не отправил событие первого шага | после `StartProcess` активировать первый блок (`startProcess` в [`App.jsx`](erp-simulator/src/App.jsx)); проверить `SELECT element_id, state FROM tokens` |
| Токен висит `ACTIVE` | шаг никто не завершил | внешний сервис должен ответить в `mes.in.v1.erp.response`; иначе через `SLA_TIMEOUT_MINUTES` (по умолчанию 30) прилетит `TIMED_OUT` |
| Процесс не доходит до `COMPLETED` | нет ветки `match element_id` в `main.rs` (для ответа ERP) | добавить ветку с активацией следующего узла/завершением |
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
./test_all.sh                          # cargo test + пересборка движка + NATS-сценарии ошибок/SLA
```

Переменные окружения, влияющие на привязки:

| Переменная | Компонент | По умолчанию | Смысл |
| --- | --- | --- | --- |
| `DATABASE_URL` | движок | `postgres://bpmn_user:...@bpmn-db-rw:5432/bpmn_db` | БД токенов и определений |
| `NATS_URL` | движок, шлюзы | `nats://nats-service:4222` | брокер |
| `SLA_CHECK_INTERVAL_SECS` | движок | `3` | период сканирования «зависших» токенов |
| `SLA_TIMEOUT_MINUTES` | движок | `30` | после какого простоя токен уходит в `TIMED_OUT` + команда в `mes.out.v1.cmd.erp` |
| `PORT` | шлюзы | `8080` | порт шлюза внутри пода |
| `JSON_BODY_LIMIT` | http-cluster | `10mb` | лимит тела (BPMN-схема приезжает в `output_data.bpmn_xml`) |
| `CORS_ORIGIN` | http-cluster | `*` | источник(и) для браузера |
| `REACT_APP_GATEWAY_URL` | клиент | `/api/v1/process/instances` | адрес HTTP-шлюза |
| `REACT_APP_WS_URL` | клиент | `ws://localhost:8081/ws/status` | адрес WS-шлюза |

## Известные ограничения

1. **Продвижение по ответу ERP захардкожено** в `match element_id`
   ([`main.rs`](bpmn-engine/src/main.rs)). Новый сервисный блок с ответом
   внешней системы не поедет дальше без правки движка — либо вынесите маппинг
   «шаг → следующий шаг» в таблицу/конфиг.
2. **Движок не исполняет граф схемы.** `sequenceFlow` не читается: следующий
   активный узел указывает вызывающая сторона (клиент) или ветка `match`.
   Схема используется как справочник `id` и как визуализация состояния.
3. **`event_name` не участвует в маршрутизации** (движок его не читает), а
   литерал `erp` в subject HTTP-шлюза (`mes.in.v1.event.erp.<event_name>`)
   зашит в код шлюза.
4. **Компенсация/ретраи не реализованы**: любой `status != SUCCESS` в ответе
   ERP сразу переводит шаг и экземпляр в `FAILED`; на таймауте движок лишь
   публикует `ActionRequiredEvent` с `action: CANCEL_OR_RETRY`.
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
8. **WS-шлюз вещает всё без фильтрации** (`mes.bpmn.v1.event.>` → всем клиентам),
   поэтому клиент сам отбрасывает служебные экземпляры (`inst-deploy-*`,
   `inst-def-*`).
9. **SLA считает таймаут от `created_at`, а не от последней активации.** В
    `save_token_state` ветка `UPDATE` меняет только `state`, поэтому повторно
    активированный узел (цикл/возврат) может уйти в `TIMED_OUT` сразу после
    активации — если строке токена уже больше `SLA_TIMEOUT_MINUTES`.
10. **Роль узла в схеме — маршрутизация UI, а не авторизация.** Движок
    `bpmn:potentialOwner` не читает и не проверяет (проверено на живом движке:
    маршрут процесса со ролями проходит целиком), WS-шлюз вещает токены всем
    клиентам, а роли живут в копии схемы у каждой сессии: переназначение роли
    руководителем не «приезжает» другим клиентам — им нужна та же схема
    (деплой/перезагрузка). Браузерный кэш схемы («Сохранить схему») переживает
    только F5 в том же браузере: соседнему пользователю схему он не передаёт.
    Роль нельзя задать событию или шлюзу: `resourceRole`
    по спецификации BPMN 2.0 есть только у Activity — SDK и редактор это
    блокируют вместо молчаливой потери элемента при сохранении.
11. **Инвариант «один `ACTIVE`-токен на экземпляр» несовместим с параллельными
   ветками.** Активируя узел, движок закрывает все прежние `ACTIVE`-токены
   экземпляра ([`db::complete_active_tokens`](bpmn-engine/src/db.rs)), потому что
   клиент показывает один «текущий шаг». `parallelGateway`/`inclusiveGateway` с
   несколькими одновременными ветками так не выразить: на экземпляре останется
   только последняя активация. Для ветвлений понадобится хранить набор активных
   токенов и снимать состояние с графа (`sequenceFlow`), а не с одной записи.
12. **Движок не читает `trg:processTrigger`.** Триггер живёт в схеме и
    настраивается в редакторе, но запуск второго процесса по-прежнему делает
    клиент отдельным `sendEvent` (демо-сценарий «передать заказ поставщику»):
    движок не разбирает `extensionElements`, а ветка продвижения в
    [`main.rs`](bpmn-engine/src/main.rs) написана только для ответов ERP. Чтобы
    запуск шёл от движка, ему понадобится разбор триггера задеплоенной схемы,
    создание экземпляра целевого процесса, вход в `firstElementId` и перенос
    перечисленных в `payloadKeys` полей контекста источника.
13. **Метрики процессов считает клиент, а не движок.** Время выполнения хранится
    в браузере (ключ `bpmn-process-metrics`) и видно только тому, кто запускал
    процессы: другой клиент, очищенное хранилище или запуск не из UI оставят
    инфографику пустой. Финишем считается токен на узле без исходящих связей по
    текущей схеме контура, поэтому правка схемы меняет и метрику. Серверная
    метрика (из `tokens.created_at`/`execution_history`) — следующий шаг: там же
    появятся время по шагам, ожидание и SLA.

## Карта исходников

| Файл | Отвечает за |
| --- | --- |
| [`bpmn-engine/src/main.rs`](bpmn-engine/src/main.rs) | Приём событий, токены, SLA-воркер, `match element_id` для ответов ERP |
| [`bpmn-engine/src/db.rs`](bpmn-engine/src/db.rs) | `process_definitions`, `process_instances`, `tokens` (id = `instance:element`) |
| [`bpmn-engine/src/nats_client.rs`](bpmn-engine/src/nats_client.rs) | Subject'ы: `mes.bpmn.v1.event.<instance>`, `mes.out.v1.cmd.<system>` |
| [`bpmn-engine/src/events.rs`](bpmn-engine/src/events.rs) | Контракты `InboundEvent`, `ErpResponseEvent`, `ActionRequiredEvent` |
| [`bpmn-gateways/http-cluster/index.js`](bpmn-gateways/http-cluster/index.js) | Валидация, subject `mes.in.v1.event.erp.<event_name>`, CORS, лимиты |
| [`bpmn-gateways/websocket-cluster/index.js`](bpmn-gateways/websocket-cluster/index.js) | Вещание `mes.bpmn.v1.event.>` в `ws://<host>:8081/ws/status` |
| [`erp-simulator/src/bpmn-sdk/client/httpClient.js`](erp-simulator/src/bpmn-sdk/client/httpClient.js) | `sendEvent(instanceId, eventName, elementId, outputData)`, `deploySchema(org, xml)` |
| [`erp-simulator/src/bpmn-sdk/constants.js`](erp-simulator/src/bpmn-sdk/constants.js) | `ENGINE_EVENTS`, `ENGINE_ELEMENTS`, `INSTANCE_PREFIX`, `TOKEN_STATES`, `ELEMENT_ID_PATTERN` |
| [`erp-simulator/src/bpmn-sdk/xml/bpmnXml.js`](erp-simulator/src/bpmn-sdk/xml/bpmnXml.js) | Поиск/переименование узлов (`id`), роли (`bpmn:potentialOwner`), триггер (`trg:processTrigger`) и кнопки участников (`act:userAction`) в XML, вывод адреса кнопки из связей (`resolveAction`, `getNextElementId`), проверка совместимости схемы с движком (`validateEngineSchema`) |
| [`erp-simulator/src/bpmn-sdk/xml/triggerModdle.js`](erp-simulator/src/bpmn-sdk/xml/triggerModdle.js) | Moddle-пакет `trg`: тип `trg:ProcessTrigger`, поля триггера и `TRIGGER_MODELE_EXTENSIONS` для редактора |
| [`erp-simulator/src/bpmn-sdk/xml/actionModdle.js`](erp-simulator/src/bpmn-sdk/xml/actionModdle.js) | Moddle-пакет `act`: тип `act:UserAction`, поля кнопки и `ACTION_MODELE_EXTENSIONS` для редактора |
| [`erp-simulator/src/bpmn-sdk/panels/EnginePanel.jsx`](erp-simulator/src/bpmn-sdk/panels/EnginePanel.jsx) | Инспектор узла: `id`, название, роль исполнителя, кнопки участников, триггер запуска другого процесса |
| [`erp-simulator/src/roles.js`](erp-simulator/src/roles.js) | Демо-пользователи и словарь ролей (`ROLE_CODES`) для инспектора узлов |
| [`erp-simulator/src/schemaStore.js`](erp-simulator/src/schemaStore.js) | Кэш схем в браузере: `saveSchema`/`loadSchema`/`clearSchema`, ключ `bpmn-schema:<ORG>` |
| [`erp-simulator/src/metricsStore.js`](erp-simulator/src/metricsStore.js) | Метрики запусков в браузере: `beginRun`/`completeRun`/`loadRuns`/`clearRuns`, ключ `bpmn-process-metrics`, агрегаты инфографики (`summarizeRuns`, `groupRunsByProcess`) |
| [`erp-simulator/src/MetricsPanel.jsx`](erp-simulator/src/MetricsPanel.jsx) | Инфографика руководителя: карточка на процесс, плитки времени, столбики последних запусков |
| [`erp-simulator/src/App.jsx`](erp-simulator/src/App.jsx) | Демо-привязки: роль → кнопка из схемы (`ACTION_ID`) → `sendEvent`; кнопки вне `ACTION_ID` — общим шагом (`schemaStepActions`); сохранение/сброс схемы; подсказки триггера — `getProcessIds` из схемы второй стороны; метрики запусков — `beginRun` на старте и `completeRun` по токену на конечном узле |
| [`tests/test_workflow.py`](tests/test_workflow.py) | E2E-проверка привязок через HTTP, БД, NATS и WS |
| [`schema.sql`](schema.sql), [`db-init-configmap.yaml`](db-init-configmap.yaml) | Таблицы и ограничения по длинам идентификаторов |