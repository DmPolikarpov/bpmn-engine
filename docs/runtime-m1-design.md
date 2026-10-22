# M1: рантайм `bpm-engine` в оркестраторе — дизайн

Документ описывает этап **M1** из [дорожной карты демо-проекта «Молочный
завод»](../erp-simulator/docs/dairy-plant-roadmap.md#m1-маршрутизация-по-графу--p0-l):
маршрут процесса должен считаться по графу схемы, а не приходить из события
клиента.

- Статус: **шаги 0–2 реализованы** (22.10.2026): вендоринг 0.2.0 с пятью
  патчами и `bpmn-engine/src/runtime/` (шаг 0); условия на связях и `variables`
  у кнопок демо-схемы, шесть новых кодов валидатора SDK, тест маршрута кнопок и
  старт клиента **одним** событием (10.2, п. 1–2) — шаг 1; `graph` включён по
  умолчанию и подтверждён интеграционным прогоном `tests/test-job.yaml`
  (10 шагов, WS-события, SLA) — шаг 2. Шаг 3 (удаление legacy-ветки) — M2.
- Область: [`bpmn-engine/src`](../bpmn-engine/src) (Rust), схема демо-проекта
  [`dairyPlant.js`](../erp-simulator/src/bpmn-sdk/defaults/dairyPlant.js), SDK
  [`bpmnXml.js`](../erp-simulator/src/bpmn-sdk/xml/bpmnXml.js).
- Вне области: воркеры ERP и `serviceTask` (M2), `messageFlow` и
  `trg:processTrigger` (M3), серверные заказы (M4), авторизация шагов (M6).

Решения, которые документ фиксирует: **движок берётся из vendored-форка**
(раздел 4), **человеческие шаги — `userTask` с переменными** (раздел 8),
**`element_id` остаётся в контракте, но перестаёт быть маршрутом** (раздел 5),
**двойная запись в legacy-таблицы** на время M1 (раздел 6).

## 1. Задача и критерий готовности

**Зачем.** Сейчас следующий узел приходит в событии (`element_id`), а движок
только записывает токен. Любая кнопка уводит процесс куда угодно, шлюзы
нарисованы, но не решают, а «Нет» на пастеризации обходится кнопкой аварии.
Демо должно показывать, что маршрут определяется схемой.

**Готово, когда** (перенесено из дорожной карты, с уточнениями):

- [ ] `Task_CheckStock → Gateway_Stock → Task_Ship` проходит без подсказки
      клиента при `stockSufficient = 'true'` и уходит в `Task_CreatePO` при
      `'false'`.
- [ ] Все пять шлюзов демо-проекта проверены Rust-тестом на обе/три ветки
      (таблица условий — приложение A).
- [ ] `Gateway_PasteurizeOK` имеет ветку «Нет» → `Task_ERP_CriticalAlert`;
      авария на пастеризации приходит из условия, а не из кнопки.
- [ ] Набор `ACTIVE`-токенов хранится движком (`process_instance` + `token`), а
      не выводится из одной записи; `parallelGateway` держит две ветки.
- [ ] Клиент не вычисляет следующий узел: подсветка приходит из событий
      движка (`token_updated`), кнопка сообщает только «я завершил этот шаг» и
      значения переменных.
- [ ] `npm run test:ci`, `npm run sdk:verify` (47/47), `npm run build`,
      `cargo test` в `bpmn-engine` — зелёные; E2E
      [`tests/test_workflow.py`](../tests/test_workflow.py) проходит.

## 2. Что происходит сегодня

| Факт | Где | Следствие |
| --- | --- | --- |
| `Engine::new(store)` создан и не используется | [`main.rs:35-36`](../bpmn-engine/src/main.rs) | предупреждение `unused variable: engine`; `tick()` в 0.2.0 — заглушка, так что «просто вызвать» нельзя |
| `parse_and_compile` вызывается только ради `process_def.id` | [`main.rs:183-192`](../bpmn-engine/src/main.rs) | `sequenceFlow` и `conditionExpression` компилируются и **выбрасываются** |
| Токен ставится по `element_id` из события | [`main.rs:194-217`](../bpmn-engine/src/main.rs) | маршрут задаёт нажатая кнопка |
| Прежние `ACTIVE` закрываются принудительно | [`db::complete_active_tokens`](../bpmn-engine/src/db.rs) | инвариант «один активный узел», параллельные ветки невыразимы |
| Ответ ERP двигает только захардкоженную пару | [`main.rs:127-158`](../bpmn-engine/src/main.rs) | `ServiceTask_ValidateOrder → ServiceTask_ReserveStock`, дальше `COMPLETED` |
| `Cargo.lock` не содержит `bpm-engine`, зато содержит `roxmltree` | `bpmn-engine/Cargo.lock` | артефакт прежней локальной реализации движка; перегенерируется при импорте зависимостей |
| `_main_tmp.rs` — побайтовая копия `main.rs`, не в git | `bpmn-engine/src/_main_tmp.rs` | мусор, удаляется отдельным коммитом |
| `publish_audit_event` и `BpmnEvent` не вызываются | [`nats_client.rs:55`](../bpmn-engine/src/nats_client.rs), [`events.rs:24`](../bpmn-engine/src/events.rs) | мёртвый код: либо включить в аудит (M6/M7), либо удалить |
| `event_id` пишется шлюзом и не читается | [`http-cluster/index.js:125`](../bpmn-gateways/http-cluster/index.js), [`events.rs:7`](../bpmn-engine/src/events.rs) | идемпотентности нет: повторная доставка двигает шаг дважды |
| `ErpResponseEvent.error_message` не читается | [`events.rs:36`](../bpmn-engine/src/events.rs) | причина отказа ERP не попадает ни в токен, ни в историю |

Почему нельзя «включить `Engine`»: в опубликованном `bpm-engine 0.2.0`
`Engine::tick()` — заглушка (`let _ = &self.store; Ok(())`), а исполнение живёт
в `BpmEngine::run_async(EngineEvent, &mut EngineContext)` с набором хендлеров и
сторов. То есть M1 — это не вызов одного метода, а подключение рантайма.

## 3. Целевая схема

```text
клиент (кабинет)
  │  POST /instances/<id>/events  {event_name, correlation_key, element_id, output_data}
  ▼
http-cluster ──► NATS JetStream  mes.in.v1.event.>      mes.in.v1.erp.>
                        │                                     │
                        ▼                                     ▼
                 ┌──────────────────────────────────────────────────┐
                 │ bpmn-engine (наш crate)                          │
                 │  runtime::mapper   NATS-сообщение → EngineEvent  │
                 │  runtime::pump     BpmEngine::run_async(...)     │
                 │  runtime::project  EngineEvent → token_updated,  │
                 │                    legacy read-model             │
                 └───────────────┬──────────────────────────────────┘
                                 │ EngineContext (vendored bpm-engine)
                                 ▼
                 process_def_store  process_store  token_store
                 external_task_store  timer_store  history  outbox
                                 │
                                 ▼
                 PostgreSQL: process_definition / process_instance / token /
                 external_task / timer / history_event / event_outbox (+ legacy
                 process_definitions / process_instances / tokens)
                                 │
                                 ▼
                 NATS mes.bpmn.v1.event.<instance> → websocket-cluster → кабинет
```

Границы ответственности:

- **вендорный движок** — граф, токены, шлюзы, external tasks, таймеры;
- **`runtime::*` в нашем crate** — перевод NATS ↔ `EngineEvent`, выбор сторов,
  восстановление, проекция в read-model и WS;
- **клиент** — только UI: показать активные узлы и отправить переменные шага.

Ключевой принцип: наш код не знает про типы `bpm-engine` за пределами
`src/runtime/`. Если форк придётся заменить (другая ревизия, свой fork,
crates.io), правится один модуль.

## 4. Зависимость: vendored fork

### 4.1 Почему не `bpm-engine = "0.2.0"` из crates.io

| Что нужно M1 | В 0.2.0 на crates.io | В master (GitHub) |
| --- | --- | --- |
| исполнение графа | `Engine::tick()` — заглушка | `BpmEngine::run_async` + `EventPump` + хендлеры |
| завершение внешней задачи | варианта `ExternalTaskCompleted` нет | `ExternalTaskCompleted` + `ExternalTaskCompletedHandler` |
| Postgres-сторы | нет (только memory-адаптер) | `bpm-engine-adapter-postgres`: `create_pool`, `migrate`, 10 сторов |
| таймеры, история, DLQ | нет | `timer_store`, `PostgresHistoryRepo`, `dead_letter` |

Поэтому движок вендорится в репозиторий: копия workspace-крейтов на
зафиксированном коммите. Это даёт две вещи, которых не даёт git-зависимость:
сборка не зависит от доступности GitHub, и нужные нам патчи (4.3) живут
в репозитории, а не в форке на стороне.

### 4.2 Раскладка

```text
bpmn-engine/
├── Cargo.toml                  # bpm-engine = { path = "vendor/bpm-engine" }
├── rust-toolchain.toml         # 1.89 — та же версия, что в Dockerfile
├── Dockerfile                  # COPY vendor ./vendor  — до сборки зависимостей
├── tests/vendor_patches.rs     # проверки трёх патчей (см. 4.3)
└── vendor/bpm-engine/
    ├── Cargo.toml              # корневой крейт bpm-engine (facade)
    ├── Cargo.lock              # lock вендора — для сверки версий при обновлении
    ├── LICENSE.txt             # MIT, сохраняется как есть
    ├── VENDORED_FROM.md        # URL, commit, дата, применённые патчи
    └── crates/
        ├── core/               # ProcessDefinition, Node, Token, EngineEvent
        ├── storage/            # трейты сторов
        ├── runtime/            # BpmEngine, pump, handlers, transition, el
        ├── bpmn/               # parse_and_compile
        ├── adapters/memory/    # unit/integration-тесты
        └── adapters/postgres/  # create_pool, migrate, сторы
```

`vendor/` лежит **внутри каталога пакета** (`bpmn-engine/`), потому что
[`Dockerfile`](../bpmn-engine/Dockerfile) собирает с контекстом `./bpmn-engine`;
иначе контекст пришлось бы расширять до корня репозитория.

### 4.3 Провенанс и обновление

- Источник: `https://github.com/Colin4k1024/bpm-engine` (канонический адрес;
  homepage репозитория указывает на прежний `fanjia1024/...`).
- В `VENDORED_FROM.md` фиксируются: commit, дата, список локальных патчей с
  указанием файлов, лицензия MIT.
- Патчи M1 — пять: два в обработчиках завершения задач, один в компиляторе,
  два в Postgres-адаптере (плюс общий хелпер `move_token_preserving_group` в
  `transition.rs`):
  1. **Переменные человеческого шага.** `UserTaskCompletedHandler` не переносит
     `e.variables` в `instance.variables` — в его теле такого цикла нет вовсе
     (`user_task_completed_handler.rs:15-44`), тогда как в
     `ExternalTaskCompletedHandler` мерж есть
     (`external_task_completed_handler.rs:43-46`). Без
     патча решения человека не доезжают до условий шлюза: гейтвей читает
     `instance.variables`, а там остаются значения старта.
  2. **Адрес токена и группа параллельной ветки.** Завершение шага закрывает
     токены по узлу (`tokens.retain(|t| t.node_id != e.node_id)` —
     `user_task_completed_handler.rs:31`) и двигает их через `move_token`,
     который теряет `parallel_group_id` (`transition.rs:21-26`). Хелпер
     `move_token_preserving_group` в вендоре **уже есть**
     (`transition.rs:30-35`), но оба хендлера вызывают `move_token`
     (`user_task_completed_handler.rs:32`,
     `external_task_completed_handler.rs:49`). В параллельном флоу это
     (а) закрывает соседнюю ветку, если два токена стоят на одном узле,
     (б) оставляет группу пустой, и `ParallelJoinRepo` считает чужие ветки.
     Патч выбирает один `Waiting`-токен узла, удаляет его по `id`, сохраняет
     группу через `move_token_preserving_group` и возвращает `TokenCompleted`
     для истории (сейчас шаг отдаёт только `TokenArrived`):

     ```rust
     let Some(idx) = instance
         .tokens
         .iter()
         .position(|t| t.node_id == e.node_id && t.status == TokenStatus::Waiting)
     else {
         return vec![];
     };
     let group = instance.tokens[idx].parallel_group_id.clone();
     let token_id = instance.tokens[idx].id.clone();
     for (k, v) in &e.variables {
         instance.variables.insert(k.clone(), v.clone());
     }
     instance.tokens.remove(idx);
     let new_tokens = move_token_preserving_group(node, group);
     out.push(EngineEvent::TokenCompleted(payloads::TokenCompleted {
         instance_id: e.instance_id.clone(),
         token_id,
     }));
     ```

     `ExternalTaskCompletedHandler` получает тот же `move_token_preserving_group`
     (мерж переменных и удаление по `token_id` там уже верные).
  3. **Флаг `default`, у которого нет `conditionExpression`.** Компилятор
     строит условие связи как `flow.condition_expression.as_ref()
     .map(|raw| parse_condition(raw, flow.is_default))` (`compiler.rs:826-829`):
     флаг `is_default` читается **внутри** замыкания, поэтому default-связь без
     условия остаётся с `None`, а `evaluate_exclusive_gateway`
     (`transition.rs:51-53`) на `None` возвращает связь немедленно — как
     безусловную и раньше всех условий. Для BPMN это неверно: default-связь по
     спеке условия не имеет, флаг и есть её условие. Проверено на вендорном
     крейте (`parse_and_compile`): `default="F2"` без условия → `cond=None`,
     с любым `conditionExpression` → `cond=Some(Default)`.

     ```rust
     let cond = if flow.is_default {
         Some(EdgeCondition::Default)
     } else {
         flow.condition_expression.as_ref().map(|raw| parse_condition(raw, false))
     };
     ```

     Без этого патча флаг работает только по случайности — когда default-связь
     объявлена последней в `<process>` (7.1, п. 3). В демо-схеме это не так:
     `SF5` «Нет» объявлена до `SF6` «Да» (`dairyPlant.js:476-477`), то есть без
     патча `Gateway_Stock` всегда уходил бы в `Task_CreatePO`.
  4. **Схема `migrate` соответствует таблицам.** `adapters/postgres/src/lib.rs`
     строит `idx_token_state_updated` по колонке `state`, которой в таблице
     `token` нет (там `status`). Без патча `migrate` падает на
     `column "state" does not exist`, и `graph` не поднимается ни на одном поде.
  5. **`load` выбирает колонки, которые читает.**
     `adapters/postgres/src/process_store.rs`: `SELECT` в
     `PostgresProcessStore::load` не выбирал `parent_instance_id` и
     `parent_token_id`, хотя строку он читает через `row.get`. Без патча первый
     же `load` после старта процесса паникует (`invalid column`), под падает, а
     экземпляр остаётся с `Ready`-токеном на `StartEvent` — «старт не
     сработал». Найдено на интеграционном прогоне 22.10.2026.
- Патчи живут в `VENDORED_FROM.md` с номерами файлов и строк; на них есть
  тесты в `bpmn-engine/tests/` (переменные доезжают до шлюза, две ветки
  `parallelGateway` не схлопываются, `default` без `conditionExpression`
  компилируется в `Default`, `load` выбирает обе `parent_*`-колонки).
  Патчи 4–5 юнит-тестом не покрыть: вендорскому прогону нужен живой Postgres,
  поэтому первый проверяется только стендом, второй — по тексту запроса.
- Обновление — вручную: `bpmn-engine/scripts/update-vendor.sh <tag|commit>`
  (скачать архив, заменить `crates/`, переприменить патчи из
  `VENDORED_FROM.md`, `cargo update`). Скрипт не запускается в CI.

### 4.4 Что проверить при импорте (приложение B)

Импорт — отдельный коммит с чек-листом: сигнатуры `EngineContext`,
`BpmEngine::run_async`, имена хендлеров, `ExternalTaskCompleted`-payload,
таблицы `migrate()`, поведение `el` для строковых литералов. Всё это
проверяется компиляцией и тестами до того, как трогаем `main.rs`.

### 4.5 Что выяснилось про рантайм (влияет на дизайн)

| Факт | Где | Следствие для M1 |
| --- | --- | --- |
| `UserTaskCompleted` не двигает токен сам: он остаётся `Waiting`, а новый токен создаёт `move_token` | `token_arrived_handler.rs:111-141` | «Завершил шаг» — это отдельное событие `UserTaskCompleted`, а `TokenArrived` на `userTask` только останавливает токен |
| У шлюза условие `None` (связь без `conditionExpression`) выигрывает немедленно, до проверки остальных | `transition.rs:51-53` | схема без условий маршрутизируется по **первой** связи, а не по `default`; условия обязательны до включения графа |
| Флаг `default` компилятор читает только вместе с `conditionExpression` | `compiler.rs:826-829` | `default`-связь без условия — это `None` (безусловная связь), а не `Default`; нужен патч 3 из 4.3 |
| `gateway.rs` — вторая, неиспользуемая реализация выбора ветки | `gateway.rs:20-36`, `lib.rs:21` | из двух реализаций правим только `transition.rs`; `gateway.rs` не трогаем |
| `bpmn:serviceTask` компилируется в `ExternalTask`, а не в `ServiceTask(fn)` | `compiler.rs:866-879` | ERP-ответ можно провести через `ExternalTaskCompleted` уже в M1, не дожидаясь M2 |
| Проходные узлы (`startEvent`, шлюз) оставляют свой токен `Waiting` навсегда | `token_arrived_handler.rs:86-96`, `:190-200` | `token`-таблица — не список активных шагов; проекция фильтрует по типу узла |
| `End` удаляет токен и отдаёт только `ProcessCompleted` | `token_arrived_handler.rs:201-208` | `token_updated COMPLETED` для последнего шага синтезирует проекция |
| Ошибка EL глушится: `eval_condition(...).unwrap_or(false)` | `transition.rs:60-63` | опечатка в условии не падает, а уводит в `default`; нужна валидация схемы |
| Каждая компиляция определения утекает: `Box::leak` для `NodeId` | `compiler.rs:1001-1008`, `node.rs:169-184` | кэш скомпилированных определений обязателен, иначе утечка на каждое событие |
| `ProcessDefinitionStore::load` компилирует XML при каждом вызове | `process_def_store.rs:61-79` | кэш должен жить в нашем декораторе стора, а не в хендлерах |
| `process_store.save` переписывает таблицу `token` целиком (delete + insert) | `process_store.rs:137-193` | писать свои строки в `token` нельзя — их сотрёт следующее сохранение; read-model только в legacy-таблицах |
| `claim_token` требует `status='Ready'` и совпадения версии | `token_store.rs:157-181` | повторная доставка `TokenArrived` безвредна (CAS), но гонку загрузки инстанса CAS не закрывает |
| Пумпа возвращает `()`, список обработанных событий не отдаёт | `pump.rs:44-51` | проекция строится по снимкам «до/после», а не по возвращённым событиям |

## 5. Контракты: NATS → `EngineEvent`

### 5.1 Входящие события

| Условие в сообщении | `EngineEvent` | Комментарий |
| --- | --- | --- |
| `event_name = DeploySchema` (служебный `inst-deploy-<org>`) и есть `bpmn_xml` | нет события: `parse_and_compile` + `deploy(process_id, xml)` | деплой определения; инстанс и токены не создаются (сейчас создаётся мусорный токен `Process_Deployment`, у которого нет узла в схеме) |
| `output_data.bpmn_xml` есть, событие обычное | `ProcessStarted { process_id, instance_id, initial_variables }` | `process_id` — `def.id` из компиляции, `initial_variables` — из `output_data`; повторный старт уже существующего инстанса игнорируется с логом |
| `element_id` не пуст, схемы нет | `UserTaskCompleted { task_id, instance_id, node_id, variables }` | `node_id` — **хост-узел**, взятый из `Waiting`-токена, а не из `element_id` (см. 5.2); `task_id` — `id` того же токена (поле обязательно в payload, хендлер его не читает — `user_task_completed_handler.rs:15-44`); `variables` — из `output_data` |
| `element_id` пуст, схемы нет | событие игнорируется с логом | сейчас так же (см. `main.rs:194`) |
| `mes.in.v1.erp.response`, `status = SUCCESS` | `ExternalTaskCompleted { instance_id, token_id, node_id, variables }` | `token_id` — `Waiting`-токен узла `serviceTask`; путь включается уже в M1: иначе ERP-сценарий E2E двигал бы токены в обход движка |
| `event_id` уже обработан | событие отбрасывается | таблица `consumed_event` (см. 9.5) |
| ни одно условие | `warn` + `ack` | «пуассон»-сообщения не должны блокировать очередь |

Правила преобразования `output_data` → `variables` (движок хранит
`HashMap<String, String>`):

- значения-скаляры (`string`, `number`, `boolean`) → строка: `true` →
  `"true"`, `100` → `"100"`, `1.5` → `"1.5"`; `null` пропускается;
- вложенные объекты и массивы (`documents`, `payload`) в переменные не
  попадают, но целиком остаются в `payload` события и в WS-сообщении — UI
  ничего не теряет;
- транспортные ключи не становятся переменными: `bpmn_xml`, `org_type`,
  `token_id`;
- пустой набор — легально: шаг без ветвления;
- переменные живут в `process_instance.variables` и переживают шаги; значения
  сравниваются с литералами условий как строки (7.2).

### 5.2 `element_id` — адрес кнопки, а не маршрут

В контракте клиента `element_id` — это **целевой** узел кнопки
(`act:userAction.elementId`), а не текущий. Поэтому «сверить `element_id` с
текущим узлом» нельзя: они совпадают только у кнопки «просто завершить шаг».
Порядок обработки события завершения:

1. из `process_store` загружается экземпляр, из `token` — токены в состоянии
   `Waiting`;
2. хост-узел берётся из токена: единственный `Waiting`-токен на узле типа
   `userTask`. Если в сообщении пришёл `output_data.token_id` (новое
   необязательное поле), токен выбирается по нему — это путь для
   `parallelGateway`, где одновременно ждут два `userTask`;
3. если `Waiting`-токенов нет — `warn` и `ack`: завершать нечего (повторная
   доставка, устаревшая вкладка);
4. маршрут считает движок: `UserTaskCompleted` → `move_token` (единственная
   исходящая связь) → шлюз выбирает ветку по `process_instance.variables`;
5. после пумпы проекция сравнивает **итоговый** активный узел с `element_id`
   и пишет `info` при расхождении (`client_target_mismatch`). Это телеметрия,
   а не управление: событие от старого клиента не должно уводить токен с
   графа.

Почему в M1 нет fallback на `element_id` (расхождение с задачей дорожной
карты «если клиент прислал `next_element_id`, сохранить текущее поведение»):

- fallback возвращает ровно ту дыру, которую закрывает M1: нажатие кнопки
  снова выбирает ветку в обход шлюза (и «Нет» на пастеризации снова можно
  обойти кнопкой аварии);
- два источника истины (граф и `element_id`) дают расходящееся состояние:
  движок уже передвинул токен, а legacy-запись пишет другой узел;
- совместимость обеспечивается не смешиванием, а переключателем
  `ENGINE_ROUTING=legacy|graph`: в `legacy` работает сегодняшний код
  (`main.rs`), в `graph` — рантайм. Смешанного режима нет (10.1).

Совместимость при этом проверяема: после добавления условий (приложение A)
целевой узел каждой кнопки совпадает с тем, что выбирает граф. Это
утверждение — тест SDK (`dairyPlant.test.js`), а не обещание: для каждого
шлюза он подставляет переменные кнопок и сверяет `elementId` с ожидаемой
веткой.

### 5.3 Исходящие события: проекция по снимку

Пумпа не возвращает обработанные события (`pump.rs:44-51`), а часть
переходов вообще не порождает `TokenArrived` (например, `End` отдаёт только
`ProcessCompleted`). Поэтому проекция не «пересказывает» события, а считает
дифф снимков `process_instance`:

- **до** пумпы: токены и их `status` (нужны также для поиска хоста);
- **после** пумпы: токены, `variables`, `state` экземпляра;
- активный шаг — `Waiting`-токен на узле `userTask` или `externalTask`
  (проходные `startEvent`/шлюзы тоже остаются `Waiting`, но шагом не
  являются — см. 4.5);
- токен, который был активным шагом и исчез или перестал быть `Waiting`,
  закрывается как `COMPLETED`.

| Что в снимке | В NATS `mes.bpmn.v1.event.<instance>` | В legacy read-model |
| --- | --- | --- |
| новый активный шаг | `{event: "token_updated", state: "ACTIVE", token_id, element_id, payload}` | `tokens` upsert `ACTIVE` |
| шаг перестал быть активным | `{event: "token_updated", state: "COMPLETED", token_id, element_id}` | `tokens.state = COMPLETED` |
| `process_instance.state = Completed` | `{event: "process_completed", instance_id}` | `process_instances.status = COMPLETED` |
| `state = Terminated` (M3+) | `{event: "process_terminated", instance_id}` | `process_instances.status = TERMINATED` |
| ошибка обработки (ядро вернуло `TokenFailed`) | `{event: "token_updated", state: "FAILED", error_message}` | `tokens.state = FAILED` |
| история (`HistoryHandler`) | не публикуется | `history_event` (вендорная таблица) |


## 6. Модель данных и двойная запись

После M1 в базе живут два семейства таблиц:

| Семейство | Таблицы | Кто пишет | Зачем |
| --- | --- | --- | --- |
| движок (источник истины) | `process_definition`, `process_instance`, `token`, `external_task`, `timer`, `history_event`, `parallel_join`, `compensation_record`, `event_outbox`, `dead_letter` | вендорные сторы через `EngineContext` | граф, токены, переменные, конкурентный доступ |
| legacy read-model | `process_definitions`, `process_instances`, `tokens`, `execution_history` | `runtime::project` (+ существующий `db.rs` в `legacy`-режиме) | кабинет, E2E-тесты, SLA-воркер, обратная совместимость |
| служебная | `consumed_event` | `runtime::pump` | идемпотентность доставки |

Правила:

1. **`token` не трогаем руками.** `process_store.save` делает
   `DELETE FROM token WHERE instance_id = $1` и вставляет токены из памяти
   (`process_store.rs:137-193`); любая наша строка в `token` будет стёрта
   следующим сохранением. Всё, что нужно снаружи, пишется в legacy `tokens`.
2. **Legacy пишется только проекцией**, одним проходом после пумпы: активные
   шаги, закрытые шаги, статус экземпляра. Никаких «промежуточных» записей из
   хендлеров.
3. **`process_instances.business_key` остаётся `NULL`.** `correlation_key`
   генерируется клиентом заново на каждый запрос
   ([`httpClient.js:160`](../erp-simulator/src/bpmn-sdk/client/httpClient.js)),
   поэтому бизнес-ключом не является; его появление — M4 (заказы на сервере).
4. **`execution_history` не пишется.** Аудит — `history_event` вендорного
   `PostgresHistoryRepo` (включаем `HistoryHandler`); legacy-таблица остаётся
   пустой, как и сегодня.
5. **`process_definitions` продолжает заполняться** при деплое: E2E
   `tests/test_workflow.py` проверяет строку по `PROCESS_DEF_ID`, а схема
   таблицы (id, name, bpmn_xml) совместима с текущим `db::save_process_definition`.

Соответствие колонок:

| Движок | Legacy | Правило |
| --- | --- | --- |
| `process_instance.id` | `process_instances.id` | 1:1, `instance_id` из NATS-сообщения |
| `process_instance.state` (`Running`/`Completed`/`Terminated`) | `process_instances.status` (`ACTIVE`/`COMPLETED`/`TERMINATED`) | `Running` → `ACTIVE`; пишется вместе с шагами |
| `process_instance.process_def_id` | `process_instances.definition_id` | `def.id` из компиляции |
| `token.node_id` при `status='Waiting'` и типе узла `userTask`/`externalTask` | `tokens.element_id`, `state='ACTIVE'` | только эти токены — «шаги»; проходные не проецируются |
| токен закрыт (нет в снимке / уже не `Waiting`) | `tokens.state='COMPLETED'` | закрываются только ранее активные шаги |
| `token.id` (UUID) | `tokens.id` | legacy PK — `"<instance>:<element>"` (как `db::save_token_state`), поэтому в WS отдаём и `token_id`, и `element_id` |

Схема новой таблицы (миграция нашего крейта, `sqlx::migrate!` уже
применяется на старте, `main.rs:31-33`):

```sql
CREATE TABLE IF NOT EXISTS consumed_event (
    event_id    TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_consumed_event_instance
    ON consumed_event(instance_id);
```

`event_id` — то, что шлюз кладёт в сообщение (`event_id: correlation_key`,
[`http-cluster/index.js:124-129`](../bpmn-gateways/http-cluster/index.js)).
Клиент генерирует его заново на каждый HTTP-запрос, поэтому таблица защищает
от повторной доставки JetStream (потеря ack, рестарт пода), а не от двойного
клика. Это осознанно: двойной клик — UX-проблема кнопки, а не транспорта.

## 7. Шлюзы и условия

### 7.1 Как движок выбирает ветку

Живая реализация — `evaluate_exclusive_gateway`
(`crates/runtime/src/transition.rs:46-68`), вызывается из
`TokenArrivedHandler` (`token_arrived_handler.rs:190-200`). Алгоритм:

```text
default_target = None
для каждой исходящей связи в порядке объявления в <process>:
    condition == None          -> вернуть эту связь сразу
    condition == Default       -> default_target = связь (продолжаем)
    condition == VariableEq    -> если переменная == значению, вернуть связь
    condition == Expression    -> если eval_condition(expr) == true, вернуть связь
вернуть default_target (или ничего — токен остаётся на шлюзе)
```

Следствия, которые важны для схемы:

1. **Связь без условия выигрывает всегда** и раньше любых условий. В текущей
   демо-схеме условий нет вообще, поэтому включение графа «как есть» увело бы
   `Gateway_Stock` в `Task_CreatePO` (первая связь `SF5` «Нет»), а
   `Gateway_Quality` — в `Task_StoreMilk` (первая `SF14`). Порядок выкатки из
   10.1 это учитывает.
2. `default` — не «единственная безусловная связь», а отдельный флаг. Парсер
   помечает связь `is_default` по атрибуту
   `<bpmn:exclusiveGateway default="SF5">` (`parser.rs:127-136`) или по
   `name="default"` у связи (`parser.rs:444-448`; bpmn-js так не пишет, но
   движок понимает оба способа). В `EdgeCondition::Default` флаг превращает
   компилятор — **только если у связи есть `conditionExpression`**
   (`compiler.rs:826-829`; пустое `${}` тоже даёт `Default`,
   `compiler.rs:848-849`). Без условия в графе остаётся `None`, и к
   default-связи применяется п. 1 — то есть она работает как безусловная и
   выигрывает у всех условий выше неё. Асимметрию снимает патч 3 из 4.3; без
   него default-связь обязана быть последней в `<process>` (п. 3).
3. Побеждает **первое** совпадение, порядок — порядок элементов
   `<bpmn:sequenceFlow>` в `<process>` (обход `model.sequence_flows`,
   `parser.rs:157-164`), а не порядок `<bpmn:outgoing>` внутри шлюза. Поэтому
   условия обязаны быть взаимоисключающими: одна переменная, разные значения.
4. `EdgeCondition::VariableEq` компилятор не создаёт (в `compiler.rs` его нет),
   так что в BPMN-схеме это состояние недостижимо.
5. Если ни одна связь не подошла и `default` нет, токен остаётся `Waiting` на
   шлюзе, и процесс «висит» без ошибки. Компилятор такое почти не пропускает
   (`compiler.rs:198-233`: больше одного default — ошибка; есть условия, но
   нет default — ошибка), но схема «все связи безусловные» компилируется и
   молча берёт первую.

### 7.2 Грамматика условий (EL вендора)

`${...}` снимает компилятор: `parse_condition` отрезает `${` и `}` и
складывает остаток в `EdgeCondition::Expression`
(`compiler.rs:838-853`). В XML обёртка обязательна для bpmn-js
(`xsi:type="bpmn:tFormalExpression"`), движку достаётся уже голое выражение.

Что реально поддерживает `eval_condition` (`el.rs:13-62`):

| Форма | Пример | Поведение |
| --- | --- | --- |
| `переменная == 'литерал'` | `${stockSufficient == 'true'}` | строковое равенство; литерал в одинарных или двойных кавычках |
| `!=` | `${packOutcome != 'crash'}` | строковое неравенство |
| `>`, `>=`, `<`, `<=` | `${temperature > 85}` | обе стороны парсятся как `f64`; иначе `false` (ошибка глушится) |
| `and`, `or` | `${labPassed == 'true' and temperature > 80}` | только с пробелами вокруг оператора, без скобок; `or` разбирается раньше `and` |
| голая переменная | `${labPassed}` | `true`, если значение непустое — **`'false'` тоже `true`**, использовать нельзя |
| `not`, скобки, арифметика, функции | — | не поддерживаются; выражение вернёт ошибку → связь считается ложной |

Правила операндов:

- **слева — только имя переменной.** Литерал слева (`'true' == x`) уйдёт в
  поиск переменной с именем `'true'`, найдёт пустую строку и сравнит с ней;
- **справа** в кавычках — литерал; без кавычек движок сначала ищет переменную
  с таким именем и, если её нет, сравнивает с самим текстом. Поэтому
  `${x == true}` без кавычек формально работает, но читается как «сравнить с
  переменной `true`» — в схемах пишем кавычки;
- отсутствующая переменная — пустая строка: `${x == 'true'}` даёт `false`,
  `${x != 'true'}` — `true`. Это и есть «fail-closed» при `default` на
  негативной ветке;
- ошибка вычисления (нечисловое сравнение, `not`, скобки) не логируется и не
  роняет процесс: `transition.rs:60-63` делает `unwrap_or(false)`. Отсюда
  требование к валидатору схемы (7.4).

### 7.3 Правила схемы для шлюзов

Их проверяет SDK при деплое и Rust-тест на демо-схеме:

1. У каждого `exclusiveGateway` **ровно одна** связь с `default`
   (`default="<flowId>"` на шлюзе), и она ведёт в «безопасную» ветку.
   `conditionExpression` на default-связи не пишем: флаг и есть её условие
   (патч 3 из 4.3), текст условия компилятор всё равно проигнорирует;
2. **все остальные** исходящие связи имеют `conditionExpression`; связь без
   условия запрещена (7.1, п. 1);
3. условия взаимоисключающие; на одну переменную — один оператор `==` с
   разными литералами, либо ортогональные переменные;
4. `userTask`/`task` имеет **одну** исходящую связь. `move_token` создаёт по
   токену на каждую связь (`transition.rs:21-26`), то есть задача с двумя
   исходящими — это неявный AND-split без слияния; развилка должна быть
   шлюзом;
5. переменные, которые читают условия, объявляются кнопками входного шага
   (8.2): у каждой переменной есть хотя бы одна `act:userAction`, которая её
   выставляет. Иначе ветка недостижима, и это ошибка схемы, а не «значение по
   умолчанию».

### 7.4 Что добавлено в валидатор SDK

`validateEngineSchema` (`bpmnXml.js:1900`) уже проверяет форму шлюзов и
наличие `default`. M1 добавляет проверки смысла — без них опечатка в условии
не падает, а молча уводит в `default`:

| Ключ → значение `SCHEMA_PROBLEM` | Когда | Почему |
| --- | --- | --- |
| `CONDITION_UNSUPPORTED` → `ConditionUnsupported` | текст `conditionExpression` не разбирается грамматикой 7.2 (скобки, `not`, литерал слева, `=` вместо `==`) | движок вернёт `false` без сообщения |
| `GATEWAY_DEFAULT_FLOW_WITH_CONDITION` → `GatewayDefaultFlowWithCondition` | у связи с `default` есть `conditionExpression` | `parse_condition` вернёт `Default` раньше разбора текста (`compiler.rs:838-841`), то есть условие не действует, а автор считает, что задал его |
| `GATEWAY_FLOW_WITHOUT_CONDITION` → `GatewayFlowWithoutCondition` | у `exclusiveGateway` есть исходящая связь без условия и без `default` | такая связь выигрывает у всех условий (7.1) |
| `GATEWAY_UNKNOWN_VARIABLE` → `GatewayUnknownVariable` | условие читает переменную, которой не выставляет ни одна `act:userAction` процесса | ветка недостижима; кнопка «Нет» будет молча ехать в `default` |
| `ACTION_VARIABLE_FORMAT` → `ActionVariableFormat` | `variables` кнопки не `ключ=значение` (пустой ключ, `;`/`=` в значении, пробелы в ключе) | строка не разложится в переменные |
| `TASK_MULTIPLE_OUTGOING` → `TaskMultipleOutgoing` | у задачи больше одной исходящей связи | неявный AND-split без слияния (7.3, п. 4) |

Соглашение по именам — как в существующем перечислении
(`bpmnXml.js:1793-1808`): ключ `SCREAMING_SNAKE`, значение `PascalCase`;
там, где у движка есть `ErrorCode`, значение совпадает с ним.

Реализация опирается на уже существующий разбор DOM: `readFlowElement`
(`bpmnXml.js:1874-1885`) отдаёт `hasCondition` и `isDefault` — к ним
добавляется `conditionText` (текст `conditionExpression`), без него
`CONDITION_UNSUPPORTED` не проверить; `listActions` (`:914-924`) даёт кнопки и
их `variables`, `getOutgoingTargetIds` (`:706`) — число исходящих связей, а
`defaultFlowId` уже читается у узла (`:1969`), так что связь «по умолчанию»
находится без нового обхода.
Ошибки уровня проекта собирает `validateProjectSchema` (`:2381`), поэтому
деплой демо-проекта проверяется целиком, а не по одной схеме.

## 8. Человеческие шаги и переменные

### 8.1 Расширение `act:userAction/@variables`

Новое необязательное строковое поле кнопки. Формат — пары
`ключ=значение` через `;`:

```xml
<act:userAction id="haveStock" label="ГП хватает — отгрузить со склада"
                role="SALES" icon="Truck" elementId="Task_Ship"
                variables="stockSufficient=true" />
<act:userAction id="needPurchase" label="ГП не хватает — закупить сырьё"
                role="SALES" icon="ShoppingCart" elementId="Task_CreatePO"
                variables="stockSufficient=false" />
```

Правила:

- ключ — `[A-Za-z_][A-Za-z0-9_]*`; значение — строка без `;`, `=` и переводов
  строки; пробелы по краям пар обрезаются (`stockSufficient = true` тоже
  читается);
- значения всегда строки, как их видит EL (`true`, `false`, `degraded`,
  `9.5`); приведение к числу делает только сравнение `>`/`<`;
- пустое значение (`note=`) — легально, это пустая строка, а не «нет
  переменной»;
- повтор ключа в одной кнопке — ошибка `SCHEMA_PROBLEM.ACTION_VARIABLE_FORMAT`
  (`ActionVariableFormat`, 7.4);
- поле добавляется в `ACTION_FIELDS` (`actionModdle.js:91-101`), и тогда
  чтение (`actionFromNode`, `actionFromAttributes`), запись
  (`buildActionTag`) и `normalizeAction` подхватывают его автоматически;
  инспектор кнопки получает ещё один input без правок UI.

Клиент при нажатии **добавляет объявленные переменные в `output_data`** того
же события:

```js
const payload = { ...eventPayload, ...parseActionVariables(action.variables) };
await client.sendEvent(instanceId, action.event, action.elementId, payload);
```

`parseActionVariables` — новая функция SDK рядом с `resolveAction*`
(`xml/bpmnXml.js`): разбирает строку по правилам выше и возвращает объект;
пустая строка — `{}`, ошибка формата — исключение (её же ловит валидатор,
7.4, чтобы схема не падала в рантайме).

Так «кнопка» и «значение переменной» — одно объявление в схеме: UI не хранит
таблицу соответствий, а движок получает ровно то, что нарисовано.

### 8.2 Кто какие переменные выставляет

Полная таблица — приложение A. Кратко:

| Входной шаг | Кнопки → значения | Шлюз |
| --- | --- | --- |
| `Task_CheckStock` | `haveStock` → `stockSufficient=true`; `needPurchase` → `false` | `Gateway_Stock` |
| `Task_AnalyzeMilk` | `qualityPassed` → `labPassed=true`; `qualityRejected` → `false` | `Gateway_Quality` |
| `Tech_Pasteurize` | `pasteurizeDone` → `pasteurizeOk=true`; `pasteurizeCrash` → `false` | `Gateway_PasteurizeOK` |
| `Tech_CoolCheck` | `techQCPassed` → `techQcPassed=true`; `techQCFailed` → `false` | `Gateway_TechQC` |
| `Tech_Pack` | `packCompleted` → `packOutcome=good`; `packDegraded` → `degraded`; `packCrashed` → `crash` | `Gateway_PackOutcome` |

### 8.3 Что происходит при нажатии кнопки

```text
клик «ГП не хватает» (needPurchase)
  output_data = { orderNum, stockSufficient: "false" }
        │ POST /instances/<id>/events → NATS mes.in.v1.event.erp.CompleteBlock
        ▼
runtime::mapper
  instance = process_store.load(instance_id)        // нет → warn + ack
  if event_id ∈ consumed_event → ack (повтор)
  host = единственный Waiting-токен типа userTask   // или output_data.token_id
        ▼
EngineEvent::UserTaskCompleted {
  task_id: host.id, instance_id, node_id: host.node_id, variables }
        ▼
runtime::pump (advisory lock, снимок «до»)
  UserTaskCompletedHandler (патч): merge vars → remove token → move_token_preserving_group
  TokenArrivedHandler (Gateway_Stock): Expression(SF6) = false → Default(SF5) (флаг default, патч 3)
  TokenArrivedHandler (Task_CreatePO): status = Waiting
        ▼
runtime::project (снимок «после»)
  diff: Task_CheckStock → COMPLETED, Task_CreatePO → ACTIVE
  legacy tokens upsert; WS token_updated {payload: output_data, token_id}
```

Ключевое: `Gateway_Stock` не появляется в WS и в `tokens` — шлюз проходной.
Клиент видит один переход «старый шаг закрыт, новый открыт», как и раньше,
но выбрал его граф.

## 9. Рантайм: модули и механика

### 9.1 Раскладка

```text
bpmn-engine/src/
├── main.rs                 // NATS-циклы, выбор режима legacy|graph
├── db.rs                   // legacy-запись (как есть: save_token_state,
│                           //   complete_active_tokens, update_process_instance_status)
└── runtime/
    ├── mod.rs              // публичные entry points: deploy, start, complete, erp
    ├── mapper.rs           // InboundEvent/ErpResponseEvent → EngineEvent
    ├── store.rs            // EngineContext, CachedDefStore, миграция consumed_event
    ├── pump.rs             // advisory lock, сборка BpmEngine, снимки до/после
    ├── project.rs          // diff снимков → legacy tokens, WS-события
    └── vars.rs             // flatten(output_data), parse action variables (Rust-тест)
```

Границы: `main.rs` знает только `runtime::{deploy, start, complete, erp_response}`;
типы `bpm_engine_*` не выходят за `runtime/`.

### 9.2 Пумпа и контекст

```rust
fn engine() -> BpmEngine {
    BpmEngine::new(vec![
        Box::new(ProcessStartHandler),
        Box::new(TokenArrivedHandler::new()),
        Box::new(UserTaskCompletedHandler),
        Box::new(ExternalTaskCompletedHandler),
        Box::new(ProcessCompletedHandler),
        Box::new(HistoryHandler),          // no-op без history_repo
    ])
}
```

`HistoryHandler` без `ctx.history_repo` сразу возвращает пусто
(`history_handler.rs:124-126`), поэтому список хендлеров от режима аудита не
зависит: аудит включается только наличием `PostgresHistoryRepo` в контексте.

Контекст собирается один раз на процесс (`EngineContext::builder`):

- обязательные: `PostgresProcessStore`, `PostgresTokenStore`,
  `CachedDefStore` (обёртка над `PostgresProcessDefStore`);
- опциональные для M1: `PostgresHistoryRepo` (аудит). `external_task_store`,
  `timer_store`, `outbox`, `compensation` не включаются — их время придёт в
  M2/M3/M5; обработчик `ExternalTaskCompleted` их и не требует
  (`external_task_completed_handler.rs:32-42`). **`PostgresParallelJoinRepo`
  тоже не подключается**: его `ensure_group` вставляет колонку `id`, которой
  нет в таблице `parallel_join` (`migrate`, `adapters/postgres/src/lib.rs`), а
  `try_join` без строки возвращает `true` — ветка «слилась» бы сразу, не
  дождавшись второй. Слияние держится на in-memory состоянии
  `TokenArrivedHandler` (ограничение 13);
- пул: адаптер принимает `deadpool_postgres::Pool` (`create_pool`,
  `adapters/postgres/src/lib.rs:62-80`), а приложение уже использует `sqlx`.
  В M1 живут **два пула к одной БД**: `deadpool` — для движка, `sqlx` — для
  `db.rs`, `consumed_event` и проекции. Переписывать `db.rs` на
  `tokio-postgres` ради одного пула не нужно: общей транзакции между движком и
  read-model всё равно нет (6), а оба пула берут URL из одного `DATABASE_URL`.

### 9.3 Конкурентность

В кластере `replicas: 3` ([`bpmn-engine-deployment.yaml:9`](../bpmn-engine-deployment.yaml)),
все поды поднимают один и тот же durable-консьюмер `engine-inbound-consumer`
(`nats_client.rs:77`), поэтому каждое сообщение достаётся **одному** поду, но
два клика по одному экземпляру могут попасть в разные поды и обработаться
одновременно. Вендорный `process_store.save` — upsert без проверки версии
(`process_store.rs:137-166`), `instance.version` никто не инкрементирует,
поэтому гонка приводит к потерянному обновлению: один под перезапишет токены
и переменные другого.

M1 сериализует обработку **по экземпляру** advisory-блокировкой PostgreSQL.
Реализация (`runtime::pump::lock_instance`) берёт её на транзакции sqlx-пула:

```rust
let mut tx = pool.begin().await?;                     // sqlx-пул
tx.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", ...).await?;

let result = pump_and_project(instance_id, event).await;

tx.commit().await?;                                   // блокировка снимается
```

- блокировка берётся **до** загрузки снимка «до» и держится до записи
  read-model;
- выбран `pg_advisory_xact_lock` на sqlx-соединении, а не `pg_advisory_lock` на
  соединении движка: сессионная блокировка «зависает» на коннекте, если тот
  вернётся в пул без `unlock`, а пумпа сама занимает коннекты `deadpool` под
  сторы. Advisory-блокировка видна любой сессии той же БД, поэтому второй пул
  её не обходит;
- под, упавший с блокировкой, теряет сессию — PostgreSQL отпускает её сам;
- основной цикл обрабатывает сообщения последовательно (без `tokio::spawn` на
  сообщение), поэтому внутри пода порядок сохраняется, а между подами его
  задаёт JetStream;
- разные экземпляры не мешают друг другу: ключ — `hashtextextended(instance_id)`.

Цена: один дополнительный коннект на под и ожидание на горячем экземпляре.
Для демо это дешевле, чем патчить все `save` вендора на optimistic locking
(это отдельный кандидат в апстрим, 12, п. 3).

### 9.4 Кэш определений

`PostgresProcessDefStore::load` делает `SELECT bpmn_xml` + полную компиляцию
на **каждый** вызов (`process_def_store.rs:61-79`), а хендлеры вызывают его на
каждое событие. Компиляция при этом утекает: `NodeId` — `&'static str`,
строки создаются через `Box::leak` (`compiler.rs:1001-1008`, комментарий в
`node.rs:169-184`). При лимите пода 128 MiB это не «стиль», а OOM.

`runtime::store::CachedDefStore` реализует `ProcessDefinitionStore` сам:

```text
load(id):
    xml = SELECT bpmn_xml FROM process_definition WHERE id = $1   // без компиляции
    hash = DefaultHasher(xml)
    if cache[id].hash == hash: return clone(def)
    def = parse_and_compile(&xml)?
    cache.insert(id, (hash, def.clone()))       // не больше 64 записей
    return def
```

- компиляция происходит только при первом обращении и при **изменении XML**;
- проверка хеша на каждом `load` делает кэш корректным между подами: под,
  который не делал деплой, увидит новую схему на следующем событии, а не
  после рестарта;
- `deploy()` по-прежнему вызывает `PostgresProcessDefStore::deploy`
  (ингерентный метод, не часть трейта — `ProcessDefinitionStore` умеет только
  `load`/`list_versions`/`get_active`/`activate`/`deprecate`,
  `storage/src/process_store.rs:96-112`);
- `list_versions`/`get_active`/`activate`/`deprecate` делегируются вендорному
  стору: версионирование в M1 не используется (деплой — upsert по `def.id`).

### 9.5 Идемпотентность и подтверждения

1. Перед пумпой: `SELECT 1 FROM consumed_event WHERE event_id = $1`. Есть
   строка — событие уже обработано, `ack` и выход.
2. Обработка под advisory-блокировкой (9.3).
3. После успешной проекции: `INSERT ... ON CONFLICT (event_id) DO NOTHING` и
   только затем `ack`. Транзиентная ошибка БД/NATS — `nak` с задержкой
   (JetStream доставит повторно); ошибка компиляции схемы — `ack` + `error!`
   (повтор не поможет; DLQ — M7).

Остаточное окно: под упал между записью инстанса и `INSERT` в
`consumed_event`. Повторная доставка тогда снова запустит пумпу. Для
`userTask` это чаще всего безопасно: после первого прохода `Waiting`-токена
на узле уже нет, маппер отбросит событие (5.2, п. 3). Для `ExternalTaskCompleted`
защита — поиск токена по `token_id`: его тоже уже нет. Полностью закрывается
только транзакцией «состояние + consumed_event», а её нет в вендорных сторах;
это цена M1, зафиксирована в 12, п. 4.

### 9.6 Ошибки и наблюдаемость

Логи — `tracing` вендора плюс наши события (структурированные поля
`instance_id`, `element_id`, `token_id`, `event`):

| Событие | Уровень | Когда |
| --- | --- | --- |
| `compile_failed` | `error` | схема не компилируется (деплой или старт) |
| `unknown_instance` | `warn` | событие по экземпляру, которого нет |
| `no_waiting_token` | `warn` | завершать нечего (повтор, чужая вкладка) |
| `duplicate_event` | `info` | `consumed_event` сработал |
| `client_target_mismatch` | `info` | `element_id` не совпал с итоговым активным узлом |
| `gateway_no_match` | `warn` | шлюз не выбрал ветку (токен остался `Waiting`) |
| `lock_wait` | `debug` | ожидание advisory-блокировки дольше 1 с |

Клиенту ошибки не уходят: в M1 нет канала `process_failed` (WS-контракт знает
только `token_updated`/`process_completed`). Симптом для пользователя —
«шаг не переключился»; диагностика — логи пода. Канал ошибок и DLQ — M6/M7.

## 10. Выкатка и совместимость

### 10.1 Порядок шагов

Переключатель — переменная окружения `ENGINE_ROUTING`:

- `graph` (**по умолчанию**) — включается `runtime::*`, `main.rs` только
  маршрутизирует сообщения в `runtime`: токен ведёт движок по графу схемы;
- `legacy` — аварийный откат на время выкатки: работает прежний `main.rs`,
  токен ставится по `element_id`, шлюзы не участвуют.

Пустое или неизвестное значение читается как `graph` (`Routing::parse`): флаг
не может незаметно выключить оркестратор из-за опечатки в манифесте.

| Шаг | Что выкатывается | Почему в этом порядке |
| --- | --- | --- |
| 0 | vendor (с патчами 4.3) + `runtime` (за флагом `legacy`), миграция `consumed_event` | код не меняет поведение; проверяются компиляция, кэш, пумпа на тестах |
| 1 | SDK: `variables` у кнопки, новые проверки валидатора; демо-схема: условия + ветка «Нет» пастеризации; клиент: переменные в `output_data`, один стартовый event (10.2, п. 1–2) | `legacy` игнорирует условия и `variables`, поэтому шаг безопасен; но к моменту включения графа схема и клиент уже готовы |
| 2 | `ENGINE_ROUTING=graph` на одном поде/стенде, затем на всех — **выполнено**: `graph` стал значением по умолчанию | включение графа на схеме без условий увело бы `Gateway_Stock` в `Task_CreatePO` (7.1, п. 1) |
| 3 | удаление legacy-ветки `main.rs`, `db::complete_active_tokens`, `_main_tmp.rs` | после того как граф отработал E2E и демо-сценарий; задача M2 |

Флаг выставляется в манифесте [`bpmn-engine-deployment.yaml`](../../bpmn-engine-deployment.yaml)
(`env: ENGINE_ROUTING`, значение `graph`): откат — правка одного значения на
`legacy` и `kubectl apply -f bpmn-engine-deployment.yaml` (затем
`kubectl rollout status deploy/bpmn-engine-deployment`). Значение читает
`Routing::from_env()` в `main.rs` и печатает режим в лог при старте
(`🔀 Маршрутизация событий: ...`) — по этой строке проверяют, что под уже в
`graph`.

При включении `graph` (22.10.2026) всплыли два дефекта вендорного
Postgres-адаптера, которых не видит юнит-прогон вендора (в нём нет живой БД):
индекс `migrate` строился по несуществующей колонке `state` (патч 4), а `SELECT`
в `PostgresProcessStore::load` не выбирал `parent_instance_id`/`parent_token_id`,
которые тут же читаются из строки (патч 5). Второй ронял под паникой сразу после
записи `Ready`-токена, поэтому старт процесса «замирал» на `StartEvent`. Разбор
патчей — в
[`vendor/bpm-engine/VENDORED_FROM.md`](../vendor/bpm-engine/VENDORED_FROM.md);
DDL дополнительно сериализуется advisory-блокировкой в
`GraphStores::ensure_schema`: три реплики поднимают `migrate` одновременно.

Ограничения шага 1:

- **Кэш схемы в браузере.** `bpmn_xml` приходит из проекта в `localStorage`;
  старый клиент после шага 2 задеплоит старую схему без условий и перезапишет
  `process_definition` (upsert по `def.id`). Автоматически поднимать версию
  проекта в шаге 1 нельзя: сохранённая копия — это правки пользователя, и
  стирать их при обновлении незачем. Устаревшую схему ловит новый валидатор:
  `GatewayFlowWithoutCondition` попадёт в `problems`, и клиент покажет причину
  вместо старта «на глазок»; актуальную схему возвращает кнопка «Демо-схема».
  Риск «старая схема у пользователя» (дорожная карта, огр. 10) закрывается
  серверным версионированием определений — M4.
- **Версия процесса.** Меняется граф — меняется `bpmn:process/@id`
  (`Process_Dairy` → `Process_Dairy_v2`), иначе старые и новые экземпляры
  делят одно определение, а старый клиент может его перезаписать. Полное
  версионирование — M4; в M1 достаточно переименования при изменении графа.

Откат: `ENGINE_ROUTING=legacy` + рестарт подов. Вендорные таблицы остаются,
legacy-путь снова пишет `tokens` по `element_id`; экземпляры, начатые в
`graph`, продолжаются кнопками (маршрут снова клиентский). Данные не
удаляются, поэтому откат обратим.

Во время rolling update поды работают в разных режимах: сообщение обработает
тот под, который его получил. Для демо это допустимо (секунды), но
переключать флаг на всех подах лучше одновременно — `kubectl rollout restart`
после смены env.

### 10.2 Что меняется в клиенте

1. **Старт — одно событие.** *(реализовано, шаг 1)* Прежде `startProcess` слал
   `StartProcess` (узел — стартовое событие) и сразу второй `CompleteBlock`
   (узел — первый шаг), потому что legacy-движок не двигает токен сам. В M1
   второй event завершил бы первый человеческий шаг, который движок только что
   активировал. Теперь `startProcess` в [`App.jsx`](../erp-simulator/src/App.jsx)
   отправляет **один** `StartProcess` с `element_id` = **первый шаг**: legacy
   поставит туда токен, граф сам дойдёт от `startEvent` до того же узла. Узел
   выводит схема (`act:userAction/@elementId`, иначе — цель единственной
   исходящей связи стартового события); если узел не выводится, событие не
   отправляется, а причина уходит в ошибку шага и журнал.
2. **Переменные кнопки.** *(реализовано, шаг 1)*
   `{...payload, ...parseActionVariables(action.variables)}` в `output_data`
   каждого действия (8.1); неразобранный JSON `variables` — ошибка шага, а не
   «пустые значения».
3. **Финиш процесса.** Обработать `{event: "process_completed"}`: закрыть
   запуск (`completeRun`) и перевести заказ в `COMPLETED`. Сейчас клиент знает
   только `token_updated` (`constants.js:102`, разбор — `tokens.js:52-70`), и
   `parseTokenMessage` отбросит сообщение без `element_id`, так что нужна
   отдельная ветка. События `ACTIVE` на `endEvent` больше не будет, а
   `isProcessFinished` (App.jsx:323) остаётся только для старых экземпляров и
   для схемы «клиент закрывает сам».
4. **Никакой арифметики маршрута.** `resolveActionTarget`/`getNextElementId`
   остаются для разметки кнопки и для `element_id` (телеметрия расхождения),
   но клиент не должен «додумывать» ветку: если движок выбрал другой узел,
   подсветка приходит из `token_updated`.
5. **`token_id` (необязательно, для параллели).** Если в `token_updated` есть
   `token_id`, клиент может вернуть его в `output_data.token_id` при
   завершении шага. Без этого параллельные ветки, стоящие на разных узлах,
   работают, а на одном узле — нет (12, п. 6).

### 10.3 Изменения демо-схемы (один коммит)

**Реализовано (шаг 1).** Схема ниже — текущее состояние `dairyPlant.js`:
условия проверяет `validateEngineSchema` (7.4), маршрут кнопок —
`dairyPlant.test.js` (`dairyPlant: маршрут кнопок через шлюзы`).

Во всех пяти шлюзах `default` уже стоит, но указывает на «Да»-ветку
(`Gateway_Stock default="SF6"`, `Gateway_Quality default="SF14"`,
`Gateway_PasteurizeOK default="SF_T5"`, `Gateway_TechQC default="SF_T8_Yes"`,
`Gateway_PackOutcome default="SF18_ToWarehouse"`), а `conditionExpression` нет
ни на одной связи. Поэтому правка — не добавление атрибута, а его **перенос**
на «Нет»-ветку плюс условие на «Да»:

- `xmlns:xsi` в `<bpmn:definitions>` процесса завода (нужен для
  `xsi:type="bpmn:tFormalExpression"` у условий);
- `Gateway_Stock`: `default="SF6"` → `default="SF5"`, условие на `SF6`;
- `Gateway_Quality`: `default="SF14"` → `default="SF15"`, условие на `SF14`;
- `Gateway_PasteurizeOK`: `default="SF_T5"` → `default="SF_T5_No"`, условие на
  `SF_T5`, новая связь `SF_T5_No` → `Task_ERP_CriticalAlert` (плюс
  `<bpmn:outgoing>` у шлюза, `<bpmn:incoming>` у задачи и `bpmndi:BPMNEdge`,
  иначе bpmn-js не нарисует связь);
- `Gateway_TechQC`: `default="SF_T8_Yes"` → `default="SF_T8_No"`, условие на
  `SF_T8_Yes`;
- `Gateway_PackOutcome`: `default="SF18_ToWarehouse"` → `default="SF_T_Err2"`,
  условия на `SF18_ToWarehouse` и `SF_T_Esc1`;
- `variables` у 11 кнопок (`haveStock`, `needPurchase`, `qualityPassed`,
  `qualityRejected`, `pasteurizeDone`, `pasteurizeCrash`, `techQCPassed`,
  `techQCFailed`, `packCompleted`, `packDegraded`, `packCrashed`).

После этого «Да» каждой кнопки ведёт в тот же узел, что и `elementId` кнопки,
а «Нет» — в негативную ветку. Тест SDK проверяет это для всех пяти шлюзов.

Порядок важен: `SF5` «Нет» объявлена в `<process>` **до** `SF6` «Да»
(`dairyPlant.js:476-477`), а `default`-связь без `conditionExpression`
компилируется в безусловную (`None`, 7.1 п. 2). Поэтому схему включаем только
вместе с патчем 3 из 4.3; без него `Gateway_Stock` уводил бы в `Task_CreatePO`
при любом решении, и `haveStock=true` никогда не доезжал бы до `Task_Ship`.

## 11. Тесты

| Уровень | Что проверяем | Где |
| --- | --- | --- |
| Rust unit | `eval_condition`: кавычки/без, `and`/`or`, отсутствующая переменная, нечисловое сравнение, голая переменная; `parse_condition` снимает `${}` | `crates/runtime/src/el.rs` (уже есть) |
| Rust unit | `output_data` → `variables`: скаляры → строки, `null`/вложенные объекты/`bpmn_xml`/`token_id`/`org_type` отбрасываются | `runtime/vars.rs` (реализовано) |
| Rust unit | шлюз: `default` при `false` (в т.ч. default-связь **без** `conditionExpression` — после патча 3), первая связь без условия выигрывает, порядок связей | `runtime/pump.rs` тесты на memory-сторах, `bpmn-engine/tests/vendor_patches.rs` |
| Rust unit | проекция: старт активирует ожидающие шаги, движение токена закрывает прежний шаг и активирует новый, `End` закрывает последний шаг, повторная доставка пуста | `runtime/project.rs` (реализовано) |
| Rust unit | кэш определений: сто `load` → одна компиляция, изменение XML → перекомпиляция, предел 64 записи | `runtime/store.rs` (реализовано) |
| Rust unit | `ENGINE_ROUTING`: `graph` (в любом регистре) включает граф, всё остальное — `legacy` | `runtime/mod.rs` (реализовано) |
| Rust unit | `default` без условия (патч 3): `parse_and_compile` даёт `Some(Default)` на default-связи и `Some(Expression)` на остальных | `bpmn-engine/tests/vendor_patches.rs` |
| Rust unit | `UserTaskCompleted` (патч): переменные попали в инстанс, удалён только один `Waiting`-токен, `parallel_group_id` сохранён | `bpmn-engine/tests/vendor_patches.rs` |
| Rust integration | пять шлюзов демо-схемы, обе/три ветки; `stockSufficient=true/false` доходит до `Task_Ship`/`Task_CreatePO` | `bpmn-engine/tests/dairy_routing.rs` (Postgres из `DATABASE_URL`, скип без него) |
| Rust integration | `parallelGateway`: две ветки живут одновременно, слияние ждёт обе | `bpmn-engine/tests/parallel.rs` |
| Rust integration | повторный `event_id` не двигает токен; повторный `UserTaskCompleted` без `Waiting`-токена — no-op | `bpmn-engine/tests/idempotency.rs` |
| JS unit | `variables` кнопки: разбор, запись/чтение XML, `normalizeAction`, ошибки формата | `bpmnXml.test.js` |
| JS unit | валидатор: шесть новых кодов (7.4), демо-проект без проблем | `bpmnXml.test.js`, `dairyPlant.test.js` |
| JS unit | маршрут кнопок: для каждого шлюза переменные кнопок дают её же `elementId` | `dairyPlant.test.js` |
| JS integration | старт — одно событие: `StartProcess` с `element_id` первого шага у заказчика, поставщика, склада и схемы без кнопки; второй `CompleteBlock` не отправляется | `App.test.js` |
| E2E | `tests/test_workflow.py` без изменений: ERP-ответы двигают `ServiceTask_*`, экземпляр закрывается `process_completed`, в `tokens` ровно один `ACTIVE` на линейном сценарии | `tests/` |

Отдельно — тест «нет утечки»: сто вызовов `CachedDefStore::load` дают одну
компиляцию (счётчик в тестовой обёртке), потому что иначе 128 MiB лимита не
хватит (9.4).

## 12. Известные ограничения и маршрут

| № | Ограничение M1 | Обход сейчас | Когда закрывается |
| --- | --- | --- | --- |
| 1 | Пять патчей вендора нужно переприменять при обновлении | патчи и их файлы — в `VENDORED_FROM.md`, порядок проверки — приложение B | при каждом `update-vendor` |
| 2 | Компиляция определения утекает (`Box::leak`) | кэш по хешу XML (9.4) | апстрим или форк с ареной строк |
| 3 | Нет optimistic locking в `process_store.save` | advisory-блокировка по экземпляру (9.3) | патч вендора (кандидат в апстрим) |
| 4 | Нет транзакции «состояние + `consumed_event`» | повтор гасится отсутствием `Waiting`-токена (9.5) | outbox/патч сторов, M5/M7 |
| 5 | EL без скобок, `not`, арифметики; ошибки глушатся | грамматика 7.2 + валидатор 7.4 | M4, если появятся сложные условия |
| 6 | Два токена на одном узле не выразить в legacy `tokens`; завершение шага без `token_id` неоднозначно | параллельные ветки — на разных узлах; Rust-тест на две `ACTIVE` | M1.1: `token_id` в клиенте + PK по токену |
| 7 | Первый `endEvent` помечает весь экземпляр `Completed`, даже если живы другие ветки | в демо один конец на ветку | M3 (семантика завершения) |
| 8 | Проходные токены (`startEvent`, шлюзы) остаются `Waiting` и копятся в `token` | не проецируются в шаги; объём мал | уборка/GC в M5 |
| 9 | SLA-воркер работает по legacy `tokens` и не двигает движок | как сегодня: `TIMED_OUT` в read-model | M6 (таймеры/эскалации) |
| 10 | Клиент остаётся «одноактивным»; параллельные ветки в кабинете не подсвечиваются | в демо-схеме нет `parallelGateway` | M1.1 (подсветка набора узлов) |
| 11 | Ошибки процесса не уходят клиенту, DLQ нет | логи пода | M6/M7 |
| 12 | Деплой определения — upsert по `def.id`, без версий | переименование `@id` при изменении графа (10.1) | M4 |
| 13 | `PostgresParallelJoinRepo` несовместим со своей схемой: `ensure_group` вставляет `id`, `try_join` без строки считает ветку слившейся | репозиторий не подключается, слияние — in-memory в `TokenArrivedHandler` (9.2); в демо нет `parallelGateway` | патч вендора (кандидат в апстрим) или своя реализация `ParallelJoinRepo` |

Маршрут после M1: M1.1 — `token_id` в контракте и подсветка набора активных
узлов; M2 — живые `serviceTask` (воркеры, ретраи, идемпотентность ответов);
M3 — сообщения между процессами вместо `startsProcess`; далее — M4–M8 по
дорожной карте. Пункты 2–4 имеют смысл как отдельный апстрим-патч вендора,
чтобы не тащить их в каждом обновлении.

## Приложение A. Условия и кнопки демо-схемы

| Шлюз | Связь | Условие | Целевой узел | Кнопка-источник |
| --- | --- | --- | --- | --- |
| `Gateway_Stock` (`default="SF5"`) | `SF6` «Да» | `${stockSufficient == 'true'}` | `Task_Ship` | `haveStock` |
| | `SF5` «Нет» (default) | — | `Task_CreatePO` | `needPurchase` |
| `Gateway_Quality` (`default="SF15"`) | `SF14` «Да» | `${labPassed == 'true'}` | `Task_StoreMilk` | `qualityPassed` |
| | `SF15` «Нет» (default) | — | `Task_ReturnMilk` | `qualityRejected` |
| `Gateway_PasteurizeOK` (`default="SF_T5_No"`) | `SF_T5` «Да» | `${pasteurizeOk == 'true'}` | `Tech_Ferment` | `pasteurizeDone` |
| | `SF_T5_No` «Нет» (default, **новая связь** — в текущей схеме её нет) | — | `Task_ERP_CriticalAlert` | `pasteurizeCrash` |
| `Gateway_TechQC` (`default="SF_T8_No"`) | `SF_T8_Yes` «Да» | `${techQcPassed == 'true'}` | `Tech_Pack` | `techQCPassed` |
| | `SF_T8_No` «Нет» (default) | — | `Tech_Rework` | `techQCFailed` |
| `Gateway_PackOutcome` (`default="SF_T_Err2"`) | `SF18_ToWarehouse` «Годная» | `${packOutcome == 'good'}` | `Task_StoreFG` | `packCompleted` |
| | `SF_T_Esc1` «Деградация» | `${packOutcome == 'degraded'}` | `Tech_LogDegradation` | `packDegraded` |
| | `SF_T_Err2` «Авария» (default) | — | `Task_ERP_CriticalAlert` | `packCrashed` |

Почему `default` — негативная ветка: отсутствие переменной (старый клиент,
ручное событие) означает «решение не принято», и безопаснее остановить линию
или вернуть партию, чем молча отгрузить. Это же делает поведение
предсказуемым при опечатке в имени переменной.

Прочерк в колонке «Условие» у строк с пометкой (default) — это
`EdgeCondition::Default` из флага `is_default` (7.1 п. 2), а **не** «связь без
условия» из 7.1 п. 1: разница между ними видна только после патча 3 из 4.3.

## Приложение B. Чек-лист импорта вендора

Прогоняется на каждый импорт и обновление (4.3, 4.4). Всё, что не сходится,
правится **до** подключения `main.rs` к `runtime`.

1. **Вендор собирается сам.** `cargo test -p bpm-engine-core -p bpm-engine-runtime`
   в `vendor/bpm-engine` без наших файлов. Версия тулчейна фиксируется
   (`rust-toolchain.toml`), чтобы обновление не приносило другой edition.
2. **Сигнатуры, на которые завязан `runtime/`:**
   - `EngineContext::builder(process_store, token_store, process_def_store)` +
     `.history_repo()` / `.parallel_join_repo()`
     (`crates/runtime/src/handler.rs:26-140`);
   - `BpmEngine::new(Vec<Box<dyn EventHandler>>)` и
     `run_async(event, &mut ctx)` → `()` (`engine.rs:27-42`);
   - `ProcessDefinitionStore` = `load`/`list_versions`/`get_active`/`activate`/
     `deprecate` (`storage/src/process_store.rs:96-112`); `deploy` — ингерентный
     метод `PostgresProcessDefStore`, в трейте его нет;
   - `TokenStore::claim_token` — CAS по версии (`token_store.rs:157-181`).
3. **Payload'ы.** Переменные везде `HashMap<String, String>`, не JSON:
   - `UserTaskCompleted { task_id, instance_id, node_id, variables }`
     (`core/src/event.rs:48-57`); `task_id` хендлер не читает, но поле
     обязательно;
   - `ExternalTaskCompleted { instance_id, token_id, node_id, variables }`
     (`:179-189`); токен удаляется по `token_id`
     (`external_task_completed_handler.rs:47-48`) — значит маппер обязан
     передать `id` `Waiting`-токена шага;
   - `ProcessStarted { process_id, instance_id, initial_variables }`.
4. **Таблицы `migrate(&Pool)`** (`adapters/postgres/src/lib.rs:106+`):
   `process_definition`, `process_instance`, `token`, `external_task`, `timer`,
   `history_event`, `event_outbox`, `compensation_record`, `parallel_join`,
   `dead_letter`.
   Сверить с 6 и `schema.sql`; наши sqlx-миграции не должны создавать таблицы с
   теми же именами.
5. **Поведение `el` и шлюза.** Прогнать шесть форм из 7.2 и порядок связей из
   7.1 на memory-сторах. Изменилось — обновить 7.2, коды 7.4 и схему
   (приложение A) одним коммитом.
6. **Патчи 4.3.** Применить все пять и прогнать `tests/vendor_patches.rs`:
   - `crates/runtime/src/user_task_completed_handler.rs` — мерж `e.variables`
     в `instance.variables`, выбор одного `Waiting`-токена узла, удаление по
     `id`, `TokenCompleted` в историю;
   - `crates/runtime/src/external_task_completed_handler.rs` — тот же
     `move_token_preserving_group` (мерж переменных и удаление по `token_id`
     там уже верные);
   - `crates/bpmn/src/compiler.rs` — `build_outgoing` ставит
     `EdgeCondition::Default` по флагу `is_default` независимо от того, есть ли
     у связи `conditionExpression` (4.3, п. 3); проверка —
     `default`-связь без условия даёт `Some(Default)`, а не `None`;
   - `crates/runtime/src/transition.rs` — трогать не нужно:
     `move_token_preserving_group` там уже есть (`transition.rs:30-35`),
     патч только переключает на неё вызовы в двух хендлерах;
   - `crates/adapters/postgres/src/lib.rs` — `migrate` строит
     `idx_token_state_updated` по `token(status, updated_at)` (4.3, п. 4):
     проверить, что `CREATE TABLE token` по-прежнему называет колонку `status`;
   - `crates/adapters/postgres/src/process_store.rs` — `SELECT` в `load`
     перечисляет все колонки, которые читает строка, включая `parent_instance_id`
     и `parent_token_id` (4.3, п. 5); ловит тест
     `instance_load_selects_parent_columns`.

   Признак несовместимости — патчи не компилируются: значит, поменялись поля
   `Token` (`id`, `node_id`, `status`, `parallel_group_id`), `Node` или
   `Node.outgoing_edges` (`target`/`condition`, `EdgeCondition::Default`); тогда
   4.3 переписывается, а не подгоняется.
7. **Только после 1–6** `main.rs` получает `ENGINE_ROUTING` (10.1): `graph` —
   режим по умолчанию, `legacy` остаётся аварийным откатом.

