# Вендорная копия `bpm-engine`

Копия upstream-репозитория `bpm-engine` (MIT), из которой собирается наш
runtime. Лежит внутри каталога пакета `bpmn-engine/`, потому что
`docker build -f bpmn-engine/Dockerfile bpmn-engine/` собирает с этим
контекстом — иначе контекст пришлось бы расширять до корня репозитория.

## Источник

| Поле | Значение |
| --- | --- |
| Репозиторий | <https://github.com/Colin4k1024/bpm-engine> (homepage upstream указывает на прежний `fanjia1024/...`) |
| Коммит | `05929961c568b29748db4c65bc14806afa00f9dd` — master, 2026-07-22, «fix: use sort_by_key in saga_compensation test for clippy compliance» |
| Дата вендоринга | 2026-10-21 |
| Архив | `bpm.tar.gz` (codeload, master), sha256 `e38f077e8de6f4315c06ca767a080c26c180693721bc627bc801ccca7c20e0f8` |
| Версия крейтов | 0.2.0 (workspace `Cargo.toml`) |
| Лицензия | MIT, `LICENSE.txt` — оригинальный файл, не менялся |

Коммит определён по содержимому архива: последняя правка master
(`tests/saga_compensation.rs`, `pending.sort_by_key(|b| std::cmp::Reverse(b.order))`)
в копии присутствует. Дата коммита — из GitHub API, не из архива: у
codeload-тарболла нет `.git`, поэтому при обновлении коммит указывается
явно (см. «Обновление»).

## Что скопировано

`Cargo.toml`, `Cargo.lock`, `LICENSE.txt`, `README.md`, `CHANGELOG.md`,
`.gitignore`, `src/` (facade-крейт), `crates/` (`core`, `storage`, `runtime`,
`bpmn`, `adapters/memory`, `adapters/postgres`, `server/rest`, `worker-sdk`),
`tests/`.

Не скопированы: `target/`, `.github/`, `docs/`, `design/`, `deploy/`,
`examples/`, `sdk/`, `scripts/`, `ui/`, `AGENTS.md`, `CLAUDE*.md`,
`CODE_OF_CONDUCT.md`, `CONTRIBUTING.md`, `SECURITY.md`, `README_zh.md` —
сборке не нужны, наших тестов не касаются.

## Локальные патчи (6)

Патчи 1–3, 5 и 6 зафиксированы тестами
[`tests/vendor_patches.rs`](../../tests/vendor_patches.rs); патч 4 без живой
БД не проверяется (вендорский тест адаптера требует Postgres).

1. **Переменные человеческого шага, адрес и группа токена** —
   `crates/runtime/src/user_task_completed_handler.rs`. Завершение шага:
   - выбирает **один** `Waiting`-токен узла (`position`), а не все токены,
     стоящие на узле (upstream: `tokens.retain(|t| t.node_id != e.node_id)`);
   - мержит `e.variables` в `instance.variables` — иначе решения человека не
     доезжают до условий шлюза, который читает `instance.variables`;
   - двигает токен через `move_token_preserving_group` (upstream: `move_token`,
     который обнуляет `parallel_group_id` и ломает `ParallelJoinRepo`);
   - отдаёт `TokenCompleted` для истории (upstream отдавал только `TokenArrived`).
2. **Группа внешней задачи** —
   `crates/runtime/src/external_task_completed_handler.rs`: `parallel_group_id`
   берётся у завершаемого токена и передаётся в `move_token_preserving_group`
   (мерж переменных и удаление по `token_id` в upstream уже верные).
3. **Флаг `default` без `conditionExpression`** —
   `crates/bpmn/src/compiler.rs::build_outgoing`: `is_default` больше не читается
   внутри замыкания `condition_expression`, поэтому default-связь без условия
   компилируется в `EdgeCondition::Default`, а не в `None`. `None` в
   `evaluate_exclusive_gateway` (`transition.rs`) выигрывает у всех условий
   немедленно, то есть без патча default работал только когда его связь
   объявлена последней.
4. **Схема `migrate` соответствует таблицам** —
   `crates/adapters/postgres/src/lib.rs`: индекс `idx_token_state_updated`
   создаётся по `token(status, updated_at)`, а не по несуществующей колонке
   `state`. Без патча `migrate` падает на `column "state" does not exist` и
   `ENGINE_ROUTING=graph` не поднимается ни на одном поде (вендорный тест
   `cargo test -p bpm-engine-adapter-postgres` требует Postgres, поэтому в
   юнит-прогон пакета не попадает).
5. **`load` читает только выбранные колонки** —
   `crates/adapters/postgres/src/process_store.rs`: `SELECT` в
   `PostgresProcessStore::load` выбирает `parent_instance_id` и
   `parent_token_id`, которые сразу же читаются через `row.get`. Без патча
   `load` паникует на `error retrieving column parent_instance_id: invalid
   column` сразу после старта экземпляра: `ProcessStartHandler` уже записал
   инстанс и токен, поэтому строка остаётся в `Ready` и процесс не двигается,
   а паника в основном цикле (future на главном потоке `#[tokio::main]`)
   роняет под — все реплики уходят в `CrashLoopBackOff`. Как и патч 4,
   вендорский юнит-прогон этого не видит: нужен живой Postgres.
6. **Корреляция сообщений** (M3) — `crates/core/src/event.rs` (`MessageSent`),
   `crates/runtime/src/token_arrived_handler.rs` и
   `crates/runtime/src/message_handler.rs`:
   - `MessageSent` несёт `node_id` (узел throw-события) и необязательные
     `correlation` (пары «ключ=значение» переменных получателя) и `payload`
     (переменные, которые сообщение переносит);
   - `MessageCatchHandler` поднимает catch-токен только у экземпляров, где все
     пары `correlation` совпадают с переменными; без карты поведение прежнее
     (upstream будил **все** экземпляры с тем же именем сообщения);
   - `payload` мержится в переменные получателя перед сдвигом токена.
   Так ответ поставщика находит экземпляр завода по `orderNum`, а не по
   `instance_id`. Проверка —
   `tests/vendor_patches.rs::message_delivery_respects_correlation`.

Хелпер `move_token_preserving_group` — upstream-код
(`crates/runtime/src/transition.rs`), это не патч.

## Сборка и тесты

- Зависимость пакета: `bpm-engine = { path = "vendor/bpm-engine" }` в
  [`../../Cargo.toml`](../../Cargo.toml); toolchain — `../../rust-toolchain.toml`
  (та же версия, что в `Dockerfile`).
- Собственный workspace вендора целиком не собирается: он поставляет только
  пакет `bpm-engine` (facade) как path-зависимость. `dev-dependencies` вендора
  (`criterion`, `reqwest`) при этом не подтягиваются.
- Тесты самого вендора запускаются из этого каталога:
  `cargo test -p bpm-engine-core -p bpm-engine-runtime -p bpm-engine-bpmn`.
- Тесты патчей — из каталога пакета: `cargo test --test vendor_patches`.
- `vendor/bpm-engine/target/` в git не попадает (`.gitignore` вендора).

## Обновление

Обновление ручное, не в CI:

1. скачать архив нужного коммита
   (`https://codeload.github.com/Colin4k1024/bpm-engine/tar.gz/<commit>`);
2. заменить `crates/`, `src/`, `tests/`, `Cargo.toml`, `Cargo.lock`,
   `LICENSE.txt`, `README.md`, `CHANGELOG.md` — состав тот же, что в разделе
   «Что скопировано»;
3. переприменить патчи из раздела выше (после обновления проверить, не
   исправлены ли они в upstream — тогда патч снимается, а тест остаётся);
4. обновить таблицу «Источник» и `Cargo.lock` пакета (`cargo update`);
5. прогнать `cargo test -p bpm-engine-core -p bpm-engine-runtime -p bpm-engine-bpmn`
   в этом каталоге и `cargo test` в пакете.
