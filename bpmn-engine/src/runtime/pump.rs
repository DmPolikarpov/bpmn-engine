//! Пумпа: снимки состояния, сборка [`BpmEngine`] и блокировка экземпляра.
//!
//! Пумпа вендора не возвращает обработанные события
//! (`pump.rs:44-51`), поэтому проекция строится по снимкам «до/после»
//! (дизайн `docs/runtime-m1-design.md`, 5.3 и 9.3).

use std::sync::Arc;

use bpm_engine::bpm_engine_core::{NodeType, ProcessDefinition, ProcessInstance, TokenStatus};
use bpm_engine::bpm_engine_runtime::{
    BpmEngine, ExternalTaskCompletedHandler, HistoryHandler, ProcessCompletedHandler,
    ProcessStartHandler, TokenArrivedHandler, UserTaskCompletedHandler,
};
use bpm_engine::bpm_engine_storage::{ProcessDefinitionStore, ProcessInstanceStore};
use sqlx::{PgPool, Postgres, Transaction};

/// Активный шаг: токен, который ждёт действия человека или воркера.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    /// Идентификатор токена движка (в legacy-таблицу не попадает).
    pub token_id: String,
    /// Узел схемы — он же `element_id` в legacy `tokens`.
    pub element_id: String,
}

/// Тип шага, на котором токен останавливается и ждёт внешнего события.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepKind {
    /// `bpmn:userTask` — ждёт нажатия кнопки человеком.
    UserTask,
    /// `bpmn:serviceTask` — ждёт ответа воркера (`ExternalTaskCompleted`).
    ExternalTask,
}

/// Согласованный снимок движка: инстанс и скомпилированное определение.
pub struct Snapshot {
    /// Инстанс со всеми токенами и переменными.
    pub instance: ProcessInstance,
    /// Определение, по которому исполняется инстанс.
    pub definition: ProcessDefinition,
}

impl Snapshot {
    /// Шаги, на которых экземпляр ждёт внешнего события.
    ///
    /// Проходные узлы (`startEvent`, шлюзы) тоже остаются `Waiting`, но шагом
    /// не являются (дизайн 4.5), поэтому фильтр по типу узла обязателен.
    pub fn waiting_steps(&self) -> Vec<Step> {
        let mut steps: Vec<Step> = self
            .instance
            .tokens
            .iter()
            .filter(|token| token.status == TokenStatus::Waiting)
            .filter(|token| step_kind(&self.definition, &token.node_id).is_some())
            .map(|token| Step {
                token_id: token.id.clone(),
                element_id: token.node_id.clone(),
            })
            .collect();
        // Порядок нужен для стабильных WS-сообщений и тестов.
        steps.sort_by(|left, right| {
            left.element_id
                .cmp(&right.element_id)
                .then_with(|| left.token_id.cmp(&right.token_id))
        });
        steps
    }

    /// Хост-узел для входящего события завершения.
    ///
    /// Приоритет: явный `token_id` из `output_data` → узел `node_id` →
    /// единственный `Waiting`-токен нужного типа. Если ждут два шага и
    /// подсказки нет, выбор неоднозначен — возвращается `None`
    /// (дизайн 5.2, п. 2–3).
    pub fn host_step(
        &self,
        kind: StepKind,
        token_id: Option<&str>,
        node_id: Option<&str>,
    ) -> Option<Step> {
        let waiting: Vec<Step> = self
            .waiting_steps()
            .into_iter()
            .filter(|step| step_kind(&self.definition, &step.element_id) == Some(kind))
            .collect();

        if let Some(token_id) = token_id {
            return waiting.into_iter().find(|step| step.token_id == token_id);
        }

        if let Some(node_id) = node_id {
            let matching: Vec<&Step> = waiting
                .iter()
                .filter(|step| step.element_id == node_id)
                .collect();
            if matching.len() == 1 {
                return Some(matching[0].clone());
            }
        }

        if waiting.len() == 1 {
            return Some(waiting[0].clone());
        }
        None
    }
}

/// Тип шага по узлу схемы (или `None`, если узел проходной).
pub fn step_kind(definition: &ProcessDefinition, node_id: &str) -> Option<StepKind> {
    match &definition.nodes.get(node_id)?.node_type {
        NodeType::UserTask { .. } => Some(StepKind::UserTask),
        NodeType::ExternalTask { .. } => Some(StepKind::ExternalTask),
        _ => None,
    }
}

/// Параметры `serviceTask` из схемы (дизайн `docs/runtime-m1-design.md`, 10.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalTaskSpec {
    /// Тема воркера: `camunda:topic` (по умолчанию `default`).
    pub task_type: String,
    /// Сколько раз задача может быть переотправлена (ретраи — следующий шаг M2).
    pub retries: i32,
    /// Таймаут выполнения в секундах: из него считается `deadline` команды.
    pub timeout_secs: u64,
}

/// Достаёт параметры `serviceTask`; для остальных узлов — `None`.
pub fn external_task_spec(
    definition: &ProcessDefinition,
    node_id: &str,
) -> Option<ExternalTaskSpec> {
    match &definition.nodes.get(node_id)?.node_type {
        NodeType::ExternalTask {
            task_type,
            retries,
            timeout_secs,
        } => Some(ExternalTaskSpec {
            task_type: task_type.clone(),
            retries: *retries,
            timeout_secs: *timeout_secs,
        }),
        _ => None,
    }
}

/// Хост-узел ответа ERP.
///
/// Явный `element_id` — адрес задачи: если такого ожидающего `serviceTask`
/// нет, ответ не применяется. Иначе устаревший или повторный ответ закрыл бы
/// соседний узел, который ждёт своей команды (дизайн 10.4). Без `element_id`
/// работает прежний выбор по `token_id` или единственному ожидающему шагу.
pub fn erp_host_step(
    snapshot: &Snapshot,
    element_id: Option<&str>,
    token_id: Option<&str>,
) -> Option<Step> {
    match element_id {
        Some(node_id) => snapshot.waiting_steps().into_iter().find(|step| {
            step.element_id == node_id
                && step_kind(&snapshot.definition, &step.element_id) == Some(StepKind::ExternalTask)
        }),
        None => snapshot.host_step(StepKind::ExternalTask, token_id, None),
    }
}

/// Список хендлеров M1 (дизайн 9.2).
///
/// `HistoryHandler` без `history_repo` в контексте сразу возвращает пусто,
/// поэтому список не зависит от режима аудита.
pub fn engine() -> BpmEngine {
    BpmEngine::new(vec![
        Box::new(ProcessStartHandler),
        Box::new(TokenArrivedHandler::new()),
        Box::new(UserTaskCompletedHandler),
        Box::new(ExternalTaskCompletedHandler),
        Box::new(ProcessCompletedHandler),
        Box::new(HistoryHandler),
    ])
}

/// Загружает снимок экземпляра вместе с его определением.
pub async fn load<P, D>(
    process_store: &Arc<P>,
    def_store: &Arc<D>,
    instance_id: &str,
) -> anyhow::Result<Option<Snapshot>>
where
    P: ProcessInstanceStore + ?Sized,
    D: ProcessDefinitionStore + ?Sized,
{
    let Some(instance) = process_store.load(instance_id).await? else {
        return Ok(None);
    };
    let Some(definition) = def_store.load(&instance.process_def_id).await? else {
        return Ok(None);
    };
    Ok(Some(Snapshot {
        instance,
        definition,
    }))
}

/// Сериализует обработку одного экземпляра между подами.
///
/// Транзакция нужна не ради атомарности записи (её как раз нет), а ради
/// `pg_advisory_xact_lock`: блокировка снимается коммитом/откатом и не может
/// «зависнуть» на соединении, вернувшемся в пул. Advisory-блокировка видна
/// любой сессии той же БД, поэтому sqlx-пул и пул движка её не обходят.
pub struct InstanceLock<'a> {
    transaction: Transaction<'a, Postgres>,
}

impl InstanceLock<'_> {
    /// Отпускает блокировку, фиксируя пустую транзакцию.
    pub async fn commit(self) -> Result<(), sqlx::Error> {
        self.transaction.commit().await
    }

    /// Отпускает блокировку откатом (используется на пути ошибки).
    pub async fn rollback(self) -> Result<(), sqlx::Error> {
        self.transaction.rollback().await
    }
}

/// Берёт advisory-блокировку по `instance_id` (дизайн 9.3).
pub async fn lock_instance<'a>(
    pool: &'a PgPool,
    instance_id: &str,
) -> Result<InstanceLock<'a>, sqlx::Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(instance_id)
        .execute(&mut *transaction)
        .await?;
    Ok(InstanceLock { transaction })
}

/// Обработан ли уже `event_id` (таблица `consumed_event`, дизайн 9.5).
pub async fn is_consumed(pool: &PgPool, event_id: &str) -> Result<bool, sqlx::Error> {
    let row: Option<(i32,)> = sqlx::query_as("SELECT 1 FROM consumed_event WHERE event_id = $1")
        .bind(event_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

/// Помечает событие обработанным; повторная запись игнорируется.
pub async fn mark_consumed(
    pool: &PgPool,
    event_id: &str,
    instance_id: &str,
    event_type: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO consumed_event (event_id, instance_id, event_type)
        VALUES ($1, $2, $3)
        ON CONFLICT (event_id) DO NOTHING
        "#,
    )
    .bind(event_id)
    .bind(instance_id)
    .bind(event_type)
    .execute(pool)
    .await?;
    Ok(())
}

/// Состояние ретрая внешней задачи (таблица `erp_retry`, миграция 0002).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryState {
    /// Сколько ретраев уже запланировано (не считая первого ответа воркера).
    pub attempts: i32,
    /// Ретрай запланирован, но ещё не отправлен: ответ на такую команду —
    /// повтор предыдущего сообщения, а не новая ошибка.
    pub pending: bool,
}

/// Задача, которую пора переотправить воркеру после backoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryJob {
    /// Ключ команды: он же ключ строки `erp_retry`.
    pub idempotency_key: String,
    /// Экземпляр процесса.
    pub instance_id: String,
    /// Узел `serviceTask`.
    pub element_id: String,
    /// Токен, который ждёт ответа: команда должна вернуть тот же ключ.
    pub token_id: String,
    /// Сколько ретраев уже сделано (для логов и `ActionRequiredEvent`).
    pub attempts: i32,
}

/// Состояние ретрая по ключу команды; `None` — ретраев ещё не было.
pub async fn load_retry(pool: &PgPool, key: &str) -> Result<Option<RetryState>, sqlx::Error> {
    let row: Option<(i32, bool)> = sqlx::query_as(
        "SELECT attempts, (dispatched_at IS NULL) AS pending FROM erp_retry WHERE idempotency_key = $1",
    )
    .bind(key)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(attempts, pending)| RetryState { attempts, pending }))
}

/// Планирует переотправку команды через `delay_secs` секунд.
///
/// `attempts` — общее число ретраев после этой записи: движок присылает
/// `attempts + 1`, чтобы счётчик в БД сходился с `camunda:retries`.
#[allow(clippy::too_many_arguments)]
pub async fn schedule_retry(
    pool: &PgPool,
    key: &str,
    instance_id: &str,
    element_id: &str,
    token_id: &str,
    attempts: i32,
    last_status: &str,
    last_error: Option<&str>,
    delay_secs: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO erp_retry (
            idempotency_key, instance_id, element_id, token_id,
            attempts, last_status, last_error, next_attempt_at, dispatched_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7,
            NOW() + ($8::bigint * INTERVAL '1 second'), NULL
        )
        ON CONFLICT (idempotency_key) DO UPDATE SET
            attempts = EXCLUDED.attempts,
            last_status = EXCLUDED.last_status,
            last_error = EXCLUDED.last_error,
            next_attempt_at = EXCLUDED.next_attempt_at,
            dispatched_at = NULL,
            updated_at = NOW()
        "#,
    )
    .bind(key)
    .bind(instance_id)
    .bind(element_id)
    .bind(token_id)
    .bind(attempts)
    .bind(last_status)
    .bind(last_error)
    .bind(delay_secs)
    .execute(pool)
    .await?;
    Ok(())
}

/// Забывает ретраи команды: шаг завершён, отменён или ретраи исчерпаны.
pub async fn clear_retry(pool: &PgPool, key: &str) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM erp_retry WHERE idempotency_key = $1")
        .bind(key)
        .execute(pool)
        .await?;
    Ok(())
}

/// Забирает до `limit` созревших ретраев и отодвигает их на `lease_secs`.
///
/// `FOR UPDATE SKIP LOCKED` разводит поды: один и тот же ретрай не уйдёт
/// воркеру дважды. Даже если публикация сорвётся, команда выйдет после
/// аренды — а лишняя доставка безопасна: ключ идемпотентности тот же.
pub async fn claim_due_retries(
    pool: &PgPool,
    limit: i64,
    lease_secs: i64,
) -> Result<Vec<RetryJob>, sqlx::Error> {
    let rows: Vec<(String, String, String, String, i32)> = sqlx::query_as(
        r#"
        UPDATE erp_retry
        SET dispatched_at = NOW(),
            next_attempt_at = NOW() + ($2::bigint * INTERVAL '1 second'),
            updated_at = NOW()
        WHERE idempotency_key IN (
            SELECT idempotency_key
            FROM erp_retry
            WHERE next_attempt_at <= NOW()
            ORDER BY next_attempt_at
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING idempotency_key, instance_id, element_id, token_id, attempts
        "#,
    )
    .bind(limit)
    .bind(lease_secs)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(idempotency_key, instance_id, element_id, token_id, attempts)| RetryJob {
                idempotency_key,
                instance_id,
                element_id,
                token_id,
                attempts,
            },
        )
        .collect())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bpm_engine::bpm_engine_adapter_memory::{MemoryRepo, ProcessDefStore};
    use bpm_engine::bpm_engine_bpmn::parse_and_compile;
    use bpm_engine::bpm_engine_core::{InstanceState, Token, TokenMode};
    use bpm_engine::bpm_engine_runtime::EngineContext;
    use serde_json::json;

    use super::*;
    use crate::runtime::mapper;

    const PROCESS_ID: &str = "runtime_pump_test";

    fn xml() -> String {
        r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="http://bpmn-engine.local">
  <process id="runtime_pump_test" name="Pump" isExecutable="true">
    <startEvent id="Start"/>
    <userTask id="Task_A" name="A"/>
    <endEvent id="End"/>
    <sequenceFlow id="Flow_Start" sourceRef="Start" targetRef="Task_A"/>
    <sequenceFlow id="Flow_A" sourceRef="Task_A" targetRef="End"/>
  </process>
</definitions>"#
            .to_string()
    }

    fn harness(xml: &str) -> (Arc<MemoryRepo>, Arc<ProcessDefStore>) {
        let definition = parse_and_compile(xml).expect("схема должна компилироваться");
        let def_store = Arc::new(ProcessDefStore::new());
        def_store.register(definition);
        (Arc::new(MemoryRepo::new()), def_store)
    }

    fn instance(tokens: Vec<Token>) -> ProcessInstance {
        ProcessInstance {
            id: "instance-1".to_string(),
            process_def_id: PROCESS_ID.to_string(),
            tenant_id: None,
            tokens,
            variables: HashMap::new(),
            state: InstanceState::Running,
            version: 0,
            parent_instance_id: None,
            parent_token_id: None,
        }
    }

    fn waiting_token(id: &str, node_id: &str) -> Token {
        Token {
            id: id.to_string(),
            node_id: node_id.to_string(),
            status: TokenStatus::Waiting,
            mode: TokenMode::Forward,
            version: 0,
            attempt: 0,
            parallel_group_id: None,
            updated_at: None,
        }
    }

    #[tokio::test]
    async fn pump_stops_on_first_user_task() {
        let (repo, def_store) = harness(&xml());
        let mut context =
            EngineContext::builder(repo.clone(), repo.clone(), def_store.clone()).build();

        engine()
            .run_async(
                mapper::process_started(PROCESS_ID, "instance-1", &json!({"order_amount": 150000})),
                &mut context,
            )
            .await;

        let snapshot = load(&repo, &def_store, "instance-1")
            .await
            .expect("чтение снимка")
            .expect("инстанс создан");
        let steps = snapshot.waiting_steps();
        assert_eq!(steps.len(), 1, "пумпа должна остановиться на первом шаге");
        assert_eq!(
            steps[0].element_id, "Task_A",
            "проходной startEvent не должен проецироваться в шаг"
        );
        assert_eq!(
            snapshot.instance.variables.get("order_amount"),
            Some(&"150000".to_string()),
            "начальные переменные попадают в инстанс"
        );
    }

    #[test]
    fn parallel_branches_are_ambiguous_without_token_hint() {
        let definition = parse_and_compile(&xml()).expect("схема компилируется");
        let snapshot = Snapshot {
            instance: instance(vec![
                waiting_token("token-1", "Task_A"),
                waiting_token("token-2", "Task_A"),
                waiting_token("token-3", "Start"),
            ]),
            definition,
        };

        assert_eq!(
            snapshot.waiting_steps().len(),
            2,
            "проходной startEvent не шаг"
        );
        assert_eq!(
            snapshot.host_step(StepKind::UserTask, None, None),
            None,
            "две ветки на одном узле без token_id неоднозначны"
        );
        assert_eq!(
            snapshot
                .host_step(StepKind::UserTask, Some("token-2"), None)
                .map(|step| step.token_id),
            Some("token-2".to_string()),
            "token_id из output_data выбирает нужную ветку"
        );
        assert_eq!(
            snapshot
                .host_step(StepKind::UserTask, None, Some("Task_A"))
                .map(|step| step.token_id),
            None,
            "подсказка по узлу тоже неоднозначна при двух токенах"
        );
    }

    #[test]
    fn external_step_is_found_by_node_id() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="http://bpmn-engine.local">
  <process id="runtime_pump_test" name="Pump" isExecutable="true">
    <startEvent id="Start"/>
    <serviceTask id="ServiceTask_ERP" name="ERP"/>
    <endEvent id="End"/>
    <sequenceFlow id="Flow_Start" sourceRef="Start" targetRef="ServiceTask_ERP"/>
    <sequenceFlow id="Flow_ERP" sourceRef="ServiceTask_ERP" targetRef="End"/>
  </process>
</definitions>"#;
        let definition = parse_and_compile(xml).expect("схема компилируется");
        let snapshot = Snapshot {
            instance: instance(vec![waiting_token("token-1", "ServiceTask_ERP")]),
            definition,
        };

        let step = snapshot
            .host_step(StepKind::ExternalTask, None, Some("ServiceTask_ERP"))
            .expect("ответ ERP находит ожидающий serviceTask");
        assert_eq!(step.element_id, "ServiceTask_ERP");
        assert_eq!(
            snapshot.host_step(StepKind::UserTask, None, None),
            None,
            "serviceTask не завершается событием userTask"
        );
    }

    #[test]
    fn erp_response_targets_only_its_node() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="http://bpmn-engine.local">
  <process id="runtime_pump_test" name="Pump" isExecutable="true">
    <startEvent id="Start"/>
    <serviceTask id="ServiceTask_A" name="A"/>
    <serviceTask id="ServiceTask_B" name="B"/>
    <endEvent id="End"/>
    <sequenceFlow id="F1" sourceRef="Start" targetRef="ServiceTask_A"/>
    <sequenceFlow id="F2" sourceRef="ServiceTask_A" targetRef="ServiceTask_B"/>
    <sequenceFlow id="F3" sourceRef="ServiceTask_B" targetRef="End"/>
  </process>
</definitions>"#;
        let definition = parse_and_compile(xml).expect("схема компилируется");
        let snapshot = Snapshot {
            instance: instance(vec![
                waiting_token("token-a", "ServiceTask_A"),
                waiting_token("token-b", "ServiceTask_B"),
            ]),
            definition,
        };

        assert_eq!(
            erp_host_step(&snapshot, Some("ServiceTask_A"), None).map(|step| step.token_id),
            Some("token-a".to_string())
        );
        assert_eq!(
            erp_host_step(&snapshot, Some("ServiceTask_Old"), None),
            None,
            "ответ на чужой/устаревший узел не должен закрывать соседний"
        );
        assert_eq!(
            erp_host_step(&snapshot, None, None),
            None,
            "без element_id и двух ожидающих задач выбор неоднозначен"
        );
    }

    #[test]
    fn external_task_spec_reads_topic_and_retries() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
             xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
             targetNamespace="http://bpmn-engine.local">
  <process id="runtime_pump_spec" name="Spec" isExecutable="true">
    <startEvent id="Start"/>
    <serviceTask id="ServiceTask_ERP" name="ERP" camunda:topic="erp" camunda:retries="2"/>
    <endEvent id="End"/>
    <sequenceFlow id="Flow_Start" sourceRef="Start" targetRef="ServiceTask_ERP"/>
    <sequenceFlow id="Flow_End" sourceRef="ServiceTask_ERP" targetRef="End"/>
  </process>
</definitions>"#;
        let definition = parse_and_compile(xml).expect("схема компилируется");

        let spec = external_task_spec(&definition, "ServiceTask_ERP").expect("serviceTask");
        assert_eq!(spec.task_type, "erp", "тема воркера — camunda:topic");
        assert_eq!(spec.retries, 2);
        assert_eq!(spec.timeout_secs, 60);
        assert_eq!(
            external_task_spec(&definition, "Start"),
            None,
            "проходной узел не является внешней задачей"
        );
    }

    /// Схема с шлюзом: ветка «Нет» — default-связь без условия (патч 3).
    fn gateway_xml() -> String {
        r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="http://bpmn-engine.local">
  <process id="gateway_test" name="Gateway" isExecutable="true">
    <startEvent id="Start"/>
    <userTask id="Task_CheckStock" name="Check"/>
    <exclusiveGateway id="Gateway_Stock" default="Flow_No"/>
    <userTask id="Task_Ship" name="Ship"/>
    <userTask id="Task_CreatePO" name="PO"/>
    <endEvent id="End_Ok"/>
    <endEvent id="End_Fail"/>
    <sequenceFlow id="Flow_Start" sourceRef="Start" targetRef="Task_CheckStock"/>
    <sequenceFlow id="Flow_Check" sourceRef="Task_CheckStock" targetRef="Gateway_Stock"/>
    <sequenceFlow id="Flow_Yes" sourceRef="Gateway_Stock" targetRef="Task_Ship">
      <conditionExpression>${stockSufficient == "true"}</conditionExpression>
    </sequenceFlow>
    <sequenceFlow id="Flow_No" sourceRef="Gateway_Stock" targetRef="Task_CreatePO"/>
    <sequenceFlow id="Flow_Ship" sourceRef="Task_Ship" targetRef="End_Ok"/>
    <sequenceFlow id="Flow_PO" sourceRef="Task_CreatePO" targetRef="End_Fail"/>
  </process>
</definitions>"#
            .to_string()
    }

    #[tokio::test]
    async fn gateway_routes_by_step_variables() {
        for (value, expected) in [("true", "Task_Ship"), ("false", "Task_CreatePO")] {
            let (repo, def_store) = harness(&gateway_xml());
            let mut context =
                EngineContext::builder(repo.clone(), repo.clone(), def_store.clone()).build();
            let engine = engine();

            engine
                .run_async(
                    mapper::process_started("gateway_test", "instance-gw", &json!({})),
                    &mut context,
                )
                .await;

            let before = load(&repo, &def_store, "instance-gw")
                .await
                .expect("снимок до")
                .expect("инстанс создан");
            let host = before
                .host_step(StepKind::UserTask, None, None)
                .expect("токен ждёт проверки склада");
            assert_eq!(host.element_id, "Task_CheckStock");

            engine
                .run_async(
                    mapper::user_task_completed(
                        "instance-gw",
                        &host,
                        &json!({"stockSufficient": value}),
                    ),
                    &mut context,
                )
                .await;

            let after = load(&repo, &def_store, "instance-gw")
                .await
                .expect("снимок после")
                .expect("инстанс жив");
            let steps = after.waiting_steps();
            assert_eq!(
                steps.len(),
                1,
                "шлюз выбирает одну ветку, получено: {:?}",
                steps
            );
            assert_eq!(
                steps[0].element_id, expected,
                "stockSufficient={value} должен вести в {expected}"
            );
        }
    }
}
