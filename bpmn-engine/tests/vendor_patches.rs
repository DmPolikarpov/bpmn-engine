//! Проверки локальных патчей вендорного `bpm-engine`.
//!
//! Патчи описаны в `vendor/bpm-engine/VENDORED_FROM.md`; тесты фиксируют их
//! наблюдаемое поведение, чтобы обновление вендора не откатило M1 молча:
//!
//! 1. переменные `userTask` доезжают до условий шлюза;
//! 2. завершение шага закрывает один токен узла и сохраняет группу ветки;
//! 3. `default`-связь без `conditionExpression` компилируется в `Default`;
//! 5. `PostgresProcessStore::load` выбирает все колонки, которые читает
//!    (проверка по исходнику: живой БД в юнит-прогоне пакета нет);
//! 6. сообщение доставляется только экземпляру с совпадающим бизнес-ключом
//!    (M3: `MessageSent::correlation`).
//!
//! Патч 4 (`migrate`) юнит-тестом не проверяется по той же причине.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bpm_engine::bpm_engine_adapter_memory::{MemoryRepo, ProcessDefStore};
use bpm_engine::bpm_engine_bpmn::parse_and_compile;
use bpm_engine::bpm_engine_core::{
    payloads, EdgeCondition, EngineEvent, InstanceState, ProcessInstance, Token, TokenMode,
    TokenStatus,
};
use bpm_engine::bpm_engine_runtime::{
    BpmEngine, EngineContext, EventHandler, ExternalTaskCompletedHandler, MessageCatchHandler,
    ProcessStartHandler, TokenArrivedHandler, UserTaskCompletedHandler,
};
use bpm_engine::bpm_engine_storage::ProcessInstanceStore;

const GATEWAY_PROCESS_ID: &str = "approval";
const BRANCH_PROCESS_ID: &str = "branch";

/// Схема со шлюзом: default-связь `Flow_Reject` объявлена **до** условной
/// `Flow_Approve` и своего `conditionExpression` не имеет — ровно тот случай,
/// в котором upstream-компилятор терял флаг `default`.
fn gateway_xml() -> String {
    r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="http://bpmn-engine.local">
  <process id="approval" name="Approval" isExecutable="true">
    <startEvent id="Start"/>
    <userTask id="Task_Review" name="Review"/>
    <exclusiveGateway id="Gateway_Decision" default="Flow_Reject"/>
    <userTask id="Task_Approve" name="Approve"/>
    <userTask id="Task_Reject" name="Reject"/>
    <endEvent id="End"/>
    <sequenceFlow id="Flow_Start" sourceRef="Start" targetRef="Task_Review"/>
    <sequenceFlow id="Flow_Review" sourceRef="Task_Review" targetRef="Gateway_Decision"/>
    <sequenceFlow id="Flow_Reject" sourceRef="Gateway_Decision" targetRef="Task_Reject"/>
    <sequenceFlow id="Flow_Approve" sourceRef="Gateway_Decision" targetRef="Task_Approve">
      <conditionExpression>${decision == "approve"}</conditionExpression>
    </sequenceFlow>
    <sequenceFlow id="Flow_ApproveEnd" sourceRef="Task_Approve" targetRef="End"/>
    <sequenceFlow id="Flow_RejectEnd" sourceRef="Task_Reject" targetRef="End"/>
  </process>
</definitions>"#
        .to_string()
}

/// Схема для проверок токенов: `Task_A` (человек) → `Task_External` (воркер) →
/// `Task_B` (человек) → конец.
fn branch_xml() -> String {
    r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="http://bpmn-engine.local">
  <process id="branch" name="Branch" isExecutable="true">
    <startEvent id="Start"/>
    <userTask id="Task_A" name="A"/>
    <serviceTask id="Task_External" name="External"/>
    <userTask id="Task_B" name="B"/>
    <endEvent id="End"/>
    <sequenceFlow id="Flow_Start" sourceRef="Start" targetRef="Task_A"/>
    <sequenceFlow id="Flow_A" sourceRef="Task_A" targetRef="Task_External"/>
    <sequenceFlow id="Flow_External" sourceRef="Task_External" targetRef="Task_B"/>
    <sequenceFlow id="Flow_B" sourceRef="Task_B" targetRef="End"/>
  </process>
</definitions>"#
        .to_string()
}

/// Стенд: скомпилированное определение в memory-сторах и `EngineContext` поверх них.
struct Harness {
    repo: Arc<MemoryRepo>,
    ctx: EngineContext,
}

async fn harness(xml: &str, instance: Option<ProcessInstance>) -> Harness {
    let definition = parse_and_compile(xml).expect("схема должна компилироваться");
    let def_store = Arc::new(ProcessDefStore::new());
    def_store.register(definition);
    let repo = Arc::new(MemoryRepo::new());
    if let Some(instance) = instance {
        repo.save(&instance).await.expect("инстанс сохраняется");
    }
    let ctx = EngineContext::builder(repo.clone(), repo.clone(), def_store).build();
    Harness { repo, ctx }
}

/// Токен, который ждёт решения человека.
fn waiting_token(id: &str, node_id: &str, group: Option<&str>) -> Token {
    Token {
        id: id.to_string(),
        node_id: node_id.to_string(),
        status: TokenStatus::Waiting,
        mode: TokenMode::Forward,
        version: 0,
        attempt: 0,
        parallel_group_id: group.map(String::from),
        updated_at: None,
    }
}

/// Записывает все события, прошедшие через пумпу (для проверки `TokenCompleted`).
#[derive(Default)]
struct Recorder {
    seen: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl EventHandler for Recorder {
    async fn handle(&self, event: &EngineEvent, _ctx: &mut EngineContext) -> Vec<EngineEvent> {
        self.seen.lock().unwrap().push(format!("{event:?}"));
        vec![]
    }
}

async fn load_instance(repo: &MemoryRepo, instance_id: &str) -> ProcessInstance {
    repo.load(instance_id)
        .await
        .expect("чтение инстанса")
        .expect("инстанс существует")
}

// --- Патч 3: default-связь без conditionExpression -------------------------

#[test]
fn default_flow_without_condition_compiles_to_default() {
    let definition = parse_and_compile(&gateway_xml()).expect("схема компилируется");
    let gateway = definition
        .nodes
        .get("Gateway_Decision")
        .expect("шлюз в определении");
    assert_eq!(gateway.outgoing_edges.len(), 2);

    let reject = gateway
        .outgoing_edges
        .iter()
        .find(|edge| edge.target == "Task_Reject")
        .expect("связь в ветку Reject");
    assert!(
        matches!(reject.condition, Some(EdgeCondition::Default)),
        "default-связь без условия должна компилироваться в Default, получено: {:?}",
        reject.condition
    );

    let approve = gateway
        .outgoing_edges
        .iter()
        .find(|edge| edge.target == "Task_Approve")
        .expect("связь в ветку Approve");
    assert!(
        matches!(approve.condition, Some(EdgeCondition::Expression(_))),
        "условная связь должна остаться выражением, получено: {:?}",
        approve.condition
    );
}

// --- Патч 1: переменные шага доезжают до шлюза -----------------------------

#[tokio::test]
async fn user_task_variables_reach_gateway() {
    let Harness { repo, mut ctx } = harness(&gateway_xml(), None).await;
    let engine = BpmEngine::new(vec![
        Box::new(ProcessStartHandler),
        Box::new(TokenArrivedHandler::new()),
        Box::new(UserTaskCompletedHandler),
    ]);

    engine
        .run_async(
            EngineEvent::ProcessStarted(payloads::ProcessStarted {
                process_id: GATEWAY_PROCESS_ID.to_string(),
                instance_id: "inst-approval".to_string(),
                initial_variables: Some(HashMap::from([("decision".to_string(), String::new())])),
            }),
            &mut ctx,
        )
        .await;

    let instance = load_instance(&repo, "inst-approval").await;
    let review = instance
        .tokens
        .iter()
        .find(|token| token.node_id == "Task_Review" && token.status == TokenStatus::Waiting)
        .expect("токен ждёт шага Review")
        .clone();

    engine
        .run_async(
            EngineEvent::UserTaskCompleted(payloads::UserTaskCompleted {
                task_id: review.id.clone(),
                instance_id: "inst-approval".to_string(),
                node_id: "Task_Review".to_string(),
                variables: HashMap::from([("decision".to_string(), "approve".to_string())]),
            }),
            &mut ctx,
        )
        .await;

    let instance = load_instance(&repo, "inst-approval").await;
    assert_eq!(
        instance.variables.get("decision"),
        Some(&"approve".to_string()),
        "переменная шага должна попасть в инстанс"
    );
    assert!(
        instance
            .tokens
            .iter()
            .any(|token| token.node_id == "Task_Approve" && token.status == TokenStatus::Waiting),
        "шлюз должен уйти в ветку Approve по переменной шага, токены: {:?}",
        instance.tokens
    );
    assert!(
        !instance
            .tokens
            .iter()
            .any(|token| token.node_id == "Task_Reject"),
        "default-ветка не должна срабатывать при выполненном условии"
    );
}

// --- Патч 1: один токен узла и сохранение группы ---------------------------

#[tokio::test]
async fn user_task_completion_closes_one_token_and_keeps_group() {
    let instance = ProcessInstance {
        id: "inst-parallel".to_string(),
        process_def_id: BRANCH_PROCESS_ID.to_string(),
        tenant_id: None,
        tokens: vec![
            waiting_token("token-1", "Task_A", Some("group-1")),
            waiting_token("token-2", "Task_A", Some("group-1")),
        ],
        variables: HashMap::new(),
        state: InstanceState::Running,
        version: 0,
        parent_instance_id: None,
        parent_token_id: None,
    };
    let Harness { repo, mut ctx } = harness(&branch_xml(), Some(instance)).await;
    let recorder = Recorder::default();
    let seen = recorder.seen.clone();
    let engine = BpmEngine::new(vec![Box::new(UserTaskCompletedHandler), Box::new(recorder)]);

    engine
        .run_async(
            EngineEvent::UserTaskCompleted(payloads::UserTaskCompleted {
                task_id: "token-1".to_string(),
                instance_id: "inst-parallel".to_string(),
                node_id: "Task_A".to_string(),
                variables: HashMap::new(),
            }),
            &mut ctx,
        )
        .await;

    let instance = load_instance(&repo, "inst-parallel").await;
    let still_waiting: Vec<&Token> = instance
        .tokens
        .iter()
        .filter(|token| token.node_id == "Task_A")
        .collect();
    assert_eq!(
        still_waiting.len(),
        1,
        "завершение шага не должно закрывать соседнюю ветку на том же узле: {:?}",
        instance.tokens
    );
    assert_eq!(still_waiting[0].id, "token-2");
    assert_eq!(still_waiting[0].status, TokenStatus::Waiting);

    let moved = instance
        .tokens
        .iter()
        .find(|token| token.node_id == "Task_External")
        .expect("шаг двигает токен по исходящей связи");
    assert_eq!(
        moved.parallel_group_id.as_deref(),
        Some("group-1"),
        "группа параллельной ветки должна сохраниться"
    );

    let events = seen.lock().unwrap().clone();
    assert!(
        events.iter().any(|event| event.contains("TokenCompleted")),
        "шаг должен отдавать TokenCompleted для истории, события: {:?}",
        events
    );
}

// --- Патч 2: внешняя задача сохраняет группу -------------------------------

#[tokio::test]
async fn external_task_completion_keeps_group() {
    let instance = ProcessInstance {
        id: "inst-external".to_string(),
        process_def_id: BRANCH_PROCESS_ID.to_string(),
        tenant_id: None,
        tokens: vec![waiting_token("token-1", "Task_External", Some("group-2"))],
        variables: HashMap::new(),
        state: InstanceState::Running,
        version: 0,
        parent_instance_id: None,
        parent_token_id: None,
    };
    let Harness { repo, mut ctx } = harness(&branch_xml(), Some(instance)).await;
    let engine = BpmEngine::new(vec![Box::new(ExternalTaskCompletedHandler)]);

    engine
        .run_async(
            EngineEvent::ExternalTaskCompleted(payloads::ExternalTaskCompleted {
                instance_id: "inst-external".to_string(),
                token_id: "token-1".to_string(),
                node_id: "Task_External".to_string(),
                variables: HashMap::from([("erp_ok".to_string(), "true".to_string())]),
            }),
            &mut ctx,
        )
        .await;

    let instance = load_instance(&repo, "inst-external").await;
    assert_eq!(
        instance.variables.get("erp_ok"),
        Some(&"true".to_string()),
        "переменные воркера должны попасть в инстанс"
    );
    let moved = instance
        .tokens
        .iter()
        .find(|token| token.node_id == "Task_B")
        .expect("внешняя задача двигает токен по исходящей связи");
    assert_eq!(
        moved.parallel_group_id.as_deref(),
        Some("group-2"),
        "группа параллельной ветки должна сохраниться"
    );
}

// --- Патч 6: корреляция сообщения по бизнес-ключу (M3) ---------------------

/// Идентификатор процесса со схемой сообщений.
const MESSAGE_PROCESS_ID: &str = "messages";

/// Схема с catch-событием: `WaitMessage` ждёт сообщение `POConfirmed`.
fn message_xml() -> String {
    r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="http://bpmn-engine.local">
  <message id="Message_PO" name="POConfirmed"/>
  <process id="messages" name="Messages" isExecutable="true">
    <startEvent id="Start"/>
    <intermediateCatchEvent id="WaitMessage">
      <messageEventDefinition messageRef="Message_PO"/>
    </intermediateCatchEvent>
    <userTask id="Task_After" name="After"/>
    <endEvent id="End"/>
    <sequenceFlow id="Flow_Start" sourceRef="Start" targetRef="WaitMessage"/>
    <sequenceFlow id="Flow_Message" sourceRef="WaitMessage" targetRef="Task_After"/>
    <sequenceFlow id="Flow_End" sourceRef="Task_After" targetRef="End"/>
  </process>
</definitions>"#
        .to_string()
}

/// Экземпляр, который ждёт сообщение и уже знает свой бизнес-ключ.
fn waiting_for_message(instance_id: &str, order_num: &str) -> ProcessInstance {
    ProcessInstance {
        id: instance_id.to_string(),
        process_def_id: MESSAGE_PROCESS_ID.to_string(),
        tenant_id: None,
        tokens: vec![waiting_token("token-wait", "WaitMessage", None)],
        variables: HashMap::from([("orderNum".to_string(), order_num.to_string())]),
        state: InstanceState::Running,
        version: 0,
        parent_instance_id: None,
        parent_token_id: None,
    }
}

#[tokio::test]
async fn message_delivery_respects_correlation() {
    let Harness { repo, mut ctx } = harness(&message_xml(), None).await;
    repo.save(&waiting_for_message("inst-a", "ORD-A"))
        .await
        .expect("инстанс A сохраняется");
    repo.save(&waiting_for_message("inst-b", "ORD-B"))
        .await
        .expect("инстанс B сохраняется");
    let engine = BpmEngine::new(vec![
        Box::new(MessageCatchHandler),
        Box::new(TokenArrivedHandler::new()),
    ]);

    engine
        .run_async(
            EngineEvent::MessageSent(payloads::MessageSent {
                instance_id: "inst-supplier".to_string(),
                message_name: "POConfirmed".to_string(),
                node_id: "Event_ThrowPO".to_string(),
                correlation: Some(HashMap::from([(
                    "orderNum".to_string(),
                    "ORD-A".to_string(),
                )])),
                payload: Some(HashMap::from([(
                    "shippedAt".to_string(),
                    "10:00".to_string(),
                )])),
            }),
            &mut ctx,
        )
        .await;

    let correlated = load_instance(&repo, "inst-a").await;
    assert!(
        correlated
            .tokens
            .iter()
            .any(|token| token.node_id == "Task_After" && token.status == TokenStatus::Waiting),
        "сообщение должно поднять токен своего заказа, токены: {:?}",
        correlated.tokens
    );
    assert_eq!(
        correlated.variables.get("shippedAt"),
        Some(&"10:00".to_string()),
        "payload сообщения доезжает до получателя"
    );

    let foreign = load_instance(&repo, "inst-b").await;
    assert!(
        !foreign
            .tokens
            .iter()
            .any(|token| token.node_id == "Task_After"),
        "экземпляр с другим orderNum не должен получать сообщение, токены: {:?}",
        foreign.tokens
    );
}

// --- Патч 5: `load` выбирает колонки, которые читает -----------------------

/// Исходник вендорного postgres-адаптера. Живого Postgres в юнит-прогоне пакета
/// нет, поэтому патч проверяется по тексту запроса: `load` падал в рантайме на
/// колонке, которой не было в его же `SELECT`.
const POSTGRES_PROCESS_STORE: &str =
    include_str!("../vendor/bpm-engine/crates/adapters/postgres/src/process_store.rs");

#[test]
fn instance_load_selects_parent_columns() {
    let load = POSTGRES_PROCESS_STORE
        .split("async fn load(&self, id: &str)")
        .nth(1)
        .expect("`PostgresProcessStore::load` найден в вендорном адаптере");
    let query = load
        .split("FROM process_instance")
        .next()
        .expect("в `load` есть запрос по `process_instance`");

    for column in ["parent_instance_id", "parent_token_id"] {
        assert!(
            query.contains(column),
            "SELECT в `load` не выбирает `{column}`, хотя строка её читает: \
             `row.get` паникует с `invalid column`, старт процесса обрывается \
             после записи Ready-токена"
        );
        assert!(
            load.contains(&format!("row.get(\"{column}\")")),
            "`{column}` больше не читается в `load` — проверка патча 5 устарела"
        );
    }
}
