//! Регрессионные проверки слияния `parallelGateway`.
//!
//! Историческая проблема (ограничение 13, `docs/runtime-m1-design.md`): счётчик
//! «сколько ветвей пришло» держался в памяти `TokenArrivedHandler`, поэтому
//! при `replicas: 3` (или рестарте между ветвями) вторая ветвь попадала в под
//! без первой записи и слияние не срабатывало. Тесты фиксируют контракт: с
//! общим `ParallelJoinRepo` слияние завершается, даже если ветви обработали
//! **разные** экземпляры движка.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bpm_engine::bpm_engine_adapter_memory::{MemoryRepo, ProcessDefStore};
use bpm_engine::bpm_engine_bpmn::parse_and_compile;
use bpm_engine::bpm_engine_core::{
    payloads, EngineEvent, InstanceState, ProcessInstance, Token, TokenStatus,
};
use bpm_engine::bpm_engine_runtime::{
    BpmEngine, EngineContext, ProcessStartHandler, TokenArrivedHandler, UserTaskCompletedHandler,
};
use bpm_engine::bpm_engine_storage::{ParallelJoinRepo, ProcessInstanceStore};

const PROCESS_ID: &str = "pq";

fn parallel_xml() -> String {
    r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="http://bpmn-engine.local">
  <process id="pq" name="Parallel" isExecutable="true">
    <startEvent id="Start"/>
    <parallelGateway id="Fork"/>
    <userTask id="Task_A"/>
    <userTask id="Task_B"/>
    <parallelGateway id="Join"/>
    <endEvent id="End"/>
    <sequenceFlow id="F_Start" sourceRef="Start" targetRef="Fork"/>
    <sequenceFlow id="F_A" sourceRef="Fork" targetRef="Task_A"/>
    <sequenceFlow id="F_B" sourceRef="Fork" targetRef="Task_B"/>
    <sequenceFlow id="F_JA" sourceRef="Task_A" targetRef="Join"/>
    <sequenceFlow id="F_JB" sourceRef="Task_B" targetRef="Join"/>
    <sequenceFlow id="F_End" sourceRef="Join" targetRef="End"/>
  </process>
</definitions>"#
        .to_string()
}

/// Общее состояние слияния, доступное нескольким «подам» (несколько `BpmEngine`).
///
/// Повторяет SQL-семантику [`crate::runtime::join_repo::PgParallelJoinRepo`]:
/// инкремент не превышает `expected`, уже слитая группа остаётся завершённой.
#[derive(Default)]
struct SharedJoinRepo {
    state: Mutex<HashMap<String, (u32, u32)>>,
}

#[async_trait]
impl ParallelJoinRepo for SharedJoinRepo {
    async fn ensure_group(&self, group_id: &str, expected: u32) -> anyhow::Result<()> {
        self.state
            .lock()
            .unwrap()
            .entry(group_id.to_string())
            .or_insert((expected, 0));
        Ok(())
    }

    async fn try_join(&self, group_id: &str) -> anyhow::Result<bool> {
        let mut state = self.state.lock().unwrap();
        let (expected, joined) = state
            .get_mut(group_id)
            .ok_or_else(|| anyhow::anyhow!("parallel join group not registered: {group_id}"))?;
        if *joined < *expected {
            *joined += 1;
        }
        Ok(*joined >= *expected)
    }
}

fn engine() -> BpmEngine {
    BpmEngine::new(vec![
        Box::new(ProcessStartHandler),
        Box::new(TokenArrivedHandler::new()),
        Box::new(UserTaskCompletedHandler),
    ])
}

async fn start(engine: &BpmEngine, ctx: &mut EngineContext) {
    engine
        .run_async(
            EngineEvent::ProcessStarted(payloads::ProcessStarted {
                process_id: PROCESS_ID.to_string(),
                instance_id: "inst-pq".to_string(),
                initial_variables: None,
            }),
            ctx,
        )
        .await;
}

async fn complete(engine: &BpmEngine, ctx: &mut EngineContext, token: &Token) {
    engine
        .run_async(
            EngineEvent::UserTaskCompleted(payloads::UserTaskCompleted {
                task_id: token.id.clone(),
                instance_id: "inst-pq".to_string(),
                node_id: token.node_id.clone(),
                variables: HashMap::new(),
            }),
            ctx,
        )
        .await;
}

async fn load_instance(repo: &MemoryRepo) -> ProcessInstance {
    repo.load("inst-pq")
        .await
        .expect("чтение инстанса")
        .expect("инстанс существует")
}

fn waiting(instance: &ProcessInstance, node_id: &str) -> Token {
    instance
        .tokens
        .iter()
        .find(|token| token.node_id == node_id && token.status == TokenStatus::Waiting)
        .unwrap_or_else(|| panic!("токен ждёт на {node_id}: {:?}", instance.tokens))
        .clone()
}

#[tokio::test]
async fn parallel_join_fires_after_both_branches_on_one_engine() {
    let repo = Arc::new(MemoryRepo::new());
    let def_store = Arc::new(ProcessDefStore::new());
    def_store.register(parse_and_compile(&parallel_xml()).expect("схема компилируется"));
    let mut ctx = EngineContext::builder(repo.clone(), repo.clone(), def_store)
        .parallel_join_repo(Arc::new(SharedJoinRepo::default()))
        .build();
    let engine = engine();

    start(&engine, &mut ctx).await;

    let instance = load_instance(&repo).await;
    let token_a = waiting(&instance, "Task_A");
    let token_b = waiting(&instance, "Task_B");
    assert_eq!(
        token_a.parallel_group_id, token_b.parallel_group_id,
        "вилка обязана дать ветвям одну группу"
    );

    complete(&engine, &mut ctx, &token_a).await;
    let instance = load_instance(&repo).await;
    assert_ne!(
        instance.state,
        InstanceState::Completed,
        "одна ветвь не завершает экземпляр"
    );
    assert!(
        waiting(&instance, "Join").parallel_group_id == token_a.parallel_group_id,
        "первая ветвь ждёт на слиянии с группой вилки"
    );

    complete(&engine, &mut ctx, &token_b).await;
    let instance = load_instance(&repo).await;
    assert_eq!(
        instance.state,
        InstanceState::Completed,
        "обе ветви закрыты — экземпляр завершён: {:?}",
        instance.tokens
    );
}

#[tokio::test]
async fn parallel_join_fires_when_branches_split_across_two_engines() {
    // Общие «БД» и состояние слияния; у каждого «пода» свои хендлеры (и свой
    // in-memory join_state, который больше не используется для слияния).
    let repo = Arc::new(MemoryRepo::new());
    let join_repo = Arc::new(SharedJoinRepo::default());

    let def_a = Arc::new(ProcessDefStore::new());
    def_a.register(parse_and_compile(&parallel_xml()).expect("схема компилируется"));
    let def_b = Arc::new(ProcessDefStore::new());
    def_b.register(parse_and_compile(&parallel_xml()).expect("схема компилируется"));

    let mut ctx_a = EngineContext::builder(repo.clone(), repo.clone(), def_a)
        .parallel_join_repo(join_repo.clone())
        .build();
    let mut ctx_b = EngineContext::builder(repo.clone(), repo.clone(), def_b)
        .parallel_join_repo(join_repo.clone())
        .build();
    let engine_a = engine();
    let engine_b = engine();

    start(&engine_a, &mut ctx_a).await;

    let instance = load_instance(&repo).await;
    let token_a = waiting(&instance, "Task_A");
    let token_b = waiting(&instance, "Task_B");

    // Первая ветвь — «под A», вторая — «под B».
    complete(&engine_a, &mut ctx_a, &token_a).await;
    let instance = load_instance(&repo).await;
    assert_ne!(
        instance.state,
        InstanceState::Completed,
        "одна ветвь не завершает экземпляр"
    );

    complete(&engine_b, &mut ctx_b, &token_b).await;
    let instance = load_instance(&repo).await;
    assert_eq!(
        instance.state,
        InstanceState::Completed,
        "слияние должно сработать, даже если ветви обработали разные поды: {:?}",
        instance.tokens
    );
}
