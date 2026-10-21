//! Проекция снимков движка в legacy read-model и WS-сообщения.
//!
//! Пумпа не возвращает обработанные события, поэтому дифф считается по
//! снимкам «до/после» (дизайн `docs/runtime-m1-design.md`, 5.3). Legacy
//! `tokens` — вторичная запись: источник истины живёт в вендорных таблицах.

use std::collections::HashSet;

use bpm_engine::bpm_engine_core::InstanceState;
use sqlx::PgPool;

use super::pump::{Snapshot, Step};
use crate::db;

/// Что изменилось за один проход пумпы.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Projection {
    /// Новые активные шаги (`Waiting`-токены на `userTask`/`serviceTask`).
    pub activated: Vec<Step>,
    /// Шаги, которые были активными и перестали ими быть.
    pub closed: Vec<Step>,
    /// Экземпляр завершился или был прерван.
    pub finished: Option<InstanceState>,
}

impl Projection {
    /// Нечего писать ни в legacy, ни в WebSocket.
    pub fn is_empty(&self) -> bool {
        self.activated.is_empty() && self.closed.is_empty() && self.finished.is_none()
    }
}

/// Сравнивает снимки и возвращает изменения.
///
/// `before = None` — старт экземпляра: все ожидающие шаги новые, закрытых нет.
pub fn diff(before: Option<&Snapshot>, after: &Snapshot) -> Projection {
    let before_steps = before.map(Snapshot::waiting_steps).unwrap_or_default();
    let after_steps = after.waiting_steps();

    // Сравниваем пары «токен + узел»: токен может остаться тем же, а шаг —
    // смениться (например, если движок переиспользует id токена).
    let before_pairs: HashSet<(&str, &str)> = before_steps
        .iter()
        .map(|step| (step.token_id.as_str(), step.element_id.as_str()))
        .collect();
    let after_pairs: HashSet<(&str, &str)> = after_steps
        .iter()
        .map(|step| (step.token_id.as_str(), step.element_id.as_str()))
        .collect();

    let activated: Vec<Step> = after_steps
        .iter()
        .filter(|step| !before_pairs.contains(&(step.token_id.as_str(), step.element_id.as_str())))
        .cloned()
        .collect();
    let closed: Vec<Step> = before_steps
        .iter()
        .filter(|step| !after_pairs.contains(&(step.token_id.as_str(), step.element_id.as_str())))
        .cloned()
        .collect();

    let previous_state = before.map(|snapshot| snapshot.instance.state);
    let finished = match after.instance.state {
        InstanceState::Running => None,
        state if previous_state == Some(state) => None,
        state => Some(state),
    };

    Projection {
        activated,
        closed,
        finished,
    }
}

/// Пишет проекцию в legacy-таблицы кабинета и E2E-тестов.
///
/// Порядок важен: сначала закрытия, потом активации. Legacy-ключ токена —
/// `"<instance>:<element>"`, поэтому при двух ветках на одном узле `ACTIVE`
/// должен победить `COMPLETED`.
pub async fn apply_legacy(
    pool: &PgPool,
    instance_id: &str,
    process_def_id: &str,
    projection: &Projection,
) -> Result<(), sqlx::Error> {
    if projection.is_empty() {
        return Ok(());
    }

    // FK `tokens.instance_id → process_instances.id` требует строки инстанса.
    db::save_process_instance(pool, instance_id, process_def_id).await?;

    for step in &projection.closed {
        db::save_token_state(pool, instance_id, &step.element_id, "COMPLETED").await?;
    }
    for step in &projection.activated {
        db::save_token_state(pool, instance_id, &step.element_id, "ACTIVE").await?;
    }
    if let Some(state) = projection.finished {
        db::update_process_instance_status(pool, instance_id, legacy_status(state)).await?;
    }

    Ok(())
}

fn legacy_status(state: InstanceState) -> &'static str {
    match state {
        InstanceState::Running => "ACTIVE",
        InstanceState::Completed => "COMPLETED",
        InstanceState::Terminated => "TERMINATED",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bpm_engine::bpm_engine_bpmn::parse_and_compile;
    use bpm_engine::bpm_engine_core::{
        ProcessDefinition, ProcessInstance, Token, TokenMode, TokenStatus,
    };

    use super::*;

    fn definition() -> ProcessDefinition {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="http://bpmn-engine.local">
  <process id="projection_test" name="Projection" isExecutable="true">
    <startEvent id="Start"/>
    <userTask id="Task_A" name="A"/>
    <userTask id="Task_B" name="B"/>
    <endEvent id="End"/>
    <sequenceFlow id="Flow_Start" sourceRef="Start" targetRef="Task_A"/>
    <sequenceFlow id="Flow_A" sourceRef="Task_A" targetRef="Task_B"/>
    <sequenceFlow id="Flow_B" sourceRef="Task_B" targetRef="End"/>
  </process>
</definitions>"#;
        parse_and_compile(xml).expect("схема компилируется")
    }

    fn token(id: &str, node_id: &str, status: TokenStatus) -> Token {
        Token {
            id: id.to_string(),
            node_id: node_id.to_string(),
            status,
            mode: TokenMode::Forward,
            version: 0,
            attempt: 0,
            parallel_group_id: None,
            updated_at: None,
        }
    }

    fn snapshot(tokens: Vec<Token>, state: InstanceState) -> Snapshot {
        Snapshot {
            instance: ProcessInstance {
                id: "instance-1".to_string(),
                process_def_id: "projection_test".to_string(),
                tenant_id: None,
                tokens,
                variables: HashMap::new(),
                state,
                version: 0,
                parent_instance_id: None,
                parent_token_id: None,
            },
            definition: definition(),
        }
    }

    fn step(token_id: &str, element_id: &str) -> Step {
        Step {
            token_id: token_id.to_string(),
            element_id: element_id.to_string(),
        }
    }

    #[test]
    fn start_activates_every_waiting_step() {
        let after = snapshot(
            vec![token("token-1", "Task_A", TokenStatus::Waiting)],
            InstanceState::Running,
        );

        let projection = diff(None, &after);

        assert_eq!(projection.activated, vec![step("token-1", "Task_A")]);
        assert!(projection.closed.is_empty());
        assert_eq!(projection.finished, None);
    }

    #[test]
    fn moved_token_closes_old_step_and_activates_new() {
        let before = snapshot(
            vec![
                token("token-1", "Task_A", TokenStatus::Waiting),
                token("token-gateway", "Start", TokenStatus::Waiting),
            ],
            InstanceState::Running,
        );
        let after = snapshot(
            vec![
                token("token-1", "Task_B", TokenStatus::Waiting),
                token("token-gateway", "Start", TokenStatus::Waiting),
            ],
            InstanceState::Running,
        );

        let projection = diff(Some(&before), &after);

        assert_eq!(projection.closed, vec![step("token-1", "Task_A")]);
        assert_eq!(projection.activated, vec![step("token-1", "Task_B")]);
        assert_eq!(projection.finished, None, "экземпляр ещё идёт");
    }

    #[test]
    fn ended_instance_closes_last_step() {
        let before = snapshot(
            vec![token("token-1", "Task_B", TokenStatus::Waiting)],
            InstanceState::Running,
        );
        let after = snapshot(vec![], InstanceState::Completed);

        let projection = diff(Some(&before), &after);

        assert_eq!(projection.closed, vec![step("token-1", "Task_B")]);
        assert!(projection.activated.is_empty());
        assert_eq!(projection.finished, Some(InstanceState::Completed));
    }

    #[test]
    fn nothing_changed_is_empty() {
        let before = snapshot(
            vec![token("token-1", "Task_A", TokenStatus::Waiting)],
            InstanceState::Running,
        );
        let after = snapshot(
            vec![token("token-1", "Task_A", TokenStatus::Waiting)],
            InstanceState::Running,
        );

        let projection = diff(Some(&before), &after);

        assert!(
            projection.is_empty(),
            "повторная доставка не должна ничего писать"
        );
    }

    #[test]
    fn repeated_completion_is_idempotent() {
        let before = snapshot(vec![], InstanceState::Completed);
        let after = snapshot(vec![], InstanceState::Completed);

        assert!(diff(Some(&before), &after).is_empty());
    }
}
