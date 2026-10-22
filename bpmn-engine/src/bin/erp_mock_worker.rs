//! Мок-воркер ERP для демо: подписывается на команды `serviceTask` и
//! отвечает в движок.
//!
//! Контракт — README §3 и `docs/runtime-m1-design.md`, 10.4:
//!
//! - читает `mes.out.v1.cmd.<topic>` (`ServiceCommand`), по умолчанию тема
//!   `erp` — её объявляют `Task_ERP_*` демо-схемы (`camunda:topic="erp"`);
//! - отвечает в `mes.in.v1.erp.response` тем же `element_id` и тем же
//!   `idempotency_key`, иначе движок отбросит ответ (`erp_target_mismatch`)
//!   или обработает его как терминальный и погасит ретраи;
//! - повторную команду с уже обработанным ключом не выполняет второй раз:
//!   публикация ответа идемпотентна (так же должен вести себя настоящий
//!   воркер 1С).
//!
//! Переменные окружения:
//!
//! | Переменная | По умолчанию | Смысл |
//! | --- | --- | --- |
//! | `NATS_URL` | `nats://nats-service:4222` | брокер |
//! | `ERP_MOCK_TOPIC` | `erp` | тема воркера (`camunda:topic`) |
//! | `ERP_MOCK_DELAY_MS` | `500` | имитация работы ERP |
//! | `ERP_MOCK_FAIL_FIRST` | `0` | сколько первых попыток на ключ провалить с `retryable = true` (демо ретраев) |
//! | `ERP_MOCK_FAIL_ELEMENT` | — | узел, который всегда отвечает `FAILED` (проверка компенсации) |
//!
//! Запуск: `cargo run --bin erp_mock_worker` или под
//! `kubectl apply -f erp-mock-worker.yaml` (тот же образ, что у движка).

use std::collections::{HashMap, HashSet};
use std::env;
use std::time::Duration;

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Команда воркеру (подмножество `ServiceCommand` движка: мок не требует
/// полей, которых не использует).
#[derive(Debug, Clone, Deserialize)]
struct ServiceCommand {
    instance_id: String,
    element_id: String,
    #[serde(default)]
    payload: Value,
    idempotency_key: String,
}

/// Ответ движку (`mes.in.v1.erp.response`).
#[derive(Debug, Clone, Serialize)]
struct ErpResponse {
    process_instance_id: String,
    element_id: String,
    status: String,
    error_message: Option<String>,
    retryable: bool,
    payload: Value,
    idempotency_key: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://nats-service:4222".to_string());
    let topic = env::var("ERP_MOCK_TOPIC").unwrap_or_else(|_| "erp".to_string());
    let delay_ms = env_secs("ERP_MOCK_DELAY_MS", 500);
    let fail_first = env_secs("ERP_MOCK_FAIL_FIRST", 0);
    let fail_element = env::var("ERP_MOCK_FAIL_ELEMENT").unwrap_or_default();

    let subject = format!("mes.out.v1.cmd.{}", topic);
    let response_subject = "mes.in.v1.erp.response";

    println!("🤖 [ERP Mock] Подключение к NATS {}...", nats_url);
    let client = async_nats::connect(&nats_url).await?;
    let mut commands = client.subscribe(subject.clone()).await?;

    println!(
        "✅ [ERP Mock] Слушаю {}, отвечаю в {} (задержка {} мс, fail_first={})",
        subject, response_subject, delay_ms, fail_first
    );

    // Ключ идемпотентности уже обработанной команды: повторная доставка не
    // должна выполнять работу дважды. Для демо память не ограничена —
    // настоящий воркер хранит ключи в своей БД.
    let mut done: HashSet<String> = HashSet::new();
    let mut attempts: HashMap<String, u64> = HashMap::new();

    while let Some(message) = commands.next().await {
        let raw: Value = match serde_json::from_slice(&message.payload) {
            Ok(raw) => raw,
            Err(e) => {
                eprintln!("❌ [ERP Mock] Ошибка десериализации сообщения: {}", e);
                continue;
            }
        };

        // По тому же subject идут компенсации (`ActionRequiredEvent`): их
        // разбирает человек, воркер только показывает, что случилось.
        if is_action_required(&raw) {
            eprintln!(
                "🚨 [ERP Mock] Компенсация: шаг {} экземпляра {} требует человека",
                raw.get("current_step")
                    .and_then(Value::as_str)
                    .unwrap_or("?"),
                raw.get("process_instance_id")
                    .and_then(Value::as_str)
                    .unwrap_or("?")
            );
            continue;
        }

        let command = match serde_json::from_value::<ServiceCommand>(raw) {
            Ok(command) => command,
            Err(e) => {
                eprintln!("❌ [ERP Mock] Это не ServiceCommand: {}", e);
                continue;
            }
        };

        if done.contains(&command.idempotency_key) {
            println!(
                "🔁 [ERP Mock] Повтор команды {}:{} — ответ уже отправлен",
                command.instance_id, command.element_id
            );
            continue;
        }

        let attempt = attempts
            .entry(command.idempotency_key.clone())
            .and_modify(|count| *count += 1)
            .or_insert(1);

        let should_fail = fail_element == command.element_id || *attempt <= fail_first;

        tokio::time::sleep(Duration::from_millis(delay_ms)).await;

        let response = if should_fail {
            println!(
                "⚠️ [ERP Mock] {}:{} — попытка {} провалена (retryable)",
                command.instance_id, command.element_id, attempt
            );
            ErpResponse {
                process_instance_id: command.instance_id.clone(),
                element_id: command.element_id.clone(),
                status: "FAILED".to_string(),
                error_message: Some("mock: ERP временно недоступна".to_string()),
                retryable: true,
                payload: json!({ "source": "erp-mock-worker", "attempt": *attempt }),
                idempotency_key: command.idempotency_key.clone(),
            }
        } else {
            let document = format!(
                "ERP-{:x}",
                fnv1a(command.idempotency_key.as_bytes()) % 0x1_0000_0000
            );
            println!(
                "✅ [ERP Mock] {}:{} — обработано, документ {}",
                command.instance_id, command.element_id, document
            );
            done.insert(command.idempotency_key.clone());

            ErpResponse {
                process_instance_id: command.instance_id.clone(),
                element_id: command.element_id.clone(),
                status: "SUCCESS".to_string(),
                error_message: None,
                retryable: false,
                payload: json!({
                    "status": "DONE",
                    "source": "erp-mock-worker",
                    "document": document,
                    "attempt": *attempt,
                    "orderNum": command.payload.get("orderNum").cloned(),
                    "processed_at": chrono::Utc::now().to_rfc3339(),
                }),
                idempotency_key: command.idempotency_key.clone(),
            }
        };

        let bytes = serde_json::to_vec(&response)?.into();
        client.publish(response_subject, bytes).await?;
        println!(
            "📤 [ERP Mock] Ответ {}:{} → {}",
            response.process_instance_id, response.element_id, response_subject
        );
    }

    Ok(())
}

/// Число из окружения; пустое или битое значение — `default`.
fn env_secs(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

/// Компенсация (`ActionRequiredEvent`) приходит в тот же subject, что команды:
/// её узнаём по `current_step`/`process_instance_id`, воркер её не исполняет.
fn is_action_required(payload: &Value) -> bool {
    payload.get("current_step").is_some() && payload.get("process_instance_id").is_some()
}

/// FNV-1a: стабильный «номер документа» из ключа идемпотентности, чтобы
/// повторный ответ на тот же ключ давал тот же документ.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_hash_is_stable_per_idempotency_key() {
        assert_eq!(
            fnv1a(b"instance-1:Task_ERP:token-1"),
            fnv1a(b"instance-1:Task_ERP:token-1")
        );
        assert_ne!(
            fnv1a(b"instance-1:Task_ERP:token-1"),
            fnv1a(b"instance-1:Task_ERP:token-2")
        );
    }

    #[test]
    fn compensation_is_not_a_command() {
        assert!(is_action_required(&json!({
            "process_instance_id": "instance-1",
            "current_step": "Task_ERP_CriticalAlert",
            "target_system": "ERP",
            "payload": { "reason": "RETRIES_EXHAUSTED" }
        })));
        assert!(!is_action_required(&json!({
            "instance_id": "instance-1",
            "element_id": "Task_ERP_CriticalAlert",
            "idempotency_key": "instance-1:Task_ERP_CriticalAlert:token-1"
        })));
    }
}
