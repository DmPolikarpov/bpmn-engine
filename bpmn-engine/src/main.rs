mod events;
mod nats_client;
mod db;

use bpm_engine::bpm_engine_runtime::Engine;
use bpm_engine::bpm_engine_adapter_memory::MemoryTokenStore;
use bpm_engine::bpm_engine_bpmn::parse_and_compile;

use chrono::Utc;
use events::{ActionRequiredEvent, ErpResponseEvent, InboundEvent};
use futures::StreamExt;
use nats_client::NatsAdapter;
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🚀 Запуск BPMN Engine...");

    let db_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://bpmn_user:bpmn_super_secret_password@bpmn-db-rw:5432/bpmn_db".to_string()
    });
    
    let pool = PgPool::connect(&db_url).await?;

    println!("⚙️ Проверка и авто-применение миграций базы данных...");
    sqlx::migrate!("./migrations").run(&pool).await?;
    println!("✅ Миграции базы данных успешно применены!");

    let store = Arc::new(MemoryTokenStore::new());
    let engine = Arc::new(Mutex::new(Engine::new(store)));

    let nats_adapter = NatsAdapter::new().await?;
    let consumer = nats_adapter.create_inbound_consumer().await?;
    let erp_consumer = nats_adapter.create_erp_response_consumer().await?;

    let nats_sla = nats_adapter.clone();
    let nats_erp = nats_adapter.clone();

    let pool_sla = pool.clone();
    let pool_erp = pool.clone();

    // 1. Фоновый воркер SLA / таймаутов
    tokio::spawn(async move {
        let interval_secs = env::var("SLA_CHECK_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(3);

        let timeout_minutes = env::var("SLA_TIMEOUT_MINUTES")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(30);

        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        
        loop {
            interval.tick().await;

            let query = format!(
                "SELECT instance_id, element_id FROM tokens WHERE state = 'ACTIVE' AND created_at < NOW() - INTERVAL '{} minutes'",
                timeout_minutes
            );
            
            match sqlx::query(&query).fetch_all(&pool_sla).await {
                Ok(rows) => {
                    for row in rows {
                        let inst_id: String = row.get("instance_id");
                        let elem_id: String = row.get("element_id");

                        let outbound_event = ActionRequiredEvent {
                            process_instance_id: inst_id.clone(),
                            current_step: elem_id.clone(),
                            target_system: "ERP".to_string(),
                            payload: json!({
                                "status": "CHECK_TIMEOUT",
                                "reason": "SLA_EXPIRED",
                                "action": "CANCEL_OR_RETRY"
                            }),
                        };

                        if let Err(e) = nats_sla.publish_outbound_command(&outbound_event).await {
                            eprintln!("❌ [SLA Worker] Ошибка отправки компенсационной команды: {}", e);
                        } else {
                            if let Err(e) = db::save_token_state(&pool_sla, &inst_id, &elem_id, "TIMED_OUT").await {
                                eprintln!("❌ [SLA Worker] Ошибка БД при обновлении статуса токена: {}", e);
                            } else {
                                let _ = nats_sla.publish_token_event(&inst_id, &elem_id, "TIMED_OUT", None).await;
                            }
                        }
                    }
                }
                Err(e) => eprintln!("❌ [SLA Worker] Ошибка выполнения query в SLA Worker: {}", e),
            }
        }
    });

    // 2. Воркер ответов от ERP
    tokio::spawn(async move {
        loop {
            match erp_consumer.messages().await {
                Ok(mut messages) => {
                    while let Some(msg_result) = messages.next().await {
                        match msg_result {
                            Ok(msg) => {
                                match serde_json::from_slice::<ErpResponseEvent>(&msg.payload) {
                                    Ok(response) => {
                                        let inst_id = &response.process_instance_id;
                                        let element_id = if !response.element_id.is_empty() {
                                            response.element_id.as_str()
                                        } else {
                                            "ServiceTask_ERP_Sync"
                                        };

                                        if response.status == "SUCCESS" {
                                            if let Err(e) = db::save_token_state(&pool_erp, inst_id, element_id, "COMPLETED").await {
                                                eprintln!("❌ [ERP Consumer] Ошибка БД: {}", e);
                                            } else {
                                                let _ = nats_erp.publish_token_event(inst_id, element_id, "COMPLETED", Some(&response.payload)).await;
                                            }

                                            match element_id {
                                                "ServiceTask_ValidateOrder" => {
                                                    let next_element = "ServiceTask_ReserveStock";
                                                    if let Err(e) = db::save_token_state(&pool_erp, inst_id, next_element, "ACTIVE").await {
                                                        eprintln!("❌ [ERP Consumer] Ошибка создания токена: {}", e);
                                                    } else {
                                                        let _ = nats_erp.publish_token_event(inst_id, next_element, "ACTIVE", Some(&response.payload)).await;
                                                    }
                                                }
                                                "ServiceTask_ReserveStock" | "ServiceTask_ERP_Sync" => {
                                                    let _ = db::update_process_instance_status(&pool_erp, inst_id, "COMPLETED").await;
                                                }
                                                _ => {}
                                            }
                                        } else {
                                            let _ = db::save_token_state(&pool_erp, inst_id, element_id, "FAILED").await;
                                            let _ = db::update_process_instance_status(&pool_erp, inst_id, "FAILED").await;
                                            let _ = nats_erp.publish_token_event(inst_id, element_id, "FAILED", Some(&response.payload)).await;
                                        }
                                    }
                                    Err(e) => eprintln!("❌ [ERP Consumer] Ошибка десериализации: {}", e),
                                }
                                let _ = msg.ack().await;
                            }
                            Err(e) => eprintln!("❌ [ERP Consumer] Ошибка сообщения: {:?}", e),
                        }
                    }
                }
                Err(e) => {
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }
    });

    // 3. Основной цикл обработки входящих событий
    loop {
        match consumer.messages().await {
            Ok(mut messages) => {
                while let Some(Ok(msg)) = messages.next().await {
                    match serde_json::from_slice::<InboundEvent>(&msg.payload) {
                        Ok(event) => {
                            let instance_id = &event.instance_id;
                            let element_id = &event.element_id;

                            if let Some(bpmn_xml) = event.payload.get("bpmn_xml").and_then(|v| v.as_str()) {
                                match parse_and_compile(bpmn_xml) {
                                    Ok(process_def) => {
                                        let process_id = &process_def.id; 
                                        let _ = db::save_process_definition(&pool, process_id, bpmn_xml).await;
                                        let _ = db::save_process_instance(&pool, instance_id, process_id).await;
                                    }
                                    Err(e) => eprintln!("❌ [Main Loop] Ошибка компиляции BPMN XML: {:?}", e),
                                }
                            }

                            if !element_id.is_empty() {
                                if let Err(e) = db::save_token_state(&pool, instance_id, element_id, "ACTIVE").await {
                                    eprintln!("❌ Ошибка обновления токена: {}", e);
                                } else {
                                    println!("💾 [Main Loop] Токен {}:{} сохранен как ACTIVE", instance_id, element_id);
                                    // ПЕРЕДАЕМ event.payload В WebSocket NATS PUBLISH!
                                    let _ = nats_adapter.publish_token_event(instance_id, element_id, "ACTIVE", Some(&event.payload)).await;
                                }
                            }
                        }
                        Err(e) => eprintln!("❌ [Main Loop] Ошибка десериализации InboundEvent: {}", e),
                    }
                    let _ = msg.ack().await;
                }
            }
            Err(_) => sleep(Duration::from_secs(2)).await,
        }
    }
}