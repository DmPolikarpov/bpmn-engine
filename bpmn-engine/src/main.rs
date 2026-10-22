mod events;
mod nats_client;
mod db;
mod runtime;

use bpm_engine::bpm_engine_bpmn::parse_and_compile;

use async_nats::jetstream::AckKind;
use events::{ActionRequiredEvent, ErpResponseEvent, InboundEvent};
use futures::StreamExt;
use nats_client::NatsAdapter;
use runtime::{GraphRuntime, Routing};
use serde_json::json;
use sqlx::{PgPool, Row};
use std::env;
use std::sync::Arc;
use std::time::Duration;
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

    // Маршрутизация: `graph` (по умолчанию) — движок ведёт экземпляр по графу
    // схемы (`src/runtime/`, дизайн `docs/runtime-m1-design.md`, 10.1);
    // `legacy` — аварийный откат, где маршрут считает клиент.
    let routing = Routing::from_env();
    println!("🔀 Маршрутизация событий: {}", routing.as_str());

    let graph: Option<Arc<GraphRuntime>> = match routing {
        Routing::Legacy => None,
        Routing::Graph => {
            runtime::init_tracing();
            let graph = GraphRuntime::new(&db_url, pool.clone()).await?;
            println!("✅ [Graph] рантайм bpm-engine подключён");
            Some(Arc::new(graph))
        }
    };

    let nats_adapter = NatsAdapter::new().await?;
    let consumer = nats_adapter.create_inbound_consumer().await?;
    let erp_consumer = nats_adapter.create_erp_response_consumer().await?;

    let nats_sla = nats_adapter.clone();
    let nats_erp = nats_adapter.clone();

    let pool_sla = pool.clone();
    let pool_erp = pool.clone();

    let graph_erp = graph.clone();

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
                                        if let Some(graph) = graph_erp.as_ref() {
                                            match graph.handle_erp(&response).await {
                                                Ok(outcome) => {
                                                    publish_outcome(&nats_erp, &response.process_instance_id, &outcome).await;
                                                    let _ = msg.ack().await;
                                                }
                                                Err(e) => {
                                                    eprintln!("❌ [Graph][ERP] Транзиентная ошибка: {:#}", e);
                                                    let _ = msg.ack_with(AckKind::Nak(None)).await;
                                                }
                                            }
                                            continue;
                                        }

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
                                                        // Тот же инвариант, что и в основном цикле: на экземпляр
                                                        // остаётся ровно один ACTIVE-токен.
                                                        match db::complete_active_tokens(&pool_erp, inst_id, next_element).await {
                                                            Ok(completed_elements) => {
                                                                for stale_element in completed_elements {
                                                                    let _ = nats_erp.publish_token_event(inst_id, &stale_element, "COMPLETED", None).await;
                                                                }
                                                            }
                                                            Err(e) => eprintln!("❌ [ERP Consumer] Ошибка закрытия прежних ACTIVE-токенов: {}", e),
                                                        }

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
                Err(_) => {
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
                            if let Some(graph) = graph.as_ref() {
                                match graph.handle_inbound(&event).await {
                                    Ok(outcome) => {
                                        publish_outcome(&nats_adapter, &event.instance_id, &outcome).await;
                                        let _ = msg.ack().await;
                                    }
                                    Err(e) => {
                                        eprintln!("❌ [Graph] Транзиентная ошибка: {:#}", e);
                                        let _ = msg.ack_with(AckKind::Nak(None)).await;
                                    }
                                }
                                continue;
                            }

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
                                    // Инвариант «один активный узел на экземпляр»: прежние
                                    // ACTIVE-токены закрываем (COMPLETED), иначе клиент
                                    // подсветит несколько активных узлов одновременно.
                                    match db::complete_active_tokens(&pool, instance_id, element_id).await {
                                        Ok(completed_elements) => {
                                            for stale_element in completed_elements {
                                                println!("🔁 [Main Loop] Прежний токен {}:{} закрыт как COMPLETED", instance_id, stale_element);
                                                let _ = nats_adapter.publish_token_event(instance_id, &stale_element, "COMPLETED", None).await;
                                            }
                                        }
                                        Err(e) => eprintln!("❌ Ошибка закрытия прежних ACTIVE-токенов: {}", e),
                                    }

                                    // ПЕРЕДАЕМ event.payload В WebSocket NATS PUBLISH!
                                    // Payload уходит только с ACTIVE: COMPLETED-события не должны
                                    // перезаписывать данные заказа на клиенте.
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

/// Публикует результат обработки события в NATS (`mes.bpmn.v1.event.<instance>`).
///
/// Ошибки публикации не отменяют уже записанное состояние: это телеметрия для
/// кабинета, а сообщение уже подтверждено.
async fn publish_outcome(nats: &NatsAdapter, instance_id: &str, outcome: &runtime::Outcome) {
    for step in &outcome.steps {
        if let Err(e) = nats
            .publish_token_event_with_token_id(
                instance_id,
                &step.element_id,
                step.state,
                step.payload.as_ref(),
                Some(&step.token_id),
            )
            .await
        {
            eprintln!(
                "❌ [Graph] Ошибка публикации {} для {}:{}: {}",
                step.state, instance_id, step.element_id, e
            );
        }
    }

    if let Some(event) = outcome.process_finished {
        if let Err(e) = nats.publish_process_finished(instance_id, event).await {
            eprintln!(
                "❌ [Graph] Ошибка публикации {} для {}: {}",
                event, instance_id, e
            );
        }
    }
}
