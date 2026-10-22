mod db;
mod events;
mod nats_client;
mod runtime;

use async_nats::jetstream::AckKind;
use events::{ActionRequiredEvent, ErpResponseEvent, InboundEvent};
use futures::StreamExt;
use nats_client::NatsAdapter;
use runtime::GraphRuntime;
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

    // Маршрут считает движок по графу схемы (`src/runtime/`, дизайн
    // `docs/runtime-m1-design.md`, 10.1). Legacy-ветка, где следующий узел
    // называл клиент, удалена шагом 3 (M2).
    runtime::init_tracing();
    let graph = Arc::new(GraphRuntime::new(&db_url, pool.clone()).await?);
    println!("✅ [Graph] рантайм bpm-engine подключён — маршрут по графу схемы");

    let nats_adapter = NatsAdapter::new().await?;
    let consumer = nats_adapter.create_inbound_consumer().await?;
    let erp_consumer = nats_adapter.create_erp_response_consumer().await?;

    let nats_sla = nats_adapter.clone();
    let nats_erp = nats_adapter.clone();

    let pool_sla = pool.clone();

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
                            eprintln!(
                                "❌ [SLA Worker] Ошибка отправки компенсационной команды: {}",
                                e
                            );
                        } else {
                            if let Err(e) =
                                db::save_token_state(&pool_sla, &inst_id, &elem_id, "TIMED_OUT")
                                    .await
                            {
                                eprintln!(
                                    "❌ [SLA Worker] Ошибка БД при обновлении статуса токена: {}",
                                    e
                                );
                            } else {
                                let _ = nats_sla
                                    .publish_token_event(&inst_id, &elem_id, "TIMED_OUT", None)
                                    .await;
                            }
                        }
                    }
                }
                Err(e) => eprintln!(
                    "❌ [SLA Worker] Ошибка выполнения query в SLA Worker: {}",
                    e
                ),
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
                                    Ok(response) => match graph_erp.handle_erp(&response).await {
                                        Ok(outcome) => {
                                            publish_outcome(
                                                &nats_erp,
                                                &response.process_instance_id,
                                                &outcome,
                                            )
                                            .await;
                                            let _ = msg.ack().await;
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "❌ [Graph][ERP] Транзиентная ошибка: {:#}",
                                                e
                                            );
                                            let _ = msg.ack_with(AckKind::Nak(None)).await;
                                        }
                                    },
                                    Err(e) => {
                                        eprintln!("❌ [ERP Consumer] Ошибка десериализации: {}", e)
                                    }
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

    // 3. Публикатор ретраев: команды, у которых истёк backoff.
    let nats_retry = nats_adapter.clone();
    let graph_retry = graph.clone();
    tokio::spawn(async move {
        let interval_secs = env::var("ERP_RETRY_POLL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(2);

        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

        loop {
            interval.tick().await;

            match graph_retry.due_retries().await {
                Ok(commands) => {
                    for command in commands {
                        if let Err(e) = nats_retry.publish_service_command(&command).await {
                            eprintln!(
                                "❌ [Retry] Ошибка публикации {}:{}: {}",
                                command.instance_id, command.element_id, e
                            );
                        }
                    }
                }
                Err(e) => eprintln!("❌ [Retry] Ошибка выборки ретраев: {:#}", e),
            }
        }
    });

    // 4. Основной цикл обработки входящих событий
    loop {
        match consumer.messages().await {
            Ok(mut messages) => {
                while let Some(Ok(msg)) = messages.next().await {
                    match serde_json::from_slice::<InboundEvent>(&msg.payload) {
                        Ok(event) => match graph.handle_inbound(&event).await {
                            Ok(outcome) => {
                                publish_outcome(&nats_adapter, &event.instance_id, &outcome).await;
                                let _ = msg.ack().await;
                            }
                            Err(e) => {
                                eprintln!("❌ [Graph] Транзиентная ошибка: {:#}", e);
                                let _ = msg.ack_with(AckKind::Nak(None)).await;
                            }
                        },
                        Err(e) => {
                            eprintln!("❌ [Main Loop] Ошибка десериализации InboundEvent: {}", e)
                        }
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
    for command in &outcome.commands {
        if let Err(e) = nats.publish_service_command(command).await {
            eprintln!(
                "❌ [Graph] Ошибка публикации команды {}:{}: {}",
                command.instance_id, command.element_id, e
            );
        }
    }

    // Компенсации идут тем же subject, что и команды воркерам: получатель
    // (ERP/MES) сам решает, показать `ActionRequiredEvent` человеку или
    // закрыть шаг вручную.
    for action in &outcome.actions {
        if let Err(e) = nats.publish_outbound_command(action).await {
            eprintln!(
                "❌ [Graph] Ошибка публикации компенсации {}:{}: {}",
                action.process_instance_id, action.current_step, e
            );
        }
    }

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
