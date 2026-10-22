use crate::events::{ActionRequiredEvent, ServiceCommand};
use async_nats::jetstream::{self, Context, consumer::pull::Config as PullConfig};
use serde_json::{json, Value};
use std::env;

#[derive(Clone)]
pub struct NatsAdapter {
    pub js: Context,
}

impl NatsAdapter {
    pub async fn new() -> Result<Self, async_nats::Error> {
        let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://nats-service:4222".to_string());
        
        println!("Connecting to NATS at {}...", nats_url);
        let client = async_nats::connect(&nats_url).await?;
        let js = jetstream::new(client);
        
        println!("Successfully connected to NATS JetStream!");
        Ok(Self { js })
    }

    /// Публикация изменения статуса токена ВМЕСТЕ с полезной нагрузкой (payload)
    pub async fn publish_token_event(
        &self,
        instance_id: &str,
        element_id: &str,
        state: &str,
        payload_data: Option<&Value>,
    ) -> Result<(), async_nats::Error> {
        self.publish_token_event_with_token_id(
            instance_id,
            element_id,
            state,
            payload_data,
            None,
        )
        .await
    }

    /// То же, но с `token_id` движка.
    ///
    /// Публикуется для M1.1 (подсветка набора активных узлов): без него два
    /// токена на одном узле не различить.
    pub async fn publish_token_event_with_token_id(
        &self,
        instance_id: &str,
        element_id: &str,
        state: &str,
        payload_data: Option<&Value>,
        token_id: Option<&str>,
    ) -> Result<(), async_nats::Error> {
        let subject = format!("mes.bpmn.v1.event.{}", instance_id);
        
        let mut payload_json = json!({
            "event": "token_updated",
            "instance_id": instance_id,
            "element_id": element_id,
            "state": state,
            "timestamp": chrono::Utc::now().to_rfc3339()
        });

        // Прикрепляем payload к отправляемому в WebSocket объекту
        if let Some(p) = payload_data {
            if let Some(obj) = payload_json.as_object_mut() {
                obj.insert("payload".to_string(), p.clone());
            }
        }

        if let Some(token_id) = token_id {
            if let Some(obj) = payload_json.as_object_mut() {
                obj.insert("token_id".to_string(), json!(token_id));
            }
        }

        let payload_bytes = serde_json::to_vec(&payload_json)?.into();
        self.js.publish(subject, payload_bytes).await?;
        println!("📡 [NATS Publish] Токен {}:{} -> {} (с payload) опубликован в NATS", instance_id, element_id, state);

        Ok(())
    }

    /// Завершение процесса: `process_completed` или `process_terminated`.
    ///
    /// Клиент закрывает запуск по этому событию: у `token_updated` нет
    /// `element_id`, поэтому старый разбор его отбрасывает (дизайн 5.3).
    pub async fn publish_process_finished(
        &self,
        instance_id: &str,
        event: &str,
    ) -> Result<(), async_nats::Error> {
        let subject = format!("mes.bpmn.v1.event.{}", instance_id);
        let payload_json = json!({
            "event": event,
            "instance_id": instance_id,
            "timestamp": chrono::Utc::now().to_rfc3339()
        });

        let payload_bytes = serde_json::to_vec(&payload_json)?.into();
        self.js.publish(subject, payload_bytes).await?;
        println!("📡 [NATS Publish] {} для {} опубликован в NATS", event, instance_id);

        Ok(())
    }

    /// SLA-команда (`ActionRequiredEvent`) — совместимый контракт M1.
    pub async fn publish_outbound_command(&self, event: &ActionRequiredEvent) -> Result<(), async_nats::Error> {
        let subject = command_subject(&event.target_system);
        let payload = serde_json::to_vec(event)?.into();

        self.js.publish(subject, payload).await?;
        Ok(())
    }

    /// Команда воркеру `serviceTask` (контракт M2) → `mes.out.v1.cmd.<system>`.
    ///
    /// Уходит сразу после активации узла; поле `target_system` в теле
    /// дублирует subject, чтобы получатель мог проверить адресацию.
    pub async fn publish_service_command(
        &self,
        command: &ServiceCommand,
    ) -> Result<(), async_nats::Error> {
        let subject = command_subject(&command.target_system);
        let payload = serde_json::to_vec(command)?.into();

        self.js.publish(subject.clone(), payload).await?;
        println!(
            "📤 [NATS Publish] Команда {}:{} → {}",
            command.instance_id, command.element_id, subject
        );
        Ok(())
    }

    pub async fn create_inbound_consumer(&self) -> Result<async_nats::jetstream::consumer::PullConsumer, async_nats::Error> {
        let consumer = self.js
            .create_consumer_on_stream(
                PullConfig {
                    durable_name: Some("engine-inbound-consumer".to_string()),
                    filter_subject: "mes.in.v1.event.>".to_string(),
                    ..Default::default()
                },
                "INBOUND_EVENTS",
            )
            .await?;
        
        Ok(consumer)
    }

    pub async fn create_erp_response_consumer(&self) -> Result<async_nats::jetstream::consumer::PullConsumer, async_nats::Error> {
        let consumer = self.js
            .create_consumer_on_stream(
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some("engine-erp-response-consumer".to_string()),
                    filter_subject: "mes.in.v1.erp.>".to_string(),
                    ..Default::default()
                },
                "ERP_RESPONSES",
            )
            .await?;
        
        Ok(consumer)
    }
}

/// Subject исходящей команды: `target_system` приводится к нижнему регистру
/// (`ERP` → `mes.out.v1.cmd.erp`), как в `nats-init-streams-job.yaml`.
fn command_subject(target_system: &str) -> String {
    format!("mes.out.v1.cmd.{}", target_system.to_lowercase())
}
