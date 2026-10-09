use crate::events::{ActionRequiredEvent, BpmnEvent};
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

        let payload_bytes = serde_json::to_vec(&payload_json)?.into();
        self.js.publish(subject, payload_bytes).await?;
        println!("📡 [NATS Publish] Токен {}:{} -> {} (с payload) опубликован в NATS", instance_id, element_id, state);

        Ok(())
    }

    pub async fn publish_audit_event(&self, event: &BpmnEvent) -> Result<(), async_nats::Error> {
        let subject = format!("mes.bpmn.v1.event.{}", event.process_id);
        let payload = serde_json::to_vec(event)?.into();

        let _ack = self.js.publish(subject, payload).await?;
        println!("Published audit event for process: {}", event.process_id);
        
        Ok(())
    }

    pub async fn publish_outbound_command(&self, event: &ActionRequiredEvent) -> Result<(), async_nats::Error> {
        let subject = format!("mes.out.v1.cmd.{}", event.target_system.to_lowercase());
        let payload = serde_json::to_vec(event)?.into();

        self.js.publish(subject, payload).await?;
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