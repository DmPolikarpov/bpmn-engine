use crate::events::{ActionRequiredEvent, ServiceCommand};
use crate::orders::{PgOrderStore, API_QUEUE, API_SUBJECT};
use async_nats::jetstream::{self, consumer::pull::Config as PullConfig, Context};
use futures::StreamExt;
use serde_json::{json, Value};
use std::env;
use std::sync::Arc;

#[derive(Clone)]
pub struct NatsAdapter {
    pub js: Context,
    /// Ядровой клиент: нужен для request/reply API (JetStream отвечает только
    /// на публикацию и вытягивание, а запрос кабинета ждёт ответ в `reply`).
    pub client: async_nats::Client,
}

impl NatsAdapter {
    pub async fn new() -> Result<Self, async_nats::Error> {
        let nats_url =
            env::var("NATS_URL").unwrap_or_else(|_| "nats://nats-service:4222".to_string());

        println!("Connecting to NATS at {}...", nats_url);
        let client = async_nats::connect(&nats_url).await?;
        let js = jetstream::new(client.clone());

        println!("Successfully connected to NATS JetStream!");
        Ok(Self { js, client })
    }

    /// NATS-API заказов (M4): HTTP-шлюз перекладывает REST в `nc.request`.
    ///
    /// Очередь [`API_QUEUE`] нужна, чтобы на запрос ответила **одна** реплика
    /// движка: у всех общий Postgres, и три одинаковых ответа только гонялись
    /// бы за первый.
    pub async fn serve_orders_api(
        &self,
        store: Arc<PgOrderStore>,
    ) -> Result<(), async_nats::Error> {
        let mut subscriber = self
            .client
            .queue_subscribe(API_SUBJECT, API_QUEUE.to_string())
            .await?;
        println!("📚 [Orders] API заказов слушает {API_SUBJECT}");

        while let Some(message) = subscriber.next().await {
            let today = chrono::Utc::now().date_naive();
            let response =
                crate::orders::handle_bytes(store.as_ref(), &message.payload, today).await;

            // Ответ уходит в `reply` запроса. В async-nats 0.34 у сообщения нет
            // метода `respond`, поэтому публикуем сами; запрос без `reply`
            // (обычный `nats pub`) просто некому адресовать.
            let Some(reply) = message.reply.clone() else {
                eprintln!("⚠️ [Orders] запрос без reply-адреса: {}", message.subject);
                continue;
            };

            match serde_json::to_vec(&response) {
                Ok(body) => {
                    if let Err(error) = self.client.publish(reply, body.into()).await {
                        eprintln!("❌ [Orders] не удалось ответить на запрос: {error}");
                    }
                }
                Err(error) => eprintln!("❌ [Orders] сериализация ответа: {error}"),
            }
        }

        Ok(())
    }

    /// NATS-API проектов (M6): схема проекта живёт в БД движка, а не только в
    /// `localStorage` браузера. Кабинет спрашивает список и схему, «Задеплоить»
    /// поднимает версию.
    ///
    /// Очередь [`crate::projects::API_QUEUE`] — по той же причине, что и у
    /// заказов: у реплик общий Postgres, отвечать должна одна.
    pub async fn serve_projects_api(
        &self,
        store: Arc<crate::projects::PgProjectStore>,
    ) -> Result<(), async_nats::Error> {
        let mut subscriber = self
            .client
            .queue_subscribe(
                crate::projects::API_SUBJECT,
                crate::projects::API_QUEUE.to_string(),
            )
            .await?;
        println!(
            "📐 [Projects] API проектов слушает {}",
            crate::projects::API_SUBJECT
        );

        while let Some(message) = subscriber.next().await {
            let response = crate::projects::handle_bytes(store.as_ref(), &message.payload).await;

            let Some(reply) = message.reply.clone() else {
                eprintln!(
                    "⚠️ [Projects] запрос без reply-адреса: {}",
                    message.subject
                );
                continue;
            };

            match serde_json::to_vec(&response) {
                Ok(body) => {
                    if let Err(error) = self.client.publish(reply, body.into()).await {
                        eprintln!("❌ [Projects] не удалось ответить на запрос: {error}");
                    }
                }
                Err(error) => eprintln!("❌ [Projects] сериализация ответа: {error}"),
            }
        }

        Ok(())
    }

    /// NATS-API серверных метрик (M7): кабинет видит инфографику запусков,
    /// которую посчитал движок, а не браузер конкретного пользователя.
    ///
    /// Очередь [`crate::metrics::API_QUEUE`] — по той же причине, что и у
    /// заказов: у реплик общий Postgres, отвечать должна одна.
    pub async fn serve_metrics_api(
        &self,
        store: Arc<crate::metrics::PgMetricsStore>,
    ) -> Result<(), async_nats::Error> {
        let mut subscriber = self
            .client
            .queue_subscribe(
                crate::metrics::API_SUBJECT,
                crate::metrics::API_QUEUE.to_string(),
            )
            .await?;
        println!(
            "📊 [Metrics] API метрик слушает {}",
            crate::metrics::API_SUBJECT
        );

        while let Some(message) = subscriber.next().await {
            let Some(reply) = message.reply.clone() else {
                eprintln!("⚠️ [Metrics] запрос без reply-адреса: {}", message.subject);
                continue;
            };

            let response = crate::metrics::handle_bytes(store.as_ref(), &message.payload).await;

            match serde_json::to_vec(&response) {
                Ok(body) => {
                    if let Err(error) = self.client.publish(reply, body.into()).await {
                        eprintln!("❌ [Metrics] не удалось ответить на запрос: {error}");
                    }
                }
                Err(error) => eprintln!("❌ [Metrics] сериализация ответа: {error}"),
            }
        }

        Ok(())
    }

    /// NATS-API проверки прав (M6): HTTP-шлюз спрашивает движок перед
    /// публикацией события, чтобы вернуть кабинету `403` с причиной.
    ///
    /// Ошибка проверки — не ответ: шлюз получит таймаут и `503`. Молча
    /// пропускать событие нельзя, иначе сбой БД открывал бы шаг любой роли.
    pub async fn serve_auth_api(
        &self,
        stores: Arc<crate::runtime::store::GraphStores>,
        legacy: sqlx::PgPool,
    ) -> Result<(), async_nats::Error> {
        let mut subscriber = self
            .client
            .queue_subscribe(crate::auth::API_SUBJECT, crate::auth::API_QUEUE.to_string())
            .await?;
        println!(
            "🔐 [Auth] API проверки прав слушает {}",
            crate::auth::API_SUBJECT
        );

        while let Some(message) = subscriber.next().await {
            let Some(reply) = message.reply.clone() else {
                eprintln!("⚠️ [Auth] запрос без reply-адреса: {}", message.subject);
                continue;
            };

            let body = match serde_json::from_slice::<crate::auth::AuthRequest>(&message.payload) {
                Ok(request) => {
                    match crate::auth::decide(stores.as_ref(), &legacy, &request).await {
                        Ok(reply_body) => serde_json::to_vec(&reply_body),
                        Err(error) => {
                            eprintln!("❌ [Auth] проверка не выполнена: {error:#}");
                            continue;
                        }
                    }
                }
                Err(error) => {
                    eprintln!("❌ [Auth] некорректный запрос проверки прав: {error}");
                    serde_json::to_vec(&json!({
                        "allowed": false,
                        "reason": "Некорректный запрос проверки прав",
                        "required": [],
                        "elementId": "",
                    }))
                }
            };

            match body {
                Ok(body) => {
                    if let Err(error) = self.client.publish(reply, body.into()).await {
                        eprintln!("❌ [Auth] не удалось ответить на запрос: {error}");
                    }
                }
                Err(error) => eprintln!("❌ [Auth] сериализация ответа: {error}"),
            }
        }

        Ok(())
    }

    /// Публикация изменения статуса токена ВМЕСТЕ с полезной нагрузкой (payload)
    pub async fn publish_token_event(
        &self,
        instance_id: &str,
        element_id: &str,
        state: &str,
        payload_data: Option<&Value>,
    ) -> Result<(), async_nats::Error> {
        self.publish_token_event_with_token_id(instance_id, element_id, state, payload_data, None)
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
        println!(
            "📡 [NATS Publish] Токен {}:{} -> {} (с payload) опубликован в NATS",
            instance_id, element_id, state
        );

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
        println!(
            "📡 [NATS Publish] {} для {} опубликован в NATS",
            event, instance_id
        );

        Ok(())
    }

    /// SLA-команда (`ActionRequiredEvent`) — совместимый контракт M1.
    pub async fn publish_outbound_command(
        &self,
        event: &ActionRequiredEvent,
    ) -> Result<(), async_nats::Error> {
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

    pub async fn create_inbound_consumer(
        &self,
    ) -> Result<async_nats::jetstream::consumer::PullConsumer, async_nats::Error> {
        let consumer = self
            .js
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

    pub async fn create_erp_response_consumer(
        &self,
    ) -> Result<async_nats::jetstream::consumer::PullConsumer, async_nats::Error> {
        let consumer = self
            .js
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
