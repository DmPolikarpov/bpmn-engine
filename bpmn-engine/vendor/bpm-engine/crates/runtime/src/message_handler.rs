//! MessageHandler: resumes tokens waiting at message intermediate catch events
//! when a matching MessageSent event is fired.
//!
//! Local patch (M3): a message may carry a correlation map
//! ([`payloads::MessageSent::correlation`]); only instances whose variables
//! contain every key/value pair are resumed. Without the map the upstream
//! broadcast behaviour is kept (all matching catch events fire). Message
//! payload ([`payloads::MessageSent::payload`]) is merged into the receiver's
//! variables before the catch token moves.

use async_trait::async_trait;
use bpm_engine_core::{payloads, EngineEvent, NodeType, TokenStatus};
use tracing::debug;

use super::handler::{EngineContext, EventHandler};
use super::transition::move_token_preserving_group;

/// When a MessageSent event is processed, scans all running instances for tokens
/// waiting at a MessageIntermediateCatch node with a matching message name,
/// and resumes them.
pub struct MessageCatchHandler;

#[async_trait]
impl EventHandler for MessageCatchHandler {
    async fn handle(&self, event: &EngineEvent, ctx: &mut EngineContext) -> Vec<EngineEvent> {
        let EngineEvent::MessageSent(e) = event else {
            return vec![];
        };
        let process_store = &ctx.process_store;
        let process_def_store = &ctx.process_def_store;

        let Ok(instance_ids) = process_store.list_running(ctx.tenant_id.as_deref()).await else {
            return vec![];
        };

        let mut out = vec![];

        for iid in instance_ids {
            let Ok(Some(mut instance)) = process_store.load(&iid).await else {
                continue;
            };
            if instance.state != bpm_engine_core::InstanceState::Running {
                continue;
            }
            if !matches_correlation(&instance, e) {
                debug!(
                    instance_id = %iid,
                    message = %e.message_name,
                    "message catch: correlation mismatch"
                );
                continue;
            }
            let Ok(Some(def)) = process_def_store.load(&instance.process_def_id).await else {
                continue;
            };

            // Collect matching token indices and node IDs first to avoid borrow issues
            let matches: Vec<(usize, String)> = instance
                .tokens
                .iter()
                .enumerate()
                .filter(|(_, t)| t.status == TokenStatus::Waiting)
                .filter_map(|(idx, t)| {
                    let node = def.nodes.get(t.node_id.as_str())?;
                    if let NodeType::MessageIntermediateCatch { message_name } = &node.node_type {
                        if message_name == &e.message_name {
                            return Some((idx, t.node_id.clone()));
                        }
                    }
                    None
                })
                .collect();

            if matches.is_empty() {
                continue;
            }

            // Перед сдвигом токена сообщение переносит свои переменные: получатель
            // видит данные отправителя (M3, `payloadKeys` триггера).
            if let Some(payload) = &e.payload {
                for (key, value) in payload {
                    instance.variables.insert(key.clone(), value.clone());
                }
            }

            for (idx, node_id) in matches {
                if let Some(node) = def.nodes.get(node_id.as_str()) {
                    debug!(
                        instance_id = %iid,
                        token_id = %instance.tokens[idx].id,
                        message = %e.message_name,
                        "message catch: resuming token"
                    );
                    let parallel_group_id = instance.tokens[idx].parallel_group_id.clone();
                    instance.tokens[idx].status = TokenStatus::Waiting;
                    let new_tokens = move_token_preserving_group(node, parallel_group_id);
                    for t in &new_tokens {
                        out.push(EngineEvent::TokenArrived(payloads::TokenArrived {
                            instance_id: iid.clone(),
                            token_id: t.id.clone(),
                            node_id: t.node_id.clone(),
                        }));
                    }
                    instance.tokens.extend(new_tokens);
                }
            }

            let _ = process_store.save(&instance).await;
        }

        out
    }
}

/// Совпадает ли сообщение с экземпляром по карте корреляции.
///
/// `None` — сообщение без корреляции: сохраняется апстрим-поведение
/// (подходит любой экземпляр с ожидающим catch-событием). Иначе все пары
/// `ключ=значение` должны быть среди переменных экземпляра — так ответ
/// поставщика находит экземпляр завода по `orderNum`, а не по `instance_id`.
fn matches_correlation(
    instance: &bpm_engine_core::ProcessInstance,
    event: &payloads::MessageSent,
) -> bool {
    match &event.correlation {
        None => true,
        Some(correlation) => correlation
            .iter()
            .all(|(key, value)| instance.variables.get(key).is_some_and(|actual| actual == value)),
    }
}
