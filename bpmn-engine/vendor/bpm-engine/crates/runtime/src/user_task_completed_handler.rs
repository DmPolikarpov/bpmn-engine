//! UserTaskCompletedHandler: UserTaskCompleted -> merge variables, move token
//! preserving the parallel group, emit TokenCompleted + TokenArrived.
//!
//! Local M1 patch (see `VENDORED_FROM.md`): upstream closes every token of the
//! node (`tokens.retain(|t| t.node_id != e.node_id)`), drops the step variables
//! and moves the token with `move_token`, which resets `parallel_group_id`.

use async_trait::async_trait;
use bpm_engine_core::{payloads, EngineEvent, TokenStatus};
use tracing::info;

use super::handler::{EngineContext, EventHandler};
use super::transition::move_token_preserving_group;

/// Handler that advances tokens when a user task is completed.
pub struct UserTaskCompletedHandler;

#[async_trait]
impl EventHandler for UserTaskCompletedHandler {
    async fn handle(&self, event: &EngineEvent, ctx: &mut EngineContext) -> Vec<EngineEvent> {
        let EngineEvent::UserTaskCompleted(e) = event else {
            return vec![];
        };
        info!(instance_id = %e.instance_id, node_id = %e.node_id, "user task completed");
        let process_store = &ctx.process_store;
        let process_def_store = &ctx.process_def_store;
        let Ok(Some(mut instance)) = process_store.load(&e.instance_id).await else {
            return vec![];
        };
        let Ok(Some(def)) = process_def_store.load(&instance.process_def_id).await else {
            return vec![];
        };
        let Some(node) = def.nodes.get(e.node_id.as_str()) else {
            return vec![];
        };
        // M1 patch: close exactly one Waiting token of this node, not every token
        // standing on it (two parallel branches may wait on the same node).
        let Some(idx) = instance
            .tokens
            .iter()
            .position(|t| t.node_id == e.node_id && t.status == TokenStatus::Waiting)
        else {
            return vec![];
        };
        let group = instance.tokens[idx].parallel_group_id.clone();
        let token_id = instance.tokens[idx].id.clone();
        // M1 patch: the step's variables must reach the gateway conditions.
        for (k, v) in &e.variables {
            instance.variables.insert(k.clone(), v.clone());
        }
        instance.tokens.remove(idx);
        let new_tokens = move_token_preserving_group(node, group);
        let mut out = vec![EngineEvent::TokenCompleted(payloads::TokenCompleted {
            instance_id: e.instance_id.clone(),
            token_id,
        })];
        for t in &new_tokens {
            out.push(EngineEvent::TokenArrived(payloads::TokenArrived {
                instance_id: e.instance_id.clone(),
                token_id: t.id.clone(),
                node_id: t.node_id.clone(),
            }));
        }
        instance.tokens.extend(new_tokens);
        let _ = process_store.save(&instance).await;
        out
    }
}
