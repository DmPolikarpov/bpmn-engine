use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundEvent {
    #[serde(default)]
    pub event_id: String,
    pub instance_id: String,
    #[serde(default)]
    pub element_id: String,
    #[serde(default)]
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionRequiredEvent {
    pub process_instance_id: String,
    pub current_step: String,
    pub target_system: String,
    pub payload: Value,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BpmnEvent {
    pub process_id: String,
    pub event_type: String,
    pub payload: Value,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ErpResponseEvent {
    pub process_instance_id: String,
    #[serde(default)]
    pub element_id: String,
    pub status: String, 
    pub error_message: Option<String>,
    pub payload: serde_json::Value,
}