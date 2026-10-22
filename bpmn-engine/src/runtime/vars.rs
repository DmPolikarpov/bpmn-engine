//! Правила преобразования `output_data` в переменные движка.
//!
//! Вендорный рантайм хранит переменные как `HashMap<String, String>`
//! (дизайн `docs/runtime-m1-design.md`, 5.1), поэтому в переменные попадают
//! только скаляры верхнего уровня. Вложенные объекты и массивы остаются в
//! `payload` события и доезжают до кабинета через WebSocket, но в условиях
//! шлюзов не участвуют.

use std::collections::HashMap;

use serde_json::Value;

/// Ключи, описывающие доставку события, а не данные процесса.
pub const TRANSPORT_KEYS: [&str; 3] = ["bpmn_xml", "org_type", "token_id"];

/// Разворачивает `output_data` в переменные процесса.
///
/// - скаляры (`string`, `number`, `boolean`) → строка: `true` → `"true"`,
///   `100` → `"100"`, `1.5` → `"1.5"`;
/// - `null`, вложенные объекты и массивы пропускаются;
/// - транспортные ключи ([`TRANSPORT_KEYS`]) переменными не становятся;
/// - пустой набор — легальный результат: шаг без ветвления.
pub fn flatten(output_data: &Value) -> HashMap<String, String> {
    let mut variables = HashMap::new();
    let Some(object) = output_data.as_object() else {
        return variables;
    };
    for (key, value) in object {
        if is_transport_key(key) {
            continue;
        }
        if let Some(rendered) = scalar_to_string(value) {
            variables.insert(key.clone(), rendered);
        }
    }
    variables
}

/// Транспортный ли это ключ (не переменная процесса).
pub fn is_transport_key(key: &str) -> bool {
    TRANSPORT_KEYS.contains(&key)
}

fn scalar_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Bool(flag) => Some(flag.to_string()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn scalars_become_strings() {
        let variables = flatten(&json!({
            "stockSufficient": "true",
            "amount": 100,
            "ratio": 1.5,
            "creditLimitOk": true
        }));

        assert_eq!(variables.get("stockSufficient"), Some(&"true".to_string()));
        assert_eq!(variables.get("amount"), Some(&"100".to_string()));
        assert_eq!(variables.get("ratio"), Some(&"1.5".to_string()));
        assert_eq!(variables.get("creditLimitOk"), Some(&"true".to_string()));
        assert_eq!(variables.len(), 4);
    }

    #[test]
    fn nested_values_and_null_are_skipped() {
        let variables = flatten(&json!({
            "documents": [{"id": 1}],
            "details": {"warehouse": "W-01"},
            "comment": null,
            "status": "OK"
        }));

        assert_eq!(variables.len(), 1);
        assert_eq!(variables.get("status"), Some(&"OK".to_string()));
    }

    #[test]
    fn transport_keys_are_not_variables() {
        let variables = flatten(&json!({
            "bpmn_xml": "<definitions/>",
            "org_type": "DAIRY",
            "token_id": "0190-token",
            "order_amount": 150000
        }));

        assert_eq!(variables.len(), 1);
        assert_eq!(variables.get("order_amount"), Some(&"150000".to_string()));
    }

    #[test]
    fn non_object_output_is_empty() {
        assert!(flatten(&json!("plain")).is_empty());
        assert!(flatten(&json!(null)).is_empty());
        assert!(flatten(&json!({})).is_empty());
    }
}
