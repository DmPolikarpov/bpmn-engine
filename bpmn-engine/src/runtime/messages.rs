//! Межпроцессные сообщения (M3): разбор пар «источник → приёмник» из схемы.
//!
//! Вендорный движок исполняет `intermediateThrowEvent` и
//! `intermediateCatchEvent` с `messageEventDefinition`, но не знает, что
//! сообщение связывает **два процесса**: пары «кто отправляет → кто ждёт»
//! описаны в схеме (`bpmn:messageFlow`), а запуск второй стороны — расширением
//! узла `trg:processTrigger` (`xml/triggerModdle.js` в клиенте).
//!
//! Модуль чистый: он не ходит в БД и не двигает токены. Рантайм
//! ([`super::GraphRuntime`]) берёт из него ответы на три вопроса:
//!
//! - какое сообщение отправляет узел (`throws`) и кто его ждёт (`catches`);
//! - какой процесс запустить и что ему передать (`triggers`, [`select_payload`]);
//! - по какому бизнес-ключу сообщение находит экземпляр второй стороны
//!   (по умолчанию `orderNum`, [`correlation`]).
//!
//! Схема нарезки (`extractProcessXml`) содержит один процесс, поэтому основная
//! пара — триггер узла; `bpmn:messageFlow` читается из проекта целиком, если он
//! задеплоен как есть (collaboration с двумя процессами).

use std::collections::HashMap;

use roxmltree::{Document, Node};

/// Namespace расширения триггера (совпадает с клиентским `TRIGGER_NAMESPACE`).
pub const TRIGGER_NAMESPACE: &str = "http://erp.mes/schema/bpmn/trigger";

/// Бизнес-ключ, по которому сообщение находит экземпляр второй стороны.
///
/// `orderNum` — номер заказа в демо-проекте: он же переносится в `payloadKeys`
/// триггера, поэтому ответ поставщика находит экземпляр завода, даже если
/// `instance_id` второй стороны другой.
pub const CORRELATION_KEY: &str = "orderNum";

/// Узел, запускающий экземпляр второго процесса (`trg:processTrigger`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessTrigger {
    /// `bpmn:process/@id` целевого процесса (обязательное поле).
    pub target_process: String,
    /// Префикс идентификатора нового экземпляра (`inst-supp-`).
    pub instance_prefix: String,
    /// Узел текущего процесса, активация которого запускает целевой.
    pub start_element: Option<String>,
    /// Узел целевого процесса, который первым станет `ACTIVE`.
    pub first_element: Option<String>,
    /// Ключи переменных, которые передаются новому экземпляру.
    pub payload_keys: Vec<String>,
}

/// `bpmn:messageFlow` между событиями двух процессов.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageFlow {
    /// `bpmn:messageFlow/@id`.
    pub id: String,
    /// Имя сообщения (`bpmn:message/@name` из `messageRef`).
    pub message: String,
    /// Узел-отправитель (`sourceRef`).
    pub source: String,
    /// Узел-приёмник (`targetRef`).
    pub target: String,
}

/// Всё, что схема говорит о сообщениях одного процесса.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageCatalog {
    /// `bpmn:process/@id` схемы.
    pub process_id: String,
    /// Узел throw-события → имя сообщения.
    pub throws: HashMap<String, String>,
    /// Узел catch-события → имя сообщения.
    pub catches: HashMap<String, String>,
    /// Узел с `trg:processTrigger` → триггер запуска.
    pub triggers: HashMap<String, ProcessTrigger>,
    /// Межпроцессные связи проекта (если задеплоен collaboration).
    pub flows: Vec<MessageFlow>,
}

impl MessageCatalog {
    /// Имя сообщения, которое отправляет узел.
    pub fn throw_message(&self, node_id: &str) -> Option<&str> {
        self.throws.get(node_id).map(String::as_str)
    }

    /// Имя сообщения, которое ждёт узел.
    pub fn catch_message(&self, node_id: &str) -> Option<&str> {
        self.catches.get(node_id).map(String::as_str)
    }

    /// Триггер запуска второго процесса на узле.
    pub fn trigger_for(&self, node_id: &str) -> Option<&ProcessTrigger> {
        self.triggers.get(node_id)
    }

    /// Узлы catch-событий, ожидающие сообщение `message_name`.
    pub fn catch_nodes(&self, message_name: &str) -> Vec<&str> {
        let mut nodes: Vec<&str> = self
            .catches
            .iter()
            .filter(|(_, name)| name.as_str() == message_name)
            .map(|(node, _)| node.as_str())
            .collect();
        nodes.sort_unstable();
        nodes
    }

    /// Связь `bpmn:messageFlow` по имени сообщения.
    pub fn flow_for(&self, message_name: &str) -> Option<&MessageFlow> {
        self.flows.iter().find(|flow| flow.message == message_name)
    }
}

/// Разбирает схему процесса; `None` — это не XML или в нём нет `bpmn:process`.
///
/// Ошибки разбора не фатальны для рантайма: сообщения — дополнительная
/// семантика, и определение без них продолжает исполняться как раньше (M1/M2).
pub fn parse(xml: &str) -> Option<MessageCatalog> {
    let document = Document::parse(xml).ok()?;
    let root = document.root_element();
    let message_defs = message_definitions(&root);

    let process = root
        .descendants()
        .find(|node| node.is_element() && node.tag_name().name() == "process")?;
    let process_id = process.attribute("id")?.to_string();

    let mut catalog = MessageCatalog {
        process_id,
        ..MessageCatalog::default()
    };

    for node in process.children().filter(Node::is_element) {
        let Some(id) = node.attribute("id").map(str::to_string) else {
            continue;
        };

        match node.tag_name().name() {
            "intermediateThrowEvent" => {
                if let Some(name) = message_event_name(&node, &message_defs) {
                    catalog.throws.insert(id.clone(), name);
                }
            }
            "intermediateCatchEvent" => {
                if let Some(name) = message_event_name(&node, &message_defs) {
                    catalog.catches.insert(id.clone(), name);
                }
            }
            _ => {}
        }

        if let Some(trigger) = trigger_of(&node) {
            catalog.triggers.insert(id, trigger);
        }
    }

    catalog.flows = message_flows(&root, &message_defs);
    Some(catalog)
}

/// `bpmn:message` верхнего уровня: `id` → `name`.
fn message_definitions(root: &Node) -> HashMap<String, String> {
    root.children()
        .filter(Node::is_element)
        .filter(|node| node.tag_name().name() == "message")
        .filter_map(|node| {
            let id = node.attribute("id")?;
            let name = node.attribute("name").unwrap_or(id);
            Some((id.to_string(), name.to_string()))
        })
        .collect()
}

/// Имя сообщения события: `messageEventDefinition/@messageRef` → `bpmn:message`,
/// иначе `name` самого события (так схемы без объявлений продолжают читаться).
fn message_event_name(node: &Node, defs: &HashMap<String, String>) -> Option<String> {
    let definition = node
        .children()
        .find(|child| child.is_element() && child.tag_name().name() == "messageEventDefinition")?;

    if let Some(reference) = definition.attribute("messageRef") {
        let reference = reference.trim();
        if let Some(name) = defs.get(reference) {
            return Some(name.clone());
        }
        if !reference.is_empty() {
            return Some(reference.to_string());
        }
    }

    node.attribute("name")
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
}

/// `trg:processTrigger` внутри `bpmn:extensionElements` узла.
fn trigger_of(node: &Node) -> Option<ProcessTrigger> {
    let extensions = node
        .children()
        .find(|child| child.is_element() && child.tag_name().name() == "extensionElements")?;

    let trigger = extensions.children().find(|child| {
        child.is_element()
            && child.tag_name().name() == "processTrigger"
            && child.tag_name().namespace() == Some(TRIGGER_NAMESPACE)
    })?;

    let target = trigger
        .attribute("targetProcessId")
        .map(str::trim)
        .filter(|target| !target.is_empty())?;

    Some(ProcessTrigger {
        target_process: target.to_string(),
        instance_prefix: trigger
            .attribute("instancePrefix")
            .map(str::trim)
            .unwrap_or_default()
            .to_string(),
        start_element: attribute(trigger, "startElementId"),
        first_element: attribute(trigger, "firstElementId"),
        payload_keys: parse_payload_keys(trigger.attribute("payloadKeys").unwrap_or_default()),
    })
}

fn attribute(node: Node, name: &str) -> Option<String> {
    node.attribute(name)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// `payloadKeys`: список через запятую без пробелов и повторов.
///
/// Канонизация та же, что в клиенте (`normalizeTrigger`): `' volume , orderNum '`
/// и `'volume,orderNum'` дают одинаковый набор.
pub fn parse_payload_keys(value: &str) -> Vec<String> {
    let mut keys: Vec<String> = Vec::new();
    for key in value.split(',') {
        let key = key.trim();
        if key.is_empty() || keys.iter().any(|known| known == key) {
            continue;
        }
        keys.push(key.to_string());
    }
    keys
}

/// Межпроцессные связи проекта (`collaboration/messageFlow`).
fn message_flows(root: &Node, defs: &HashMap<String, String>) -> Vec<MessageFlow> {
    root.descendants()
        .filter(Node::is_element)
        .filter(|node| node.tag_name().name() == "messageFlow")
        .filter_map(|node| {
            let source = node.attribute("sourceRef")?.trim();
            let target = node.attribute("targetRef")?.trim();
            if source.is_empty() || target.is_empty() {
                return None;
            }

            let reference = node.attribute("messageRef").unwrap_or_default().trim();
            let message = defs
                .get(reference)
                .cloned()
                .unwrap_or_else(|| reference.to_string());
            if message.is_empty() {
                return None;
            }

            Some(MessageFlow {
                id: node.attribute("id").unwrap_or_default().to_string(),
                message,
                source: source.to_string(),
                target: target.to_string(),
            })
        })
        .collect()
}

/// Переменные, перечисленные в `payload_keys`.
///
/// Передаётся только перечисленное: контекст целиком между сторонами не
/// «переливается». Пустой список — пустой payload (сообщение без данных).
pub fn select_payload(
    variables: &HashMap<String, String>,
    payload_keys: &[String],
) -> HashMap<String, String> {
    payload_keys
        .iter()
        .filter_map(|key| variables.get(key).map(|value| (key.clone(), value.clone())))
        .collect()
}

/// Карта корреляции сообщения: `{orderNum: <значение>}`.
///
/// Пустая карта — у отправителя нет бизнес-ключа: доставлять сообщение «во все
/// экземпляры» нельзя, поэтому рантайм такое сообщение не отправляет
/// (см. [`correlation_value`]).
pub fn correlation(variables: &HashMap<String, String>) -> HashMap<String, String> {
    correlation_value(variables)
        .map(|value| HashMap::from([(CORRELATION_KEY.to_string(), value)]))
        .unwrap_or_default()
}

/// Значение бизнес-ключа отправителя (`orderNum`).
pub fn correlation_value(variables: &HashMap<String, String>) -> Option<String> {
    variables
        .get(CORRELATION_KEY)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// Совпадает ли экземпляр с картой корреляции (все пары `ключ=значение`).
pub fn matches_correlation(
    variables: &HashMap<String, String>,
    correlation: &HashMap<String, String>,
) -> bool {
    correlation
        .iter()
        .all(|(key, value)| variables.get(key).is_some_and(|actual| actual == value))
}

/// Идентификатор экземпляра второй стороны: `<префикс><бизнес-ключ>`.
///
/// Детерминированный id — это идемпотентность триггера: повторное сообщение по
/// тому же заказу не создаёт второй экземпляр (рантайм проверяет его наличие).
/// В ключе остаются только ASCII-буквы, цифры, `_` и `-`: id становится токеном
/// subject NATS (`mes.bpmn.v1.event.<instance>`), а `№101` и `ORD 7` — `101` и
/// `ORD-7`.
pub fn instance_id(instance_prefix: &str, business_key: &str) -> Option<String> {
    let key: String = business_key
        .trim()
        .chars()
        .map(|symbol| {
            if symbol.is_ascii_alphanumeric() || symbol == '_' || symbol == '-' {
                symbol
            } else {
                '-'
            }
        })
        .collect();

    let key = key.trim_matches('-');
    if key.is_empty() {
        return None;
    }

    Some(format!("{instance_prefix}{key}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROJECT_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:trg="http://erp.mes/schema/bpmn/trigger">
  <bpmn:message id="Message_PurchaseOrder" name="PurchaseOrder" />
  <bpmn:message id="Message_PurchaseOrderConfirmed" name="PurchaseOrderConfirmed" />
  <bpmn:collaboration id="Collaboration_1">
    <bpmn:messageFlow id="MF_PO" sourceRef="Event_SendPO" targetRef="Task_ConfirmPO"
                      messageRef="Message_PurchaseOrder" />
  </bpmn:collaboration>
  <bpmn:process id="Process_Dairy" isExecutable="true">
    <bpmn:startEvent id="Start_Dairy" />
    <bpmn:userTask id="Task_SendPO" />
    <bpmn:intermediateThrowEvent id="Event_SendPO">
      <bpmn:extensionElements>
        <trg:processTrigger targetProcessId="Process_Supplier"
                            instancePrefix="inst-supp-"
                            startElementId="Task_SendPO"
                            firstElementId="Task_ConfirmPO"
                            payloadKeys=" orderNum , volume ,, orderNum " />
      </bpmn:extensionElements>
      <bpmn:messageEventDefinition messageRef="Message_PurchaseOrder" />
    </bpmn:intermediateThrowEvent>
    <bpmn:intermediateCatchEvent id="Task_WaitPOConfirmed">
      <bpmn:messageEventDefinition messageRef="Message_PurchaseOrderConfirmed" />
    </bpmn:intermediateCatchEvent>
  </bpmn:process>
</bpmn:definitions>"#;

    fn catalog() -> MessageCatalog {
        parse(PROJECT_XML).expect("схема разбирается")
    }

    #[test]
    fn throw_and_catch_messages_resolve_through_message_definitions() {
        let catalog = catalog();

        assert_eq!(catalog.process_id, "Process_Dairy");
        assert_eq!(
            catalog.throw_message("Event_SendPO"),
            Some("PurchaseOrder"),
            "имя сообщения берётся из bpmn:message по messageRef"
        );
        assert_eq!(
            catalog.catch_message("Task_WaitPOConfirmed"),
            Some("PurchaseOrderConfirmed")
        );
        assert_eq!(
            catalog.catch_nodes("PurchaseOrderConfirmed"),
            vec!["Task_WaitPOConfirmed"]
        );
        assert!(catalog.catch_nodes("MilkShipped").is_empty());
    }

    #[test]
    fn trigger_reads_fields_and_canonises_payload_keys() {
        let catalog = catalog();
        let trigger = catalog.trigger_for("Event_SendPO").expect("триггер узла");

        assert_eq!(trigger.target_process, "Process_Supplier");
        assert_eq!(trigger.instance_prefix, "inst-supp-");
        assert_eq!(trigger.start_element.as_deref(), Some("Task_SendPO"));
        assert_eq!(trigger.first_element.as_deref(), Some("Task_ConfirmPO"));
        assert_eq!(
            trigger.payload_keys,
            vec!["orderNum".to_string(), "volume".to_string()],
            "повторы и пробелы убираются"
        );
    }

    #[test]
    fn message_flow_links_two_pools() {
        let catalog = catalog();
        let flow = catalog.flow_for("PurchaseOrder").expect("связь проекта");

        assert_eq!(flow.id, "MF_PO");
        assert_eq!(flow.source, "Event_SendPO");
        assert_eq!(flow.target, "Task_ConfirmPO");
    }

    #[test]
    fn schema_without_messages_is_not_an_error() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="plain" isExecutable="true">
    <bpmn:startEvent id="Start" />
    <bpmn:endEvent id="End" />
    <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="End" />
  </bpmn:process>
</bpmn:definitions>"#;

        let catalog = parse(xml).expect("схема без сообщений разбирается");
        assert_eq!(catalog.process_id, "plain");
        assert!(catalog.throws.is_empty());
        assert!(catalog.triggers.is_empty());
        assert!(catalog.flows.is_empty());
    }

    #[test]
    fn broken_xml_is_none() {
        assert!(parse("<definitions>").is_none());
        assert!(parse("not xml at all").is_none());
    }

    #[test]
    fn payload_carries_only_listed_keys() {
        let variables = HashMap::from([
            ("orderNum".to_string(), "ORD-7".to_string()),
            ("volume".to_string(), "100".to_string()),
            ("secret".to_string(), "hidden".to_string()),
        ]);
        let keys = vec![
            "orderNum".to_string(),
            "volume".to_string(),
            "missing".to_string(),
        ];

        let payload = select_payload(&variables, &keys);

        assert_eq!(payload.len(), 2);
        assert_eq!(payload.get("orderNum").map(String::as_str), Some("ORD-7"));
        assert_eq!(payload.get("volume").map(String::as_str), Some("100"));
        assert!(!payload.contains_key("secret"));
        assert!(
            select_payload(&variables, &[]).is_empty(),
            "пустой payloadKeys не переносит контекст целиком"
        );
    }

    #[test]
    fn correlation_uses_business_key_not_instance_id() {
        let variables = HashMap::from([
            ("orderNum".to_string(), "ORD-7".to_string()),
            ("instance_id".to_string(), "inst-cust-123".to_string()),
        ]);

        let correlation = correlation(&variables);

        assert_eq!(
            correlation.get("orderNum").map(String::as_str),
            Some("ORD-7")
        );
        assert_eq!(correlation.len(), 1);
        assert!(matches_correlation(&variables, &correlation));
        assert!(
            !matches_correlation(
                &HashMap::from([("orderNum".to_string(), "ORD-8".to_string())]),
                &correlation
            ),
            "чужой заказ не должен получать сообщение"
        );
    }

    #[test]
    fn instance_id_is_deterministic_and_safe() {
        assert_eq!(
            instance_id("inst-supp-", "ORD 7").as_deref(),
            Some("inst-supp-ORD-7")
        );
        assert_eq!(
            instance_id("inst-supp-", "ORD:7/1").as_deref(),
            Some("inst-supp-ORD-7-1")
        );
        assert_eq!(
            instance_id("inst-supp-", "№101").as_deref(),
            Some("inst-supp-101"),
            "не-ASCII символы не попадают в subject NATS"
        );
        assert_eq!(instance_id("inst-supp-", "   "), None);
    }
}
