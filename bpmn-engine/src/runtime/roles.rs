//! Роль как авторизация (M6).
//!
//! До M6 роль была фильтром кнопок в кабинете, а движок исполнял шаг по любому
//! событию: прямой `sendEvent` от имени чужой роли двигал процесс
//! (`erp-simulator/docs/dairy-plant-roadmap.md`, ограничения 8 и 10).
//!
//! Модуль отвечает на один вопрос: **какая роль вправе выполнить узел схемы**.
//! Источник ответа — сама схема, а не клиент:
//!
//! - у задачи роль объявляет `bpmn:potentialOwner/@name` (спецификация BPMN 2.0
//!   разрешает `resourceRole` только у Activity);
//! - у стартового события `potentialOwner` нет, поэтому роль кнопки старта
//!   лежит в расширении `act:userAction/@role` (та же запись, что рисует
//!   кнопку в кабинете: `xml/bpmnXml.js`);
//! - узел без роли — общая очередь: так ведёт себя и кабинет, поэтому
//!   загруженный BPMN без назначений отыгрывается без правок.
//!
//! Коды ролей сравниваются без нормализации регистра: клиент адресует шаг
//! машинным кодом (`SALES`), а в схеме он мог быть записан иначе
//! (`ROLE_PATTERN` в `constants.js` регистр не фиксирует).
//!
//! Модуль чистый: он не ходит в БД, не двигает токены и не публикует события —
//! XML приходит из деплоя (`runtime::store::GraphStores::load_xml`).

use roxmltree::{Document, Node};

/// Роль исполнителя задачи (`bpmn:potentialOwner`).
pub const POTENTIAL_OWNER: &str = "potentialOwner";

/// Кнопка шага (`act:userAction`) — у событий нет `resourceRole`.
pub const USER_ACTION: &str = "userAction";

/// Требования узла к роли.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Roles {
    /// Роль не назначена: шаг — общая очередь, пускать любую сессию.
    Open,
    /// Разрешены только перечисленные роли (коды из схемы).
    Only(Vec<String>),
}

impl Roles {
    /// Вправе ли сессия с ролью `actor` выполнить узел.
    ///
    /// Пустой `actor` не проходит проверку `Only`: «роль не указана» — это не
    /// «любая роль». Решение, что делать с событиями без роли (NATS-воркеры,
    /// E2E-скрипты), принимает рантайм, а не эта функция.
    pub fn allows(&self, actor: &str) -> bool {
        match self {
            Roles::Open => true,
            Roles::Only(roles) => {
                let actor = actor.trim();
                !actor.is_empty() && roles.iter().any(|role| role.eq_ignore_ascii_case(actor))
            }
        }
    }

    /// Роли, которые требует узел (для текста отказа и ответа API).
    pub fn required(&self) -> Vec<String> {
        match self {
            Roles::Open => Vec::new(),
            Roles::Only(roles) => roles.clone(),
        }
    }
}

/// Решение проверки прав: пускать или нет и что сказать участнику.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    /// Можно ли выполнить шаг.
    pub allowed: bool,
    /// Роли, закреплённые за шагом (пусто — общая очередь).
    pub required: Vec<String>,
    /// Причина отказа для кабинета (`""` — отказа нет).
    pub reason: String,
}

/// Проверка роли сессии против ролей шага (`bpmn:userTask`).
pub fn decide(actor: &str, node: &str, roles: &Roles) -> Decision {
    decide_action(actor, &format!("завершить шаг {node}"), roles)
}

/// Проверка роли сессии против ролей кнопки старта.
pub fn decide_start(actor: &str, process: &str, roles: &Roles) -> Decision {
    decide_action(actor, &format!("запустить процесс {process}"), roles)
}

/// Общая проверка: отказ объясняет, какое действие и какая роль нужны.
///
/// `action` — что сессия пыталась сделать («завершить шаг Task_Approve»),
/// поэтому текст читается и в кабинете, и в журнале:
/// «Роль WAREHOUSE не может завершить шаг Task_Approve: нужна роль SALES.»
fn decide_action(actor: &str, action: &str, roles: &Roles) -> Decision {
    let actor = actor.trim();

    if roles.allows(actor) {
        return Decision {
            allowed: true,
            required: roles.required(),
            reason: String::new(),
        };
    }

    let required = roles.required();
    let roles_text = required.join(", ");
    let reason = if actor.is_empty() {
        format!("Роль сессии не указана: {action} может только роль {roles_text}.")
    } else {
        format!("Роль {actor} не может {action}: нужна роль {roles_text}.")
    };

    Decision {
        allowed: false,
        required,
        reason,
    }
}

/// `bpmn:process/@id` схемы — для текста отказа на старте.
pub fn process_id(xml: &str) -> Option<String> {
    let document = Document::parse(xml).ok()?;
    document
        .descendants()
        .find(|node| node.is_element() && node.tag_name().name() == "process")?
        .attribute("id")
        .map(str::to_string)
}

/// Роли узла-хоста: `bpmn:potentialOwner/@name`, при их отсутствии —
/// `act:userAction/@role` (кнопки на узле).
///
/// Незнакомый `node_id` — `Roles::Open`: компилятор движка и разбор ролей
/// читают одну и ту же схему, поэтому расхождение означает узел без
/// объявленной роли, а не подделку события.
pub fn node_roles(xml: &str, node_id: &str) -> Result<Roles, String> {
    let document = parse(xml)?;

    let Some(node) = document
        .descendants()
        .find(|node| node.is_element() && node.attribute("id") == Some(node_id))
    else {
        return Ok(Roles::Open);
    };

    let owners = collect(node, POTENTIAL_OWNER, "name");
    if !owners.is_empty() {
        return Ok(Roles::Only(owners));
    }

    Ok(only_or_open(collect(node, USER_ACTION, "role")))
}

/// Роли кнопок старта: `act:userAction/@role` внутри `bpmn:startEvent`.
///
/// Стартов без кнопки (таймер, сообщение) проверка не касается: если роли не
/// объявила ни одна кнопка старта, старт — общая очередь.
pub fn start_roles(xml: &str) -> Result<Roles, String> {
    let document = parse(xml)?;

    let roles: Vec<String> = document
        .descendants()
        .filter(|node| node.is_element() && node.tag_name().name() == "startEvent")
        .flat_map(|start| collect(start, USER_ACTION, "role"))
        .fold(Vec::new(), |mut acc, role| {
            if !acc
                .iter()
                .any(|existing: &String| existing.eq_ignore_ascii_case(&role))
            {
                acc.push(role);
            }
            acc
        });

    Ok(only_or_open(roles))
}

/// Разбор схемы с причиной отказа: битый XML — это ошибка деплоя, а не
/// «роли не назначены», и рантайм должен её залогировать.
fn parse(xml: &str) -> Result<Document<'_>, String> {
    Document::parse(xml).map_err(|error| format!("схема не разобрана: {error}"))
}

/// Значения атрибута `attribute` у потомков с локальным именем `name`.
fn collect(node: Node<'_, '_>, name: &str, attribute: &str) -> Vec<String> {
    let mut roles: Vec<String> = Vec::new();

    for child in node
        .descendants()
        .filter(|child| child.is_element() && child.tag_name().name() == name)
    {
        let Some(role) = child
            .attribute(attribute)
            .map(str::trim)
            .filter(|role| !role.is_empty())
        else {
            continue;
        };

        if !roles
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(role))
        {
            roles.push(role.to_string());
        }
    }

    roles
}

fn only_or_open(roles: Vec<String>) -> Roles {
    if roles.is_empty() {
        Roles::Open
    } else {
        Roles::Only(roles)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Схема демо-проекта в миниатюре: кнопка старта, задача с двумя ролями,
    /// задача без роли и задача, где роль объявлена только кнопкой.
    const XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:act="http://erp.mes/schema/bpmn/action">
  <bpmn:process id="Process_Sales">
    <bpmn:startEvent id="Event_Start">
      <bpmn:extensionElements>
        <act:userAction id="createOrder" role="SALES" elementId="Task_ReceiveOrder" />
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:userTask id="Task_Approve">
      <bpmn:potentialOwner name="SALES" />
      <bpmn:potentialOwner name="DIRECTOR" />
    </bpmn:userTask>
    <bpmn:userTask id="Task_ButtonOnly">
      <bpmn:extensionElements>
        <act:userAction id="approve" role="QUALITY" elementId="Task_Next" />
      </bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:userTask id="Task_Free" />
  </bpmn:process>
</bpmn:definitions>"#;

    #[test]
    fn node_roles_reads_potential_owner() {
        let roles = node_roles(XML, "Task_Approve").expect("схема разобрана");

        assert_eq!(
            roles,
            Roles::Only(vec!["SALES".to_string(), "DIRECTOR".to_string()])
        );
        assert!(roles.allows("SALES"));
        assert!(
            roles.allows("director"),
            "код роли сравнивается без регистра"
        );
        assert!(!roles.allows("WAREHOUSE"));
        assert!(!roles.allows(""), "пустая роль не подходит под требование");
    }

    #[test]
    fn node_roles_falls_back_to_button_role() {
        let roles = node_roles(XML, "Task_ButtonOnly").expect("схема разобрана");

        assert_eq!(roles, Roles::Only(vec!["QUALITY".to_string()]));
    }

    #[test]
    fn node_without_role_is_open_queue() {
        assert_eq!(node_roles(XML, "Task_Free"), Ok(Roles::Open));
        // Незнакомый узел не делает событие запрещённым: роль просто не объявлена.
        assert_eq!(node_roles(XML, "Task_Unknown"), Ok(Roles::Open));
        assert!(Roles::Open.allows(""));
        assert!(Roles::Open.allows("ANY"));
    }

    #[test]
    fn start_roles_come_from_start_buttons() {
        assert_eq!(start_roles(XML), Ok(Roles::Only(vec!["SALES".to_string()])));

        let without_button = r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="P"><bpmn:startEvent id="S" /></bpmn:process>
        </bpmn:definitions>"#;

        assert_eq!(start_roles(without_button), Ok(Roles::Open));
    }

    #[test]
    fn broken_xml_is_an_error() {
        assert!(node_roles("<bpmn:definitions>", "Task").is_err());
        assert!(start_roles("не XML").is_err());
    }

    #[test]
    fn decide_explains_the_refusal() {
        let roles = Roles::Only(vec!["SALES".to_string()]);

        let allowed = decide("SALES", "Task_Approve", &roles);
        assert!(allowed.allowed);
        assert_eq!(allowed.reason, "");

        let foreign = decide("WAREHOUSE", "Task_Approve", &roles);
        assert!(!foreign.allowed);
        assert_eq!(
            foreign.reason,
            "Роль WAREHOUSE не может завершить шаг Task_Approve: нужна роль SALES."
        );
        assert_eq!(foreign.required, vec!["SALES".to_string()]);

        let unknown = decide("", "Task_Approve", &roles);
        assert!(!unknown.allowed);
        assert!(unknown.reason.contains("Роль сессии не указана"));

        // Общая очередь пускает любого, в том числе событие без роли.
        assert!(decide("", "Task_Free", &Roles::Open).allowed);
    }

    #[test]
    fn start_refusal_names_the_process() {
        let roles = Roles::Only(vec!["SALES".to_string()]);

        let decision = decide_start("CLIENT", "Process_Sales", &roles);

        assert!(!decision.allowed);
        assert_eq!(
            decision.reason,
            "Роль CLIENT не может запустить процесс Process_Sales: нужна роль SALES."
        );
        assert_eq!(process_id(XML), Some("Process_Sales".to_string()));
        assert_eq!(process_id("не XML"), None);
    }
}
