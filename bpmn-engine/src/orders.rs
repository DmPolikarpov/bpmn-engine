//! Заказы (M4): бизнес-объект демо-контура и API кабинета.
//!
//! Заказ — не переменная процесса, а запись в БД: `orderNum` объявлен
//! бизнес-ключом и корреляционным ключом сообщений (M3), поэтому список
//! переживает F5 и общий для всех браузеров.
//!
//! Движок владеет таблицей и отвечает на NATS-запрос
//! [`API_SUBJECT`], а HTTP-шлюз остаётся мостом: он перекладывает REST в
//! этот запрос и обратно. Так у шлюза нет ни пула к БД, ни второй копии
//! правил валидации.
//!
//! Контракт — JSON в camelCase: те же имена полей уезжают в переменные
//! процесса (`orderNum`, `volumeLitres`, …), поэтому кабинету не нужен
//! маппинг.

use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

/// Subject запроса кабинета (`nc.request` из HTTP-шлюза).
pub const API_SUBJECT: &str = "mes.in.v1.api.orders";

/// Имя очереди: на запрос отвечает одна реплика движка, а не все три.
pub const API_QUEUE: &str = "orders-api";

/// Заказ создан, процесс по нему ещё не запускали.
pub const STATUS_NEW: &str = "NEW";

/// По заказу запущен экземпляр процесса (хотя бы одной стороны).
pub const STATUS_IN_PROGRESS: &str = "IN_PROGRESS";

/// Состояние контура, у которого нет экземпляра процесса.
///
/// В проде его подставляет SQL (`COALESCE(pi.status, 'IDLE')`), поэтому
/// константа нужна только in-memory хранилищу тестов (`Order::from_new`);
/// значение обязано совпадать с тем, что отдаёт `SELECT_ORDER`.
#[allow(dead_code)]
pub const IDLE: &str = "IDLE";

/// Максимальная длина `orderNum`: совпадает с `business_key` процессов.
const MAX_ORDER_NUM: usize = 64;
const MAX_NOMENCLATURE: usize = 160;
const MAX_PACKAGING: usize = 80;
const MAX_COUNTERPARTY: usize = 160;

/// Верхняя граница объёма партии, л: 100 000 л — это 100 тонн, больше
/// демо-завод не принимает.
const MAX_VOLUME_LITRES: f64 = 100_000.0;

/// Горизонт планирования, дней: срок дальше года — опечатка в дате.
const MAX_DUE_HORIZON_DAYS: i64 = 365;

/// Заказ в том виде, в котором его создаёт кабинет.
///
/// Поля предметные, а не «объём строкой»: карточка заказа показывает
/// номенклатуру, жирность и упаковку, а процесс получает их переменными.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewOrder {
    /// Бизнес-ключ и корреляционный ключ (`№101`).
    pub order_num: String,
    /// Номенклатура: `Молоко питьевое, 3,2%`.
    pub nomenclature: String,
    /// Объём партии, л.
    pub volume_liters: f64,
    /// Жирность, %.
    pub fat_content: f64,
    /// Упаковка: `ПЭТ 1 л`, `Тетра Пак 0,5 л`, …
    pub packaging: String,
    /// Срок поставки (дата без времени).
    pub due_date: NaiveDate,
    /// Контрагент-заказчик.
    pub counterparty: String,
}

impl NewOrder {
    /// Обрезает пробелы: их не видно в UI, но они ломают сравнение ключей.
    pub fn normalized(mut self) -> Self {
        self.order_num = self.order_num.trim().to_string();
        self.nomenclature = self.nomenclature.trim().to_string();
        self.packaging = self.packaging.trim().to_string();
        self.counterparty = self.counterparty.trim().to_string();
        self
    }
}

/// Заказ из БД вместе с состоянием запущенных по нему экземпляров.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    pub order_num: String,
    pub nomenclature: String,
    pub volume_liters: f64,
    pub fat_content: f64,
    pub packaging: String,
    pub due_date: NaiveDate,
    pub counterparty: String,
    pub status: String,
    pub customer_instance_id: Option<String>,
    /// Текущий шаг заказчика (`tokens.state = 'ACTIVE'`), `None` — процесса нет
    /// или он завершён.
    pub customer_active_element_id: Option<String>,
    /// `IDLE` (процесса нет) / `ACTIVE` / `COMPLETED` / `TERMINATED`.
    pub customer_process_state: String,
    pub supplier_instance_id: Option<String>,
    pub supplier_active_element_id: Option<String>,
    pub supplier_process_state: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Order {
    /// Новая запись до сохранения: `IDLE` у обеих сторон, `NEW` в статусе.
    ///
    /// В проде карточку собирает `SELECT_ORDER`/`map_row` (там же считаются
    /// шаги и состояния), поэтому конструктор нужен только in-memory
    /// хранилищу тестов — отсюда `allow(dead_code)`.
    #[allow(dead_code)]
    pub fn from_new(order: NewOrder, now: DateTime<Utc>) -> Self {
        Self {
            order_num: order.order_num,
            nomenclature: order.nomenclature,
            volume_liters: order.volume_liters,
            fat_content: order.fat_content,
            packaging: order.packaging,
            due_date: order.due_date,
            counterparty: order.counterparty,
            status: STATUS_NEW.to_string(),
            customer_instance_id: None,
            customer_active_element_id: None,
            customer_process_state: IDLE.to_string(),
            supplier_instance_id: None,
            supplier_active_element_id: None,
            supplier_process_state: IDLE.to_string(),
            created_at: now,
            updated_at: now,
        }
    }
}


/// Ошибка одного поля — тот же формат, что читает клиент шлюза
/// (`httpClient.readErrorDetails`: `details[].field` + `.message`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldError {
    pub field: String,
    pub message: String,
}

impl FieldError {
    fn new(field: &str, message: &str) -> Self {
        Self {
            field: field.to_string(),
            message: message.to_string(),
        }
    }
}

/// Проверка заказа на входе: обязательные поля и диапазоны.
///
/// `today` передаётся аргументом, а не берётся из системных часов: иначе тест
/// на «срок в прошлом» зависел бы от даты прогона.
pub fn validate(order: &NewOrder, today: NaiveDate) -> Vec<FieldError> {
    let mut errors = Vec::new();

    check_text(&mut errors, "orderNum", &order.order_num, MAX_ORDER_NUM);
    check_text(
        &mut errors,
        "nomenclature",
        &order.nomenclature,
        MAX_NOMENCLATURE,
    );
    check_text(&mut errors, "packaging", &order.packaging, MAX_PACKAGING);
    check_text(
        &mut errors,
        "counterparty",
        &order.counterparty,
        MAX_COUNTERPARTY,
    );

    if !order.volume_liters.is_finite()
        || order.volume_liters <= 0.0
        || order.volume_liters > MAX_VOLUME_LITRES
    {
        errors.push(FieldError::new(
            "volumeLiters",
            &format!("объём должен быть больше 0 и не больше {MAX_VOLUME_LITRES} л"),
        ));
    }

    if !order.fat_content.is_finite() || !(0.0..=100.0).contains(&order.fat_content) {
        errors.push(FieldError::new(
            "fatContent",
            "жирность должна быть в диапазоне 0…100 %",
        ));
    }

    let horizon = today + Duration::days(MAX_DUE_HORIZON_DAYS);
    if order.due_date < today {
        errors.push(FieldError::new(
            "dueDate",
            "срок поставки не может быть в прошлом",
        ));
    } else if order.due_date > horizon {
        errors.push(FieldError::new(
            "dueDate",
            &format!("срок поставки дальше горизонта планирования ({MAX_DUE_HORIZON_DAYS} дней)"),
        ));
    }

    errors
}

fn check_text(errors: &mut Vec<FieldError>, field: &str, value: &str, max_len: usize) {
    let trimmed = value.trim();

    if trimmed.is_empty() {
        errors.push(FieldError::new(field, "поле обязательно"));
    } else if trimmed.chars().count() > max_len {
        errors.push(FieldError::new(
            field,
            &format!("не длиннее {max_len} символов"),
        ));
    }
}

/// Сторона обмена, к которой привязывается экземпляр процесса.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Org {
    Customer,
    Supplier,
}

impl Org {
    /// Разбирает `org` из запроса (`CUSTOMER` / `SUPPLIER`), регистр не важен.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "CUSTOMER" => Some(Self::Customer),
            "SUPPLIER" => Some(Self::Supplier),
            _ => None,
        }
    }
}

/// Запрос кабинета. Неизвестные поля игнорируются: кабинет может прислать
/// `elementId` для телеметрии, движку он не нужен.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub action: String,
    #[serde(default)]
    pub order_num: Option<String>,
    #[serde(default)]
    pub order: Option<NewOrder>,
    #[serde(default)]
    pub org: Option<String>,
    #[serde(default)]
    pub instance_id: Option<String>,
}

/// Ответ API. `status` повторяет HTTP-код, чтобы шлюз не угадывал его по
/// тексту ошибки и отдавал 400/404/409 как есть.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<Order>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orders: Option<Vec<Order>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Vec<FieldError>>,
}

impl Response {
    pub fn with_order(order: Order) -> Self {
        Self {
            ok: true,
            order: Some(order),
            ..Default::default()
        }
    }

    pub fn with_orders(orders: Vec<Order>) -> Self {
        Self {
            ok: true,
            orders: Some(orders),
            ..Default::default()
        }
    }

    /// Отказ с HTTP-кодом и текстом; `details` — только у валидации.
    pub fn failure(status: u16, error: impl Into<String>) -> Self {
        Self {
            ok: false,
            status: Some(status),
            error: Some(error.into()),
            ..Default::default()
        }
    }

    pub fn invalid(details: Vec<FieldError>) -> Self {
        Self {
            ok: false,
            status: Some(400),
            error: Some("Заказ не прошёл валидацию".to_string()),
            details: Some(details),
            ..Default::default()
        }
    }
}

/// Результат вставки: гонка двух браузеров не должна затирать первый заказ.
#[derive(Debug, Clone, PartialEq)]
pub enum CreateOutcome {
    Created(Order),
    Duplicate(Order),
}

/// Хранилище заказов. Трейт отделяет API от Postgres: контракт запроса и
/// ответа проверяется in-memory стором, без живой БД.
#[async_trait::async_trait]
pub trait OrderStore: Send + Sync {
    async fn list(&self) -> anyhow::Result<Vec<Order>>;
    async fn get(&self, order_num: &str) -> anyhow::Result<Option<Order>>;
    async fn create(&self, order: NewOrder) -> anyhow::Result<CreateOutcome>;
    /// Привязывает экземпляр процесса к стороне заказа. `None` — заказа нет.
    async fn attach(
        &self,
        order_num: &str,
        org: Org,
        instance_id: &str,
    ) -> anyhow::Result<Option<Order>>;
}

/// Обработчик запроса: единственное место, где решается, что вернуть.
///
/// `today` — дата планирования (в проде `Utc::now()`, в тестах фиксирована).
pub async fn handle(store: &dyn OrderStore, request: Request, today: NaiveDate) -> Response {
    let action = request.action.trim().to_ascii_lowercase();

    match action.as_str() {
        "list" => match store.list().await {
            Ok(orders) => Response::with_orders(orders),
            Err(error) => internal(error),
        },
        "get" => {
            let Some(order_num) = non_empty(request.order_num.as_deref()) else {
                return Response::failure(400, "не указан номер заказа (orderNum)");
            };
            match store.get(order_num).await {
                Ok(Some(order)) => Response::with_order(order),
                Ok(None) => Response::failure(404, format!("заказ {order_num} не найден")),
                Err(error) => internal(error),
            }
        }
        "create" => {
            let Some(order) = request.order else {
                return Response::failure(400, "не передано тело заказа (order)");
            };
            let order = order.normalized();
            let errors = validate(&order, today);

            if !errors.is_empty() {
                return Response::invalid(errors);
            }

            match store.create(order).await {
                Ok(CreateOutcome::Created(order)) => Response::with_order(order),
                Ok(CreateOutcome::Duplicate(order)) => Response::failure(
                    409,
                    format!("заказ {} уже существует", order.order_num),
                ),
                Err(error) => internal(error),
            }
        }
        "attach" => {
            let Some(order_num) = non_empty(request.order_num.as_deref()) else {
                return Response::failure(400, "не указан номер заказа (orderNum)");
            };
            let Some(instance_id) = non_empty(request.instance_id.as_deref()) else {
                return Response::failure(400, "не указан экземпляр процесса (instanceId)");
            };
            let Some(org) = request.org.as_deref().and_then(Org::parse) else {
                return Response::failure(400, "org должен быть CUSTOMER или SUPPLIER");
            };

            match store.attach(order_num, org, instance_id).await {
                Ok(Some(order)) => Response::with_order(order),
                Ok(None) => Response::failure(404, format!("заказ {order_num} не найден")),
                Err(error) => internal(error),
            }
        }
        other => Response::failure(400, format!("неизвестное действие: {other}")),
    }
}

/// Разбор тела NATS-запроса и обработка. Битый JSON — это 400, а не 500:
/// кабинет должен увидеть причину, а не «оркестратор упал».
pub async fn handle_bytes(store: &dyn OrderStore, body: &[u8], today: NaiveDate) -> Response {
    match serde_json::from_slice::<Request>(body) {
        Ok(request) => handle(store, request, today).await,
        Err(error) => Response::failure(400, format!("некорректный JSON запроса: {error}")),
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn internal(error: anyhow::Error) -> Response {
    eprintln!("❌ [Orders] ошибка хранилища: {error:#}");
    Response::failure(500, "ошибка хранилища заказов")
}

/// Общий SELECT заказа.
///
/// Активный шаг и состояние контура считаются по legacy read-model
/// (`tokens`, `process_instances`), а не хранятся копией в `orders`: источник
/// истины один, и после F5 карточка показывает тот же шаг, что и подсветка.
const SELECT_ORDER: &str = r#"
SELECT
    o.order_num,
    o.nomenclature,
    o.volume_liters,
    o.fat_content,
    o.packaging,
    o.due_date,
    o.counterparty,
    o.status,
    o.customer_instance_id,
    o.supplier_instance_id,
    (SELECT t.element_id FROM tokens t
      WHERE t.instance_id = o.customer_instance_id AND t.state = 'ACTIVE'
      ORDER BY t.created_at DESC, t.element_id LIMIT 1) AS customer_active_element_id,
    (SELECT t.element_id FROM tokens t
      WHERE t.instance_id = o.supplier_instance_id AND t.state = 'ACTIVE'
      ORDER BY t.created_at DESC, t.element_id LIMIT 1) AS supplier_active_element_id,
    COALESCE(pi_c.status, 'IDLE') AS customer_process_state,
    COALESCE(pi_s.status, 'IDLE') AS supplier_process_state,
    o.created_at,
    o.updated_at
FROM orders o
LEFT JOIN process_instances pi_c ON pi_c.id = o.customer_instance_id
LEFT JOIN process_instances pi_s ON pi_s.id = o.supplier_instance_id
"#;

fn map_row(row: &sqlx::postgres::PgRow) -> Order {
    Order {
        order_num: row.get("order_num"),
        nomenclature: row.get("nomenclature"),
        volume_liters: row.get("volume_liters"),
        fat_content: row.get("fat_content"),
        packaging: row.get("packaging"),
        due_date: row.get("due_date"),
        counterparty: row.get("counterparty"),
        status: row.get("status"),
        customer_instance_id: row.get("customer_instance_id"),
        customer_active_element_id: row.get("customer_active_element_id"),
        customer_process_state: row.get("customer_process_state"),
        supplier_instance_id: row.get("supplier_instance_id"),
        supplier_active_element_id: row.get("supplier_active_element_id"),
        supplier_process_state: row.get("supplier_process_state"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

/// Postgres-хранилище заказов (миграция `0003_orders.sql`).
pub struct PgOrderStore {
    pool: PgPool,
}

impl PgOrderStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Карточка заказа этим же SELECT'ом — после вставки и привязки.
    async fn fetch(&self, order_num: &str) -> anyhow::Result<Option<Order>> {
        let sql = format!("{SELECT_ORDER} WHERE o.order_num = $1");
        let row = sqlx::query(&sql)
            .bind(order_num)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.as_ref().map(map_row))
    }
}

#[async_trait::async_trait]
impl OrderStore for PgOrderStore {
    async fn list(&self) -> anyhow::Result<Vec<Order>> {
        // Лимит защищает кабинет от таблицы на тысячи строк: демо-контур
        // показывает последние двести заказов.
        let sql = format!("{SELECT_ORDER} ORDER BY o.created_at DESC, o.order_num LIMIT 200");
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;

        Ok(rows.iter().map(map_row).collect())
    }

    async fn get(&self, order_num: &str) -> anyhow::Result<Option<Order>> {
        self.fetch(order_num).await
    }

    async fn create(&self, order: NewOrder) -> anyhow::Result<CreateOutcome> {
        // `ON CONFLICT DO NOTHING` вместо «сначала SELECT»: два браузера
        // могут нажать «Принять заказ» одновременно, и второй обязан получить
        // 409, а не перезаписать первый заказ.
        let inserted = sqlx::query(
            r#"
            INSERT INTO orders
                (order_num, nomenclature, volume_liters, fat_content,
                 packaging, due_date, counterparty, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (order_num) DO NOTHING
            RETURNING order_num
            "#,
        )
        .bind(&order.order_num)
        .bind(&order.nomenclature)
        .bind(order.volume_liters)
        .bind(order.fat_content)
        .bind(&order.packaging)
        .bind(order.due_date)
        .bind(&order.counterparty)
        .bind(STATUS_NEW)
        .fetch_optional(&self.pool)
        .await?;

        let stored = self
            .fetch(&order.order_num)
            .await?
            .ok_or_else(|| anyhow::anyhow!("заказ {} не найден после вставки", order.order_num))?;

        Ok(match inserted {
            Some(_) => CreateOutcome::Created(stored),
            None => CreateOutcome::Duplicate(stored),
        })
    }

    async fn attach(
        &self,
        order_num: &str,
        org: Org,
        instance_id: &str,
    ) -> anyhow::Result<Option<Order>> {
        let sql = match org {
            Org::Customer => {
                r#"
                UPDATE orders
                SET customer_instance_id = $1,
                    status = CASE WHEN status = $3 THEN $4 ELSE status END,
                    updated_at = NOW()
                WHERE order_num = $2
                "#
            }
            Org::Supplier => {
                r#"
                UPDATE orders
                SET supplier_instance_id = $1,
                    status = CASE WHEN status = $3 THEN $4 ELSE status END,
                    updated_at = NOW()
                WHERE order_num = $2
                "#
            }
        };

        let result = sqlx::query(sql)
            .bind(instance_id)
            .bind(order_num)
            .bind(STATUS_NEW)
            .bind(STATUS_IN_PROGRESS)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            return Ok(None);
        }

        self.fetch(order_num).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use chrono::TimeZone;
    use serde_json::json;

    use super::*;

    /// Дата прогона фиксирована: границы срока не зависят от дня запуска.
    fn today() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 10, 23).expect("дата существует")
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 23, 9, 0, 0)
            .single()
            .expect("время существует")
    }

    fn valid_order() -> NewOrder {
        NewOrder {
            order_num: "№101".to_string(),
            nomenclature: "Молоко питьевое, 3,2%".to_string(),
            volume_liters: 1200.0,
            fat_content: 3.2,
            packaging: "ПЭТ 1 л".to_string(),
            due_date: NaiveDate::from_ymd_opt(2026, 10, 30).expect("дата существует"),
            counterparty: "ООО «Ромашка»".to_string(),
        }
    }

    fn fields(errors: &[FieldError]) -> Vec<&str> {
        errors.iter().map(|error| error.field.as_str()).collect()
    }

    #[test]
    fn validate_accepts_full_order() {
        assert!(validate(&valid_order(), today()).is_empty());
    }

    #[test]
    fn validate_requires_text_fields() {
        let mut order = valid_order();
        order.order_num = "   ".to_string();
        order.nomenclature = String::new();
        order.packaging = String::new();
        order.counterparty = String::new();

        let errors = validate(&order, today());

        assert_eq!(
            fields(&errors),
            vec!["orderNum", "nomenclature", "packaging", "counterparty"]
        );
        assert!(errors.iter().all(|error| error.message == "поле обязательно"));
    }

    #[test]
    fn validate_checks_volume_range() {
        for volume in [0.0, -1.0, MAX_VOLUME_LITRES + 1.0] {
            let mut order = valid_order();
            order.volume_liters = volume;

            let errors = validate(&order, today());

            assert_eq!(fields(&errors), vec!["volumeLiters"], "объём {volume}");
        }
    }

    #[test]
    fn validate_checks_fat_range() {
        for fat in [-0.1, 100.5] {
            let mut order = valid_order();
            order.fat_content = fat;

            let errors = validate(&order, today());

            assert_eq!(fields(&errors), vec!["fatContent"], "жирность {fat}");
        }

        let mut order = valid_order();
        order.fat_content = 0.0;
        assert!(validate(&order, today()).is_empty(), "обезжиренное молоко");
    }

    #[test]
    fn validate_rejects_past_and_far_due_dates() {
        let mut past = valid_order();
        past.due_date = today() - Duration::days(1);
        assert_eq!(fields(&validate(&past, today())), vec!["dueDate"]);

        let mut far = valid_order();
        far.due_date = today() + Duration::days(MAX_DUE_HORIZON_DAYS + 1);
        assert_eq!(fields(&validate(&far, today())), vec!["dueDate"]);

        let mut edge = valid_order();
        edge.due_date = today() + Duration::days(MAX_DUE_HORIZON_DAYS);
        assert!(
            validate(&edge, today()).is_empty(),
            "граница горизонта ещё допустима"
        );
    }

    #[test]
    fn validate_limits_text_length() {
        let mut order = valid_order();
        order.order_num = "№".repeat(MAX_ORDER_NUM + 1);
        order.nomenclature = "м".repeat(MAX_NOMENCLATURE + 1);

        assert_eq!(
            fields(&validate(&order, today())),
            vec!["orderNum", "nomenclature"]
        );
    }

    /// In-memory стор: контракт API проверяется без живой БД.
    #[derive(Default)]
    struct MemoryStore {
        orders: Mutex<Vec<Order>>,
    }

    impl MemoryStore {
        fn seed(&self, order: Order) {
            self.orders.lock().expect("мьютекс").push(order);
        }
    }

    #[async_trait::async_trait]
    impl OrderStore for MemoryStore {
        async fn list(&self) -> anyhow::Result<Vec<Order>> {
            Ok(self.orders.lock().expect("мьютекс").clone())
        }

        async fn get(&self, order_num: &str) -> anyhow::Result<Option<Order>> {
            Ok(self
                .orders
                .lock()
                .expect("мьютекс")
                .iter()
                .find(|order| order.order_num == order_num)
                .cloned())
        }

        async fn create(&self, order: NewOrder) -> anyhow::Result<CreateOutcome> {
            let mut orders = self.orders.lock().expect("мьютекс");

            if let Some(existing) = orders
                .iter()
                .find(|item| item.order_num == order.order_num)
            {
                return Ok(CreateOutcome::Duplicate(existing.clone()));
            }

            let created = Order::from_new(order, now());
            orders.push(created.clone());

            Ok(CreateOutcome::Created(created))
        }

        async fn attach(
            &self,
            order_num: &str,
            org: Org,
            instance_id: &str,
        ) -> anyhow::Result<Option<Order>> {
            let mut orders = self.orders.lock().expect("мьютекс");
            let Some(order) = orders
                .iter_mut()
                .find(|order| order.order_num == order_num)
            else {
                return Ok(None);
            };

            match org {
                Org::Customer => {
                    order.customer_instance_id = Some(instance_id.to_string());
                    order.customer_process_state = "ACTIVE".to_string();
                }
                Org::Supplier => {
                    order.supplier_instance_id = Some(instance_id.to_string());
                    order.supplier_process_state = "ACTIVE".to_string();
                }
            }

            if order.status == STATUS_NEW {
                order.status = STATUS_IN_PROGRESS.to_string();
            }

            Ok(Some(order.clone()))
        }
    }

    async fn request(store: &MemoryStore, body: serde_json::Value) -> Response {
        handle_bytes(store, body.to_string().as_bytes(), today()).await
    }

    fn new_order_body() -> serde_json::Value {
        json!({
            "action": "create",
            "order": {
                "orderNum": "  №101  ",
                "nomenclature": "Молоко питьевое, 3,2%",
                "volumeLiters": 1200.0,
                "fatContent": 3.2,
                "packaging": "ПЭТ 1 л",
                "dueDate": "2026-10-30",
                "counterparty": "ООО «Ромашка»"
            }
        })
    }

    #[tokio::test]
    async fn create_stores_normalized_order() {
        let store = MemoryStore::default();

        let response = request(&store, new_order_body()).await;

        assert!(response.ok);
        assert_eq!(response.status, None);
        let order = response.order.expect("карточка заказа");
        assert_eq!(order.order_num, "№101", "пробелы обрезаны");
        assert_eq!(order.status, STATUS_NEW);
        assert_eq!(order.customer_process_state, IDLE);
        assert_eq!(order.supplier_process_state, IDLE);
    }

    #[tokio::test]
    async fn create_rejects_invalid_order_with_field_details() {
        let store = MemoryStore::default();
        let mut body = new_order_body();
        body["order"]["volumeLiters"] = json!(0);
        body["order"]["counterparty"] = json!("");

        let response = request(&store, body).await;

        assert!(!response.ok);
        assert_eq!(response.status, Some(400));
        assert_eq!(
            fields(response.details.as_deref().expect("детали")),
            vec!["counterparty", "volumeLiters"]
        );
        assert!(store.orders.lock().expect("мьютекс").is_empty());
    }

    #[tokio::test]
    async fn create_duplicate_returns_conflict() {
        let store = MemoryStore::default();
        store.seed(Order::from_new(valid_order(), now()));

        let response = request(&store, new_order_body()).await;

        assert!(!response.ok);
        assert_eq!(response.status, Some(409));
        assert!(response.error.expect("текст ошибки").contains("№101"));
    }

    #[tokio::test]
    async fn get_returns_order_or_not_found() {
        let store = MemoryStore::default();
        store.seed(Order::from_new(valid_order(), now()));

        let found = request(&store, json!({"action": "get", "orderNum": "№101"})).await;
        assert!(found.ok);
        assert_eq!(found.order.expect("заказ").order_num, "№101");

        let missing = request(&store, json!({"action": "get", "orderNum": "№999"})).await;
        assert_eq!(missing.status, Some(404));

        let without_key = request(&store, json!({"action": "get"})).await;
        assert_eq!(without_key.status, Some(400));
    }

    #[tokio::test]
    async fn attach_links_instance_and_moves_status() {
        let store = MemoryStore::default();
        store.seed(Order::from_new(valid_order(), now()));

        let response = request(
            &store,
            json!({
                "action": "attach",
                "orderNum": "№101",
                "org": "customer",
                "instanceId": "inst-cust-abc123"
            }),
        )
        .await;

        assert!(response.ok);
        let order = response.order.expect("заказ");
        assert_eq!(order.status, STATUS_IN_PROGRESS);
        assert_eq!(
            order.customer_instance_id.as_deref(),
            Some("inst-cust-abc123")
        );
        assert_eq!(order.customer_process_state, "ACTIVE");
        assert_eq!(order.supplier_instance_id, None);
    }

    #[tokio::test]
    async fn attach_validates_org_and_instance() {
        let store = MemoryStore::default();
        store.seed(Order::from_new(valid_order(), now()));

        let bad_org = request(
            &store,
            json!({
                "action": "attach",
                "orderNum": "№101",
                "org": "WAREHOUSE",
                "instanceId": "inst-x"
            }),
        )
        .await;
        assert_eq!(bad_org.status, Some(400));

        let no_instance = request(
            &store,
            json!({"action": "attach", "orderNum": "№101", "org": "CUSTOMER"}),
        )
        .await;
        assert_eq!(no_instance.status, Some(400));

        let unknown_order = request(
            &store,
            json!({
                "action": "attach",
                "orderNum": "№999",
                "org": "CUSTOMER",
                "instanceId": "inst-x"
            }),
        )
        .await;
        assert_eq!(unknown_order.status, Some(404));
    }

    #[tokio::test]
    async fn list_returns_all_orders() {
        let store = MemoryStore::default();
        store.seed(Order::from_new(valid_order(), now()));
        let mut second = valid_order();
        second.order_num = "№102".to_string();
        store.seed(Order::from_new(second, now()));

        let response = request(&store, json!({"action": "list"})).await;

        assert!(response.ok);
        assert_eq!(response.orders.expect("список").len(), 2);
    }

    #[tokio::test]
    async fn unknown_action_and_broken_json_return_400() {
        let store = MemoryStore::default();

        let unknown = request(&store, json!({"action": "drop"})).await;
        assert_eq!(unknown.status, Some(400));

        let broken = handle_bytes(&store, b"{not json", today()).await;
        assert_eq!(broken.status, Some(400));
        assert!(broken.error.expect("текст").contains("JSON"));
    }

    /// Контракт с кабинетом: поля заказа — camelCase, `status` — HTTP-код.
    #[test]
    fn response_uses_camel_case_keys() {
        let order = Order::from_new(valid_order(), now());
        let value = serde_json::to_value(Response::with_order(order)).expect("сериализация");

        assert!(value["order"]["orderNum"].is_string());
        assert!(value["order"]["volumeLiters"].is_number());
        assert!(value["order"]["fatContent"].is_number());
        assert!(value["order"]["dueDate"].is_string());
        assert!(value["order"]["customerProcessState"].is_string());
        assert_eq!(value["ok"], json!(true));

        let failure = serde_json::to_value(Response::failure(409, "дубль")).expect("сериализация");
        assert_eq!(failure["status"], json!(409));
        assert!(failure.get("details").is_none(), "нет пустых полей");
    }
}
