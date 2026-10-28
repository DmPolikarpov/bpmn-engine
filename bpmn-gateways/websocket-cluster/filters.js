/**
 * Фильтры WS-подписки (M7).
 *
 * До M7 шлюз вещал `mes.bpmn.v1.event.>` всем клиентам: кабинет получал токены
 * чужих экземпляров и отбрасывал их сам. Теперь клиент объявляет, какие
 * экземпляры и роль ему нужны, а шлюз отсекает остальное **до** отправки.
 *
 * Контракт (совместим с прежним «ничего не указал — получай всё»):
 *
 * - в адресе: `/ws/status?instanceId=inst-1&instanceId=inst-2&role=operator`
 *   (`instances=inst-1,inst-2` — то же самое);
 * - сообщением: `{"type":"subscribe","instanceIds":["inst-1"],"role":"operator"}`,
 *   `{"type":"subscribe"}` или `{"type":"unsubscribe"}` — сбросить фильтры;
 * - ответ шлюза: `{"type":"subscribed","instanceIds":[...],"role":"..."}`.
 *
 * Роль фильтрует только те события, где она объявлена (`role`/`roles`/`actor` в
 * сообщении или его `payload`): движок пока не прикладывает роль к
 * `token_updated`, и отбрасывать все события «без роли» значило бы погасить
 * подсветку схемы.
 */

/** Больше — уже не подписка, а выгрузка: защита от опечатки в адресе. */
const MAX_INSTANCES = 100;

const INSTANCE_KEYS = ['instance_id', 'instanceId'];
const ROLE_KEYS = ['role', 'roles', 'actor'];
const SUBJECT_PREFIX = 'mes.bpmn.v1.event.';

/** Приводит вход к массиву уникальных непустых id (строка — через запятую). */
function toIdList(value) {
    if (value === undefined || value === null) return [];

    const list = Array.isArray(value) ? value : String(value).split(',');
    const ids = [];

    for (const item of list) {
        if (typeof item !== 'string' && typeof item !== 'number') continue;

        const id = String(item).trim();
        if (!id || ids.includes(id)) continue;

        ids.push(id);
        if (ids.length >= MAX_INSTANCES) break;
    }

    return ids;
}

function normaliseRole(value) {
    return typeof value === 'string' ? value.trim() : '';
}

/** Фильтры из произвольного объекта (тело запроса клиента или разбор адреса). */
function normaliseFilters(raw = {}) {
    const source = raw && typeof raw === 'object' ? raw : {};

    return {
        instanceIds: toIdList(
            source.instanceIds !== undefined ? source.instanceIds : source.instance_id
        ),
        role: normaliseRole(source.role),
    };
}

/** Пустые фильтры — «получать всё» (поведение до M7). */
function isEmpty(filters) {
    return !filters || (filters.instanceIds.length === 0 && !filters.role);
}

/** Фильтры из адреса подключения: `/ws/status?instanceId=...&role=...`. */
function parseQuery(url) {
    const query = String(url || '').split('?')[1] || '';
    const params = new URLSearchParams(query);

    const instanceIds = [];
    for (const key of ['instanceId', 'instance_id', 'instances']) {
        for (const value of params.getAll(key)) instanceIds.push(...toIdList(value));
    }

    return normaliseFilters({ instanceIds, role: params.get('role') });
}

/**
 * id экземпляра: поле сообщения, его `payload` или суффикс subject.
 *
 * Subject — последний резерв: движок всегда публикует
 * `mes.bpmn.v1.event.<instance>`, даже если в теле поля нет.
 */
function instanceIdOf(message, subject) {
    if (message && typeof message === 'object') {
        for (const key of INSTANCE_KEYS) {
            const value = message[key];
            if (typeof value === 'string' && value) return value;
        }

        const payload = message.payload;
        if (payload && typeof payload === 'object') {
            for (const key of INSTANCE_KEYS) {
                const value = payload[key];
                if (typeof value === 'string' && value) return value;
            }
        }
    }

    if (typeof subject === 'string' && subject.startsWith(SUBJECT_PREFIX)) {
        return subject.slice(SUBJECT_PREFIX.length) || null;
    }

    return null;
}

/** Роли, объявленные в сообщении. Пусто — роль события неизвестна. */
function rolesOf(message) {
    const roles = new Set();

    const take = (source) => {
        if (!source || typeof source !== 'object') return;

        for (const key of ROLE_KEYS) {
            const value = source[key];
            if (typeof value === 'string' && value.trim()) roles.add(value.trim());
            else if (Array.isArray(value)) {
                value.forEach((item) => {
                    if (typeof item === 'string' && item.trim()) roles.add(item.trim());
                });
            }
        }
    };

    take(message);
    if (message && typeof message === 'object') take(message.payload);

    return roles;
}

/**
 * Подходит ли событие фильтрам клиента.
 *
 * - без `instanceIds` экземпляр не проверяется;
 * - без `role` роль не проверяется;
 * - событие без объявленной роли проходит фильтр по роли (см. шапку модуля).
 */
function matches(filters, message, subject) {
    if (isEmpty(filters)) return true;

    if (filters.instanceIds.length > 0) {
        const instanceId = instanceIdOf(message, subject);
        if (!instanceId || !filters.instanceIds.includes(instanceId)) return false;
    }

    if (filters.role) {
        const roles = rolesOf(message);
        if (roles.size > 0 && !roles.has(filters.role)) return false;
    }

    return true;
}

/**
 * Разбирает управляющее сообщение клиента.
 *
 * Возвращает `{ filters, reply }`: новые фильтры и текст ответа (`null` —
 * сообщение не управляющее, фильтры не меняются).
 */
function applyControl(current, text) {
    let message;
    try {
        message = JSON.parse(text);
    } catch (error) {
        return { filters: current, reply: null };
    }

    if (!message || typeof message !== 'object') return { filters: current, reply: null };
    if (message.type !== 'subscribe' && message.type !== 'unsubscribe') {
        return { filters: current, reply: null };
    }

    const filters =
        message.type === 'unsubscribe' ? normaliseFilters({}) : normaliseFilters(message);

    return {
        filters,
        reply: JSON.stringify({ type: 'subscribed', ...filters }),
    };
}

module.exports = {
    MAX_INSTANCES,
    SUBJECT_PREFIX,
    applyControl,
    instanceIdOf,
    isEmpty,
    matches,
    normaliseFilters,
    parseQuery,
    rolesOf,
    toIdList,
};

