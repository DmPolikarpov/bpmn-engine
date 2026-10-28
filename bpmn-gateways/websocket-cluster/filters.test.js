// Тесты фильтров WS-подписки (M7). Запуск: npm test (node --test filters.test.js).
const test = require('node:test');
const assert = require('node:assert/strict');

const filters = require('./filters');

const subject = (instanceId) => `mes.bpmn.v1.event.${instanceId}`;
const token = (instanceId, elementId, extra = {}) => ({
    event: 'token_updated',
    instance_id: instanceId,
    element_id: elementId,
    state: 'ACTIVE',
    ...extra,
});

test('parseQuery читает экземпляры и роль из адреса', () => {
    const parsed = filters.parseQuery('/ws/status?instanceId=inst-1&instanceId=inst-2&role=OPERATOR');

    assert.deepEqual(parsed.instanceIds, ['inst-1', 'inst-2']);
    assert.equal(parsed.role, 'OPERATOR');

    assert.deepEqual(filters.parseQuery('/ws/status?instances=a,b').instanceIds, ['a', 'b']);
    assert.deepEqual(filters.parseQuery('/ws/status').instanceIds, []);
    assert.equal(filters.parseQuery('/ws/status').role, '');
    assert.ok(filters.isEmpty(filters.parseQuery('/ws/status')));
});

test('normaliseFilters убирает пустые, повторы и ограничивает список', () => {
    assert.deepEqual(filters.normaliseFilters({ instanceIds: [' a ', '', 'a', 7] }), {
        instanceIds: ['a', '7'],
        role: '',
    });
    assert.deepEqual(filters.normaliseFilters({ instance_id: 'inst-1', role: ' operator ' }), {
        instanceIds: ['inst-1'],
        role: 'operator',
    });

    const many = Array.from({ length: 150 }, (_, index) => `inst-${index}`);
    assert.equal(
        filters.normaliseFilters({ instanceIds: many }).instanceIds.length,
        filters.MAX_INSTANCES
    );
});

test('пустые фильтры пропускают всё — поведение до M7', () => {
    const all = filters.normaliseFilters({});

    assert.ok(filters.matches(all, token('inst-1', 'Task_A'), subject('inst-1')));
    assert.ok(filters.matches(all, null, subject('inst-9')));
});

test('фильтр по экземпляру отсекает чужие события', () => {
    const only = filters.parseQuery('/ws/status?instanceId=inst-1');

    assert.ok(filters.matches(only, token('inst-1', 'Task_A'), subject('inst-1')));
    assert.ok(!filters.matches(only, token('inst-2', 'Task_A'), subject('inst-2')));
    // Тело без `instance_id` — берём id из subject.
    assert.ok(filters.matches(only, { event: 'process_completed' }, subject('inst-1')));
    assert.ok(!filters.matches(only, { event: 'process_completed' }, subject('inst-2')));
    // Незнакомый subject без id — не отдаём: лучше пропустить, чем утечь.
    assert.ok(!filters.matches(only, { event: 'x' }, 'mes.bpmn.v1.event.'));
});

test('instanceIdOf читает payload как резерв', () => {
    assert.equal(
        filters.instanceIdOf({ event: 'token_updated', payload: { instance_id: 'inst-7' } }),
        'inst-7'
    );
    assert.equal(filters.instanceIdOf(token('inst-3', 'Task_A')), 'inst-3');
    assert.equal(filters.instanceIdOf({}, 'mes.bpmn.v1.event.inst-4'), 'inst-4');
    assert.equal(filters.instanceIdOf({}, 'other.subject'), null);
});

test('фильтр по роли отсекает объявленную чужую роль', () => {
    const operator = filters.parseQuery('/ws/status?role=OPERATOR');

    assert.ok(filters.matches(operator, token('inst-1', 'Task_A'), subject('inst-1')));
    assert.ok(
        filters.matches(
            operator,
            token('inst-1', 'Task_A', { payload: { roles: ['OPERATOR', 'SALES'] } }),
            subject('inst-1')
        )
    );
    assert.ok(
        !filters.matches(
            operator,
            token('inst-1', 'Task_A', { payload: { role: 'SALES' } }),
            subject('inst-1')
        )
    );
    // `actor` тоже считается объявленной ролью.
    assert.ok(!filters.matches(operator, { instance_id: 'inst-1', actor: 'SALES' }, subject('inst-1')));
});

test('экземпляр и роль работают вместе', () => {
    const parsed = filters.parseQuery('/ws/status?instanceId=inst-1&role=OPERATOR');

    assert.ok(
        filters.matches(parsed, token('inst-1', 'Task_A', { role: 'OPERATOR' }), subject('inst-1'))
    );
    assert.ok(!filters.matches(parsed, token('inst-2', 'Task_A', { role: 'OPERATOR' }), subject('inst-2')));
    assert.ok(!filters.matches(parsed, token('inst-1', 'Task_A', { role: 'SALES' }), subject('inst-1')));
});

test('applyControl меняет фильтры по сообщению клиента', () => {
    const empty = filters.normaliseFilters({});

    const subscribed = filters.applyControl(
        empty,
        JSON.stringify({ type: 'subscribe', instanceIds: ['inst-1'], role: 'operator' })
    );
    assert.deepEqual(subscribed.filters, { instanceIds: ['inst-1'], role: 'operator' });
    assert.deepEqual(JSON.parse(subscribed.reply), {
        type: 'subscribed',
        instanceIds: ['inst-1'],
        role: 'operator',
    });

    const reset = filters.applyControl(subscribed.filters, JSON.stringify({ type: 'unsubscribe' }));
    assert.ok(filters.isEmpty(reset.filters));
    assert.equal(JSON.parse(reset.reply).type, 'subscribed');

    // Не управляющее сообщение и битый JSON фильтры не трогают.
    assert.equal(filters.applyControl(subscribed.filters, JSON.stringify(token('inst-1', 'Task_A'))).reply, null);
    assert.deepEqual(
        filters.applyControl(subscribed.filters, 'не json').filters,
        subscribed.filters
    );
});
