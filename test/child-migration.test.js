import assert from 'node:assert/strict';
import test from 'node:test';
import { rewriteStoredSession } from '../src/revision.js';
import { catalog, childFixture, turn } from './child-fixture.js';

const rewrite = persistence => rewriteStoredSession(persistence, {
  sessionId: 'parent', messageId: 'question-2', text: 'revised',
});

async function records(persistence, id) {
  const handle = await persistence.open(id, 'read');
  try { return (await handle.read()).events; } finally { await handle.close(); }
}

test('late V3 migration catalog facts preserve existing children while a later child is withdrawn', {
  skip: process.platform === 'win32',
}, async t => {
  const { persistence, bytes } = await childFixture(t, {
    parent: { createdAt: 10, events: [
      ...turn(1, 20),
      ...turn(2, 50, [[53, 'subagent/catalog', catalog('new-child', 53)]]),
      [58, 'subagent/catalog', catalog('migrated-child', 24, 'unknown')],
    ] },
    'migrated-child': { parentSession: 'parent', createdAt: 24, events: turn(1, 30) },
    'new-child': { parentSession: 'parent', createdAt: 53, events: turn(1, 60) },
  });
  const originalChild = await bytes('migrated-child');
  const result = await rewrite(persistence);
  assert.deepEqual(result.removedChildren, ['new-child']);
  assert.equal(await persistence.stat('new-child'), undefined);
  assert.deepEqual(await bytes('migrated-child'), originalChild);
  const revised = await records(persistence, 'parent');
  assert.ok(revised.every((event, seq) => event.seq === seq));
  assert.deepEqual(revised.filter(event => event.type === 'subagent/catalog').map(event => event.data),
    [catalog('migrated-child', 24, 'unknown')]);
});

test('registration and immutable birth time disagree before any files are changed', async t => {
  const { persistence, bytes } = await childFixture(t, {
    parent: { createdAt: 10, events: [...turn(1, 20), ...turn(2, 50), [60, 'subagent/catalog', catalog('child', 55)]] },
    child: { parentSession: 'parent', createdAt: 24, events: turn(1, 30) },
  });
  const before = await bytes('parent');
  const childBefore = await bytes('child');
  await assert.rejects(rewrite(persistence), { name: 'RevisionError', message: /创建时间与登记不匹配/ });
  assert.deepEqual(await bytes('parent'), before);
  assert.deepEqual(await bytes('child'), childBefore);
});

test('a late fact on the exact rollback timestamp is ambiguous and changes no files', async t => {
  const { persistence, bytes } = await childFixture(t, {
    parent: { createdAt: 10, events: [...turn(1, 20), ...turn(2, 50), [60, 'subagent/catalog', catalog('child', 50)]] },
    child: { parentSession: 'parent', createdAt: 50, events: turn(1, 60) },
  });
  const before = await bytes('parent');
  await assert.rejects(rewrite(persistence), { name: 'RevisionError', message: /创建时间与改写点重合/ });
  assert.deepEqual(await bytes('parent'), before);
  assert.ok(await persistence.stat('child'));
});
