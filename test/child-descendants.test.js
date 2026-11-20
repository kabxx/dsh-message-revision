import assert from 'node:assert/strict';
import test from 'node:test';
import { rewriteStoredSession } from '../src/revision.js';
import { catalog, childFixture, turn } from './child-fixture.js';

function family({ lateGrandchild = false, openGrandchild = false, missingGrandchild = false } = {}) {
  return {
    parent: { createdAt: 10, events: [
      ...turn(1, 20, [[23, 'subagent/catalog', catalog('child', 23)]]),
      ...turn(2, 60),
    ] },
    child: { parentSession: 'parent', createdAt: 23, events: turn(1, 30, [
      [33, 'subagent/catalog', catalog('grandchild', 33)],
    ]) },
    ...(missingGrandchild ? {} : {
      grandchild: { parentSession: 'child', createdAt: 33, events: [
        ...turn(1, 41),
        ...(openGrandchild ? [[51, 'turn/start', { turn: 2 }]] : []),
        ...(lateGrandchild ? turn(2, 65) : []),
      ] },
    }),
  };
}

const rewrite = (persistence, hooks) => rewriteStoredSession(persistence, {
  sessionId: 'parent', messageId: 'question-2', text: 'revised',
}, hooks);

test('post-cut writes by a retained grandchild block the rewrite without changing any rollout', async t => {
  const { persistence, bytes, writers } = await childFixture(t, family({ lateGrandchild: true }));
  const ids = ['parent', 'child', 'grandchild'];
  const before = await Promise.all(ids.map(bytes));
  await assert.rejects(rewrite(persistence), { name: 'RevisionError', message: /后代跨过改写点/ });
  assert.deepEqual(await Promise.all(ids.map(bytes)), before);
  assert.equal(writers.size, 0);
});

test('retained descendants stay locked through candidate publishing and release before reopen', async t => {
  const { persistence, bytes, writers } = await childFixture(t, family());
  const before = await Promise.all(['child', 'grandchild'].map(bytes));
  let published = false;
  assert.deepEqual((await rewrite(persistence, {
    beforeCommit(ids) {
      assert.deepEqual(ids, ['parent']);
      assert.deepEqual(writers, new Set(['parent', 'child', 'grandchild']));
    },
    verifyReload() {
      assert.equal(writers.size, 0);
      published = true;
    },
  })).removedChildren, []);
  assert.equal(published, true);
  assert.deepEqual(await Promise.all(['child', 'grandchild'].map(bytes)), before);
});

test('a separate writer for any retained descendant prevents publishing', async t => {
  const { persistence, bytes, writers } = await childFixture(t, family());
  const before = await bytes('parent');
  const lease = await persistence.open('grandchild', 'write');
  try {
    assert.deepEqual(writers, new Set(['grandchild']));
    await assert.rejects(rewrite(persistence), { name: 'RevisionError', message: /后代仍处于打开状态/ });
    assert.deepEqual(await bytes('parent'), before);
    assert.deepEqual(writers, new Set(['grandchild']));
  } finally {
    await lease.close();
  }
});

for (const [options, message] of [
  [{ openGrandchild: true }, /后代存在未关闭的回合/],
  [{ missingGrandchild: true }, /子会话记录不可用/],
]) {
  test(`a retained grandchild with ${Object.keys(options)[0]} blocks publishing`, async t => {
    const { persistence, bytes, writers } = await childFixture(t, family(options));
    const before = await bytes('parent');
    await assert.rejects(rewrite(persistence), { name: 'RevisionError', message });
    assert.deepEqual(await bytes('parent'), before);
    assert.equal(writers.size, 0);
  });
}
