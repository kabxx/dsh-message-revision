import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { runInNewContext } from 'node:vm';

const source = await readFile(new URL('../src/client.js', import.meta.url), 'utf8');
const viewSource = source.slice(source.indexOf('    function replaceMethod('), source.indexOf('    function hookFor('));
const { watchSessionViews } = runInNewContext(`${viewSource}\n({ watchSessionViews });`, { setTimeout, clearTimeout });

function fixture(ids) {
  const removals = [];
  const reuploads = [];
  const bindings = new Map(ids.map(id => {
    const input = { draft: `bottom draft for ${id}`, attachmentIds: [`file:${id}`, `image:${id}`] };
    const session = {
      removed: false,
      history: ['old prompt'],
      openState: 'open',
      resyncs: 0,
      projectionSeq: 20,
      notifier: { markDirty() {} },
      projections: { clear() { session.projectionSeq = -1; } },
      getSnapshot() { return { removed: session.removed, openState: session.openState, subagent: null }; },
      async resync() {
        session.resyncs++;
        await session.gate;
        session.history = ['new prompt'];
      },
    };
    const conversation = {
      input: { for(scope) {
        assert.equal(scope, ctx);
        return { state: { getSnapshot: () => input } };
      } },
      async sendSession(target) { return { kind: 'sent', id: target.sessionId }; },
      rebindDraftFiles(id, attachmentIds, uploadImages) {
        reuploads.push({ id, attachmentIds, uploadImages, beforeResync: session.resyncs === 0 });
      },
    };
    const ctx = { get: key => key === 'conversation' ? conversation : undefined };
    return [id, { sessionId: id, session, ctx, input }];
  }));
  const projectionStores = new Map([...bindings].map(([id, binding]) => [id, binding.session.projections]));
  const manager = {
    projectionStores,
    handleSessionRemoved(id) {
      removals.push(id);
      projectionStores.delete(id);
      const session = bindings.get(id)?.session;
      if (session) session.removed = true;
    },
    handleSessionAdded(summary) {
      if (summary.agentAvailable && !projectionStores.has(summary.sessionId)) projectionStores.set(summary.sessionId, {});
    },
  };
  const views = watchSessionViews({ manager, binding: id => bindings.get(id) });
  return { views, manager, bindings, removals, reuploads };
}

const settle = () => new Promise(resolve => setImmediate(resolve));
const live = sessionId => ({ sessionId, agentAvailable: true });

test('same-ID removal and live addition update both retained tabs without reviving a genuine deletion', async () => {
  const origin = fixture(['shared', 'gone']);
  const another = fixture(['shared', 'gone']);
  const held = origin.views.holdSessionView('shared');
  try {
    for (const tab of [origin, another]) {
      tab.manager.handleSessionRemoved('shared');
      tab.manager.handleSessionRemoved('gone');
    }
    assert.deepEqual(origin.removals, ['gone']);
    assert.deepEqual(another.removals, ['shared', 'gone']);
    another.manager.handleSessionAdded({ sessionId: 'shared', agentAvailable: false });
    await settle();
    assert.equal(another.bindings.get('shared').session.removed, true);
    const reconnecting = held.reconnect();
    origin.manager.handleSessionAdded(live('shared'));
    another.manager.handleSessionAdded(live('shared'));
    another.manager.handleSessionAdded(live('shared'));
    await reconnecting;
    await settle();
    for (const tab of [origin, another]) {
      const shared = tab.bindings.get('shared').session;
      assert.equal(shared.removed, false);
      assert.equal(shared.resyncs, 1);
      assert.deepEqual(shared.history, ['new prompt']);
      assert.equal(shared.projectionSeq, -1);
      assert.equal(tab.manager.projectionStores.get('shared'), shared.projections);
      assert.equal(tab.bindings.get('gone').session.removed, true);
      assert.deepEqual(tab.reuploads, [{
        id: 'shared', attachmentIds: ['file:shared', 'image:shared'], uploadImages: true, beforeResync: true,
      }]);
      assert.deepEqual(tab.bindings.get('shared').input, {
        draft: 'bottom draft for shared', attachmentIds: ['file:shared', 'image:shared'],
      });
    }
  } finally {
    held.release();
    origin.views.dispose();
    another.views.dispose();
  }
});

test('a later removal supersedes an in-flight resync', async () => {
  const { views, manager, bindings } = fixture(['shared']);
  try {
    const { session } = bindings.get('shared');
    const gate = Promise.withResolvers();
    session.gate = gate.promise;
    manager.handleSessionRemoved('shared');
    manager.handleSessionAdded(live('shared'));
    await settle();
    assert.equal(session.resyncs, 1);
    manager.handleSessionRemoved('shared');
    gate.resolve();
    await settle();
    assert.equal(session.removed, true);
  } finally {
    views.dispose();
  }
});

test('the initiating tab refreshes attachments even when the native event pair is missed', async () => {
  const { views, reuploads } = fixture(['shared']);
  const held = views.holdSessionView('shared');
  try {
    await held.reconnect();
    assert.deepEqual(reuploads, [{
      id: 'shared', attachmentIds: ['file:shared', 'image:shared'], uploadImages: true, beforeResync: false,
    }]);
  } finally {
    held.release();
    views.dispose();
  }
});

test('the initiating page holds the bottom send until its draft attachments rebind', async () => {
  const { views, manager, bindings, reuploads } = fixture(['shared', 'other']);
  const composer = bindings.get('shared').ctx.get('conversation');
  const held = views.holdSessionView('shared');
  try {
    manager.handleSessionRemoved('shared');
    assert.equal((await composer.sendSession({ sessionId: 'shared' })).kind, 'error');
    assert.equal((await composer.sendSession({ sessionId: 'other' })).kind, 'sent');
    manager.handleSessionAdded(live('shared'));
    assert.equal(reuploads[0].id, 'shared');
    assert.equal((await composer.sendSession({ sessionId: 'shared' })).kind, 'sent');
  } finally {
    held.release();
    views.dispose();
  }
});

test('an unmatched held removal is replayed through its original manager', () => {
  const { views, manager, bindings, removals } = fixture(['shared']);
  const held = views.holdSessionView('shared');
  try {
    manager.handleSessionRemoved('shared');
    assert.equal(bindings.get('shared').session.removed, false);
    held.release();
    assert.equal(bindings.get('shared').session.removed, true);
    assert.deepEqual(removals, ['shared']);
  } finally {
    views.dispose();
  }
});
