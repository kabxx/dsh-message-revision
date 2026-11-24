import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { runInNewContext } from 'node:vm';

const source = await readFile(new URL('../src/client.js', import.meta.url), 'utf8');
const viewSource = source.slice(source.indexOf('    function replaceMethod('), source.indexOf('    function hookFor('));
const { watchSessionViews, connectRevisionViews, inheritedBoundary, isInheritedMessage, isEditableSessionMessage } = runInNewContext(
  `${viewSource}\n({ watchSessionViews, connectRevisionViews, inheritedBoundary, isInheritedMessage, isEditableSessionMessage });`, { setTimeout, clearTimeout },
);

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
      getSnapshot() { return { removed: session.removed, openState: session.openState, openError: session.openError, subagent: session.subagent ?? null }; },
      installWindow(entries) { session.history = entries; },
      handleRemoved() { session.removed = true; },
      async resync() {
        session.resyncs++;
        const observedError = session.nextError;
        await session.gate;
        session.openError = observedError;
        session.openState = session.openError ? 'error' : 'open';
        if (!session.openError) session.history = ['new prompt'];
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
  const views = watchSessionViews({ manager, scopes: bindings, binding: id => bindings.get(id) });
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

function eventFeed() {
  const streams = new Set();
  const remote = { $stream({ open }) {
    const controller = new AbortController();
    const stream = {
      async *[Symbol.asyncIterator]() {
        for await (const value of open(controller.signal)) yield { value, accept() {} };
      },
      dispose() { controller.abort(); },
    };
    return stream;
  } };
  async function* watch(cursor, signal) {
    const changes = [];
    let wake;
    const stream = value => { changes.push(value); wake?.(); };
    streams.add(stream);
    const abort = () => wake?.();
    signal.addEventListener('abort', abort);
    try {
      while (!signal.aborted) {
        if (changes.length) yield changes.shift();
        else await new Promise(resolve => { wake = resolve; });
      }
    } finally {
      signal.removeEventListener('abort', abort);
      streams.delete(stream);
    }
  }
  let cursor = 0;
  function publish(type, data) {
    for (const stream of streams) stream({ type, cursor: String(++cursor), ...data });
  }
  return { streams, remote, watch, publish };
}

test('the host reloads affected child views even when the initiating tab has closed', async () => {
  const { streams, remote, watch, publish } = eventFeed();
  const origin = fixture(['parent']);
  const other = fixture(['child', 'unaffected', 'withdrawn', 'reused', 'uncertain']);
  other.bindings.get('withdrawn').session.nextError = { code: 'subagent/not-found' };
  // A fresh native read can reopen a reused ID even if the old view was removed.
  other.bindings.get('reused').session.removed = true;
  other.bindings.get('uncertain').session.nextError = { code: 'gateway/internal' };
  const closeOrigin = connectRevisionViews(origin.views, remote, watch);
  const closeOther = connectRevisionViews(other.views, remote, watch);
  try {
    await closeOrigin();
    await settle();
    assert.equal(streams.size, 1);
    publish('revision', { rewrittenChildren: ['child', 'child'], withdrawnChildren: ['withdrawn', 'reused', 'uncertain'] });
    await settle();
    const { session } = other.bindings.get('child');
    assert.equal(session.resyncs, 1);
    assert.equal(session.projectionSeq, -1);
    assert.equal(session.removed, false);
    assert.deepEqual(session.history, ['new prompt']);
    const withdrawn = other.bindings.get('withdrawn').session;
    assert.equal(withdrawn.resyncs, 1);
    assert.equal(withdrawn.removed, true);
    assert.equal(withdrawn.history.length, 0);
    const reused = other.bindings.get('reused').session;
    assert.equal(reused.resyncs, 1);
    assert.equal(reused.removed, false);
    assert.deepEqual(reused.history, ['new prompt']);
    assert.equal(other.bindings.get('uncertain').session.removed, false);
    assert.equal(origin.bindings.get('parent').session.resyncs, 0);
    assert.equal(other.bindings.get('unaffected').session.resyncs, 0);
  } finally {
    await closeOther();
    origin.views.dispose();
    other.views.dispose();
  }
  await settle();
  assert.equal(streams.size, 0);
});

test('a host stream reset checks open child bindings and leaves other views alone', async () => {
  const { remote, watch, publish } = eventFeed();
  const { views, bindings } = fixture(['parent', 'child', 'gone', 'cold']);
  for (const id of ['child', 'gone', 'cold']) {
    bindings.get(id).session.subagent = { address: { kind: 'subagent', childSessionId: id } };
  }
  bindings.get('gone').session.nextError = { code: 'session/not-found' };
  bindings.get('cold').session.openState = 'cold';
  const close = connectRevisionViews(views, remote, watch);
  try {
    publish('reset', {});
    await settle();
    assert.equal(bindings.get('child').session.resyncs, 1);
    assert.equal(bindings.get('gone').session.removed, true);
    assert.equal(bindings.get('gone').session.history.length, 0);
    assert.equal(bindings.get('parent').session.resyncs, 0);
    assert.equal(bindings.get('cold').session.resyncs, 0);
  } finally {
    await close();
    views.dispose();
  }
});

test('a replacement addition invalidates a pending not-found response for a withdrawn child', async () => {
  const { views, manager, bindings } = fixture(['child']);
  try {
    const { session } = bindings.get('child');
    const gate = Promise.withResolvers();
    session.nextError = { code: 'subagent/not-found' };
    session.gate = gate.promise;
    const reload = views.reloadChildren([], ['child']);
    await settle();
    assert.equal(session.resyncs, 1);
    session.nextError = undefined;
    manager.handleSessionAdded(live('child'));
    gate.resolve();
    await reload;
    assert.equal(session.resyncs, 2);
    assert.equal(session.removed, false);
    assert.deepEqual(session.history, ['new prompt']);
  } finally {
    views.dispose();
  }
});

test('a committed notice refreshes the parent catalog in a list-only tab', async () => {
  const { remote, watch, publish } = eventFeed();
  const { views, manager, bindings } = fixture([]);
  const read = [];
  let catalog = [{ childId: 'withdrawn' }];
  let unaffected = [{ childId: 'other-child' }];
  let aborted = false;
  manager.projectionStores.set('parent', { values: () => ({ subagentCatalog: catalog }), clear() { catalog = []; } });
  manager.projectionStores.set('other-parent', { values: () => ({ subagentCatalog: unaffected }), clear() { unaffected = []; } });
  manager.projectionInflight = new Map([['parent', { controller: { abort() { aborted = true; } } }]]);
  manager.projectionLoads = new Map([['parent', { state: 'ready' }]]);
  manager.refreshProjections = async id => {
    assert.equal(bindings.has(id), false);
    assert.equal(manager.projectionLoads.has(id), false);
    read.push(id);
    if (id === 'parent') catalog = [{ childId: 'retained' }];
  };
  const close = connectRevisionViews(views, remote, watch);
  try {
    publish('revision', { rewrittenChildren: [], withdrawnChildren: ['withdrawn'], affectedParentIds: ['parent', 'parent'] });
    await settle();
    assert.equal(aborted, true);
    assert.deepEqual(read, ['parent']);
    assert.deepEqual(catalog, [{ childId: 'retained' }]);
    assert.deepEqual(unaffected, [{ childId: 'other-child' }]);
    publish('reset', {});
    await settle();
    assert.deepEqual(read, ['parent', 'parent', 'other-parent']);
  } finally {
    await close();
    views.dispose();
  }
});

test('a list response captured before revision cannot restore a withdrawn child', async () => {
  const { views, manager } = fixture([]);
  const oldList = Promise.withResolvers();
  const oldCatalog = [{ id: 'withdrawn' }];
  const newCatalog = [{ id: 'retained' }];
  let catalog = oldCatalog;
  const sequence = [];
  manager.projectionStores.set('parent', { clear() { catalog = []; } });
  manager.listInflight = oldList.promise.then(() => {
    catalog = oldCatalog;
    sequence.push('old list');
    manager.listInflight = null;
  });
  manager.refreshProjections = async id => {
    assert.equal(id, 'parent');
    assert.deepEqual(catalog, []);
    catalog = newCatalog;
    sequence.push('new projection');
  };
  try {
    const refreshing = views.reloadChildren([], ['withdrawn'], ['parent']);
    await settle();
    assert.deepEqual(sequence, []);
    oldList.resolve();
    await refreshing;
    assert.deepEqual(sequence, ['old list', 'new projection']);
    assert.deepEqual(catalog, newCatalog);
  } finally {
    oldList.resolve();
    views.dispose();
  }
});

test('a verified native resync supersedes a held removal even without an addition', async () => {
  const { views, manager, bindings, removals } = fixture(['shared']);
  const held = views.holdSessionView('shared');
  try {
    manager.handleSessionRemoved('shared');
    await held.reconnect();
    held.release();
    assert.equal(bindings.get('shared').session.removed, false);
    assert.deepEqual(removals, []);
    manager.handleSessionRemoved('shared');
    assert.equal(bindings.get('shared').session.removed, true);
    assert.deepEqual(removals, ['shared']);
  } finally {
    held.release();
    views.dispose();
  }
});

test('a held removal after a verified reopen is still replayed', async () => {
  const { views, manager, bindings, removals } = fixture(['shared']);
  const held = views.holdSessionView('shared');
  try {
    manager.handleSessionRemoved('shared');
    manager.handleSessionAdded(live('shared'));
    await held.reconnect();
    manager.handleSessionRemoved('shared');
    held.release();
    assert.equal(bindings.get('shared').session.removed, true);
    assert.deepEqual(removals, ['shared']);
  } finally {
    held.release();
    views.dispose();
  }
});

test('the latest native inherited marker separates fork history from editable messages', () => {
  const inherited = { kind: 'user' };
  const own = { kind: 'user' };
  const older = { kind: 'user' };
  let rows = [
    { kind: 'user', data: { source: inherited, seq: 8 } },
    { kind: 'user', data: { source: own, seq: 17 } },
  ];
  const nodes = { values: () => rows };
  const marker = (seq, inherited) => ({ event: { type: 'session/end-seed', seq, data: { inherited } } });
  let window = { revision: 1, entries: [marker(9, false)], change: { kind: 'replace' } };
  const events = { getSnapshot: () => window };
  assert.equal(inheritedBoundary(events), -1);
  assert.equal(isInheritedMessage(nodes, inherited, inheritedBoundary(events)), false);
  window = { revision: 2, change: { kind: 'prepend', entries: [marker(4, true)] } };
  assert.equal(inheritedBoundary(events), 4);
  window = { revision: 3, change: { kind: 'append', entries: [marker(15, true)] } };
  assert.equal(inheritedBoundary(events), 15);
  assert.equal(isInheritedMessage(nodes, inherited, inheritedBoundary(events)), true);
  assert.equal(isInheritedMessage(nodes, own, inheritedBoundary(events)), false);
  rows = [{ kind: 'user', data: { source: older, seq: 2 } }, ...rows];
  assert.equal(isInheritedMessage(nodes, older, inheritedBoundary(events)), true);
  window = { revision: 4, change: { kind: 'settle-assistant' } };
  assert.equal(inheritedBoundary(events), 15);
  window = { revision: 5, entries: [marker(18, false)], change: { kind: 'replace' } };
  assert.equal(isInheritedMessage(nodes, inherited, inheritedBoundary(events)), false);
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

test('the edit action is hidden on subagent and LiveShare guest sessions even when prompt access is granted', () => {
  assert.equal(isEditableSessionMessage({ allowed: true, subagent: false, inherited: false }), true);
  assert.equal(isEditableSessionMessage({ allowed: true, subagent: true, inherited: false }), false);
  assert.equal(isEditableSessionMessage({ allowed: false, subagent: false, inherited: false }), false);
  assert.equal(isEditableSessionMessage({ allowed: true, subagent: false, inherited: true }), false);

  let loaded;
  let onSubscribe;
  const react = {
    Fragment: Symbol('Fragment'),
    createElement(type, props, ...children) { return { type, props, children }; },
    useCallback: fn => fn,
    useEffect() {},
    useLayoutEffect() {},
    useMemo: fn => fn(),
    useRef: current => ({ current }),
    useState: initial => [typeof initial === 'function' ? initial() : initial, () => {}],
    useSyncExternalStore: (subscribe, snapshot) => {
      onSubscribe?.(subscribe);
      return snapshot();
    },
  };
  runInNewContext(source, {
    window: { __ModuleLoader__: { load(entry) { loaded = entry; } } },
    AbortController, Map, Set, WeakMap, Symbol, Object, Array, Promise, Error,
  });
  const plugin = loaded.factory(id => {
    if (id === 'react') return react;
    throw new Error(`Unexpected module request: ${id}`);
  });

  function mount(ownerAllowed, { asyncSessions = false } = {}) {
    const registered = new Map();
    const disposals = [];
    let locales;
    let sessionsInjected = false;
    let attachSessions;
    const scope = {
      get(key) { return scope[key]; },
      effect(fn) {
        const dispose = fn();
        return () => { disposals.push('effect'); dispose?.(); };
      },
      inject(services, callback) {
        if (services.includes('remoteAccess')) {
          callback(scope);
          return { dispose() { disposals.push('remoteAccess'); } };
        }
        if (services.includes('sessions')) {
          sessionsInjected = true;
          const run = () => callback(scope);
          if (asyncSessions) attachSessions = run;
          else run();
          return { dispose() { disposals.push('sessions'); } };
        }
        return { dispose() {} };
      },
      remoteAccess: {
        whenAccess(action, callback) {
          assert.equal(action, 'settings/describe');
          if (!ownerAllowed) return () => {};
          const cleanups = [...callback()];
          for (const dispose of cleanups) assert.equal(typeof dispose, 'function');
          return () => { for (const dispose of cleanups.toReversed()) dispose(); };
        },
      },
      locale: { register(_name, strings) { locales = strings; return () => { disposals.push('locale'); }; } },
      slots: {
        inject(name, setup) {
          const dispose = setup();
          return () => { disposals.push(`slot:${name}`); dispose?.(); };
        },
        register(spec, component) {
          registered.set(spec.name, { spec, component });
          return () => { registered.delete(spec.name); disposals.push(`register:${spec.name}`); };
        },
        registerFactory() { return () => { disposals.push('factory'); }; },
      },
    };
    let revokeAccess;
    const originalWhenAccess = scope.remoteAccess.whenAccess;
    scope.remoteAccess.whenAccess = (action, callback) => {
      revokeAccess = originalWhenAccess(action, callback);
      return revokeAccess;
    };
    plugin.apply(scope);
    return { registered, locales, sessionsInjected, disposals, revokeAccess, attachSessions, scope };
  }

  const guestMount = mount(false);
  assert.equal(guestMount.registered.size, 0);
  assert.equal(guestMount.sessionsInjected, false);

  const { registered, locales, sessionsInjected, disposals, revokeAccess } = mount(true);
  assert.equal(sessionsInjected, true);
  const { component: RevisionAction } = registered.get('conversation.chat.user.before');
  const matched = { kind: 'user' };
  const render = (snapshot, {
    byId = {},
    useRemoteAccess = () => true,
    useAccess = () => true,
  } = {}) => RevisionAction({
    t: key => key, matched, sessionId: snapshot.sessionId,
    useChat: select => select({ nodes: new Map() }),
    useSession: select => select(snapshot),
    useSessions: select => select({ byId }),
    useAccess,
    useRemoteAccess,
    renderFactorySlot: () => null,
  });

  const ordinary = render({ sessionId: 'main', subagent: null, removed: false, openState: 'open', running: false, pendingSubmissions: [] });
  assert.ok(ordinary !== null);
  assert.equal(ordinary.children[0]?.props?.className, 'dsh-revision-pencil');

  const liveShareGuest = render(
    { sessionId: 'main', subagent: null, removed: false, openState: 'open', running: false, pendingSubmissions: [] },
    { useRemoteAccess: () => false, useAccess: action => action === 'session/prompt' },
  );
  assert.equal(liveShareGuest, null);

  const promptWithoutQueue = render(
    { sessionId: 'main', subagent: null, removed: false, openState: 'open', running: false, pendingSubmissions: [] },
    { useRemoteAccess: () => true, useAccess: action => action === 'session/prompt' },
  );
  assert.equal(promptWithoutQueue, null);

  const subagent = render({
    sessionId: 'child',
    subagent: { address: { parentSessionId: 'main', childSessionId: 'child', mode: 'continuable' } },
    removed: false, openState: 'open', running: false, pendingSubmissions: [],
  });
  assert.equal(subagent, null);

  const orphanSubagent = render(
    { sessionId: 'orphan-child', subagent: null, removed: false, openState: 'open', running: false, pendingSubmissions: [] },
    { byId: { 'orphan-child': { origin: 'subagent' } } },
  );
  assert.equal(orphanSubagent, null);

  revokeAccess();
  assert.equal(registered.size, 0);
  assert.ok(disposals.includes('sessions'));
  assert.ok(disposals.includes('locale'));
  assert.ok(disposals.includes('register:conversation.chat.user.before'));

  // Async sessions injection notifies subscribed RevisionAction instances so inherited boundaries update.
  const subscriptions = [];
  onSubscribe = subscribe => subscriptions.push(subscribe);
  try {
    const delayed = mount(true, { asyncSessions: true });
    const inheritedNode = { kind: 'user', data: { source: matched, seq: 1 } };
    const nodeRows = [inheritedNode];
    const nodesStore = { values: () => nodeRows };
    const { component: DelayedAction } = delayed.registered.get('conversation.chat.user.before');
    let notified = 0;
    const renderDelayed = () => DelayedAction({
      t: key => key, matched, sessionId: 'fork',
      useChat: select => select({ nodes: nodesStore }),
      useSession: select => select({ sessionId: 'fork', subagent: null, removed: false, openState: 'open', running: false, pendingSubmissions: [] }),
      useSessions: select => select({ byId: {} }),
      useAccess: () => true,
      useRemoteAccess: () => true,
      renderFactorySlot: () => null,
    });
    assert.ok(renderDelayed() !== null);
    const unsubs = subscriptions.map(sub => sub(() => { notified++; }));
    const forkEventSource = {
      getSnapshot: () => ({
        revision: 1,
        entries: [{ event: { seq: 3, type: 'session/end-seed', data: { inherited: true } } }],
        change: { kind: 'replace' },
      }),
      subscribe: () => () => {},
    };
    delayed.scope.sessions = {
      manager: {
        handleSessionRemoved() {},
        handleSessionAdded() {},
      },
      binding: id => id === 'fork' ? { eventSource: forkEventSource } : undefined,
    };
    delayed.attachSessions();
    assert.ok(notified >= 1);
    assert.equal(renderDelayed(), null);
    for (const unsub of unsubs) unsub?.();
  } finally {
    onSubscribe = undefined;
  }

  assert.equal(locales.zh.nonIndependentTurn, '此消息没有可独立回退的对话轮次。');
  assert.equal(locales.zh.subagentUnsupported, '子智能体会话不支持直接编辑，请在主会话中操作。');
  assert.equal(locales.zh.queuePending, '会话仍有排队消息，请等队列清空后再试。');
  assert.equal(locales.zh.inheritedDuplicate, '无法替换继承历史中的同一条消息。');
  assert.equal(typeof locales.en.nonIndependentTurn, 'string');
  assert.equal(typeof locales.en.subagentUnsupported, 'string');
  assert.equal(typeof locales.en.queuePending, 'string');
  assert.equal(typeof locales.en.inheritedDuplicate, 'string');
});
