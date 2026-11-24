import assert from 'node:assert/strict';
import test from 'node:test';
import { apply, reviseSession } from '../src/index.js';
import { planRevision } from '../src/revision.js';

const user = (id, text) => ({ id, role: 'user', source: { kind: 'user' }, content: [{ type: 'text', text }] });

function queuedHistory({ withSteering = false } = {}) {
  const events = [];
  const add = (type, data) => events.push({ type, seq: events.length, time: events.length + 1, data });
  const first = user('turn-1', 'first question');
  const queuedA = user('queued-a', 'queued prompt A');
  const queuedB = user('queued-b', 'queued prompt B');
  const steer = user('steer-1', 'steering note');

  add('agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [first] });
  add('turn/start', { turn: 1 });
  add('agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [], outcome: 'claimed' });
  add('user/message', first);
  add('agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [queuedA] });
  if (withSteering) {
    add('agent/inbox/spliced', { target: 'next-step', start: 0, inserted: [steer] });
  } else {
    add('agent/inbox/spliced', { target: 'next-turn', start: 1, inserted: [queuedB] });
  }
  add('turn/end', { turn: 1, reason: { kind: 'completed' } });

  add('turn/start', { turn: 2 });
  add('agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [], outcome: 'claimed' });
  add('user/message', queuedA);
  if (withSteering) {
    add('agent/inbox/spliced', { target: 'next-step', start: 0, removedCount: 1, inserted: [], outcome: 'claimed' });
    add('user/message', steer);
  }
  add('turn/end', { turn: 2, reason: { kind: 'completed' } });

  if (!withSteering) {
    add('turn/start', { turn: 3 });
    add('agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [], outcome: 'claimed' });
    add('user/message', queuedB);
    add('turn/end', { turn: 3, reason: { kind: 'completed' } });
  }
  return { events, first, queuedA, queuedB };
}

function* chunked(events) {
  for (let offset = 0; offset < events.length; offset += 3) yield* events.slice(offset, offset + 3);
}

test('planRevision validates iterable logs and rejects turns started with extra queued or steering input', () => {
  const queued = queuedHistory();
  assert.throws(() => planRevision(chunked(queued.events), queued.queuedA.id, 0), {
    name: 'RevisionError', message: '此消息没有可独立回退的对话轮次。',
  });
  const second = planRevision(chunked(queued.events), queued.queuedB.id, 0);
  assert.equal(second.message.id, queued.queuedB.id);
  assert.equal(queued.events[second.cut].type, 'turn/start');
  assert.equal(queued.events[second.cut].data.turn, 3);

  const steered = queuedHistory({ withSteering: true });
  assert.throws(() => planRevision(chunked(steered.events), steered.queuedA.id, 0), {
    name: 'RevisionError', message: '此消息没有可独立回退的对话轮次。',
  });
});

test('preview rejects non-independent queued turns and subagent sessions before opening the editor', async () => {
  const { events, queuedA, queuedB } = queuedHistory();
  const makeSession = (id, header) => ({
    id, header, inheritedEventCount: 0,
    get seq() { return events.length; },
    snapshotEvents(start = 0, end = events.length) { return events.slice(start, end); },
  });
  const mainSession = makeSession('main-session', { type: 'session', version: 4, id: 'main-session' });
  const childSession = makeSession('child-session', {
    type: 'session', version: 4, id: 'child-session', origin: 'subagent', parentSession: 'main-session',
  });
  const makeAgent = session => ({
    id: session.id, session, status: 'idle', phase: { kind: 'idle' },
    inbox: { hasPending: false, nextTurn: [], nextStep: [] },
    model: { current: () => ({ provider: 'fixture', model: 'fixture' }) },
  });
  const agents = new Map([['main-session', makeAgent(mainSession)], ['child-session', makeAgent(childSession)]]);
  const sessions = new Map([['main-session', mainSession], ['child-session', childSession]]);
  let handler;
  const disposers = [];
  const ctx = {
    agents,
    sessions: { get: id => sessions.get(id), async flush() { return true; } },
    sessionPersistence: {
      async stat(id) { return { header: sessions.get(id).header, revision: `rev-${id}` }; },
    },
    sessionController: {},
    connection: { fetch: { register(route) { handler = route.fetch; return () => {}; } } },
    get() { return undefined; },
    effect(fn) { disposers.push(fn()); },
    on() { return () => {}; },
    logger: { warn() {} },
  };
  await apply(ctx);
  try {
    const post = body => handler(new Request('http://localhost/api/message-revision', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
    }));
    const rejectedA = await post({ sessionId: 'main-session', messageId: queuedA.id });
    assert.equal(rejectedA.status, 409);
    assert.deepEqual(await rejectedA.json(), { ok: false, error: '此消息没有可独立回退的对话轮次。' });

    const allowedB = await post({ sessionId: 'main-session', messageId: queuedB.id });
    assert.equal(allowedB.status, 200);
    const previewB = await allowedB.json();
    assert.equal(previewB.ok, true);
    assert.equal(previewB.text, 'queued prompt B');
    assert.equal(previewB.revision, 'rev-main-session');

    const rejectedChild = await post({ sessionId: 'child-session', messageId: queuedB.id });
    assert.equal(rejectedChild.status, 409);
    assert.deepEqual(await rejectedChild.json(), { ok: false, error: '子智能体会话不支持直接编辑，请在主会话中操作。' });

    // Historical ordinary forks retain parentSession without origin: 'subagent' and stay editable.
    const forkEvents = [];
    const addFork = (type, data) => forkEvents.push({ type, seq: forkEvents.length, time: forkEvents.length + 1, data });
    const inheritedPrompt = user('turn-1', 'first question');
    const forkPrompt = user('fork-turn', 'follow-up in fork');
    addFork('agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [inheritedPrompt] });
    addFork('turn/start', { turn: 1 });
    addFork('agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [], outcome: 'claimed' });
    addFork('user/message', inheritedPrompt);
    addFork('turn/end', { turn: 1, reason: { kind: 'completed' } });
    addFork('session/end-seed', { inherited: true });
    addFork('agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [forkPrompt] });
    addFork('turn/start', { turn: 2 });
    addFork('agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [], outcome: 'claimed' });
    addFork('user/message', forkPrompt);
    addFork('turn/end', { turn: 2, reason: { kind: 'completed' } });
    const forkSession = {
      id: 'ordinary-fork',
      header: { type: 'session', version: 4, id: 'ordinary-fork', parentSession: 'main-session', isSeeded: true },
      inheritedEventCount: 5,
      get seq() { return forkEvents.length; },
      snapshotEvents(start = 0, end = forkEvents.length) { return forkEvents.slice(start, end); },
    };
    sessions.set('ordinary-fork', forkSession);
    agents.set('ordinary-fork', makeAgent(forkSession));
    const allowedFork = await post({ sessionId: 'ordinary-fork', messageId: forkPrompt.id });
    assert.equal(allowedFork.status, 200);
    const previewFork = await allowedFork.json();
    assert.equal(previewFork.ok, true);
    assert.equal(previewFork.text, 'follow-up in fork');
    const rejectedInherited = await post({ sessionId: 'ordinary-fork', messageId: inheritedPrompt.id });
    assert.equal(rejectedInherited.status, 409);
    assert.deepEqual(await rejectedInherited.json(), { ok: false, error: '此消息没有可独立回退的对话轮次。' });

    // Even an unchanged submission to a non-independent turn must reject before reporting unchanged.
    ctx.waterfall = async (_event, _payload, fallback) => fallback();
    await assert.rejects(reviseSession(ctx, {
      sessionId: 'main-session', messageId: queuedA.id, text: 'queued prompt A', expectedRevision: 'rev-main-session',
    }), { name: 'RevisionError', message: '此消息没有可独立回退的对话轮次。' });
  } finally {
    for (const dispose of disposers.toReversed()) dispose?.();
  }
});

test('planRevision rejects mid-turn steering and malformed queue targets without crashing', () => {
  const events = [];
  const add = (type, data) => events.push({ type, seq: events.length, time: events.length + 1, data });
  const initial = user('initial', 'initial prompt');
  const steer = user('mid-steer', 'steering note');
  add('agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [initial] });
  add('turn/start', { turn: 1 });
  add('agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [], outcome: 'claimed' });
  add('user/message', initial);
  add('agent/inbox/spliced', { target: 'next-step', start: 0, inserted: [steer] });
  add('agent/inbox/spliced', { target: 'next-step', start: 0, removedCount: 1, inserted: [], outcome: 'claimed' });
  add('user/message', steer);
  add('turn/end', { turn: 1, reason: { kind: 'completed' } });

  assert.equal(planRevision(chunked(events), initial.id, 0).message.id, initial.id);
  assert.throws(() => planRevision(chunked(events), steer.id, 0), {
    name: 'RevisionError', message: '此消息没有可独立回退的对话轮次。',
  });
  assert.throws(() => planRevision([
    { type: 'agent/inbox/spliced', seq: 0, time: 1, data: { target: 'toString', start: 0, inserted: [] } },
  ], initial.id, 0), { name: 'RevisionError', message: '无法安全复原会话的消息队列。' });
});
