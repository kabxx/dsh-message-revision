import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { EventEmitter, once } from 'node:events';
import { appendFile, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { setTimeout } from 'node:timers/promises';
import { apply } from '../src/index.js';
import { readRewriteJournals } from '../src/rewrite-journal.js';
import { assertTerminated, skipWithoutNativeLocks, spawnTestEnv } from './platform-fixture.js';

const journalModule = new URL('../src/rewrite-journal.js', import.meta.url).href;
const interruptedCommit = `
  import { readFile, writeFile } from 'node:fs/promises';
  import { join } from 'node:path';
  const [module, input] = process.argv.slice(1);
  const { parent, artifact, message, legacyBudget } = JSON.parse(input);
  const { prepareRewriteSetJournal, publishRewriteSetArtifacts, markRewriteCommitted } = await import(module);
  const journal = await prepareRewriteSetJournal(parent, [], [artifact], { messageId: message.id });
  if (legacyBudget) {
    const path = join(journal.directory, 'intent.json');
    const intent = JSON.parse(await readFile(path, 'utf8'));
    intent.delivery.budget = { bytes: Number.MAX_SAFE_INTEGER, records: 2 };
    await writeFile(path, JSON.stringify(intent));
  }
  await publishRewriteSetArtifacts(journal);
  await markRewriteCommitted(journal);
  process.kill(process.pid, 'SIGKILL');
`;

async function interrupted(t, { legacyBudget = false } = {}) {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'dsh-delivery-')));
  const path = join(root, 'sessions', 'project', 'parent', 'session.v4.jsonl');
  await mkdir(dirname(path), { recursive: true });
  const header = { type: 'session', version: 4, id: 'parent', createdAt: 1, cwd: '/', isSeeded: false, agentPreset: 'standard' };
  const parent = { header, path };
  const snapshot = { header, location: { kind: 'jsonl', path }, revision: 'fake-revision' };
  const message = { id: 'revised', source: { kind: 'user' }, content: [{ type: 'text', text: 'revision' }] };
  const artifact = { ...parent, candidatePath: join(root, 'candidate'), retained: 1 };
  const queued = { seq: 0, type: 'agent/inbox/spliced', time: 2,
    data: { target: 'next-turn', start: 0, inserted: [message] } };
  await writeFile(path, `${JSON.stringify(header)}\n`);
  await writeFile(artifact.candidatePath, `${JSON.stringify(header)}\n${JSON.stringify(queued)}\n`);
  const child = spawn(process.execPath, ['--input-type=module', '-e', interruptedCommit, journalModule,
    JSON.stringify({ parent, artifact, message, legacyBudget })], { stdio: ['ignore', 'ignore', 'pipe'], env: spawnTestEnv() });
  let stderr = '';
  child.stderr.setEncoding('utf8').on('data', chunk => { stderr += chunk; });
  const [code, signal] = await once(child, 'close');
  assertTerminated(code, signal, stderr);
  assert.equal((await readRewriteJournals(snapshot))[0].intent.delivery.messageId, message.id);
  t.after(async () => {
    assert.equal(root, await realpath(root));
    await rm(root, { recursive: true });
  });
  return { path, snapshot, message, queued };
}

function host({ snapshot, message, queued, path }, {
  locked = false, canceled = false, failsAfterPublish = false, interruptedClaim = false, loggedClaim = false,
  unrelatedTurn = false, executionStarted = false,
} = {}) {
  const bus = new EventEmitter();
  const scopes = [];
  const agents = new Map();
  const sessions = new Map();
  let wakes = 0;
  let flushes = 0;
  let coldAttempts = 0;
  let turn = 0;
  const retried = [];
  const warnings = [];
  const copy = [queued];
  const pending = canceled ? [] : [message];
  const other = { ...message, id: 'other', content: [{ type: 'text', text: 'unrelated' }] };
  function record(type, data) {
    return copy.push({ seq: copy.length, type, time: 3, data });
  }
  function consume(outcome) {
    pending.shift();
    record('agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [], ...(outcome && { outcome }) });
  }
  if (canceled) consume('canceled');
  if (interruptedClaim || loggedClaim) {
    record('turn/start', { turn: ++turn });
    consume();
    if (loggedClaim) record('user/message', message);
    if (executionStarted) {
      record('step/start', { turn, step: 1 });
      record('assistant/attempt', { turn, step: 1, stream: [] });
      record('step/end', { turn, step: 1 });
    }
    record('turn/end', { turn, reason: { kind: 'interrupted' } });
  }
  if (unrelatedTurn) {
    pending.push(other);
    record('agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [other] });
    record('turn/start', { turn: ++turn });
    consume();
    record('step/start', { turn, step: 1 });
    record('user/message', other);
    record('assistant/message', { turn, step: 1, message: { role: 'assistant', content: [{ type: 'text', text: 'reply to B' }] } });
    record('step/end', { turn, step: 1 });
    record('turn/end', { turn, reason: { kind: 'completed' } });
  }
  function claimPending() {
    turn++;
    const accepted = pending[0];
    copy.push({ seq: copy.length, type: 'turn/start', time: 4, data: { turn } });
    consume();
    copy.push({ seq: copy.length, type: 'user/message', time: 4, data: accepted });
  }
  const session = {
    get seq() { return copy.length; },
    snapshotEvents(start, end) { return copy.slice(start, end); },
  };
  const agent = {
    id: snapshot.header.id, session, status: 'idle',
    inbox: { nextTurn: pending, nextStep: [] },
    wakeDriver() { wakes++; claimPending(); },
    followup(input) {
      retried.push(input);
      pending.push(input);
      copy.push({ ...queued, seq: copy.length, time: 4, data: { ...queued.data, inserted: [input] } });
      claimPending();
    },
  };
  function publish() {
    agents.set(agent.id, agent);
    sessions.set(agent.id, session);
    bus.emit('agent/created', { agent });
    return agent;
  }
  // The test initializes the in-memory Agent from the candidate and the
  // simulated native startup events, then flushes any new events together.
  let persisted = 1;
  const context = {
    agents, sessions: { get: id => sessions.get(id), async flush() {
      flushes++;
      await appendFile(path, copy.slice(persisted).map(event => `${JSON.stringify(event)}\n`).join(''));
      persisted = copy.length;
      return true;
    } },
    sessionPersistence: { list: async () => [snapshot], stat: async () => snapshot },
    sessionController: { async resolveAgent() {
      coldAttempts++;
      if (locked) return { error: new Error('Native writer is locked') };
      const opened = publish();
      if (!failsAfterPublish) return { agent: opened };
      agents.delete(opened.id);
      sessions.delete(opened.id);
      return { error: new Error('Native hook rejected the new Agent') };
    } },
    connection: { fetch: { register() { return () => {}; } } },
    effect(register) { scopes.push(register()); },
    on(event, action) { bus.on(event, action); return () => bus.off(event, action); },
    logger: { warn(...args) { warnings.push(args); } },
  };
  return { context, publish, retried, stats: () => ({ wakes, flushes, coldAttempts }), warnings,
    stop() { for (const scope of scopes.toReversed()) scope(); } };
}

async function until(predicate) {
  for (let attempt = 0; attempt < 200; attempt++) {
    if (await predicate()) return;
    await setTimeout(10);
  }
  assert.fail('Timed out waiting for revision startup recovery');
}

async function retired(snapshot) {
  return (await readRewriteJournals(snapshot)).length === 0;
}

test('a committed prompt survives process exit and wakes its existing queue once', { skip: skipWithoutNativeLocks }, async t => {
  const fixture = await interrupted(t);
  const [journal] = await readRewriteJournals(fixture.snapshot);
  assert.deepEqual(journal.intent.delivery, { messageId: fixture.message.id });
  const first = host(fixture);
  await apply(first.context);
  try {
    await until(() => retired(fixture.snapshot));
    assert.deepEqual(first.stats(), { wakes: 1, flushes: 1, coldAttempts: 1 });
    assert.deepEqual(first.warnings, []);
  } finally { first.stop(); }
  const second = host(fixture);
  await apply(second.context);
  try {
    await setTimeout(40);
    assert.deepEqual(second.stats(), { wakes: 0, flushes: 0, coldAttempts: 0 });
  } finally { second.stop(); }
});

test('a prior journal recovers even when its old memory estimate would reject reopening', { skip: skipWithoutNativeLocks }, async t => {
  const fixture = await interrupted(t, { legacyBudget: true });
  const [journal] = await readRewriteJournals(fixture.snapshot);
  assert.equal(journal.intent.delivery.budget.bytes, Number.MAX_SAFE_INTEGER);
  const recovered = host(fixture);
  await apply(recovered.context);
  try {
    await until(() => retired(fixture.snapshot));
    assert.deepEqual(recovered.stats(), { wakes: 1, flushes: 1, coldAttempts: 1 });
    assert.deepEqual(recovered.warnings, []);
  } finally { recovered.stop(); }
});

test('a locked committed revision waits for the native session to open', { skip: skipWithoutNativeLocks }, async t => {
  const fixture = await interrupted(t);
  const reopened = host(fixture, { locked: true });
  await apply(reopened.context);
  try {
    await until(() => reopened.warnings.length === 1);
    assert.equal(await retired(fixture.snapshot), false);
    assert.deepEqual(reopened.stats(), { wakes: 0, flushes: 0, coldAttempts: 1 });
    reopened.publish();
    await until(() => retired(fixture.snapshot));
    assert.deepEqual(reopened.stats(), { wakes: 1, flushes: 1, coldAttempts: 1 });
  } finally { reopened.stop(); }
});

test('a rejected Agent initialization does not schedule itself again', { skip: skipWithoutNativeLocks }, async t => {
  const fixture = await interrupted(t);
  const rejected = host(fixture, { failsAfterPublish: true });
  await apply(rejected.context);
  try {
    await until(() => rejected.warnings.length === 1);
    await setTimeout(50);
    assert.equal(await retired(fixture.snapshot), false);
    assert.deepEqual(rejected.stats(), { wakes: 0, flushes: 0, coldAttempts: 1 });
    rejected.publish();
    await until(() => retired(fixture.snapshot));
    assert.deepEqual(rejected.stats(), { wakes: 1, flushes: 1, coldAttempts: 1 });
  } finally { rejected.stop(); }
});

test('a canceled native inbox item is never reinserted during recovery', { skip: skipWithoutNativeLocks }, async t => {
  const fixture = await interrupted(t);
  const canceled = host(fixture, { canceled: true });
  canceled.publish();
  await apply(canceled.context);
  try {
    await until(() => retired(fixture.snapshot));
    assert.deepEqual(canceled.stats(), { wakes: 0, flushes: 1, coldAttempts: 0 });
  } finally { canceled.stop(); }
});

for (const variant of ['interruptedClaim', 'loggedClaim']) {
  test(`startup reconciles a ${variant} using the native input ID`, { skip: skipWithoutNativeLocks }, async t => {
    const fixture = await interrupted(t);
    const recovering = host(fixture, { [variant]: true });
    await apply(recovering.context);
    try {
      await until(() => retired(fixture.snapshot));
      assert.deepEqual(recovering.retried, variant === 'interruptedClaim' ? [fixture.message] : []);
      assert.deepEqual(recovering.stats(), { wakes: 0, flushes: 1, coldAttempts: 1 });
      const records = (await readFile(fixture.path, 'utf8')).trimEnd().split('\n').map(JSON.parse);
      assert.deepEqual(records.filter(event => event.type === 'user/message').map(event => event.data), [fixture.message]);
    } finally { recovering.stop(); }
  });
}

test('an unrelated completed turn does not block recovery or alter the newer message', { skip: skipWithoutNativeLocks }, async t => {
  const fixture = await interrupted(t);
  const recovering = host(fixture, { interruptedClaim: true, unrelatedTurn: true });
  await apply(recovering.context);
  try {
    await until(() => retired(fixture.snapshot));
    assert.deepEqual(recovering.retried, [fixture.message]);
    assert.deepEqual(recovering.stats(), { wakes: 0, flushes: 1, coldAttempts: 1 });
    const records = (await readFile(fixture.path, 'utf8')).trimEnd().split('\n').map(JSON.parse);
    const messages = records.filter(event => event.type === 'user/message').map(event => event.data);
    assert.deepEqual(messages.map(message => message.id), ['other', fixture.message.id]);
    assert.equal(messages[0].content[0].text, 'unrelated');
    assert.deepEqual(messages[1], fixture.message);
    assert.equal(records.find(event => event.type === 'assistant/message')?.data.message.content[0].text, 'reply to B');
  } finally { recovering.stop(); }
});

test('an interrupted turn with a native assistant attempt never resubmits its original input', { skip: skipWithoutNativeLocks }, async t => {
  const fixture = await interrupted(t);
  const recovering = host(fixture, { interruptedClaim: true, executionStarted: true, unrelatedTurn: true });
  await apply(recovering.context);
  try {
    await until(() => retired(fixture.snapshot));
    assert.deepEqual(recovering.retried, []);
    assert.deepEqual(recovering.stats(), { wakes: 0, flushes: 1, coldAttempts: 1 });
  } finally { recovering.stop(); }
});
