import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { readFile, readdir } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { recoverStoredSessions, rewriteStoredSession } from '../src/revision.js';
import { catalog, childFixture, turn } from './child-fixture.js';

const moduleUrl = new URL('../src/revision.js', import.meta.url).href;
const journalPath = parentPath => join(dirname(parentPath), 'plugins', '@kabxx', 'dsh-message-revision', 'v0.1.0');
const input = { sessionId: 'parent', messageId: 'question-2', text: 'updated question' };

async function fixture(t) {
  const state = await childFixture(t, {
    parent: { createdAt: 10, events: [...turn(1, 20), ...turn(2, 60, [[63, 'subagent/catalog', catalog('child', 63)]])] },
    child: { createdAt: 63, parentSession: 'parent', events: turn(1, 72) },
  });
  const snapshots = await Promise.all(['parent', 'child'].map(id => state.persistence.stat(id)));
  state.persistence.list = async () => (await Promise.all(snapshots.map(({ header: { id } }) => state.persistence.stat(id)))).filter(Boolean);
  const known = snapshots.map(({ header, location }) => ({ id: header.id, path: location.path }));
  return { ...state, known, parentPath: known[0].path, childPath: known[1].path };
}

const worker = `
  import { readFile, stat } from 'node:fs/promises';
  import { spawnSync } from 'node:child_process';
  import { createRequire, syncBuiltinESMExports } from 'node:module';
  import { basename } from 'node:path';
  const fs = createRequire(import.meta.url)('node:fs/promises');
  const [entry, mode, entries, input] = process.argv.slice(1);
  const locations = new Map(JSON.parse(entries).map(x => [x.id, x.path]));
  const actualRename = fs.rename;
  const actualRm = fs.rm;
  fs.rename = async (from, to) => {
    if (basename(to) === 'rewrite' && mode === 'preparing') process.kill(process.pid, 'SIGKILL');
    if (basename(to) === 'committed' && mode === 'commit-failed') throw new Error('injected commit marker failure');
    if (to === locations.get('parent') && mode === 'before-publish') process.kill(process.pid, 'SIGKILL');
    await actualRename(from, to);
    if (to === locations.get('parent') && mode === 'after-publish') process.kill(process.pid, 'SIGKILL');
  };
  fs.rm = async (path, options) => {
    if (mode === 'cleanup-failed' && basename(path).startsWith('discard-')) throw new Error('injected cleanup failure');
    return actualRm(path, options);
  };
  syncBuiltinESMExports();
  const { rewriteStoredSession } = await import(entry);
  function load(path) {
    return readFile(path, 'utf8').then(raw => raw.trimEnd().split('\\n').map(JSON.parse));
  }
  const persistence = {
    async stat(id) {
      const path = locations.get(id);
      if (!path) return undefined;
      const info = await stat(path, { bigint: true }).catch(error => error.code === 'ENOENT' ? undefined : Promise.reject(error));
      if (!info) return undefined;
      return { header: (await load(path))[0], location: { kind: 'jsonl', path },
        revision: String(info.ino) + ':' + String(info.size) + ':' + String(info.mtimeNs) };
    },
    async open(id) {
      const path = locations.get(id);
      return { header: (await load(path))[0], inheritedEventCount: 0,
        read: async () => ({ events: (await load(path)).slice(1) }), close: async () => {} };
    },
  };
  let dispatched = false;
  const result = await rewriteStoredSession(persistence, JSON.parse(input), {
    verifyReload() {},
    afterCommit() { dispatched = true; },
  });
  console.log(JSON.stringify({ result, dispatched }));
`;

async function subprocess(mode, known) {
  const proc = spawn(process.execPath, ['--input-type=module', '-e', worker, moduleUrl, mode, JSON.stringify(known), JSON.stringify(input)],
    { stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '';
  let stderr = '';
  proc.stdout.setEncoding('utf8').on('data', data => { stdout += data; });
  proc.stderr.setEncoding('utf8').on('data', data => { stderr += data; });
  const [code, signal] = await once(proc, 'close');
  return { code, signal, stdout, stderr };
}

for (const mode of ['before-publish', 'after-publish']) {
  test(`a fresh DSH Host restores the child and parent after SIGKILL ${mode}`, async t => {
    const { persistence, known, parentPath, childPath, writers } = await fixture(t);
    const original = await readFile(parentPath);
    const originalChild = await readFile(childPath);
    const killed = await subprocess(mode, known);
    assert.equal(killed.signal, 'SIGKILL', killed.stderr);
    assert.equal(await persistence.stat('child'), undefined);
    const path = journalPath(parentPath);
    assert.deepEqual(await readFile(join(path, 'rewrite', 'original')), original);
    assert.deepEqual(await readFile(join(path, 'rewrite', 'child-0', 'session.v4.jsonl')), originalChild);
    const beforeRecovery = await readFile(parentPath);
    assert.equal(beforeRecovery.equals(original), mode === 'before-publish');
    const errors = [];
    const observed = [];
    await recoverStoredSessions(persistence, {
      canRollback(id) { observed.push(id); assert.ok(writers.has(id)); return true; },
      onError: (...err) => errors.push(err),
    });
    assert.deepEqual(observed, ['parent']);
    assert.deepEqual(errors, []);
    assert.deepEqual(await readFile(parentPath), original);
    assert.deepEqual(await readFile(childPath), originalChild);
    assert.deepEqual(await readdir(path), []);
    await recoverStoredSessions(persistence, { onError: (...err) => errors.push(err) });
    assert.deepEqual(errors, []);
  });
}

test('startup removes abandoned preparation only under the original writer lock', async t => {
  const { persistence, known, parentPath, childPath, writers } = await fixture(t);
  const original = await readFile(parentPath);
  const originalChild = await readFile(childPath);
  const reply = await subprocess('preparing', known);
  assert.equal(reply.signal, 'SIGKILL', reply.stderr);
  const path = journalPath(parentPath);
  const [preparing] = await readdir(path);
  assert.match(preparing, /^preparing-/u);
  const errors = [];
  const held = await persistence.open('parent', 'write');
  try {
    await recoverStoredSessions(persistence, { onError: (_, error) => errors.push(error) });
    assert.equal(errors[0]?.name, 'SessionAlreadyOwnedError');
    assert.deepEqual(await readdir(path), [preparing]);
  } finally {
    await held.close();
  }
  await recoverStoredSessions(persistence, { onError: (_, error) => errors.push(error) });
  assert.equal(errors.length, 1);
  assert.deepEqual(await readdir(path), []);
  assert.deepEqual(await readFile(parentPath), original);
  assert.deepEqual(await readFile(childPath), originalChild);
  assert.equal(writers.size, 0);
});

test('commit marker failure restores the original before any dispatch', async t => {
  const { known, parentPath, childPath } = await fixture(t);
  const original = await readFile(parentPath);
  const originalChild = await readFile(childPath);
  const reply = await subprocess('commit-failed', known);
  assert.equal(reply.code, 1, reply.stderr);
  assert.match(reply.stderr, /已恢复原记录/u);
  assert.deepEqual(await readFile(parentPath), original);
  assert.deepEqual(await readFile(childPath), originalChild);
  assert.deepEqual(await readdir(journalPath(parentPath)), []);
});

test('cleanup failure follows dispatch, reports a warning and remains safely retryable', async t => {
  const { persistence, known, parentPath } = await fixture(t);
  const reply = await subprocess('cleanup-failed', known);
  assert.equal(reply.code, 0, reply.stderr);
  assert.deepEqual(JSON.parse(reply.stdout), {
    result: { messageId: 'question-2', removedChildren: ['child'], cleanupWarning: true }, dispatched: true,
  });
  assert.match(await readFile(parentPath, 'utf8'), /updated question/);
  const path = journalPath(parentPath);
  const [retired] = await readdir(path);
  assert.match(retired, /^discard-/u);
  const errors = [];
  await recoverStoredSessions(persistence, { onError: (...err) => errors.push(err) });
  assert.deepEqual(errors, []);
  assert.deepEqual(await readdir(path), []);
  assert.equal(await persistence.stat('child'), undefined);
  assert.match(await readFile(parentPath, 'utf8'), /updated question/);
});

test('startup recovery preserves data when parent ownership or history conflicts', async t => {
  const { persistence, known, parentPath, childPath } = await fixture(t);
  const killed = await subprocess('after-publish', known);
  assert.equal(killed.signal, 'SIGKILL', killed.stderr);
  const candidate = await readFile(parentPath);
  const held = await persistence.open('parent', 'write');
  const errors = [];
  try {
    await recoverStoredSessions(persistence, { onError: (...err) => errors.push(err) });
    assert.equal(errors[0]?.[1].name, 'SessionAlreadyOwnedError');
    assert.deepEqual(await readFile(parentPath), candidate);
    assert.equal(await persistence.stat('child'), undefined);
    await assert.rejects(readFile(childPath), { code: 'ENOENT' });
  } finally {
    await held.close();
  }
  await recoverStoredSessions(persistence, { canRollback: () => false, onError: (...err) => errors.push(err) });
  assert.match(errors[1]?.[1].message, /会话已变化/);
  assert.equal(await persistence.stat('child'), undefined);
  assert.deepEqual(await readFile(parentPath), candidate);
  assert.equal((await readdir(journalPath(parentPath)))[0], 'rewrite');
});
