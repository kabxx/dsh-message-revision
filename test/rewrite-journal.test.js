import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdir, readFile, readdir, stat, truncate, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import test from 'node:test';
import manifest from '../package.json' with { type: 'json' };
import { acquireNativeLease } from '../src/native-lease.js';
import { recoverStoredSessions, rewriteStoredSession } from '../src/revision.js';
import { catalog, childFixture, turn } from './child-fixture.js';
import { assertTerminated, nativeLocksAvailable, skipWithoutNativeLocks, spawnTestEnv } from './platform-fixture.js';

const moduleUrl = new URL('../src/revision.js', import.meta.url).href;
const journalPath = parentPath => join(dirname(parentPath), 'plugins', ...manifest.name.split('/'), `v${manifest.version}`);
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
  import { createRequire, syncBuiltinESMExports } from 'node:module';
  import { basename, join } from 'node:path';
  const fs = createRequire(import.meta.url)('node:fs/promises');
  const [entry, mode, entries, input] = process.argv.slice(1);
  const locations = new Map(JSON.parse(entries).map(x => [x.id, x.path]));
  const stripNs = value => value.startsWith('\\\\\\\\?\\\\') ? value.slice(4) : value;
  const samePath = (a, b) => process.platform === 'win32' ? a.toLowerCase() === b.toLowerCase() : a === b;
  const actualRename = fs.rename;
  const actualRm = fs.rm;
  function beforeMove(to) {
    const clean = stripNs(to);
    if (basename(clean) === 'rewrite' && mode === 'preparing') process.kill(process.pid, 'SIGKILL');
    if (basename(clean) === 'committed' && mode === 'commit-failed') throw new Error('injected commit marker failure');
    if (samePath(clean, locations.get('parent')) && mode === 'before-publish') process.kill(process.pid, 'SIGKILL');
  }
  function afterMove(to) {
    const clean = stripNs(to);
    if (samePath(clean, locations.get('parent')) && mode === 'after-publish') process.kill(process.pid, 'SIGKILL');
  }
  fs.rename = async (from, to) => {
    beforeMove(to);
    await actualRename(from, to);
    afterMove(to);
  };
  if (process.platform === 'win32') {
    const { loadHostKoffi } = await import(new URL('./platform.js', entry).href);
    const koffi = loadHostKoffi();
    if (koffi) {
      const actualLoad = koffi.load.bind(koffi);
      koffi.load = name => {
        const lib = actualLoad(name);
        if (String(name).toLowerCase() !== 'kernel32.dll') return lib;
        const actualFunc = lib.func.bind(lib);
        return {
          ...lib,
          func(...args) {
            const fn = actualFunc(...args);
            if (!(String(args[0]).includes('MoveFileExW') || args[1] === 'MoveFileExW')) return fn;
            return (from, to, flags) => {
              beforeMove(to);
              const ok = fn(from, to, flags);
              if (ok !== 0) afterMove(to);
              return ok;
            };
          },
        };
      };
    }
  }
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
    { stdio: ['ignore', 'pipe', 'pipe'], env: spawnTestEnv() });
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
    assertTerminated(killed.code, killed.signal, killed.stderr);
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

test('startup removes abandoned preparation only under the original writer lock', {
  skip: skipWithoutNativeLocks,
}, async t => {
  const { persistence, known, parentPath, childPath, writers } = await fixture(t);
  const original = await readFile(parentPath);
  const originalChild = await readFile(childPath);
  const reply = await subprocess('preparing', known);
  assertTerminated(reply.code, reply.signal, reply.stderr);
  const path = journalPath(parentPath);
  const [preparing] = await readdir(path);
  assert.match(preparing, /^preparing-/u);
  const errors = [];
  const held = nativeLocksAvailable
    ? await acquireNativeLease(parentPath) : await persistence.open('parent', 'write');
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

test('startup removes an interrupted preparation beside a large parent without loading its log', { skip: skipWithoutNativeLocks }, async t => {
  const { persistence, parentPath, childPath } = await fixture(t);
  const original = await readFile(parentPath);
  const originalChild = await readFile(childPath);
  const size = 300 * 1024 * 1024;
  await truncate(parentPath, size);
  const directory = journalPath(parentPath);
  const preparation = join(directory, 'preparing-01234567-89ab-cdef-0123-456789abcdef');
  await mkdir(join(preparation, 'artifacts'), { recursive: true });
  await writeFile(join(preparation, 'artifacts', 'artifact-0.original'), original);
  persistence.open = async () => assert.fail('Startup must not open the full native log for preparing debris');
  const errors = [];
  await recoverStoredSessions(persistence, { onError: (_, error) => errors.push(error) });
  assert.deepEqual(errors, []);
  assert.deepEqual(await readdir(directory), []);
  assert.equal((await stat(parentPath)).size, size);
  assert.deepEqual(await readFile(childPath), originalChild);
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
  assertTerminated(killed.code, killed.signal, killed.stderr);
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
