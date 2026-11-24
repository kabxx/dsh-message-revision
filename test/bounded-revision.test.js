import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, mkdir, readFile, readdir, realpath, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, relative } from 'node:path';
import { constants, zstdCompressSync } from 'node:zlib';
import { prepareBoundedRevision, revalidateWork } from '../src/bounded-revision.js';
import { commitBoundedRevision } from '../src/bounded-commit.js';
import { acquireWin32Semaphore, dshSessionLockName, releaseWin32Semaphore, rolloutStorageRoot } from '../src/platform.js';
import { readRewriteJournals, retireRewriteJournal } from '../src/rewrite-journal.js';
import { catalog, childFixture, timeline, turn } from './child-fixture.js';
import { skipWithoutNativeLocks } from './platform-fixture.js';

const options = { skip: skipWithoutNativeLocks };
const amend = async () => ({ message: { id: 'question-2', content: [{ type: 'text', text: 'revised' }] } });

async function prepare(persistence, snapshot) {
  return prepareBoundedRevision(persistence, snapshot, 'question-2', amend);
}

test('production path rewinds late grandchild history without opening native event handles', options, async t => {
  const family = {
    parent: { createdAt: 10, events: [
      ...turn(1, 20, [[23, 'subagent/catalog', catalog('child', 23)]]),
      ...turn(2, 60, [[63, 'subagent/catalog', catalog('withdrawn', 63)]]),
    ] },
    child: { parentSession: 'parent', createdAt: 23, events: turn(1, 30, [[33, 'subagent/catalog', catalog('grandchild', 33)]]) },
    grandchild: { parentSession: 'child', createdAt: 33, events: [...turn(1, 41), ...turn(2, 65)] },
    withdrawn: { parentSession: 'parent', createdAt: 63, events: [] },
  };
  const { persistence, bytes } = await childFixture(t, family);
  persistence.list = () => Promise.all(Object.keys(family).map(id => persistence.stat(id)));
  persistence.open = () => assert.fail('A bounded rewrite must not acquire a heavyweight native writer');
  const before = await bytes('child');
  const snapshot = await persistence.stat('parent');
  const withdrawn = await persistence.stat('withdrawn');
  // The withdrawn body is deliberately invalid. It must never be decoded.
  await writeFile(withdrawn.location.path, `${JSON.stringify(withdrawn.header)}\nnot valid JSON\n`);
  const work = await prepare(persistence, snapshot);
  try {
    assert.deepEqual(work.artifacts.map(file => file.header.id), ['parent', 'grandchild']);
    assert.deepEqual(work.withdrawn.map(entry => entry.header.id), ['withdrawn']);
    assert.deepEqual(work.affectedParentIds, ['parent']);
    const hooks = {
      async verifyReload() {
        assert.equal(await persistence.stat('withdrawn'), undefined);
        const grandchild = (await bytes('grandchild')).toString('utf8').trimEnd().split('\n').slice(1).map(JSON.parse);
        assert.deepEqual(grandchild, timeline(turn(1, 41)));
      },
      afterCommit() {},
    };
    await commitBoundedRevision(persistence, work, hooks);
    assert.deepEqual(await bytes('child'), before);
    assert.match((await bytes('parent')).toString(), /"text":"revised"/u);
  } finally {
    await work.dispose();
  }
});

test('Windows history edits through a storage junction share DSH locks and recover cleanly', {
  skip: process.platform !== 'win32' || skipWithoutNativeLocks,
}, async t => {
  const { persistence, bytes } = await childFixture(t, {
    parent: { createdAt: 10, events: [
      ...turn(1, 20), ...turn(2, 60, [[63, 'subagent/catalog', catalog('child', 63)]]),
    ] },
    child: { parentSession: 'parent', createdAt: 63, events: [] },
  });
  const directStat = persistence.stat;
  const directPath = (await directStat('parent')).location.path;
  const root = rolloutStorageRoot(directPath);
  const junction = join(dirname(root), 'linked-sessions');
  await symlink(root, junction, 'junction');
  persistence.stat = async id => {
    const snapshot = await directStat(id);
    if (!snapshot) return undefined;
    const path = join(junction, relative(root, snapshot.location.path));
    return { ...snapshot, location: { kind: 'jsonl', path } };
  };
  persistence.list = async () => (await Promise.all(['parent', 'child'].map(persistence.stat))).filter(Boolean);
  const originalParent = await bytes('parent');
  const originalChild = await bytes('child');
  const snapshot = await persistence.stat('parent');
  const work = await prepare(persistence, snapshot);
  const hooks = {
    async verifyReload() {
      assert.equal(await persistence.stat('child'), undefined);
      assert.match((await bytes('parent')).toString(), /"text":"revised"/u);
    },
    afterCommit() {},
  };
  try {
    assert.equal(work.parent.path, directPath);
    assert.equal(work.parent.dshPath, snapshot.location.path);
    for (const path of [snapshot.location.path, directPath]) {
      const lock = join(dirname(path), 'session.lock');
      const semaphore = acquireWin32Semaphore(dshSessionLockName(lock), lock);
      assert.equal(semaphore.busy, false);
      try {
        await assert.rejects(commitBoundedRevision(persistence, work, hooks), { name: 'SessionAlreadyOwnedError' });
      } finally {
        releaseWin32Semaphore(semaphore.handle);
      }
    }
    await assert.rejects(commitBoundedRevision(persistence, work, {
      ...hooks,
      async verifyReload() {
        await hooks.verifyReload();
        throw new Error('simulated reopen failure');
      },
    }), /已恢复原记录/u);
    assert.deepEqual(await bytes('parent'), originalParent);
    assert.deepEqual(await bytes('child'), originalChild);
  } finally {
    await work.dispose();
  }
  const retry = await prepare(persistence, await persistence.stat('parent'));
  try {
    await commitBoundedRevision(persistence, retry, { ...hooks, messageId: 'question-2' });
    const [journal] = await readRewriteJournals(await persistence.stat('parent'));
    assert.equal(journal.committed, true);
    assert.equal(journal.parentPath, directPath);
    assert.equal(journal.dshPath, snapshot.location.path);
    await retireRewriteJournal(journal);
    assert.deepEqual(await readRewriteJournals(await persistence.stat('parent')), []);
  } finally {
    await retry.dispose();
  }
});

for (const hasChildren of [false, true]) {
  test(`clock reversal ${hasChildren ? 'rejects a child cutoff' : 'allows a parent-only sequence cutoff'}`, options, async t => {
    const family = { parent: { createdAt: 10, events: [
      ...turn(1, 92, hasChildren ? [[95, 'subagent/catalog', catalog('child', 23)]] : []),
      ...turn(2, 71),
    ] } };
    if (hasChildren) family.child = { parentSession: 'parent', createdAt: 23, events: turn(1, 81) };
    const { persistence, bytes } = await childFixture(t, family);
    persistence.list = () => Promise.all(Object.keys(family).map(id => persistence.stat(id)));
    const snapshot = await persistence.stat('parent');
    const before = await bytes('parent');
    if (hasChildren) {
      const childBefore = await bytes('child');
      await assert.rejects(prepare(persistence, snapshot), { name: 'RevisionError', message: /主会话日志的时间顺序/u });
      assert.deepEqual(await bytes('child'), childBefore);
    } else {
      const work = await prepare(persistence, snapshot);
      await work.dispose();
    }
    assert.deepEqual(await bytes('parent'), before);
  });
}

test('withdrawal invalidates each still-visible direct parent catalog', options, async t => {
  const { persistence } = await childFixture(t, {
    parent: { createdAt: 10, events: [
      ...turn(1, 20, [[23, 'subagent/catalog', catalog('child', 23)]]),
      ...turn(2, 60, [[63, 'subagent/catalog', catalog('withdrawn', 63)]]),
    ] },
    child: { parentSession: 'parent', createdAt: 23, events: [
      ...turn(1, 31), [65, 'subagent/catalog', catalog('withdrawn-grandchild', 65)],
    ] },
    withdrawn: { parentSession: 'parent', createdAt: 63, events: [] },
    'withdrawn-grandchild': { parentSession: 'child', createdAt: 65, events: [] },
    'withdrawn-descendant': { parentSession: 'withdrawn-grandchild', createdAt: 66, events: [] },
  });
  const ids = ['parent', 'child', 'withdrawn', 'withdrawn-grandchild', 'withdrawn-descendant'];
  persistence.list = () => Promise.all(ids.map(id => persistence.stat(id)));
  const work = await prepare(persistence, await persistence.stat('parent'));
  try {
    assert.deepEqual(work.affectedParentIds, ['parent', 'child']);
    assert.deepEqual(work.artifacts.map(entry => entry.header.id), ['parent', 'child']);
    assert.deepEqual(work.withdrawn.map(entry => entry.header.id), ids.slice(2));
  } finally {
    await work.dispose();
  }
});

for (const scenario of ['same-millisecond input', 'first child missing its catalog entry']) {
  test(`preparation rejects ${scenario} before changing either log`, options, async t => {
    const sameMillisecond = scenario === 'same-millisecond input';
    const family = {
      parent: { createdAt: 10, events: [
        ...turn(1, 20, sameMillisecond ? [[23, 'subagent/catalog', catalog('child', 23)]] : []),
        ...turn(2, 60),
      ] },
      child: { parentSession: 'parent', createdAt: 23, events: [
        ...turn(1, 30),
        ...(sameMillisecond ? [[60, 'agent/inbox/spliced', {
          target: 'next-turn', start: 0,
          inserted: [{ id: 'pending', role: 'user', source: { kind: 'user' }, content: [{ type: 'text', text: 'keep this instruction' }] }],
        }]] : []),
      ] },
    };
    const { persistence, bytes } = await childFixture(t, family);
    const snapshot = await persistence.stat('parent');
    const child = await persistence.stat('child');
    persistence.list = async () => [snapshot, child];
    persistence.open = () => assert.fail('Preparation must not open a native writer');
    const directories = [snapshot, child].map(entry => dirname(entry.location.path));
    const before = await Promise.all([bytes('parent'), bytes('child')]);
    const namesBefore = await Promise.all(directories.map(path => readdir(path)));
    await assert.rejects(prepare(persistence, snapshot), { name: 'RevisionError',
      message: sameMillisecond ? /无法确定.*先后顺序/u : /没有原生登记/u });
    assert.deepEqual(await Promise.all([bytes('parent'), bytes('child')]), before);
    assert.deepEqual(await Promise.all(directories.map(path => readdir(path))), namesBefore);
  });
}

for (const scenario of ['complete plain', 'complete zstd', 'crosses cutoff', 'unsupported generation']) {
  test(`legacy child ${scenario} is handled before modifying history`, options, async t => {
    const family = {
      parent: { createdAt: 10, events: [
        ...turn(1, 20), ...turn(2, 60),
        [70, 'subagent/catalog', catalog('child', 23)],
      ] },
      child: { parentSession: 'parent', createdAt: 23, events: [] },
    };
    const { persistence, bytes } = await childFixture(t, family);
    const parent = await persistence.stat('parent');
    const current = await persistence.stat('child');
    const legacyVersion = scenario === 'unsupported generation' ? 2 : 3;
    const compressed = scenario === 'complete zstd';
    const path = join(dirname(current.location.path), `session.v${legacyVersion}.jsonl${compressed ? '.zstd' : ''}`);
    const header = { ...current.header, version: 3, delegationDepth: 1, isSeeded: false };
    const own = [[26, 'session/end-seed', { inherited: false }], [31, 'turn/start', { turn: 1 }],
      [32, 'step/start', { turn: 1, step: 1 }], [33, 'step/end', { turn: 1, step: 1 }],
      [scenario === 'crosses cutoff' ? 61 : 34, 'turn/end', { turn: 1, reason: { kind: 'completed' } }]];
    const encode = rows => Buffer.from(`${rows.map(record => JSON.stringify(record)).join('\n')}\n`);
    const codec = data => zstdCompressSync(data, { params: { [constants.ZSTD_c_checksumFlag]: 1 } });
    const raw = compressed ? Buffer.concat([codec(encode([header])), codec(encode(timeline(own)))])
      : encode([header, ...timeline(own)]);
    await writeFile(path, raw, { flag: 'wx' });
    const legacy = { header: { ...header, version: 4 }, formatStatus: 'migration-required',
      location: { kind: 'jsonl', path }, revision: 'legacy-fixture' };
    const statOf = persistence.stat.bind(persistence);
    persistence.stat = id => id === 'child' ? assert.fail('Legacy revalidation should share the authoritative inventory') : statOf(id);
    let corpusRevision = 0;
    persistence.list = async () => [parent, { ...legacy, revision: `legacy-file:unrelated-corpus-${++corpusRevision}` }];
    persistence.open = () => assert.fail('Legacy child inspection must use the bounded worker');
    const before = await bytes('parent');
    if (scenario.startsWith('complete')) {
      const work = await prepare(persistence, parent);
      try {
        assert.deepEqual(work.artifacts.map(file => file.header.id), ['parent']);
        assert.deepEqual(work.withdrawn, []);
        await commitBoundedRevision(persistence, work, { verifyReload() {}, afterCommit() {} });
      } finally {
        await work.dispose();
      }
      assert.match((await bytes('parent')).toString(), /"text":"revised"/u);
      assert.match((await bytes('parent')).toString(), /"childId":"child"/u);
      assert.equal((await statOf('child')).location.path, current.location.path);
    } else {
      await assert.rejects(prepare(persistence, parent), { name: 'RevisionError', message: scenario === 'crosses cutoff'
        ? /旧版子会话跨过编辑点.*迁移/u : /格式不支持/u });
      assert.deepEqual(await bytes('parent'), before);
    }
    assert.deepEqual(await readFile(path), raw);
  });
}

test('legacy revalidation rejects a changed file, generation or related catalog edge', options, async t => {
  const { persistence } = await childFixture(t, {
    parent: { createdAt: 10, events: [
      ...turn(1, 20, [[23, 'subagent/catalog', catalog('legacy-parent', 23)]]), ...turn(2, 60),
    ] },
    'legacy-parent': { parentSession: 'parent', createdAt: 23, events: [] },
  });
  const parent = await persistence.stat('parent');
  const current = await persistence.stat('legacy-parent');
  const path = join(dirname(current.location.path), 'session.v3.jsonl');
  const header = { ...current.header, version: 4, delegationDepth: 1, isSeeded: false };
  const raw = `${JSON.stringify({ ...header, version: 3 })}\n`;
  await writeFile(path, raw, { flag: 'wx' });
  const legacy = { ...current, header, location: { kind: 'jsonl', path }, formatStatus: 'migration-required' };
  let inventory = [parent, legacy];
  persistence.list = async () => inventory;
  const work = await prepare(persistence, parent);
  try {
    inventory = [parent, current];
    await assert.rejects(revalidateWork(persistence, work), { name: 'RevisionError', message: /会话状态已变更/u });
    inventory = [parent, legacy, { header: { id: 'new-grandchild', origin: 'subagent', parentSession: 'legacy-parent' } }];
    await assert.rejects(revalidateWork(persistence, work), { name: 'RevisionError', message: /会话状态已变更/u });
    inventory = [parent, { ...legacy, header: { ...legacy.header, parentSession: 'different-parent' } }];
    await assert.rejects(revalidateWork(persistence, work), { name: 'RevisionError', message: /会话状态已变更/u });
    inventory = [parent, legacy];
    await writeFile(path, `${raw}${JSON.stringify({ seq: 0, time: 35, type: 'turn/start', data: { turn: 1 } })}\n`);
    await assert.rejects(revalidateWork(persistence, work), { name: 'RevisionError', message: /会话状态已变更/u });
  } finally {
    await work.dispose();
  }
});

test('a missing legacy grandchild edge names the parent DSH needs to migrate', options, async t => {
  const { persistence, bytes } = await childFixture(t, {
    parent: { createdAt: 10, events: [
      ...turn(1, 20, [[23, 'subagent/catalog', catalog('legacy-parent', 23)]]), ...turn(2, 60),
    ] },
    'legacy-parent': { parentSession: 'parent', createdAt: 23, events: [] },
    grandchild: { parentSession: 'legacy-parent', createdAt: 30, events: [] },
  });
  const parent = await persistence.stat('parent');
  const current = await persistence.stat('legacy-parent');
  const grandchild = await persistence.stat('grandchild');
  const path = join(dirname(current.location.path), 'session.v3.jsonl');
  const header = { ...current.header, version: 4, delegationDepth: 1, isSeeded: false };
  const raw = `${JSON.stringify({ ...header, version: 3 })}\n`;
  await writeFile(path, raw, { flag: 'wx' });
  persistence.list = async () => [parent, { ...current, header, location: { kind: 'jsonl', path } }, grandchild];
  const before = await bytes('parent');
  await assert.rejects(prepare(persistence, parent), { name: 'RevisionError', message: /legacy-parent.*在 DSH 中迁移这个直接父会话/u });
  assert.deepEqual(await bytes('parent'), before);
  assert.equal(await readFile(path, 'utf8'), raw);
});

test('native inventory size only limits descendants involved in revision', options, async t => {
  const { persistence, bytes } = await childFixture(t, { parent: {
    createdAt: 10, events: [...turn(1, 20), ...turn(2, 60)],
  } });
  const parent = await persistence.stat('parent');
  const unrelated = Array.from({ length: 50_001 }, (_, index) => ({ header: {
    id: `unrelated-${index}`, parentSession: 'different-root', origin: 'subagent',
  } }));
  persistence.list = async () => [parent, ...unrelated];
  const work = await prepare(persistence, parent);
  try {
    assert.deepEqual(work.entries.map(entry => entry.header.id), ['parent']);
    assert.match((await bytes('parent')).toString(), /"text":"original"/u);
  } finally {
    await work.dispose();
  }
});

test('submission preserves the worker reason for a non-independent turn', options, async t => {
  const { persistence, bytes } = await childFixture(t, { parent: {
    createdAt: 10, events: turn(1, 20, [[24, 'user/message', {
      id: 'question-2', role: 'user', source: { kind: 'user' }, content: [{ type: 'text', text: 'second message in one turn' }],
    }]]),
  } });
  const parent = await persistence.stat('parent');
  const before = await bytes('parent');
  persistence.list = () => assert.fail('A non-independent turn must never reach inventory');
  await assert.rejects(prepare(persistence, parent), {
    name: 'RevisionError', message: '此消息没有可独立回退的对话轮次。',
  });
  assert.deepEqual(await bytes('parent'), before);
});

test('submission preserves inherited-duplicate reason and allows owned turns on historical ordinary forks', options, async t => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'dsh-revision-fork-')));
  t.after(async () => {
    assert.equal(await realpath(root), root);
    await rm(root, { recursive: true, force: true });
  });
  const storage = join(root, 'sessions', 'project');
  const writeSession = async (id, header, events) => {
    const path = join(storage, id, 'session.v4.jsonl');
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, `${[header, ...timeline(events)].map(row => JSON.stringify(row)).join('\n')}\n`);
    return { header, revision: `rev-${id}`, location: { kind: 'jsonl', path } };
  };

  const duplicateMsg = { id: 'question-2', role: 'user', source: { kind: 'user' }, content: [{ type: 'text', text: 'earlier copy' }] };
  const dupHeader = { type: 'session', version: 4, id: 'dup-fork', parentSession: 'origin-main', isSeeded: true, createdAt: 10 };
  const dupSnap = await writeSession('dup-fork', dupHeader, [
    [15, 'agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [duplicateMsg] }],
    [16, 'agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [], outcome: 'canceled' }],
    [25, 'session/end-seed', { inherited: true }],
    ...turn(2, 60),
  ]);
  const dupPersistence = { list: () => assert.fail('inherited-duplicate must reject before inventory'), stat: async () => dupSnap };
  await assert.rejects(prepare(dupPersistence, dupSnap), {
    name: 'RevisionError', message: '无法替换继承历史中的同一条消息。',
  });

  const forkHeader = { type: 'session', version: 4, id: 'ordinary-fork', parentSession: 'origin-main', isSeeded: true, createdAt: 10 };
  const forkSnap = await writeSession('ordinary-fork', forkHeader, [
    ...turn(1, 20),
    [25, 'session/end-seed', { inherited: true }],
    ...turn(2, 60),
  ]);
  const forkPersistence = { list: async () => [forkSnap], stat: async () => forkSnap };
  const work = await prepare(forkPersistence, forkSnap);
  try {
    assert.deepEqual(work.entries.map(entry => entry.header.id), ['ordinary-fork']);
  } finally {
    await work.dispose();
  }
});

test('unsupported hosts give a platform error before inspecting any rollout', async () => {
  const original = Object.getOwnPropertyDescriptor(process, 'platform');
  try {
    for (const platform of ['freebsd', 'aix', 'sunos']) {
      Object.defineProperty(process, 'platform', { ...original, value: platform });
      await assert.rejects(prepareBoundedRevision(undefined, undefined, 'question-2', amend), {
        name: 'RevisionError', message: /Linux、macOS 或 Windows/u,
      });
    }
  } finally {
    Object.defineProperty(process, 'platform', original);
  }
});

test('a 10,000-child inventory rejects before staging or attempting a lease', { ...options, timeout: 25_000 }, async t => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'dsh-revision-wide-')));
  t.after(async () => {
    assert.equal(await realpath(root), root);
    await rm(root, { recursive: true, force: true });
  });
  const storage = join(root, 'sessions', 'project');
  const pathFor = id => join(storage, id, 'session.v4.jsonl');
  const headers = Array.from({ length: 10_000 }, (_, index) => ({ type: 'session', version: 4,
    id: `child-${index}`, origin: 'subagent', parentSession: 'parent', createdAt: 63 }));
  const parent = { type: 'session', version: 4, id: 'parent', createdAt: 10 };
  const events = [...turn(1, 20), ...turn(2, 60, headers.map(({ id }) => [63, 'subagent/catalog', catalog(id, 63)]))];
  const source = [parent, ...timeline(events)].map(row => JSON.stringify(row)).join('\n') + '\n';
  for (const header of [parent, ...headers.slice(0, 127)]) {
    await mkdir(dirname(pathFor(header.id)), { recursive: true });
    await writeFile(pathFor(header.id), header === parent ? source : `${JSON.stringify(header)}\n`);
  }
  const snapshot = header => ({ header, revision: 'isolated-inventory', location: { kind: 'jsonl', path: pathFor(header.id) } });
  const persistence = { list: async () => [parent, ...headers].map(snapshot), open: () => assert.fail('Must reject before opening a writer') };
  await assert.rejects(prepare(persistence, snapshot(parent)), { name: 'RevisionError', message: /会话过多/u });
  assert.equal(await readFile(pathFor(parent.id), 'utf8'), source);
  await assert.rejects(stat(join(dirname(pathFor(parent.id)), 'session.lock')), { code: 'ENOENT' });
});
