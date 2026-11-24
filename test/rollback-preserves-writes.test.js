import assert from 'node:assert/strict';
import { appendFile, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { restoreBoundedJournal } from '../src/bounded-commit.js';
import { recoverStoredSessions } from '../src/revision.js';
import {
  forgetRewriteJournal, prepareRewriteJournal, prepareRewriteSetJournal, publishRewriteSetArtifacts, readRewriteJournals,
  retireRewriteJournal, stageWithdrawnChildren,
} from '../src/rewrite-journal.js';
import { skipWithoutNativeLocks } from './platform-fixture.js';

const resume = { type: 'session/end-seed', data: {} };
const row = value => `${JSON.stringify(value)}\n`;

async function fixture(t) {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'dsh-rollback-writes-')));
  t.after(async () => {
    assert.equal(await realpath(root), root);
    assert.ok(root.startsWith(join(await realpath(tmpdir()), 'dsh-rollback-writes-')));
    await rm(root, { recursive: true });
  });
  async function entry(id, parentSession) {
    const path = join(root, 'sessions', 'project', id, 'session.v4.jsonl');
    const header = { type: 'session', version: 4, id, createdAt: 1, isSeeded: false,
      ...(parentSession && { parentSession, origin: 'subagent' }) };
    const old = Buffer.from(row(header) + row({ type: 'system/message', data: { label: 'original' }, seq: 0, time: 10 }));
    const candidate = Buffer.from(row(header) + row({ type: 'system/message', data: { label: 'candidate' }, seq: 0, time: 10 }));
    const candidatePath = join(root, `candidate-${id}`);
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, old);
    await writeFile(candidatePath, candidate);
    return { header, path, candidatePath, retained: 1, old, candidate };
  }
  const parent = await entry('parent');
  const child = await entry('child', 'parent');
  const withdrawn = await entry('withdrawn', 'parent');
  const snapshot = { header: parent.header, location: { kind: 'jsonl', path: parent.path }, revision: 'fixture' };
  return { parent, child, withdrawn, snapshot };
}

const cases = [
  { name: 'allows the solitary stock resume marker', added: [resume], restore: true },
  { name: 'preserves an independent model selection', added: [resume, { type: 'model/selection', data: { provider: 'example', model: 'saved' } }] },
  { name: 'preserves an independent permission change', added: [resume, { type: 'approval/policy', data: { policy: 'on-request' } }] },
  { name: 'preserves independent cancellation', added: [resume, { type: 'agent/inbox/spliced', data: {
    target: 'next-turn', start: 0, removedCount: 1, inserted: [], outcome: 'canceled',
  } }] },
  { name: 'preserves records of unknown origin', added: [resume, { type: 'unattributed/event', data: {} }] },
  { name: 'preserves multiple appended markers', added: [resume, resume] },
  { name: 'preserves an inherited seed marker', added: [{ type: 'session/end-seed', data: { inherited: true } }],
    conflict: { name: 'RolloutWorkerError', message: /继承边界不匹配/u } },
  { name: 'preserves a child that was independently opened', added: [resume], appendToChild: true },
];

for (const { name, added, restore, appendToChild, conflict = { name: 'RewriteRollbackConflictError' } } of cases) {
  test(`multi-log rollback ${name}`, { skip: skipWithoutNativeLocks }, async t => {
    const { parent, child, withdrawn, snapshot } = await fixture(t);
    const journal = await prepareRewriteSetJournal(parent, [withdrawn], [parent, child]);
    try {
      await stageWithdrawnChildren(journal);
      await publishRewriteSetArtifacts(journal);
      const modified = appendToChild ? child : parent;
      await appendFile(modified.path, added.map((event, index) => row({ ...event, seq: index + 1, time: index + 11 })).join(''));
      const before = await Promise.all([parent, child].map(entry => readFile(entry.path)));
      if (restore) {
        await restoreBoundedJournal(journal);
        assert.deepEqual(await Promise.all([parent, child, withdrawn].map(entry => readFile(entry.path))),
          [parent.old, child.old, withdrawn.old]);
        await retireRewriteJournal(journal);
      } else {
        await assert.rejects(restoreBoundedJournal(journal), conflict);
        assert.deepEqual(await Promise.all([parent, child].map(entry => readFile(entry.path))), before);
        await assert.rejects(readFile(withdrawn.path), { code: 'ENOENT' });
        assert.deepEqual(await readFile(join(journal.directory, 'child-0', 'session.v4.jsonl')), withdrawn.old);
        forgetRewriteJournal(journal);
        assert.equal((await readRewriteJournals(snapshot)).length, 1);
      }
    } finally { forgetRewriteJournal(journal); }
  });
}

for (const restore of [false, true]) {
  test(`legacy rollback ${restore ? 'allows the stock resume marker' : 'preserves an independent model selection'}`, async t => {
    const { parent, snapshot } = await fixture(t);
    const source = { path: parent.path, handle: { header: parent.header } };
    const journal = await prepareRewriteJournal(source, [], {
      original: parent.old, buffers: [parent.candidate], retained: 1,
    });
    forgetRewriteJournal(journal);
    const events = [resume, ...(restore ? [] : [{ type: 'model/selection', data: { provider: 'example', model: 'saved' } }])];
    const published = Buffer.concat([parent.candidate,
      Buffer.from(events.map((event, index) => row({ ...event, seq: index + 1, time: index + 11 })).join(''))]);
    await writeFile(parent.path, published);
    const failures = [];
    const persistence = {
      async list() { return [snapshot]; },
      async stat() { return snapshot; },
      async open() {
        return { header: parent.header, async close() {}, async read() {
          const [, ...lines] = (await readFile(parent.path, 'utf8')).trimEnd().split('\n');
          return { events: lines.map(line => JSON.parse(line)) };
        } };
      },
    };
    await recoverStoredSessions(persistence, { onError: (_, error) => failures.push(error) });
    assert.deepEqual(await readFile(parent.path), restore ? parent.old : published);
    if (restore) assert.equal(failures.length, 0);
    else assert.match(failures[0]?.message, /无法自动回滚/u);
    assert.equal((await readRewriteJournals(snapshot)).length, restore ? 0 : 1);
  });
}
