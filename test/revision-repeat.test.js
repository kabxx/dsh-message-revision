import assert from 'node:assert/strict';
import { mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { zstdCompressSync } from 'node:zlib';
import { planRevision, rewriteStoredSession } from '../src/revision.js';
import { decompressZstdConcat } from '../src/rollout-stream.js';

const text = value => ({ type: 'text', text: value });
const user = (id, value, source = {}) => ({ id, role: 'user', source: { kind: 'user', ...source }, content: [text(value)] });
const target = 'edited-message';

function history() {
  const events = [];
  function add(type, data) {
    events.push({ type, seq: events.length, time: events.length + 1, data });
  }
  function enqueue(message) {
    add('agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [message] });
  }
  function answer(turn, message) {
    add('turn/start', { turn });
    add('agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [] });
    add('user/message', message);
    add('turn/end', { turn });
  }
  const other = user('untouched-message', 'untouched body');
  enqueue(other);
  answer(1, other);
  // Reopening a prior edit canceled its queued copy before submitting it again.
  enqueue(user(target, 'previous edit B', { rpcId: 'first-input' }));
  add('agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [], outcome: 'canceled' });
  const latest = user(target, 'previous edit B', { rpcId: 'second-input' });
  enqueue(latest);
  answer(2, latest);
  return events;
}

for (const compressed of [false, true]) {
  test(`revising twice replaces all retained copies in ${compressed ? 'compressed' : 'plain'} rollout`, async () => {
    const directory = await realpath(await mkdtemp(join(tmpdir(), 'dsh-message-revision-repeat-')));
    const path = join(directory, `session.v4.jsonl${compressed ? '.zstd' : ''}`);
    const id = 'repeat-fixture';
    const events = history();
    const header = { type: 'session', version: 4, id };
    const snapshot = { revision: 'fixture-revision', location: { kind: 'jsonl', path } };
    const persistence = {
      async stat() { return snapshot; },
      async open(sessionId, mode) {
        assert.equal(sessionId, id);
        assert.equal(mode, 'write');
        return { header, inheritedEventCount: 0, async read() { return { events }; }, async close() {} };
      },
    };
    try {
      const content = Buffer.from(`${[header, ...events].map(value => JSON.stringify(value)).join('\n')}\n`);
      await writeFile(path, compressed ? zstdCompressSync(content) : content);
      await rewriteStoredSession(persistence, { sessionId: id, messageId: target, text: 'final edit C' });
      const raw = await readFile(path);
      const decoded = (compressed ? decompressZstdConcat(raw) : raw).toString('utf8');
      assert.doesNotMatch(decoded, /previous edit B/u);
      const [savedHeader, ...saved] = decoded.trimEnd().split('\n').map(line => JSON.parse(line));
      assert.deepEqual(savedHeader, header);
      assert.deepEqual(saved.slice(0, 5), events.slice(0, 5));
      assert.deepEqual(saved[6], events[6]);
      const copies = saved.flatMap(event => event.type === 'agent/inbox/spliced' ? event.data.inserted : [])
        .filter(item => item.id === target);
      assert.deepEqual(copies, [user(target, 'final edit C', { rpcId: 'first-input' }),
        user(target, 'final edit C', { rpcId: 'second-input' })]);
      assert.equal(saved.length, 8);
    } finally {
      // Only remove the exact private fixture directory created in this test.
      assert.match(directory, /(?:^|[/\\])dsh-message-revision-repeat-[^/\\]+$/u);
      assert.equal(await realpath(directory), join(await realpath(tmpdir()), directory.split(/[/\\]/u).at(-1)));
      await rm(directory, { recursive: true });
    }
  });
}

test('refuses a retained duplicate in an inherited copy of the original history', () => {
  assert.throws(() => planRevision(history(), target, 6), { name: 'RevisionError' });
});
