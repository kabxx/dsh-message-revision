import { mkdir, mkdtemp, readFile, realpath, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

export function timeline(events) {
  return events.map(([time, type, data], seq) => ({ time, type, data, seq }));
}

export function catalog(childId, childCreatedAt, mode = 'one-shot') {
  return { version: mode === 'unknown' ? 1 : 0, childId, childCreatedAt, mode };
}

export function turn(turnNumber, time, extra = []) {
  const message = { id: `question-${turnNumber}`, role: 'user', source: { kind: 'user' }, content: [{ type: 'text', text: 'original' }] };
  return [
    [time - 1, 'agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [message] }],
    [time, 'turn/start', { turn: turnNumber }],
    [time + 1, 'agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [], outcome: 'claimed' }],
    [time + 2, 'user/message', message],
    ...extra,
    [time + 8, 'turn/end', { turn: turnNumber }],
  ];
}

export async function childFixture(t, sessions) {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'dsh-child-revision-test-')));
  const entries = new Map();
  const writers = new Set();
  t.after(() => rm(directory, { recursive: true, force: true }));
  for (const [id, { parentSession, createdAt, events, inheritedEventCount = 0 }] of Object.entries(sessions)) {
    const header = { type: 'session', version: 4, id, createdAt,
      ...(parentSession === undefined ? {} : { parentSession, origin: 'subagent' }) };
    const path = join(directory, 'sessions', 'project', id, 'session.v4.jsonl');
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, [header, ...timeline(events)].map(event => JSON.stringify(event)).join('\n') + '\n');
    entries.set(id, { header, path, inheritedEventCount });
  }
  const persistence = {
    async stat(id) {
      const entry = entries.get(id);
      if (!entry) return undefined;
      const info = await stat(entry.path, { bigint: true }).catch(error => {
        if (error.code !== 'ENOENT') throw error;
        return undefined;
      });
      return info && { header: entry.header, location: { kind: 'jsonl', path: entry.path },
        revision: `${info.ino}:${info.size}:${info.mtimeNs}:${info.ctimeNs}` };
    },
    async open(id, access) {
      const { header, path, inheritedEventCount } = entries.get(id);
      if (access === 'write') {
        if (writers.has(id)) {
          throw Object.assign(new Error('writer already held'), { name: 'SessionAlreadyOwnedError' });
        }
        writers.add(id);
      }
      return { header, inheritedEventCount,
        read: async () => ({ events: (await readFile(path, 'utf8')).trimEnd().split('\n').slice(1).map(JSON.parse) }),
        close: async () => { if (access === 'write') writers.delete(id); } };
    },
  };
  return { persistence, bytes: id => readFile(entries.get(id).path), writers };
}
