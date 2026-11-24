import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { forgetRewriteJournal, markRewriteCommitted, prepareRewriteSetJournal, readRewriteJournals, retireRewriteJournal } from '../src/rewrite-journal.js';
import { readDarwinProcessStart, readWin32ProcessCreationTime } from '../src/platform.js';
import { nativeLocksAvailable, spawnTestEnv } from './platform-fixture.js';

const journalModule = new URL('../src/rewrite-journal.js', import.meta.url).href;
const revisionModule = new URL('../src/revision.js', import.meta.url).href;

async function fixture(t) {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'dsh-journal-resilience-')));
  t.after(async () => {
    assert.equal(root, await realpath(root));
    await rm(root, { recursive: true });
  });
  const path = join(root, 'sessions', 'project', 'parent', 'session.v4.jsonl');
  await mkdir(dirname(path), { recursive: true });
  const header = { type: 'session', version: 4, id: 'parent', createdAt: 1 };
  const bytes = `${JSON.stringify(header)}\n`;
  const candidatePath = join(root, 'candidate');
  await writeFile(path, bytes);
  await writeFile(candidatePath, `${bytes}candidate\n`);
  return { parent: { path, header }, artifact: { path, header, candidatePath, retained: 1 },
    snapshot: { header, location: { kind: 'jsonl', path } } };
}

const retirementWorker = `
  import assert from 'node:assert/strict';
  import { createRequire, syncBuiltinESMExports } from 'node:module';
  import { basename, dirname, join } from 'node:path';
  import { setTimeout } from 'node:timers/promises';
  const [journalModule, revisionModule, data, mode] = process.argv.slice(1);
  const { parent, artifact, snapshot } = JSON.parse(data);
  const stripNs = value => value.startsWith('\\\\\\\\?\\\\') ? value.slice(4) : value;
  const fs = createRequire(import.meta.url)('node:fs/promises');
  const rename = fs.rename;
  let failures = 0;
  let failRetire = true;
  function maybeFail(from, to) {
    if (failRetire && basename(stripNs(from)) === 'rewrite' && basename(stripNs(to)).startsWith('discard-')) {
      failures++;
      throw Object.assign(new Error('simulated transient rename error'), { code: 'EIO' });
    }
  }
  fs.rename = async (from, to) => {
    maybeFail(from, to);
    return rename(from, to);
  };
  if (process.platform === 'win32') {
    const { loadHostKoffi } = await import(new URL('./platform.js', journalModule).href);
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
              maybeFail(from, to);
              return fn(from, to, flags);
            };
          },
        };
      };
    }
  }
  syncBuiltinESMExports();
  const { forgetRewriteJournal, hasPendingRewrite, markRewriteCommitted, prepareRewriteSetJournal,
    scheduleSettledRewriteRetirement, retireRewriteJournal } = await import(journalModule);
  const { assertNoPendingRewrite } = await import(revisionModule);
  const journal = await prepareRewriteSetJournal(parent, [], [artifact], { messageId: 'revised' });
  assert.throws(() => scheduleSettledRewriteRetirement(journal), /unverified or unsettled/u);
  await markRewriteCommitted(journal);
  await assert.rejects(assertNoPendingRewrite(snapshot), /尚未完成/u);
  // A delivery owner registers the journal only after the revised prompt is durable.
  scheduleSettledRewriteRetirement(journal);
  forgetRewriteJournal(journal);
  for (let i = 0; i < 100 && failures === 0; i++) await setTimeout(10);
  assert.equal(failures, 1);
  assert.equal(await hasPendingRewrite(parent.path), true);
  failRetire = false;
  if (mode === 'background') {
    for (let i = 0; i < 100 && await hasPendingRewrite(parent.path); i++) await setTimeout(10);
    assert.equal(await hasPendingRewrite(parent.path), false);
  }
  await assertNoPendingRewrite(snapshot);
  assert.equal(await hasPendingRewrite(parent.path), false);
  const next = await prepareRewriteSetJournal(parent, [], [artifact]);
  // A canceled retry must not move a later transaction with the same canonical path.
  await setTimeout(150);
  assert.equal(await hasPendingRewrite(parent.path), true);
  await retireRewriteJournal(next);
  forgetRewriteJournal(next);
  for (let i = 0; i < 100 && (await fs.readdir(dirname(next.directory))).length > 0; i++) await setTimeout(10);
  assert.deepEqual(await fs.readdir(dirname(next.directory)), []);
`;

for (const [mode, operation] of [['background', 'retried in the background'], ['admission', 'retried at the next edit']]) {
  test(`a transient retirement error is ${operation}`, async t => {
    const state = await fixture(t);
    const child = spawn(process.execPath, ['--input-type=module', '-e', retirementWorker,
      journalModule, revisionModule, JSON.stringify(state), mode], { stdio: ['ignore', 'ignore', 'pipe'], env: spawnTestEnv() });
    const timer = setTimeout(() => child.kill(), 6000);
    let stderr = '';
    child.stderr.setEncoding('utf8').on('data', chunk => { stderr += chunk; });
    try {
      const [code, signal] = await once(child, 'close');
      assert.equal(signal, null, stderr);
      assert.equal(code, 0, stderr);
    } finally { clearTimeout(timer); }
  });
}

test('owner start time distinguishes an active creator from a reused numeric PID', {
  skip: (process.platform === 'win32' && !nativeLocksAvailable)
    || !['linux', 'darwin', 'win32'].includes(process.platform),
}, async t => {
  const { parent, artifact, snapshot } = await fixture(t);
  const other = spawn(process.execPath, ['-e', 'process.stdin.resume()'], {
    stdio: ['pipe', 'ignore', 'ignore'],
    env: spawnTestEnv(),
  });
  const exited = once(other, 'close');
  const journal = await prepareRewriteSetJournal(parent, [], [artifact]);
  forgetRewriteJournal(journal);
  const path = join(journal.directory, 'intent.json');
  const { intent } = journal;
  try {
    let owner;
    let mismatchedOwner;
    if (process.platform === 'linux') {
      assert.match(intent.owner.startTicks, /^\d+$/u);
      const stat = await readFile(`/proc/${other.pid}/stat`, 'utf8');
      const startTicks = stat.slice(stat.lastIndexOf(')') + 1).trim().split(/\s+/u)[19];
      owner = { ...intent.owner, pid: other.pid, startTicks };
      mismatchedOwner = { ...owner, startTicks: String(BigInt(startTicks) + 1n) };
    } else if (process.platform === 'darwin') {
      assert.ok(typeof intent.owner.darwinBootId === 'string' && intent.owner.darwinBootId.length > 0);
      assert.ok(intent.owner.darwinStartLabel.length > 0);
      const darwinStartLabel = await readDarwinProcessStart(other.pid);
      assert.ok(darwinStartLabel);
      owner = { ...intent.owner, pid: other.pid, darwinStartLabel };
      mismatchedOwner = { ...owner, darwinStartLabel: 'Thu Jan  1 00:00:00 1970' };
    } else {
      assert.match(intent.owner.win32CreationTime, /^\d+$/u);
      assert.match(intent.owner.win32BootId ?? '', /^[a-f0-9]{32}$/u);
      const win32CreationTime = readWin32ProcessCreationTime(other.pid);
      assert.match(win32CreationTime, /^\d+$/u);
      owner = { ...intent.owner, pid: other.pid, win32CreationTime };
      mismatchedOwner = { ...owner, win32CreationTime: String(BigInt(win32CreationTime) + 1n) };
    }
    await writeFile(path, JSON.stringify({ ...intent, owner }));
    await assert.rejects(readRewriteJournals(snapshot), /still running/u);
    await writeFile(path, JSON.stringify({ ...intent, owner: mismatchedOwner }));
    assert.equal((await readRewriteJournals(snapshot)).length, 1);
    const legacy = { host: owner.host, pid: other.pid };
    await writeFile(path, JSON.stringify({ ...intent, owner: legacy }));
    await assert.rejects(readRewriteJournals(snapshot), /still running/u);
    other.stdin.end();
    await exited;
    assert.equal((await readRewriteJournals(snapshot)).length, 1);
    await retireRewriteJournal(journal);
  } finally {
    other.stdin.destroy();
    other.kill();
    await exited;
  }
});
