import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { appendFile, lstat, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { createRequire, syncBuiltinESMExports } from 'node:module';
import { tmpdir } from 'node:os';
import { basename, dirname, join } from 'node:path';
import test from 'node:test';
import {
  assertRewriteSetSpace, forgetRewriteJournal, listRewriteDebris, markRewriteCommitted, matchJournalRevision,
  prepareRewriteSetJournal, publishRewriteSetArtifacts,
  readRewriteJournals, resolveRewriteSetChildPath, restoreRewriteSetArtifacts, retireRewriteJournal,
  rewriteSetArtifactPath, stageWithdrawnChildren,
} from '../src/rewrite-journal.js';
import { assertTerminated, spawnTestEnv } from './platform-fixture.js';

const journalModule = new URL('../src/rewrite-journal.js', import.meta.url).href;

async function fixture(t) {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'dsh-multi-log-journal-')));
  t.after(async () => {
    assert.equal(root, await realpath(root));
    await rm(root, { recursive: true });
  });
  async function entry(id, createdAt, parentSession) {
    const path = join(root, 'sessions', 'project', id, 'session.v4.jsonl');
    await mkdir(dirname(path), { recursive: true });
    const header = { type: 'session', version: 4, id, createdAt, parentSession };
    await writeFile(path, `${JSON.stringify(header)}\noriginal ${id}\n`);
    return { header, path };
  }
  const parent = await entry('parent', 1);
  const child = await entry('child', 2, 'parent');
  const withdrawn = await entry('withdrawn', 3, 'child');
  const artifacts = [parent, child].map(entry => ({ ...entry, candidatePath: join(root, `candidate-${entry.header.id}`), retained: 1 }));
  for (const artifact of artifacts) {
    await writeFile(artifact.candidatePath, `${JSON.stringify(artifact.header)}\ncandidate ${artifact.header.id}\n`);
  }
  const original = await Promise.all([parent, child, withdrawn].map(entry => readFile(entry.path)));
  const snapshot = { header: parent.header, location: { kind: 'jsonl', path: parent.path } };
  return { parent, child, withdrawn, artifacts, original, snapshot };
}

test('rewrite budgets the combined journal copies on their destination before writing any artifacts', async t => {
  const { parent, child, withdrawn, artifacts, original, snapshot } = await fixture(t);
  const fs = createRequire(import.meta.url)('node:fs/promises');
  const open = fs.open;
  const blockSize = 1024n;
  const expectedBlocks = (4n * 1024n * 1024n) / blockSize + 12n;
  let freeBlocks = expectedBlocks - 1n;
  const openedCopies = [];
  const checks = [];
  t.mock.method(fs, 'statfs', async (path, options) => {
    assert.equal(options.bigint, true);
    checks.push(path);
    return { bsize: 4096n, frsize: blockSize, bavail: freeBlocks };
  });
  t.mock.method(fs, 'open', async (path, flags, ...args) => {
    if (flags === 'wx' && basename(dirname(path)) === 'artifacts') openedCopies.push(path);
    return open(path, flags, ...args);
  });
  syncBuiltinESMExports();
  try {
    const tooSmall = {
      name: 'RewriteJournalSpaceError', requiredBytes: expectedBlocks * blockSize, availableBytes: freeBlocks * blockSize,
    };
    await assert.rejects(assertRewriteSetSpace(parent, artifacts), tooSmall);
    await assert.rejects(prepareRewriteSetJournal(parent, [withdrawn], artifacts), tooSmall);
    assert.deepEqual(openedCopies, []);
    assert.deepEqual(await readRewriteJournals(snapshot), []);
    assert.deepEqual((await listRewriteDebris(parent.path)).preparations, []);
    assert.deepEqual(await Promise.all([parent, child, withdrawn].map(entry => readFile(entry.path))), original);
    assert.equal(checks[0], dirname(parent.path));
    assert.match(checks[1], /[\\/]preparing-[^\\/]+$/u);

    // This accounts for both new copies per artifact. The staged candidates
    // and original live logs have already been deducted by the filesystem.
    freeBlocks = expectedBlocks;
    const journal = await prepareRewriteSetJournal(parent, [withdrawn], artifacts);
    try {
      assert.equal(openedCopies.length, 4);
      await retireRewriteJournal(journal);
    } finally { forgetRewriteJournal(journal); }
  } finally {
    t.mock.restoreAll();
    syncBuiltinESMExports();
  }
});

const worker = `
  import { createRequire, syncBuiltinESMExports } from 'node:module';
  import { join } from 'node:path';
  const [entry, config] = process.argv.slice(1);
  const { parent, child, withdrawn, artifacts } = JSON.parse(config);
  const stripNs = value => value.startsWith('\\\\\\\\?\\\\') ? value.slice(4) : value;
  const samePath = (a, b) => process.platform === 'win32' ? a.toLowerCase() === b.toLowerCase() : a === b;
  const fs = createRequire(import.meta.url)('node:fs/promises');
  const rename = fs.rename;
  fs.rename = async (from, to) => {
    await rename(from, to);
    if (samePath(stripNs(to), child.path)) process.kill(process.pid, 'SIGKILL');
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
              const ok = fn(from, to, flags);
              if (ok !== 0 && samePath(stripNs(to), child.path)) process.kill(process.pid, 'SIGKILL');
              return ok;
            };
          },
        };
      };
    }
  }
  syncBuiltinESMExports();
  const { prepareRewriteSetJournal, stageWithdrawnChildren, publishRewriteSetArtifacts } = await import(entry);
  const journal = await prepareRewriteSetJournal(parent, [withdrawn], artifacts);
  await stageWithdrawnChildren(journal);
  await publishRewriteSetArtifacts(journal);
`;

test('a fresh process rolls back a partially published multi-log rewrite', async t => {
  const state = await fixture(t);
  const { parent, child, withdrawn, artifacts, original, snapshot } = state;
  const proc = spawn(process.execPath, ['--input-type=module', '-e', worker, journalModule,
    JSON.stringify({ parent, child, withdrawn, artifacts })], { stdio: ['ignore', 'ignore', 'pipe'], env: spawnTestEnv() });
  let stderr = '';
  proc.stderr.setEncoding('utf8').on('data', chunk => { stderr += chunk; });
  const [code, signal] = await once(proc, 'close');
  assertTerminated(code, signal, stderr);
  const [journal] = await readRewriteJournals(snapshot);
  assert.equal(journal.intent.format, 2);
  assert.equal(rewriteSetArtifactPath(journal, 1), child.path);
  assert.equal(await matchJournalRevision(journal, 0), 'original');
  assert.equal(await matchJournalRevision(journal, 1), 'candidate');
  await assert.rejects(lstat(withdrawn.path), { code: 'ENOENT' });
  assert.equal(await resolveRewriteSetChildPath(journal, 0), join(journal.directory, 'child-0', 'session.v4.jsonl'));
  await restoreRewriteSetArtifacts(journal);
  assert.equal(await resolveRewriteSetChildPath(journal, 0), withdrawn.path);
  // Recovery remains safe if interrupted before removing the journal.
  await restoreRewriteSetArtifacts(journal);
  assert.deepEqual(await Promise.all([parent, child, withdrawn].map(entry => readFile(entry.path))), original);
  await retireRewriteJournal(journal);
  assert.deepEqual(await readRewriteJournals(snapshot), []);
});

test('an appended candidate suffix must be validated before any file or directory can be rolled back', async t => {
  const { parent, child, withdrawn, artifacts, original } = await fixture(t);
  const journal = await prepareRewriteSetJournal(parent, [withdrawn], artifacts);
  try {
    await stageWithdrawnChildren(journal);
    await publishRewriteSetArtifacts(journal);
    const candidate = await readFile(child.path);
    await appendFile(parent.path, 'host initialization event\n');
    await assert.rejects(restoreRewriteSetArtifacts(journal), /cannot be restored safely/u);
    assert.deepEqual(await readFile(child.path), candidate);
    await assert.rejects(lstat(withdrawn.path), { code: 'ENOENT' });
    const calls = [];
    await restoreRewriteSetArtifacts(journal, { canRestoreArtifact(artifact, match, index, path) {
      assert.equal(match, 'candidate');
      assert.equal(rewriteSetArtifactPath(journal, index), path);
      calls.push(artifact.id);
      return true;
    } });
    assert.deepEqual(calls, ['parent', 'child']);
    assert.deepEqual(await Promise.all([parent, child, withdrawn].map(entry => readFile(entry.path))), original);
    await retireRewriteJournal(journal);
  } finally { forgetRewriteJournal(journal); }
});

test('withdrawn directory validation rejects changed identity before moving it', async t => {
  const { parent, withdrawn, artifacts } = await fixture(t);
  const journal = await prepareRewriteSetJournal(parent, [withdrawn], artifacts);
  try {
    assert.ok(journal.intent.children[0].identity.ino);
    assert.equal(journal.intent.children[0].digest, undefined);
    await appendFile(withdrawn.path, 'new content\n');
    await assert.rejects(stageWithdrawnChildren(journal), /child data has changed/u);
    assert.match(await readFile(withdrawn.path, 'utf8'), /new content/u);
    await retireRewriteJournal(journal);
  } finally { forgetRewriteJournal(journal); }
});

test('a committed rewrite retains its new logs and leaves a reused child path intact', async t => {
  const { parent, child, withdrawn, artifacts } = await fixture(t);
  const journal = await prepareRewriteSetJournal(parent, [withdrawn], artifacts);
  try {
    await stageWithdrawnChildren(journal);
    await publishRewriteSetArtifacts(journal);
    await markRewriteCommitted(journal);
    await mkdir(dirname(withdrawn.path));
    await writeFile(withdrawn.path, 'a new child with the same ID');
    await retireRewriteJournal(journal);
    assert.deepEqual(await readFile(parent.path), await readFile(artifacts[0].candidatePath));
    assert.deepEqual(await readFile(child.path), await readFile(artifacts[1].candidatePath));
    assert.equal(await readFile(withdrawn.path, 'utf8'), 'a new child with the same ID');
  } finally { forgetRewriteJournal(journal); }
});
