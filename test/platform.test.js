import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import test from 'node:test';
import { constants, createZstdCompress, zstdCompressSync } from 'node:zlib';
import {
  acquireWin32Semaphore, candidateLockName, createWin32MemoryJob, dshSessionLockName,
  isCanonicalPath, isSamePath, isSupportedPlatform, moveEntryDurable, pathKey,
  readDarwinProcessRssBytes, readDarwinProcessScope, readDarwinProcessStart,
  readWin32BootId, readWin32ProcessCreationTime, releaseWin32Semaphore, replaceFileDurable, resolveRolloutPath, workerEnv,
} from '../src/platform.js';
import { decompressZstdConcat } from '../src/rollout-stream.js';
import { nativeLocksAvailable } from './platform-fixture.js';

test('isSupportedPlatform accepts linux, darwin, and win32 and rejects unsupported OSes', () => {
  for (const platform of ['linux', 'darwin', 'win32']) assert.equal(isSupportedPlatform(platform), true);
  for (const platform of ['freebsd', 'aix', 'sunos', 'android', '']) assert.equal(isSupportedPlatform(platform), false);
});

test('dshSessionLockName matches DSH win32 SessionWriteLease naming formula exactly', () => {
  const sample = join(resolve('.'), 'session.lock');
  const expected = `Local\\dsh-session-lock-${createHash('sha256').update(resolve(sample).toLowerCase()).digest('hex')}`;
  assert.equal(dshSessionLockName(sample), expected);
  assert.notEqual(candidateLockName(resolve('.'), 'host'), candidateLockName(resolve('.'), 'worker'));
});

test('isSamePath and pathKey match platform case sensitivity', () => {
  const sample = resolve('SubDir', 'File.JSONL');
  assert.equal(isSamePath(sample, sample), true);
  assert.equal(isSamePath(sample, sample.toLowerCase()), process.platform === 'win32');
  assert.equal(pathKey(sample) === pathKey(sample.toLowerCase()), process.platform === 'win32');
  const unnormalized = `${resolve('.')}${process.platform === 'win32' ? '\\SubDir\\..\\SubDir\\File.JSONL' : '/SubDir/../SubDir/File.JSONL'}`;
  assert.equal(isSamePath(resolve(unnormalized), unnormalized), false);
});

test('decompressZstdConcat handles concatenated frames, trailing empty streaming frames, and output limits', async () => {
  const streamFrame = async data => {
    const z = createZstdCompress({ params: { [constants.ZSTD_c_checksumFlag]: 1 } });
    const chunks = [];
    z.on('data', chunk => chunks.push(chunk));
    await new Promise((done, fail) => { z.once('end', done); z.once('error', fail); z.end(data); });
    return Buffer.concat(chunks);
  };
  const combined = Buffer.concat([
    zstdCompressSync(Buffer.from('abc')),
    zstdCompressSync(Buffer.from('def')),
    await streamFrame(Buffer.alloc(0)),
  ]);
  assert.equal(decompressZstdConcat(combined, { maxOutputLength: 6 }).toString('utf8'), 'abcdef');
  assert.throws(() => decompressZstdConcat(combined, { maxOutputLength: 5 }), /超出解压大小上限/u);
});

test('workerEnv strips NODE_OPTIONS and forwards ELECTRON_RUN_AS_NODE under Electron', () => {
  const env = workerEnv({ EXTRA_FLAG: '1' });
  assert.equal(env.EXTRA_FLAG, '1');
  assert.equal(env.NODE_OPTIONS, undefined);
  if (process.versions.electron) assert.equal(env.ELECTRON_RUN_AS_NODE, '1');
});

test('replaceFileDurable and moveEntryDurable replace files and move directories safely', async t => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'dsh-platform-test-')));
  t.after(() => rm(root, { recursive: true, force: true }));
  assert.equal(await isCanonicalPath(root), true);
  const src = join(root, 'a.tmp');
  const dst = join(root, 'target.jsonl');
  await writeFile(dst, 'old');
  await writeFile(src, 'new');
  await replaceFileDurable(src, dst);
  assert.equal(await readFile(dst, 'utf8'), 'new');

  const dirFrom = join(root, 'dir-a');
  const dirTo = join(root, 'dir-b');
  await mkdir(dirFrom);
  await writeFile(join(dirFrom, 'file.txt'), 'payload');
  await moveEntryDurable(dirFrom, dirTo);
  assert.equal(await readFile(join(dirTo, 'file.txt'), 'utf8'), 'payload');
});

test('Windows rollout paths can follow a storage-root junction but reject internal junctions', {
  skip: process.platform !== 'win32',
}, async t => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'dsh-storage-junction-')));
  t.after(() => rm(root, { recursive: true, force: true }));
  const storage = join(root, 'target');
  const session = join(storage, 'project', 'session');
  const log = 'session.v4.jsonl';
  await mkdir(session, { recursive: true });
  await writeFile(join(session, log), 'original');
  const junction = join(root, 'configured-root');
  await symlink(storage, junction, 'junction');
  assert.equal(await resolveRolloutPath(join(junction, 'project', 'session', log)), join(session, log));

  await symlink(join(storage, 'project'), join(storage, 'linked-project'), 'junction');
  await symlink(session, join(storage, 'project', 'linked-session'), 'junction');
  assert.equal(await resolveRolloutPath(join(junction, 'linked-project', 'session', log)), undefined);
  assert.equal(await resolveRolloutPath(join(junction, 'project', 'linked-session', log)), undefined);
});

test('Win32 native semaphore, Job Object, and process creation time work on Windows with DSH koffi', {
  skip: process.platform !== 'win32' || !nativeLocksAvailable,
}, async t => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'dsh-win32-platform-')));
  t.after(() => rm(root, { recursive: true, force: true }));
  const lockPath = join(root, 'session.lock');
  const name = dshSessionLockName(lockPath);
  const first = acquireWin32Semaphore(name, lockPath);
  assert.equal(first.busy, false);
  try {
    const second = acquireWin32Semaphore(name, lockPath);
    assert.equal(second.busy, true);
  } finally {
    releaseWin32Semaphore(first.handle);
  }
  const third = acquireWin32Semaphore(name, lockPath);
  assert.equal(third.busy, false);
  releaseWin32Semaphore(third.handle);

  const created = readWin32ProcessCreationTime(process.pid);
  assert.match(created, /^\d+$/u);
  const bootId = readWin32BootId();
  assert.match(bootId ?? '', /^[a-f0-9]{32}$/u);

  const job = createWin32MemoryJob(256 * 1024 * 1024);
  assert.ok(job !== null);
  job.close();
});

test('macOS process scope, start label, and RSS probe work on Darwin', {
  skip: process.platform !== 'darwin',
}, async () => {
  const scope = await readDarwinProcessScope();
  assert.ok(typeof scope?.darwinBootId === 'string' && scope.darwinBootId.length > 0);
  const start = await readDarwinProcessStart(process.pid);
  assert.ok(typeof start === 'string' && start.length > 0);
  assert.equal(await readDarwinProcessStart(99998), null);
  const rss = await readDarwinProcessRssBytes(process.pid);
  assert.ok(Number.isSafeInteger(rss) && rss > 0);
});
