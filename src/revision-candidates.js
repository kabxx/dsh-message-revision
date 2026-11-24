import { randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { lstat, mkdir, open, opendir, realpath, rmdir, unlink } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join, resolve, sep } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import manifest from '../package.json' with { type: 'json' };
import { MAX_NATIVE_LEASES } from './native-lease.js';
import {
  acquireWin32Semaphore, candidateLockName, isCanonicalPath, isSamePath, isSupportedPlatform,
  pathKey, releaseWin32Semaphore, tryPosixExclusiveLock,
} from './platform.js';

const namespace = ['plugins', ...manifest.name.split('/')];
const versionName = `v${manifest.version}`;
const versionPattern = /^v\d+\.\d+\.\d+(?:-[0-9a-z.-]+)?$/iu;
const rolloutName = /^session\.v4\.jsonl(?:\.zstd)?$/u;
const candidateName = /^candidate-[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/u;
const slotFiles = Object.freeze({ host: 'host.lock', worker: 'worker.lock' });
const pendingRetries = new Set();
const retryDelays = [250, 1000, 2000, 5000, 10_000, ...Array(8).fill(30_000)];
const directoryFlags = constants.O_RDONLY | (constants.O_DIRECTORY ?? 0) | (constants.O_NOFOLLOW ?? 0);
const transientWin32Codes = new Set(['EBUSY', 'EPERM', 'EACCES']);

function check(ok, message) {
  if (!ok) throw new Error(message);
}

function stat(path) {
  return lstat(path, { bigint: true }).catch(error => {
    if (error.code !== 'ENOENT') throw error;
    return undefined;
  });
}

function sameInode(first, second) {
  return first !== undefined && second !== undefined && first.dev === second.dev && first.ino === second.ino;
}

function nativeSessionDirectory(path) {
  check(typeof path === 'string' && isAbsolute(path) && isSamePath(resolve(path), path) && rolloutName.test(basename(path)),
    'Invalid native rollout path for revision candidates');
  return dirname(path);
}

async function checkedDirectory(path, create) {
  if (create) {
    await mkdir(path, { mode: 0o700 }).catch(error => {
      if (error.code !== 'EEXIST') throw error;
    });
  }
  const identity = await stat(path);
  if (identity === undefined) return false;
  check(identity.isDirectory() && await isCanonicalPath(path), 'Unsafe revision candidate directory');
  return true;
}

async function candidateNamespace(session, create) {
  if (!await checkedDirectory(session, false)) return undefined;
  let path = session;
  for (const part of namespace) {
    path = join(path, part);
    if (!await checkedDirectory(path, create)) return undefined;
  }
  return path;
}

async function retryWin32Fs(operation) {
  for (let attempt = 0; ; attempt++) {
    try {
      return await operation();
    } catch (error) {
      if (process.platform !== 'win32' || !transientWin32Codes.has(error?.code) || attempt >= 4) throw error;
      await delay(25 * (attempt + 1));
    }
  }
}

async function acquireDirectoryWin32(path, slots, { createHost = false } = {}) {
  const identity = await stat(path);
  check(identity?.isDirectory() && await isCanonicalPath(path), 'Revision candidate namespace changed');
  if (slots.includes('host') && !createHost) {
    let hasCandidates = false;
    for await (const entry of await opendir(path)) {
      if (entry.isDirectory() && candidateName.test(entry.name)) {
        hasCandidates = true;
        break;
      }
    }
    if (!hasCandidates) return null;
  }
  const held = [];
  let acquired = false;
  try {
    for (const slot of slots) {
      const result = acquireWin32Semaphore(candidateLockName(path, slot), path);
      if (result.busy) return undefined;
      held.push(result.handle);
    }
    check(sameInode(await stat(path), identity) && await isCanonicalPath(path), 'Revision candidate namespace changed');
    acquired = true;
    return {
      basePath: path,
      async release() {
        let failure;
        for (const handle of held.toReversed()) {
          try { releaseWin32Semaphore(handle); } catch (error) { failure ??= error; }
        }
        if (failure) throw failure;
      },
    };
  } finally {
    if (!acquired) {
      for (const handle of held.toReversed()) {
        try { releaseWin32Semaphore(handle); } catch { /* Best-effort rollback. */ }
      }
    }
  }
}

async function acquireDirectoryPosix(path, slots, { createHost = false, createWorker = false } = {}) {
  const directory = await open(path, directoryFlags);
  const basePath = process.platform === 'linux' ? `/proc/self/fd/${directory.fd}` : path;
  const held = [];
  let acquired = false;
  try {
    const identity = await directory.stat({ bigint: true });
    check(sameInode(await stat(path), identity) && await isCanonicalPath(path),
      'Revision candidate namespace changed');
    const baseFlags = constants.O_RDWR | constants.O_NOFOLLOW | constants.O_NONBLOCK;
    for (const slot of slots) {
      const fileName = slotFiles[slot];
      const shouldCreate = slot === 'host' ? createHost : createWorker;
      const handle = await open(join(basePath, fileName), shouldCreate ? baseFlags | constants.O_CREAT : baseFlags, 0o600);
      held.push({ fileName, handle });
      const lockIdentity = await handle.stat({ bigint: true });
      check(lockIdentity.isFile() && lockIdentity.nlink === 1n, 'Unsafe revision candidate lock');
      held[held.length - 1].identity = lockIdentity;
      if (!await tryPosixExclusiveLock(handle.fd)) return undefined;
    }
    check(sameInode(await stat(path), identity) && await isCanonicalPath(path), 'Revision candidate namespace changed');
    for (const { fileName, identity: lockIdentity } of held) {
      check(sameInode(await stat(join(basePath, fileName)), lockIdentity), 'Revision candidate namespace changed');
    }
    acquired = true;
    return {
      basePath,
      async release() {
        let failure;
        for (const { handle } of held.toReversed()) {
          try { await handle.close(); } catch (error) { failure ??= error; }
        }
        try { await directory.close(); } catch (error) { failure ??= error; }
        if (failure) throw failure;
      },
    };
  } finally {
    if (!acquired) {
      for (const { handle } of held.toReversed()) {
        try { await handle.close(); } catch { /* Best-effort cleanup. */ }
      }
      await directory.close();
    }
  }
}

function acquireDirectory(path, slots, options) {
  if (process.platform === 'win32') return acquireDirectoryWin32(path, slots, options);
  return acquireDirectoryPosix(path, slots, options);
}

function candidateFileName(name) {
  return /^(0|[1-9]\d{0,2})$/u.test(name) && Number(name) < MAX_NATIVE_LEASES;
}

async function removeCandidate(scope, name, expected) {
  const path = join(scope.basePath, name);
  const entry = await stat(path);
  if (!entry?.isDirectory() || entry.isSymbolicLink() || (expected && !sameInode(entry, expected))) return false;
  if (process.platform === 'win32') {
    check(await isCanonicalPath(path), 'Unsafe revision candidate directory');
    const files = [];
    for await (const child of await opendir(path)) {
      check(files.length < MAX_NATIVE_LEASES && candidateFileName(child.name) && child.isFile() && !child.isSymbolicLink(),
        'Unexpected file in revision candidate directory');
      const file = join(path, child.name);
      const info = await stat(file);
      check(info?.isFile() && !info.isSymbolicLink() && info.nlink === 1n, 'Unsafe revision candidate output file');
      files.push(file);
    }
    if (!sameInode(await stat(path), entry)) return false;
    for (const file of files) await retryWin32Fs(() => unlink(file));
    await retryWin32Fs(() => rmdir(path));
    return true;
  }
  const directory = await open(path, directoryFlags);
  try {
    const identity = await directory.stat({ bigint: true });
    if (!sameInode(entry, identity)) return false;
    const basePath = process.platform === 'linux' ? `/proc/self/fd/${directory.fd}` : path;
    if (process.platform !== 'linux') check(await isCanonicalPath(path), 'Unsafe revision candidate directory');
    const files = [];
    for await (const child of await opendir(basePath)) {
      // Only remove the flat, numbered output files produced by the worker.
      check(files.length < MAX_NATIVE_LEASES && candidateFileName(child.name) && child.isFile(),
        'Unexpected file in revision candidate directory');
      const file = join(basePath, child.name);
      const info = await stat(file);
      check(info?.isFile() && info.nlink === 1n, 'Unsafe revision candidate output file');
      files.push(file);
    }
    if (!sameInode(await stat(path), identity)) return false;
    for (const file of files) await unlink(file);
    await rmdir(path);
    return true;
  } finally {
    await directory.close();
  }
}

/** Hold the Host lock slot for the whole preparation and commit window. */
export async function createRevisionCandidateDirectory(parentRolloutPath) {
  check(isSupportedPlatform(process.platform), '当前操作系统不支持保护编辑候选文件。');
  const session = nativeSessionDirectory(parentRolloutPath);
  const root = await candidateNamespace(session, true);
  check(root, 'Native session directory is missing');
  const version = join(root, versionName);
  await checkedDirectory(version, true);
  const scope = await acquireDirectory(version, ['host'], { createHost: true });
  check(scope, '正在清理上次编辑的候选文件，请稍后重试。');
  try {
    const name = `candidate-${randomUUID()}`;
    const path = join(scope.basePath, name);
    await mkdir(path, { mode: 0o700 });
    const identity = await stat(path);
    let disposing;
    return {
      directory: join(version, name),
      dispose() {
        return disposing ??= removeCandidate(scope, name, identity).finally(() => scope.release());
      },
    };
  } catch (error) {
    await scope.release();
    throw error;
  }
}

/** A detached worker holds the Worker lock slot so cleanup waits even after Host termination. */
export async function withRevisionCandidateOutput(output, operation) {
  const directory = typeof output === 'string' ? dirname(output) : '';
  if (!basename(directory).startsWith('candidate-')) return operation();
  check(isAbsolute(output) && isSamePath(resolve(output), output) && candidateName.test(basename(directory))
    && candidateFileName(basename(output)), 'Invalid revision candidate output');
  const version = dirname(directory);
  const suffix = join(...namespace, versionName);
  check(version.length > suffix.length + 1 && isSamePath(version.slice(-suffix.length - 1), `${sep}${suffix}`),
    'Invalid revision candidate namespace');
  const session = version.slice(0, -suffix.length - 1);
  check(isSamePath(await candidateNamespace(session, false), dirname(version)), 'Revision candidate namespace disappeared');
  if (process.platform !== 'win32') {
    check((await stat(join(version, slotFiles.host)))?.isFile(), 'Revision candidate namespace disappeared');
  }
  const scope = await acquireDirectory(version, ['worker'], { createWorker: true });
  check(scope, 'Revision candidate cleanup is in progress');
  try {
    const anchored = join(scope.basePath, basename(directory));
    check((await stat(anchored))?.isDirectory() && isSamePath(await realpath(anchored), directory),
      'Revision candidate directory disappeared');
    return await operation();
  } finally {
    await scope.release();
  }
}

async function sweepRevisionCandidates(parentRolloutPath) {
  const session = nativeSessionDirectory(parentRolloutPath);
  const root = await candidateNamespace(session, false);
  if (root === undefined) return { removed: 0, busy: false };
  let removed = 0;
  let busy = false;
  let failure;
  for await (const version of await opendir(root)) {
    if (!version.isDirectory() || !versionPattern.test(version.name)) continue;
    const path = join(root, version.name);
    const scope = await acquireDirectory(path, ['host', 'worker'], { createHost: false, createWorker: true }).catch(error => {
      if (error.code === 'ENOENT') return null;
      throw error;
    });
    if (scope === null) continue;
    if (!scope) {
      busy = true;
      continue;
    }
    try {
      for await (const entry of await opendir(scope.basePath)) {
        if (!entry.isDirectory() || !candidateName.test(entry.name)) continue;
        try {
          if (await removeCandidate(scope, entry.name)) removed++;
        } catch (error) {
          failure ??= error;
        }
      }
    } finally {
      await scope.release();
    }
  }
  if (failure) throw failure;
  return { removed, busy };
}

function retryCleanup(path, onError) {
  const key = pathKey(path);
  if (pendingRetries.has(key) || pendingRetries.size >= MAX_NATIVE_LEASES) return;
  pendingRetries.add(key);
  function schedule(attempt) {
    if (attempt === retryDelays.length) {
      pendingRetries.delete(key);
      return;
    }
    const timer = setTimeout(async () => {
      try {
        const { busy } = await sweepRevisionCandidates(path);
        if (busy) schedule(attempt + 1);
        else pendingRetries.delete(key);
      } catch (error) {
        pendingRetries.delete(key);
        onError?.(error);
      }
    }, retryDelays[attempt]);
    timer.unref();
  }
  schedule(0);
}

/** Reclaim closed preparations in the plugin namespace without opening any rollout. */
export async function cleanupRevisionCandidates(parentRolloutPath, { retryBusy = false, onError } = {}) {
  if (!isSupportedPlatform(process.platform)) return 0;
  const { removed, busy } = await sweepRevisionCandidates(parentRolloutPath);
  if (busy && retryBusy) retryCleanup(parentRolloutPath, onError);
  return removed;
}
