import { constants } from 'node:fs';
import { lstat, open, realpath } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join, resolve } from 'node:path';
import {
  acquireWin32Semaphore, dshSessionLockName, isCanonicalPath, isSamePath, isSupportedPlatform,
  releaseWin32Semaphore, tryPosixExclusiveLock,
} from './platform.js';

export const MAX_NATIVE_LEASES = 128;
const reservationsKey = Symbol.for('dsh-message-revision.native-leases');
const slots = globalThis[reservationsKey] ??= { count: 0 };
const lockName = 'session.lock';
const logName = /^session\.v[34]\.jsonl(?:\.zstd)?$/u;

function validate(condition, message) {
  if (!condition) throw new Error(message);
}

function sameInode(first, second) {
  return first !== undefined && second !== undefined && first.dev === second.dev && first.ino === second.ino;
}

async function inodeAt(path) {
  try {
    return await lstat(path, { bigint: true });
  } catch (error) {
    if (error.code === 'ENOENT') return undefined;
    throw error;
  }
}

function alreadyOwned() {
  const error = new Error('会话写锁正被占用，请关闭相关会话后重试。');
  error.name = 'SessionAlreadyOwnedError';
  return error;
}

/** Acquire the same kernel lease DSH rc.5 uses for a materialized rollout on Linux, macOS, and Windows. */
export async function acquireNativeLease(rolloutPath, dshPath = rolloutPath) {
  validate(isSupportedPlatform(process.platform), '当前系统不支持轻量获取 DSH 会话写锁。');
  validate(typeof rolloutPath === 'string' && isAbsolute(rolloutPath) && isSamePath(resolve(rolloutPath), rolloutPath)
    && logName.test(basename(rolloutPath)), '会话日志路径异常。');
  validate(slots.count < MAX_NATIVE_LEASES, '本次编辑涉及的会话过多，无法安全获取写锁。');
  slots.count++;

  const directory = dirname(rolloutPath);
  const lockPath = join(directory, lockName);
  let acquired;
  try {
    const original = await inodeAt(rolloutPath);
    validate(original?.isFile() && await isCanonicalPath(rolloutPath), '会话日志路径异常。');

    if (process.platform === 'win32') {
      const dirIdentity = await inodeAt(directory);
      const dshDirectory = dirname(dshPath);
      const lockPaths = [join(dshDirectory, lockName)];
      // DSH hashes its configured path. Also exclude writers using the target directly.
      if (!isSamePath(lockPaths[0], lockPath)) lockPaths.push(lockPath);
      async function sameDirectory() {
        return sameInode(await inodeAt(directory), dirIdentity) && await isCanonicalPath(directory)
          && (lockPaths.length === 1 || isSamePath(await realpath(dshDirectory), directory));
      }
      validate(dirIdentity?.isDirectory() && await sameDirectory(), '会话目录已变化。');
      const held = [];
      function releaseAll() {
        let failure;
        for (const handle of held.reverse()) {
          try { releaseWin32Semaphore(handle); } catch (error) { failure ??= error; }
        }
        if (failure) throw failure;
      }
      try {
        for (const path of lockPaths) {
          const semaphore = acquireWin32Semaphore(dshSessionLockName(path), path);
          if (semaphore.busy) throw alreadyOwned();
          held.push(semaphore.handle);
        }
        validate(await sameDirectory(), '会话目录已变化。');
        validate(sameInode(await inodeAt(rolloutPath), original), '会话日志文件已变化。');
        let release;
        acquired = {
          async assertHeld() {
            validate(!release && await sameDirectory(), '会话写锁已失效。');
          },
          close() {
            return release ??= Promise.resolve().then(releaseAll).finally(() => { slots.count--; });
          },
        };
        return acquired;
      } finally {
        if (!acquired) releaseAll();
      }
    }

    const dir = await open(directory, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
    try {
      const dirIdentity = await dir.stat({ bigint: true });
      validate(sameInode(await inodeAt(directory), dirIdentity)
        && await isCanonicalPath(directory), '会话目录已变化。');
      const openPath = process.platform === 'linux' ? `/proc/self/fd/${dir.fd}/${lockName}` : lockPath;
      for (let attempt = 0; attempt < 3; attempt++) {
        const handle = await open(openPath,
          constants.O_WRONLY | constants.O_CREAT | constants.O_NONBLOCK | constants.O_NOFOLLOW, 0o600);
        try {
          const identity = await handle.stat({ bigint: true });
          if (identity.isFile() && identity.nlink === 0n) continue;
          validate(identity.isFile() && identity.nlink === 1n, '会话写锁文件异常。');
          if (!await tryPosixExclusiveLock(handle.fd)) throw alreadyOwned();
          validate(sameInode(await inodeAt(directory), dirIdentity)
            && await isCanonicalPath(directory), '会话目录已变化。');
          validate(sameInode(await inodeAt(rolloutPath), original), '会话日志文件已变化。');
          if (!sameInode(await inodeAt(lockPath), identity)) continue;

          let release;
          acquired = {
            // Check the canonical directory and lock inode before each mutation.
            async assertHeld() {
              validate(!release && sameInode(await inodeAt(directory), dirIdentity)
                && await isCanonicalPath(directory)
                && sameInode(await inodeAt(lockPath), identity), '会话写锁已失效。');
            },
            close() {
              return release ??= handle.close().finally(() => { slots.count--; });
            },
          };
          return acquired;
        } finally {
          if (!acquired) await handle.close();
        }
      }
      throw alreadyOwned();
    } finally {
      try {
        await dir.close();
      } catch (error) {
        await acquired?.close();
        throw error;
      }
    }
  } finally {
    if (!acquired) slots.count--;
  }
}
