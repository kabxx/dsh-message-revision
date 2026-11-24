import { execFile, spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { constants, realpathSync } from 'node:fs';
import { access, lstat, realpath, rename } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, isAbsolute, join, relative, resolve, toNamespacedPath } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';

export const SUPPORTED_PLATFORMS = Object.freeze(new Set(['linux', 'darwin', 'win32']));
export const JOB_OBJECT_LIMIT_PROCESS_MEMORY = 0x00000100;
export const JOB_OBJECT_LIMIT_JOB_MEMORY = 0x00000200;
export const JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000;

const flockConflict = 73;
const moveReplaceExisting = 0x00000001;
const moveWriteThrough = 0x00000008;
const jobObjectExtendedLimitInfo = 9;
const processSetQuota = 0x0100;
const processTerminate = 0x0001;
const processQueryLimitedInformation = 0x1000;
const waitObject0 = 0;
const waitTimeout = 0x00000102;
const stillActive = 259;
const systemBootEnvironmentInfo = 90;
const errorFileNotFound = 2;
const errorPathNotFound = 3;
const errorAccessDenied = 5;
const errorNotSameDevice = 17;
const errorSharingViolation = 32;
const errorFileExists = 80;
const errorInvalidParameter = 87;
const errorInvalidName = 123;
const errorAlreadyExists = 183;
const transientMoveCodes = new Set(['EBUSY', 'EPERM', 'EACCES']);
const moveRetryDelays = [25, 50, 100, 200, 400];

let cachedHostBase;
let cachedFlockPromise;
let cachedFlockExecutablePromise;
let cachedKoffi;
let cachedWin32;
let cachedWin32BootId;
let cachedDarwinBoot;

export function isSupportedPlatform(platform = process.platform) {
  return SUPPORTED_PLATFORMS.has(platform);
}

export function isSamePath(left, right) {
  if (typeof left !== 'string' || typeof right !== 'string') return false;
  return process.platform === 'win32' ? left.toLowerCase() === right.toLowerCase() : left === right;
}

export function pathKey(path) {
  const normalized = resolve(path);
  return process.platform === 'win32' ? normalized.toLowerCase() : normalized;
}

export async function isCanonicalPath(path) {
  if (typeof path !== 'string' || path.length === 0) return false;
  return isSamePath(await realpath(path), path);
}

export function rolloutStorageRoot(path) {
  return dirname(dirname(dirname(path)));
}

/** Resolve only the configured Windows storage root, keeping the DSH log layout below it intact. */
export async function resolveRolloutPath(path) {
  if (process.platform !== 'win32') return path;
  if (!isAbsolute(path) || !isSamePath(resolve(path), path)) return undefined;
  const actual = await realpath(path);
  if (isSamePath(actual, path)) return path;
  const session = dirname(path);
  const project = dirname(session);
  const root = rolloutStorageRoot(path);
  const [physicalRoot, projectStat, sessionStat, fileStat] = await Promise.all([
    realpath(root), lstat(project), lstat(session), lstat(path),
  ]);
  if (!projectStat.isDirectory() || !sessionStat.isDirectory() || !fileStat.isFile()
    || !isSamePath(actual, join(physicalRoot, relative(root, path)))) return undefined;
  return actual;
}

export function openReadOnlyFlags() {
  return constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0) | (process.platform === 'win32' ? 0 : (constants.O_NONBLOCK ?? 0));
}

function candidateResolveAnchors() {
  const anchors = [];
  const seen = new Set();
  const add = value => {
    if (typeof value !== 'string' || value.length === 0 || seen.has(value)) return;
    seen.add(value);
    anchors.push(value);
  };
  add(cachedHostBase);
  if (process.env.DSH_MESSAGE_REVISION_HOST_BASE) add(process.env.DSH_MESSAGE_REVISION_HOST_BASE);
  if (process.env.DSH_HOST_ROOT) add(join(process.env.DSH_HOST_ROOT, 'package.json'));
  if (typeof process.resourcesPath === 'string' && process.resourcesPath.length > 0) {
    add(join(process.resourcesPath, 'app.asar', 'dsh', 'package.json'));
    add(join(process.resourcesPath, 'app', 'dsh', 'package.json'));
  }
  if (typeof process.argv[1] === 'string' && process.argv[1].length > 0) {
    const argvPaths = [resolve(process.argv[1])];
    try {
      const realArgv = realpathSync(process.argv[1]);
      if (!isSamePath(realArgv, argvPaths[0])) argvPaths.push(realArgv);
    } catch { /* Non-filesystem argv[1] such as -e or virtual entry. */ }
    for (const startPath of argvPaths) {
      let current = startPath;
      add(current);
      for (let depth = 0; depth < 6; depth++) {
        const parent = dirname(current);
        if (parent === current) break;
        current = parent;
        add(join(current, 'package.json'));
        add(join(current, 'dsh', 'package.json'));
      }
    }
  }
  add(fileURLToPath(import.meta.url));
  return anchors;
}

function expandHostAnchors(anchor) {
  const expanded = [];
  const seen = new Set();
  const queue = [anchor];
  const hops = ['@deepseek-ai/dsh-session-persistence-jsonl/package.json', '@deepseek-ai/dsh-base/package.json'];
  while (queue.length > 0) {
    const current = queue.shift();
    if (!current || seen.has(current)) continue;
    seen.add(current);
    expanded.push(current);
    let req;
    try { req = createRequire(current); } catch { continue; }
    for (const dep of hops) {
      try {
        const next = req.resolve(dep);
        if (!seen.has(next)) queue.push(next);
      } catch { /* Dependency not reachable from this anchor. */ }
    }
  }
  return expanded;
}

export function hostResolveBase() {
  return cachedHostBase ?? process.env.DSH_MESSAGE_REVISION_HOST_BASE;
}

export function workerEnv(extra = {}) {
  const env = { ...process.env, ...extra };
  delete env.NODE_OPTIONS;
  if (process.versions.electron) env.ELECTRON_RUN_AS_NODE = '1';
  const base = hostResolveBase();
  if (base) env.DSH_MESSAGE_REVISION_HOST_BASE = base;
  return env;
}

export function loadHostFlock() {
  if (cachedFlockPromise !== undefined) return cachedFlockPromise;
  cachedFlockPromise = (async () => {
    for (const rootAnchor of candidateResolveAnchors()) {
      for (const anchor of expandHostAnchors(rootAnchor)) {
        try {
          const resolved = createRequire(anchor).resolve('@deepseek-ai/node-addon-system/flock');
          const mod = await import(pathToFileURL(resolved).href);
          if (typeof mod?.tryLockExclusive === 'function') {
            cachedHostBase ??= anchor;
            return mod.tryLockExclusive;
          }
        } catch { /* Try the next host anchor. */ }
      }
    }
    return null;
  })();
  return cachedFlockPromise;
}

export function loadHostKoffi() {
  if (cachedKoffi !== undefined) return cachedKoffi;
  for (const rootAnchor of candidateResolveAnchors()) {
    for (const anchor of expandHostAnchors(rootAnchor)) {
      try {
        const req = createRequire(anchor);
        const resolved = req.resolve('koffi');
        const mod = req(resolved);
        const koffi = mod?.default ?? mod;
        if (typeof koffi?.load === 'function') {
          cachedHostBase ??= anchor;
          cachedKoffi = koffi;
          return koffi;
        }
      } catch { /* Try the next host anchor. */ }
    }
  }
  cachedKoffi = null;
  return null;
}

async function flockExecutable() {
  cachedFlockExecutablePromise ??= (async () => {
    for (const path of ['/usr/bin/flock', '/bin/flock']) {
      try {
        await access(path, constants.X_OK);
        return path;
      } catch (error) {
        if (error.code !== 'ENOENT' && error.code !== 'EACCES') throw error;
      }
    }
    return null;
  })();
  return cachedFlockExecutablePromise;
}

function tryLinuxFlockCli(executable, descriptor) {
  return new Promise((complete, fail) => {
    // Linux flock is attached to the shared open file description. The helper
    // inherits fd 3. Host ownership continues until its FileHandle closes.
    const child = spawn(executable, ['--exclusive', '--nonblock', '--conflict-exit-code', String(flockConflict), '3'], {
      stdio: ['ignore', 'ignore', 'ignore', descriptor],
    });
    let failure;
    const timeout = setTimeout(() => child.kill('SIGKILL'), 5_000);
    timeout.unref();
    child.once('error', error => { failure = error; });
    child.once('close', (code, signal) => {
      clearTimeout(timeout);
      if (failure) fail(failure);
      else if (code === 0) complete(true);
      else if (code === flockConflict) complete(false);
      else fail(new Error(`无法取得 DSH 会话写锁（flock ${signal ?? code}）。`));
    });
  });
}

/** Acquire a non-blocking POSIX exclusive flock via DSH's native addon (or Linux /usr/bin/flock fallback). */
export async function tryPosixExclusiveLock(descriptor) {
  const nativeFlock = await loadHostFlock();
  if (nativeFlock) {
    try {
      await nativeFlock(descriptor);
      return true;
    } catch (error) {
      if (error?.code === 'EAGAIN' || error?.code === 'EWOULDBLOCK') return false;
      const isFlockSyscallError = error?.code === 'EBADF' || error?.code === 'EINVAL' || error?.code === 'EINTR';
      if (process.platform === 'linux' && !isFlockSyscallError) {
        // The addon loads its native binary lazily on first call; fall back to
        // system flock on Linux if the prebuilt binary is unavailable.
        cachedFlockPromise = Promise.resolve(null);
      } else {
        throw error;
      }
    }
  }
  if (process.platform === 'linux') {
    const executable = await flockExecutable();
    if (executable) return tryLinuxFlockCli(executable, descriptor);
    throw new Error('未找到系统 flock，无法安全获取 DSH 会话写锁。');
  }
  throw new Error('未找到 DSH 原生 flock 模块，无法安全获取会话写锁。');
}

function win32Errno(code) {
  switch (code) {
    case errorFileNotFound:
    case errorPathNotFound: return 'ENOENT';
    case errorAccessDenied: return 'EACCES';
    case errorNotSameDevice: return 'EXDEV';
    case errorSharingViolation: return 'EBUSY';
    case errorFileExists:
    case errorAlreadyExists: return 'EEXIST';
    case errorInvalidParameter:
    case errorInvalidName: return 'EINVAL';
    default: return 'EIO';
  }
}

function win32Error(syscall, win32Code, path, dest) {
  const code = win32Errno(win32Code);
  const error = new Error(`${syscall} ${code} (Win32 ${win32Code}): ${path}${dest ? ` -> ${dest}` : ''}`);
  error.code = code;
  error.errno = win32Code;
  error.syscall = syscall;
  error.path = path;
  if (dest !== undefined) error.dest = dest;
  error.win32Code = win32Code;
  return error;
}

function isNullHandle(handle) {
  return handle === 0 || handle === 0n || handle === null || handle === undefined;
}

function win32Bindings() {
  if (cachedWin32 !== undefined) return cachedWin32;
  const koffi = loadHostKoffi();
  if (!koffi) {
    cachedWin32 = null;
    return null;
  }
  const kernel32 = koffi.load('kernel32.dll');
  const basicLimitStruct = koffi.struct({
    PerProcessUserTimeLimit: 'int64',
    PerJobUserTimeLimit: 'int64',
    LimitFlags: 'uint32',
    MinimumWorkingSetSize: 'uintptr',
    MaximumWorkingSetSize: 'uintptr',
    ActiveProcessLimit: 'uint32',
    Affinity: 'uintptr',
    PriorityClass: 'uint32',
    SchedulingClass: 'uint32',
  });
  const ioCountersStruct = koffi.struct({
    ReadOperationCount: 'uint64',
    WriteOperationCount: 'uint64',
    OtherOperationCount: 'uint64',
    ReadTransferCount: 'uint64',
    WriteTransferCount: 'uint64',
    OtherTransferCount: 'uint64',
  });
  const extendedLimitStruct = koffi.struct({
    BasicLimitInformation: basicLimitStruct,
    IoInfo: ioCountersStruct,
    ProcessMemoryLimit: 'uintptr',
    JobMemoryLimit: 'uintptr',
    PeakProcessMemoryUsed: 'uintptr',
    PeakJobMemoryUsed: 'uintptr',
  });
  cachedWin32 = {
    koffi,
    extendedLimitStruct,
    extendedLimitSize: koffi.sizeof(extendedLimitStruct),
    moveFileExW: kernel32.func('__stdcall', 'MoveFileExW', 'int', ['str16', 'str16', 'uint']),
    createSemaphoreW: kernel32.func('__stdcall', 'CreateSemaphoreW', 'intptr', ['void*', 'int', 'int', 'str16']),
    waitForSingleObject: kernel32.func('__stdcall', 'WaitForSingleObject', 'uint', ['intptr', 'uint']),
    releaseSemaphore: kernel32.func('__stdcall', 'ReleaseSemaphore', 'int', ['intptr', 'int', 'void*']),
    closeHandle: kernel32.func('__stdcall', 'CloseHandle', 'int', ['intptr']),
    getLastError: kernel32.func('__stdcall', 'GetLastError', 'uint', []),
    createJobObjectW: kernel32.func('__stdcall', 'CreateJobObjectW', 'intptr', ['void*', 'str16']),
    setInformationJobObject: kernel32.func('__stdcall', 'SetInformationJobObject', 'int', ['intptr', 'int', 'void*', 'uint']),
    queryInformationJobObject: kernel32.func('__stdcall', 'QueryInformationJobObject', 'int', ['intptr', 'int', 'void*', 'uint', 'void*']),
    assignProcessToJobObject: kernel32.func('__stdcall', 'AssignProcessToJobObject', 'int', ['intptr', 'intptr']),
    terminateJobObject: kernel32.func('__stdcall', 'TerminateJobObject', 'int', ['intptr', 'uint']),
    openProcess: kernel32.func('__stdcall', 'OpenProcess', 'intptr', ['uint', 'int', 'uint']),
    getProcessTimes: kernel32.func('__stdcall', 'GetProcessTimes', 'int', ['intptr', 'void*', 'void*', 'void*', 'void*']),
    getExitCodeProcess: kernel32.func('__stdcall', 'GetExitCodeProcess', 'int', ['intptr', 'void*']),
    ntQuerySystemInformation: (() => {
      try {
        return koffi.load('ntdll.dll').func('__stdcall', 'NtQuerySystemInformation', 'int', ['uint', 'void*', 'uint', 'void*']);
      } catch {
        return null;
      }
    })(),
  };
  return cachedWin32;
}

function requireWin32() {
  const api = win32Bindings();
  if (!api) throw new Error('未找到 DSH 内置 koffi 模块，无法调用 Windows 原生锁与进程接口。');
  return api;
}

/** Exact Win32 semaphore name used by DSH SessionWriteLease (dsh-session-persistence-jsonl/src/win32.ts). */
export function dshSessionLockName(lockPath) {
  return `Local\\dsh-session-lock-${createHash('sha256').update(resolve(lockPath).toLowerCase()).digest('hex')}`;
}

export function candidateLockName(versionDirectory, slot) {
  return `Local\\dsh-msgrev-cand-${createHash('sha256').update(`${resolve(versionDirectory).toLowerCase()}:${slot}`).digest('hex')}`;
}

export function acquireWin32Semaphore(name, path = name) {
  const api = requireWin32();
  const handle = api.createSemaphoreW(null, 1, 1, name);
  if (isNullHandle(handle)) throw win32Error('CreateSemaphoreW', api.getLastError(), path, name);
  const wait = api.waitForSingleObject(handle, 0);
  if (wait === waitObject0) return { busy: false, handle };
  api.closeHandle(handle);
  if (wait === waitTimeout) return { busy: true };
  throw win32Error('WaitForSingleObject', api.getLastError(), path, name);
}

export function releaseWin32Semaphore(handle) {
  const api = requireWin32();
  const released = api.releaseSemaphore(handle, 1, null);
  const releaseError = released === 0 ? api.getLastError() : 0;
  const closed = api.closeHandle(handle);
  const closeError = closed === 0 ? api.getLastError() : 0;
  if (released === 0) throw win32Error('ReleaseSemaphore', releaseError, `handle:${handle}`);
  if (closed === 0) throw win32Error('CloseHandle', closeError, `handle:${handle}`);
}

async function moveWithRetry(from, to, flags) {
  if (process.platform !== 'win32') {
    await rename(from, to);
    return;
  }
  const api = win32Bindings();
  const source = toNamespacedPath(from);
  const destination = toNamespacedPath(to);
  for (let attempt = 0; ; attempt++) {
    try {
      if (api) {
        const ok = api.moveFileExW(source, destination, flags);
        if (ok === 0) throw win32Error('MoveFileExW', api.getLastError(), from, to);
      } else {
        if ((flags & moveReplaceExisting) === 0) {
          const exists = await lstat(to).then(() => true, error => error.code === 'ENOENT' ? false : Promise.reject(error));
          if (exists) {
            const error = new Error(`EEXIST: destination already exists, rename '${from}' -> '${to}'`);
            error.code = 'EEXIST';
            throw error;
          }
        }
        await rename(from, to);
      }
      return;
    } catch (error) {
      if (!transientMoveCodes.has(error?.code) || attempt >= moveRetryDelays.length) throw error;
      await delay(moveRetryDelays[attempt]);
    }
  }
}

export function replaceFileDurable(from, to) {
  return moveWithRetry(from, to, moveReplaceExisting | moveWriteThrough);
}

export function moveEntryDurable(from, to) {
  return moveWithRetry(from, to, moveWriteThrough);
}

export function createWin32MemoryJob(limitBytes) {
  const api = win32Bindings();
  if (!api) return null;
  const handle = api.createJobObjectW(null, null);
  if (isNullHandle(handle)) throw win32Error('CreateJobObjectW', api.getLastError(), 'job');
  const flags = JOB_OBJECT_LIMIT_PROCESS_MEMORY | JOB_OBJECT_LIMIT_JOB_MEMORY | JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
  const buffer = Buffer.alloc(api.extendedLimitSize);
  api.koffi.encode(buffer, api.extendedLimitStruct, {
    BasicLimitInformation: {
      PerProcessUserTimeLimit: 0,
      PerJobUserTimeLimit: 0,
      LimitFlags: flags,
      MinimumWorkingSetSize: 0,
      MaximumWorkingSetSize: 0,
      ActiveProcessLimit: 0,
      Affinity: 0,
      PriorityClass: 0,
      SchedulingClass: 0,
    },
    IoInfo: {
      ReadOperationCount: 0,
      WriteOperationCount: 0,
      OtherOperationCount: 0,
      ReadTransferCount: 0,
      WriteTransferCount: 0,
      OtherTransferCount: 0,
    },
    ProcessMemoryLimit: limitBytes,
    JobMemoryLimit: limitBytes,
    PeakProcessMemoryUsed: 0,
    PeakJobMemoryUsed: 0,
  });
  if (api.setInformationJobObject(handle, jobObjectExtendedLimitInfo, buffer, buffer.length) === 0) {
    const code = api.getLastError();
    api.closeHandle(handle);
    throw win32Error('SetInformationJobObject', code, 'job');
  }
  let closed = false;
  return {
    assign(pid) {
      if (closed || !Number.isSafeInteger(pid) || pid <= 0) return false;
      const processHandle = api.openProcess(processSetQuota | processTerminate, 0, pid);
      if (isNullHandle(processHandle)) return false;
      try {
        return api.assignProcessToJobObject(handle, processHandle) !== 0;
      } finally {
        api.closeHandle(processHandle);
      }
    },
    terminate(exitCode = 1) {
      if (closed) return;
      api.terminateJobObject(handle, exitCode);
    },
    close() {
      if (closed) return;
      closed = true;
      api.closeHandle(handle);
    },
  };
}

export function queryCurrentWin32JobLimits() {
  const api = win32Bindings();
  if (!api) return null;
  const buffer = Buffer.alloc(api.extendedLimitSize);
  if (api.queryInformationJobObject(0, jobObjectExtendedLimitInfo, buffer, buffer.length, null) === 0) return null;
  const decoded = api.koffi.decode(buffer, api.extendedLimitStruct);
  return {
    flags: Number(decoded.BasicLimitInformation.LimitFlags),
    processMemoryLimit: Number(decoded.ProcessMemoryLimit),
    jobMemoryLimit: Number(decoded.JobMemoryLimit),
  };
}

export function readWin32BootId() {
  if (process.platform !== 'win32') return undefined;
  if (cachedWin32BootId) return cachedWin32BootId;
  const api = win32Bindings();
  if (!api?.ntQuerySystemInformation) return undefined;
  const buffer = Buffer.alloc(64);
  const returned = Buffer.alloc(4);
  if (api.ntQuerySystemInformation(systemBootEnvironmentInfo, buffer, buffer.length, returned) !== 0) return undefined;
  if (returned.readUInt32LE(0) < 16) return undefined;
  const id = buffer.subarray(0, 16).toString('hex');
  if (/^0+$/u.test(id)) return undefined;
  cachedWin32BootId = id;
  return id;
}

export function readWin32ProcessCreationTime(pid) {
  if (process.platform !== 'win32') return undefined;
  const api = win32Bindings();
  if (!api || !Number.isSafeInteger(pid) || pid <= 0) return undefined;
  const handle = api.openProcess(processQueryLimitedInformation, 0, pid);
  if (isNullHandle(handle)) {
    const code = api.getLastError();
    if (code === errorInvalidParameter) return null;
    if (code === errorAccessDenied) throw win32Error('OpenProcess', code, `pid:${pid}`);
    return undefined;
  }
  try {
    const exitCode = Buffer.alloc(4);
    if (api.getExitCodeProcess(handle, exitCode) === 0) return undefined;
    if (exitCode.readUInt32LE(0) !== stillActive) return null;
    const created = Buffer.alloc(8);
    const exited = Buffer.alloc(8);
    const kernel = Buffer.alloc(8);
    const user = Buffer.alloc(8);
    if (api.getProcessTimes(handle, created, exited, kernel, user) === 0) return undefined;
    return created.readBigUInt64LE(0).toString();
  } finally {
    api.closeHandle(handle);
  }
}

export function killWin32ProcessTree(pid) {
  if (process.platform !== 'win32' || !Number.isSafeInteger(pid) || pid <= 0) return;
  try {
    const killer = spawn('taskkill', ['/PID', String(pid), '/T', '/F'], {
      stdio: 'ignore',
      windowsHide: true,
    });
    killer.unref();
    killer.on('error', () => {});
  } catch { /* Best-effort process tree termination. */ }
}

function execText(file, args, env) {
  return new Promise((complete, fail) => {
    execFile(file, args, { encoding: 'utf8', timeout: 5_000, env }, (error, stdout, stderr) => {
      if (error) {
        error.stderr = stderr;
        fail(error);
      } else {
        complete(stdout);
      }
    });
  });
}

export async function readDarwinProcessScope() {
  if (process.platform !== 'darwin') return undefined;
  if (cachedDarwinBoot) return cachedDarwinBoot;
  try {
    const uuid = (await execText('/usr/sbin/sysctl', ['-n', 'kern.bootsessionuuid'])).trim();
    if (uuid.length === 0) return undefined;
    cachedDarwinBoot = { darwinBootId: uuid };
    return cachedDarwinBoot;
  } catch {
    return undefined;
  }
}

export async function readDarwinProcessStart(pid) {
  if (process.platform !== 'darwin' || !Number.isSafeInteger(pid) || pid <= 0) return undefined;
  try {
    process.kill(pid, 0);
  } catch (error) {
    if (error.code === 'ESRCH') return null;
    if (error.code !== 'EPERM') throw error;
  }
  try {
    const output = await execText('/bin/ps', ['-o', 'lstart=', '-p', String(pid)], {
      ...process.env,
      LC_ALL: 'C',
      TZ: 'UTC',
    });
    const text = output.trim();
    return text.length > 0 ? text : null;
  } catch (error) {
    if (error.code === 1 && !String(error.stderr ?? '').trim()) return null;
    throw error;
  }
}

export async function readDarwinProcessRssBytes(pid) {
  if (process.platform !== 'darwin' || !Number.isSafeInteger(pid) || pid <= 0) return undefined;
  try {
    const output = await execText('/bin/ps', ['-o', 'rss=', '-p', String(pid)]);
    const trimmed = output.trim();
    if (trimmed.length === 0) return undefined;
    const kib = Number(trimmed);
    return Number.isSafeInteger(kib) && kib >= 0 ? kib * 1024 : undefined;
  } catch {
    return undefined;
  }
}
