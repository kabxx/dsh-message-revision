import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import {
  createWin32MemoryJob, isSupportedPlatform, killWin32ProcessTree, loadHostFlock,
  readDarwinProcessRssBytes, workerEnv,
} from './platform.js';

const entry = fileURLToPath(new URL('./rollout-worker.js', import.meta.url));
const maxMessageBytes = 8 * 1024 * 1024;
const defaultTimeout = 180_000;
const win32JobMemoryBytes = process.versions.electron ? 384 * 1024 * 1024 : 256 * 1024 * 1024;
const darwinRssLimitBytes = process.versions.electron ? 384 * 1024 * 1024 : 256 * 1024 * 1024;
const darwinWatchdogIntervalMs = 150;
const processKey = Symbol.for('dsh-message-revision.rollout-process');
const userFacingReasons = Object.freeze({
  busy: '回复生成中，请先停止或等待完成。',
  'queue-pending': '会话仍有排队消息，请等队列清空后再试。',
  'non-independent-turn': '此消息没有可独立回退的对话轮次。',
  'inherited-duplicate': '无法替换继承历史中的同一条消息。',
});

export class RolloutWorkerError extends Error {
  constructor(message, options, reason) {
    super(message, options);
    this.name = 'RolloutWorkerError';
    if (reason !== undefined) this.reason = reason;
  }
}

function startWorker(command, args, request, {
  signal,
  timeout,
  detached = true,
  windowsHide = false,
  beforeRequest,
  watchChild,
  onTerminate,
  onClose,
}) {
  return new Promise(resolve => {
    const child = spawn(command, args, {
      stdio: ['pipe', 'pipe', 'ignore'],
      detached,
      windowsHide,
      env: workerEnv(),
    });
    const output = [];
    let size = 0;
    let ready = false;
    let error;
    let closed = false;
    let stopWatch;
    const terminate = reason => {
      error ??= reason;
      try { onTerminate?.(child); } catch { /* Fall through to direct signal. */ }
      if (child.pid) {
        if (detached && process.platform !== 'win32') {
          try { process.kill(-child.pid, 'SIGKILL'); } catch { child.kill('SIGKILL'); }
        } else {
          try { child.kill('SIGKILL'); } catch { /* Process may have exited. */ }
        }
      }
    };
    const abort = () => terminate(new RolloutWorkerError('日志准备已取消。', undefined, 'aborted'));
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) abort();
    const deadline = setTimeout(() => terminate(new RolloutWorkerError('日志准备超时，原会话已保留。', undefined, 'timeout')), timeout);
    child.on('error', cause => { error ??= cause; });
    child.stdout.on('data', chunk => {
      if (error) return;
      size += chunk.length;
      if (size > maxMessageBytes) {
        terminate(new RolloutWorkerError('日志准备结果超出大小上限。'));
        return;
      }
      output.push(chunk);
      if (output[0].length >= 3) ready = output[0].subarray(0, 3).toString() === 'R1\n';
      else if (size >= 3) ready = Buffer.concat(output, size).subarray(0, 3).toString() === 'R1\n';
    });
    child.stdin.on('error', () => {});
    let launchOk = true;
    if (beforeRequest && !error) {
      try {
        launchOk = beforeRequest(child) !== false;
      } catch (cause) {
        error ??= cause;
        launchOk = false;
      }
    }
    if (!launchOk) {
      terminate(error ?? new Error('无法将日志准备进程加入隔离容器。'));
    } else if (!error) {
      stopWatch = watchChild?.(child, terminate);
      child.stdin.end(request);
    }
    child.on('close', (code, exitSignal) => {
      if (closed) return;
      closed = true;
      clearTimeout(deadline);
      stopWatch?.();
      signal?.removeEventListener('abort', abort);
      try { onClose?.(); } catch { /* Ignore post-close cleanup errors. */ }
      // Timed-out and canceled requests must not start a fallback process.
      if (error instanceof RolloutWorkerError) return resolve({ error, ready: true });
      if (error || code !== 0 || !ready) {
        return resolve({ ready, error: new RolloutWorkerError(ready
          ? '日志准备进程达到资源上限或意外退出，原会话已保留。'
          : '无法启动受保护的日志准备进程。', { cause: error, ...(exitSignal && { exitSignal }) }) });
      }
      try {
        const result = JSON.parse(Buffer.concat(output, size).subarray(3).toString('utf8'));
        if (result.ok !== true) {
          const reason = typeof result.reason === 'string' && Object.hasOwn(userFacingReasons, result.reason)
            ? result.reason : undefined;
          return resolve({ ready, error: new RolloutWorkerError(
            reason === undefined ? result.message : userFacingReasons[reason], undefined, reason,
          ) });
        }
        resolve({ ready, value: result.value });
      } catch (cause) {
        resolve({ ready, error: new RolloutWorkerError('无法读取日志准备结果。', { cause }) });
      }
    });
  });
}

async function runLinuxWorker(node, request, { signal, timeout }) {
  await loadHostFlock();
  const launchAt = Date.now();
  const result = await startWorker('systemd-run', [
    '--user', '--scope', '--quiet', '--collect', `--unit=dsh-message-revision-${randomUUID()}`,
    '-p', 'MemoryMax=256M', '-p', 'MemorySwapMax=0', '--',
    'prlimit', '--core=0', '--', ...node, entry, 'dsh-rollout-worker', 'cgroup',
  ], request, { signal, timeout });
  if (result.ready) {
    if (result.error) throw result.error;
    return result.value;
  }
  const remaining = timeout - (Date.now() - launchAt);
  if (remaining <= 0 || signal?.aborted) throw result.error;
  // The V8 code range is reserved address space. A larger RLIMIT_AS is
  // required to boot Node even with a small old generation.
  const fallback = await startWorker('prlimit', [
    '--as=1073741824', '--core=0', '--', ...node, '--jitless',
    entry, 'dsh-rollout-worker', 'rlimit',
  ], request, { signal, timeout: remaining });
  if (fallback.error) throw fallback.error;
  return fallback.value;
}

async function runWin32Worker(node, request, { signal, timeout }) {
  const launchAt = Date.now();
  let job = null;
  try {
    job = createWin32MemoryJob(win32JobMemoryBytes);
  } catch {
    job = null;
  }
  if (job) {
    const result = await startWorker(node[0], [
      ...node.slice(1), entry, 'dsh-rollout-worker', 'win32-job',
    ], request, {
      signal,
      timeout,
      detached: false,
      windowsHide: true,
      beforeRequest: child => child.pid !== undefined && job.assign(child.pid),
      onTerminate: () => job.terminate(1),
      onClose: () => job.close(),
    });
    if (result.ready) {
      if (result.error) throw result.error;
      return result.value;
    }
    const remaining = timeout - (Date.now() - launchAt);
    if (remaining <= 0 || signal?.aborted) throw result.error;
    const fallback = await startWorker(node[0], [
      ...node.slice(1), entry, 'dsh-rollout-worker', 'win32-watchdog',
    ], request, {
      signal,
      timeout: remaining,
      detached: false,
      windowsHide: true,
      onTerminate: child => killWin32ProcessTree(child.pid),
    });
    if (fallback.error) throw fallback.error;
    return fallback.value;
  }
  const fallback = await startWorker(node[0], [
    ...node.slice(1), entry, 'dsh-rollout-worker', 'win32-watchdog',
  ], request, {
    signal,
    timeout,
    detached: false,
    windowsHide: true,
    onTerminate: child => killWin32ProcessTree(child.pid),
  });
  if (fallback.error) throw fallback.error;
  return fallback.value;
}

async function runDarwinWorker(node, request, { signal, timeout }) {
  // Warm up host flock anchor before spawning so the worker inherits DSH_MESSAGE_REVISION_HOST_BASE.
  await loadHostFlock();
  const result = await startWorker(node[0], [
    ...node.slice(1), entry, 'dsh-rollout-worker', 'darwin-rlimit',
  ], request, {
    signal,
    timeout,
    detached: true,
    watchChild(child, terminate) {
      let stopped = false;
      let checking = false;
      const timer = setInterval(async () => {
        if (stopped || checking || !child.pid) return;
        checking = true;
        try {
          const rss = await readDarwinProcessRssBytes(child.pid);
          if (!stopped && rss !== undefined && rss > darwinRssLimitBytes) {
            terminate(new RolloutWorkerError('日志准备进程达到资源上限或意外退出，原会话已保留。'));
          }
        } finally {
          checking = false;
        }
      }, darwinWatchdogIntervalMs);
      timer.unref();
      return () => {
        stopped = true;
        clearInterval(timer);
      };
    },
  });
  if (result.error) throw result.error;
  return result.value;
}

/** Process exactly one bounded preparation task per Host process. */
export async function runRolloutWorker(operation, input, { signal, timeout = defaultTimeout } = {}) {
  if (globalThis[processKey]) throw new RolloutWorkerError('已有会话正在准备编辑，请稍后重试。', undefined, 'worker-busy');
  if (signal?.aborted) throw new RolloutWorkerError('日志准备已取消。', undefined, 'aborted');
  if (!isSupportedPlatform(process.platform)) {
    throw new RolloutWorkerError('当前系统无法启动受内存约束的日志准备进程。');
  }
  if (!Number.isSafeInteger(timeout) || timeout <= 0 || timeout > defaultTimeout) {
    throw new RolloutWorkerError('日志准备时限无效。');
  }
  const request = JSON.stringify({ operation, input });
  if (Buffer.byteLength(request) > maxMessageBytes) throw new RolloutWorkerError('日志准备参数超出大小上限。');
  globalThis[processKey] = true;
  try {
    const node = [process.execPath, '--max-old-space-size=128'];
    if (process.platform === 'linux') return await runLinuxWorker(node, request, { signal, timeout });
    if (process.platform === 'win32') return await runWin32Worker(node, request, { signal, timeout });
    return await runDarwinWorker(node, request, { signal, timeout });
  } finally {
    globalThis[processKey] = false;
  }
}
