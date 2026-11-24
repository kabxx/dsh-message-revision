import { writeSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { getHeapStatistics } from 'node:v8';
import {
  JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOB_OBJECT_LIMIT_PROCESS_MEMORY, queryCurrentWin32JobLimits,
} from './platform.js';
import { withRevisionCandidateOutput } from './revision-candidates.js';
import { isProvisionalResumeMarker } from './rollback-events.js';
import {
  checkRolloutIdentity, probeSeededSubagentTail, readRolloutLines, readRolloutLinesFromOffset,
  requireRollout, writeRolloutCandidate,
} from './rollout-stream.js';

export const maxMessageBytes = 8 * 1024 * 1024;
const maxCatalogBytes = 4 * 1024 * 1024;
const maxQueue = 4096;
const maxCopies = 8192;

const createStats = () => ({ bytes: 0, records: 0, maxRecordBytes: 0 });

function measure(stats, line) {
  stats.bytes += line.length;
  stats.records++;
  stats.maxRecordBytes = Math.max(stats.maxRecordBytes, line.length);
}

function createPlanTracker(messageId) {
  const queues = { 'next-turn': [], 'next-step': [] };
  let turn;
  let selected = false;
  let checkpoint;
  let latestTime = 0;
  let copies = [];
  let prefix;
  return {
    accept(event, stats, framePrefix) {
      const { seq, data, time } = event;
      // Sequence numbers order the parent log, but comparing parent and child
      // logs requires the wall clock to agree on both sides of this cutoff.
      if (checkpoint && time < checkpoint.time) checkpoint.timestampAmbiguous = true;
      if (turn?.input && time < turn.time) turn.timestampAmbiguous = true;
      const earlierTime = latestTime;
      latestTime = Math.max(latestTime, time);
      if (event.type === 'agent/inbox/spliced') {
        const queue = queues[data.target];
        const removed = data.removedCount ?? 0;
        requireRollout(Array.isArray(queue) && Number.isSafeInteger(data.start) && data.start >= 0
          && data.start <= queue.length && Number.isSafeInteger(removed) && removed >= 0
          && removed <= queue.length - data.start && Array.isArray(data.inserted)
          && queue.length - removed + data.inserted.length <= maxQueue,
        '会话消息队列无效或超出检查上限。');
        const inserted = data.inserted.map((message, offset) => {
          requireRollout(typeof message?.id === 'string' && message.id.length <= 2048, '消息记录无效。');
          if (message.id !== messageId) return { id: message.id };
          requireRollout(copies.length < maxCopies, '同一消息的历史入队记录超出检查上限。');
          const item = { index: seq, offset, kind: message.source?.kind };
          if (copies.length === 0) prefix = framePrefix;
          copies.push(item);
          return { id: message.id, ...item };
        });
        queue.splice(data.start, removed, ...inserted);
      } else if (event.type === 'turn/start') {
        requireRollout(turn === undefined, '会话日志中存在未关闭的对话轮次。');
        const candidate = queues['next-turn'][0];
        turn = { number: data.turn, index: seq, time, queuedCount: queues['next-turn'].length,
          hasSteering: queues['next-step'].length > 0, input: candidate?.id === messageId ? candidate : undefined,
          timestampAmbiguous: earlierTime > time, stats: candidate?.id === messageId ? { ...stats } : undefined };
      } else if (event.type === 'turn/end') {
        requireRollout(turn?.number === data.turn, '会话日志的轮次边界不完整。');
        turn = undefined;
      } else if (event.type === 'user/message' && data?.id === messageId && data.source?.kind === 'user') {
        requireRollout(!selected, '日志中找到了重复的用户消息。');
        selected = true;
        checkpoint = turn;
        if (checkpoint) copies = copies.filter(item => item.index < checkpoint.index);
      }
    },
    finish(inheritedCount) {
      if (turn !== undefined) throw Object.assign(new Error('回复生成中，请先停止或等待完成。'), { reason: 'busy' });
      if (!(selected && checkpoint?.input && checkpoint.index > inheritedCount
        && checkpoint.input.index >= inheritedCount && !checkpoint.hasSteering && checkpoint.queuedCount === 1)) {
        throw Object.assign(new Error('此消息没有可独立回退的对话轮次。'), { reason: 'non-independent-turn' });
      }
      if (queues['next-turn'].length !== 0 || queues['next-step'].length !== 0) {
        throw Object.assign(new Error('会话仍有排队消息，请等队列清空后再试。'), { reason: 'queue-pending' });
      }
      const retainedInputs = copies.filter(item => item.index < checkpoint.index);
      if (!retainedInputs.every(item => item.index >= inheritedCount && item.kind === 'user')) {
        throw Object.assign(new Error('无法替换继承历史中的同一条消息。'), { reason: 'inherited-duplicate' });
      }
      const { index, offset } = checkpoint.input;
      return { plan: { cut: checkpoint.index, time: checkpoint.time, input: { index, offset },
        ...(checkpoint.timestampAmbiguous && { timestampAmbiguous: true }),
        retainedInputs: retainedInputs.map(({ index, offset }) => ({ index, offset })) }, prefixStats: checkpoint.stats, prefix };
    },
  };
}

async function scanSeededSubagentTail({ path, sessionId, cutoff }) {
  const probe = await probeSeededSubagentTail(path, sessionId, cutoff);
  if (!probe) return null;
  const tailStats = createStats();
  const catalog = [];
  let catalogBytes = 0;
  let seen = 0;
  let sawDescriptor = false;
  let inTurn;
  for await (const { record, line } of readRolloutLinesFromOffset(path, probe.tailOffset, probe.startSeq, probe.identity)) {
    if (seen === 0) {
      if (record.type === 'subagent/descriptor') sawDescriptor = true;
      else if (probe.header.agentPreset === undefined || record.type !== 'agent-preset/selected'
        || record.data?.agentPreset !== probe.header.agentPreset) return null;
    } else if (seen === 1 && !sawDescriptor) {
      if (record.type === 'subagent/descriptor') sawDescriptor = true;
      else return null;
    }
    if (record.type === 'session/end-seed' && record.data?.inherited === true) return null;
    if (record.time >= cutoff) return null;
    if (record.type === 'subagent/catalog') {
      catalogBytes += line.length;
      requireRollout(catalogBytes <= maxCatalogBytes, '子智能体登记超出单次检查上限。');
      catalog.push(record);
    }
    if (record.type === 'turn/start') {
      if (inTurn !== undefined || record.data?.turn === undefined) return null;
      inTurn = record.data.turn;
    } else if (record.type === 'turn/end') {
      if (inTurn === undefined || inTurn !== record.data?.turn) return null;
      inTurn = undefined;
    }
    measure(tailStats, line);
    seen++;
  }
  if (!sawDescriptor || inTurn !== undefined) return null;
  const totalRecords = probe.startSeq + seen;
  const stats = {
    bytes: probe.seedFrameBytes + tailStats.bytes,
    records: totalRecords,
    maxRecordBytes: tailStats.maxRecordBytes,
  };
  return {
    header: probe.header,
    identity: probe.identity,
    catalog,
    stats,
    inheritedCount: probe.inheritedCount,
    prefixStats: stats,
    cut: totalRecords,
    crosses: false,
    inTurnAtCut: false,
    inTurn: false,
    ambiguous: false,
    provisional: true,
    fastSeededTail: true,
  };
}

async function scan({ path, sessionId, cutoff, inheritedCount: expectedInheritedCount, fromSeq, permitResumeMarker, messageId, allowLegacy }, planning = false) {
  requireRollout(typeof sessionId === 'string' && sessionId.length <= 2048, '会话标识无效。');
  requireRollout(cutoff === undefined || (Number.isSafeInteger(cutoff) && cutoff >= 0), '回退时刻无效。');
  requireRollout(fromSeq === undefined || (Number.isSafeInteger(fromSeq) && fromSeq >= 0), '恢复位置无效。');
  requireRollout(!planning || (typeof messageId === 'string' && messageId.length <= 2048), '消息标识无效。');
  if (!planning && cutoff !== undefined && expectedInheritedCount === undefined
    && fromSeq === undefined && allowLegacy !== true) {
    const fast = await scanSeededSubagentTail({ path, sessionId, cutoff });
    if (fast) return fast;
  }
  const tracker = planning ? createPlanTracker(messageId) : undefined;
  const stats = createStats();
  let prefixStats;
  let header;
  let identity;
  let catalog = [];
  let catalogBytes = 0;
  let inheritedCount = 0;
  let sawInherited = false;
  let inTurn;
  let legacyStep;
  let inTurnAtCut = false;
  let cut;
  let prefix;
  let ambiguous = false;
  let provisional = true;
  const legacy = allowLegacy === true && !planning && cutoff !== undefined;
  for await (const item of readRolloutLines(path, sessionId, {
    allowLegacy: legacy, trackZstdPrefix: planning || (cutoff !== undefined && !legacy),
  })) {
    const { record, line, index, framePrefix } = item;
    if (index === -1) {
      header = record;
      identity = item.identity;
      continue;
    }
    const inheritedMarker = record.type === 'session/end-seed' && record.data?.inherited === true;
    if (inheritedMarker) {
      requireRollout(header.isSeeded === true, '日志文件头与继承边界不匹配。');
      inheritedCount = index;
      sawInherited = true;
      catalog = [];
      catalogBytes = 0;
      cut = undefined;
      prefixStats = undefined;
      prefix = undefined;
      ambiguous = false;
      inTurnAtCut = false;
    } else {
      if (record.type === 'subagent/catalog') {
        catalogBytes += line.length;
        requireRollout(catalogBytes <= maxCatalogBytes, '子智能体登记超出单次检查上限。');
        catalog.push(record);
      }
      if (cutoff !== undefined) {
        // Session sequence numbers order only this rollout. A millisecond tie
        // with the parent's cutoff does not tell us which event came first.
        if (record.time === cutoff) ambiguous = true;
        if (cut === undefined && record.time >= cutoff) {
          cut = index;
          inTurnAtCut = inTurn !== undefined;
          prefixStats = { ...stats };
          prefix = framePrefix;
        } else if (cut !== undefined && record.time < cutoff) {
          ambiguous = true;
        }
      }
    }
    tracker?.accept(record, stats, framePrefix);
    if (record.type === 'turn/start') {
      requireRollout(inTurn === undefined, '会话日志中存在嵌套对话轮次。');
      if (header.version === 3) requireRollout(Number.isSafeInteger(record.data?.turn) && record.data.turn > 0,
        '旧版子会话轮次无效。');
      inTurn = record.data.turn;
    } else if (record.type === 'turn/end') {
      requireRollout(inTurn !== undefined && inTurn === record.data.turn && legacyStep === undefined,
        '会话日志的轮次边界不完整。');
      inTurn = undefined;
    } else if (header.version === 3 && record.type === 'step/start') {
      requireRollout(legacyStep === undefined && inTurn !== undefined && record.data?.turn === inTurn
        && Number.isSafeInteger(record.data.step) && record.data.step > 0, '旧版子会话的步骤边界不完整。');
      legacyStep = record.data.step;
    } else if (header.version === 3 && record.type === 'step/end') {
      requireRollout(legacyStep !== undefined && record.data?.turn === inTurn && record.data.step === legacyStep,
        '旧版子会话的步骤边界不完整。');
      legacyStep = undefined;
    }
    if (fromSeq !== undefined && index >= fromSeq
      && !(permitResumeMarker === true && isProvisionalResumeMarker(record, fromSeq))) provisional = false;
    measure(stats, line);
  }
  requireRollout(header.isSeeded !== true || sawInherited, '缺少可验证的继承边界。');
  requireRollout(expectedInheritedCount === undefined || expectedInheritedCount === inheritedCount, '继承的会话历史已变化。');
  const common = { header, identity, catalog, stats, inheritedCount };
  if (tracker) return { ...common, ...tracker.finish(inheritedCount) };
  return { ...common, prefixStats: prefixStats ?? stats, cut: cut ?? stats.records, crosses: cut !== undefined,
    inTurnAtCut: cut === undefined ? inTurn !== undefined : inTurnAtCut, inTurn: inTurn !== undefined,
    ambiguous, provisional: provisional && (fromSeq === undefined || stats.records >= fromSeq), prefix };
}

async function prepare({ path, sessionId, output, cut, inputCopies = [], message, appendEvents = [], identity: expected, prefix }) {
  requireRollout(Number.isSafeInteger(cut) && cut >= 0 && Array.isArray(inputCopies) && inputCopies.length <= maxCopies
    && Array.isArray(appendEvents) && (inputCopies.length === 0 || Array.isArray(message?.content)), '编辑参数无效。');
  const swaps = new Map();
  let firstChanged = cut;
  for (const { index, offset } of inputCopies) {
    requireRollout(Number.isSafeInteger(index) && index >= 0 && index < cut
      && Number.isSafeInteger(offset) && offset >= 0, '原消息的位置无效。');
    const offsets = swaps.get(index) ?? [];
    requireRollout(!offsets.includes(offset), '原消息的位置重复。');
    offsets.push(offset);
    swaps.set(index, offsets);
    if (index < firstChanged) firstChanged = index;
  }
  // The Host passes this boundary only from a completed scan of this file
  // identity. Older callers without a boundary use the streaming fallback.
  const prefixCopy = expected && path.endsWith('.zstd') && Number.isSafeInteger(prefix?.end) && prefix.end > 0
    && Number.isSafeInteger(prefix.startSeq) && prefix.startSeq > 0 && prefix.startSeq <= firstChanged
    ? { sourcePath: path, end: prefix.end, startSeq: prefix.startSeq, expected } : undefined;
  const stats = createStats();
  let identity = prefixCopy?.expected;
  let copied = prefixCopy?.startSeq ?? 0;
  function applySwaps(item) {
    const offsets = swaps.get(item.index);
    if (!offsets) return;
    for (const offset of offsets) {
      const occurrence = item.record.data?.inserted?.[offset];
      requireRollout(item.record.type === 'agent/inbox/spliced' && occurrence?.id === message.id
        && occurrence.source?.kind === 'user', '无法定位原消息的入队记录。');
      item.record.data.inserted[offset] = { ...occurrence, content: message.content };
    }
    item.line = Buffer.from(`${JSON.stringify(item.record)}\n`);
    swaps.delete(item.index);
  }
  async function* outputLines() {
    if (prefixCopy) {
      if (copied < cut) {
        for await (const item of readRolloutLinesFromOffset(path, prefixCopy.end, copied, prefixCopy.expected)) {
          if (item.index >= cut) break;
          applySwaps(item);
          copied++;
          measure(stats, item.line);
          yield item;
          if (copied === cut) break;
        }
      }
      requireRollout(copied === cut && swaps.size === 0, '日志没有包含完整的候选前缀。');
      await checkRolloutIdentity(path, prefixCopy.expected);
    } else {
      for await (const item of readRolloutLines(path, sessionId)) {
        if (item.index === -1) {
          identity = item.identity;
          if (expected) await checkRolloutIdentity(path, expected);
          yield item;
        } else {
          applySwaps(item);
          copied++;
          measure(stats, item.line);
          yield item;
        }
        if (copied === cut) break;
      }
      requireRollout(copied === cut && swaps.size === 0, '日志没有包含完整的候选前缀。');
      await checkRolloutIdentity(path, identity);
    }
    for (const entry of appendEvents) {
      const index = copied++;
      const time = entry.time ?? Date.now();
      requireRollout(typeof entry.type === 'string' && Number.isSafeInteger(time) && time >= 0
        && Object.hasOwn(entry, 'data'), '附加会话事件无效。');
      const line = Buffer.from(`${JSON.stringify({ ...entry, seq: index, time })}\n`);
      measure(stats, line);
      yield { line, index };
    }
  }
  const result = await writeRolloutCandidate(output, path.endsWith('.zstd'), outputLines(), prefixCopy);
  return { path: output, ...result, retained: copied, writtenStats: stats, identity, copiedPrefixBytes: prefixCopy?.end ?? 0 };
}

export async function runOperation(operation, input) {
  switch (operation) {
    case 'plan': return scan(input, true);
    case 'inspect':
    case 'scan': return scan(input);
    case 'scanMany': {
      requireRollout(Array.isArray(input.items) && input.items.length <= 10_000, '批量会话核对超出上限。');
      const result = [];
      let bytes = 0;
      for (const item of input.items) {
        const summary = await scan(item);
        bytes += Buffer.byteLength(JSON.stringify(summary));
        requireRollout(bytes < maxMessageBytes - 16 * 1024, '批量会话核对结果过大。');
        result.push(summary);
      }
      return result;
    }
    case 'prepare': return withRevisionCandidateOutput(input.output, () => prepare(input));
    default: throw new Error('不支持此日志操作。');
  }
}

const heapLimitCeilingBytes = 384 * 1024 * 1024;
const win32JobCeilingBytes = process.versions.electron ? 384 * 1024 * 1024 : 256 * 1024 * 1024;
const watchdogRssLimitBytes = process.versions.electron ? 384 * 1024 * 1024 : 256 * 1024 * 1024;

function assertBoundedHeap() {
  requireRollout(process.execArgv.includes('--max-old-space-size=128')
    && getHeapStatistics().heap_size_limit <= heapLimitCeilingBytes, '系统未应用日志准备堆内存上限。');
}

function startRssWatchdog() {
  const timer = setInterval(() => {
    if (process.memoryUsage.rss() > watchdogRssLimitBytes) process.exit(137);
  }, 100);
  timer.unref();
}

async function verifyIsolation(mode) {
  if (mode === 'cgroup') {
    const cgroup = (await readFile('/proc/self/cgroup', 'utf8')).split('\n').find(line => line.startsWith('0::'));
    const relative = cgroup?.slice(3);
    requireRollout(relative?.startsWith('/') && /\/dsh-message-revision-[a-f0-9-]+\.scope$/u.test(relative),
      '日志准备进程未进入隔离组。');
    const directory = join('/sys/fs/cgroup', relative);
    const memory = Number((await readFile(join(directory, 'memory.max'), 'utf8')).trim());
    const swap = Number((await readFile(join(directory, 'memory.swap.max'), 'utf8')).trim());
    requireRollout(Number.isSafeInteger(memory) && memory > 0 && memory <= 268435456 && swap === 0,
      '系统未应用日志准备内存上限。');
  } else if (mode === 'rlimit') {
    const limits = await readFile('/proc/self/limits', 'utf8');
    const match = /^Max address space\s+(\d+)\s+(\d+)\s+bytes/mu.exec(limits);
    requireRollout(match && Number(match[2]) <= 1073741824, '系统未应用进程地址空间上限。');
  } else if (mode === 'win32-job') {
    requireRollout(process.platform === 'win32', '日志准备进程隔离模式与当前系统不匹配。');
    assertBoundedHeap();
    const limits = queryCurrentWin32JobLimits();
    requireRollout(limits !== null
      && (limits.flags & JOB_OBJECT_LIMIT_PROCESS_MEMORY) !== 0
      && (limits.flags & JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE) !== 0
      && Number.isSafeInteger(limits.processMemoryLimit)
      && limits.processMemoryLimit > 0 && limits.processMemoryLimit <= win32JobCeilingBytes,
    '系统未应用日志准备内存上限。');
  } else if (mode === 'darwin-rlimit' || mode === 'win32-watchdog') {
    requireRollout(process.platform === (mode === 'darwin-rlimit' ? 'darwin' : 'win32'),
      '日志准备进程隔离模式与当前系统不匹配。');
    assertBoundedHeap();
    startRssWatchdog();
  } else {
    throw new Error('不支持的日志准备隔离模式。');
  }
}

async function readRequestBytes() {
  const chunks = [];
  let bytes = 0;
  for await (const chunk of process.stdin) {
    bytes += chunk.length;
    requireRollout(bytes <= maxMessageBytes, '日志准备参数超出大小上限。');
    chunks.push(chunk);
  }
  return Buffer.concat(chunks, bytes);
}

async function main() {
  const mode = process.argv[3];
  let rawRequest;
  if (mode === 'win32-job') {
    // Host assigns the Job Object before closing stdin, so reading the bounded
    // request first ensures the kernel job limits are active before self-check.
    rawRequest = await readRequestBytes();
    await verifyIsolation(mode);
    writeSync(1, 'R1\n');
  } else {
    await verifyIsolation(mode);
    writeSync(1, 'R1\n');
    rawRequest = await readRequestBytes();
  }
  try {
    const payload = JSON.parse(rawRequest.toString('utf8'));
    const value = await runOperation(payload.operation, payload.input);
    const json = JSON.stringify({ ok: true, value });
    requireRollout(Buffer.byteLength(json) < maxMessageBytes - 3, '日志准备结果超出大小上限。');
    await new Promise((resolve, reject) => process.stdout.write(json, error => error ? reject(error) : resolve()));
  } catch (error) {
    const message = error instanceof SyntaxError ? '会话日志包含无效 JSON。'
      : error?.code === 'Z_BUF_ERROR' || error?.code?.startsWith?.('ZSTD_') ? '压缩日志无效或尚未完整落盘。'
        : error?.code ? '读取或准备会话日志时遇到文件错误。' : error?.message ?? '会话日志准备失败。';
    await new Promise(resolve => process.stdout.write(JSON.stringify({ ok: false, message, reason: error?.reason }), resolve));
  }
}

if (process.argv[2] === 'dsh-rollout-worker') {
  await main().catch(() => { process.exitCode = 1; });
}
