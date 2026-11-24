import { cleanupRevisionCandidates } from './revision-candidates.js';
import { acquireNativeLease } from './native-lease.js';
import {
  isCanonicalPath, isSamePath, isSupportedPlatform, pathKey, replaceFileDurable, resolveRolloutPath,
} from './platform.js';
import { decompressZstdConcat } from './rollout-stream.js';
import { isProvisionalResumeMarker } from './rollback-events.js';
import { randomUUID } from 'node:crypto';
import { lstat, open, readFile, unlink } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join } from 'node:path';
import { setTimeout as wait } from 'node:timers/promises';
import {
  cleanRewriteDebris, clearFailedCommitMarker, forgetRewriteJournal, hasPendingRewrite, listRewriteDebris, markRewriteCommitted,
  matchJournalRevision, prepareRewriteJournal, readRewriteJournals, readRewriteOriginal, restoreWithdrawnChildren,
  retireRewriteJournal, retrySettledRewriteRetirement, stageWithdrawnChildren,
} from './rewrite-journal.js';
import { promisify } from 'node:util';
import { constants, zstdCompress } from 'node:zlib';

const compress = promisify(zstdCompress);
const utf8 = new TextDecoder('utf-8', { fatal: true });
const maxBytes = 256 * 1024 * 1024;
const maxQueue = 4096;
const maxCopies = 8192;
const logName = /^session\.v4\.jsonl(?:\.zstd)?$/u;

export class RevisionError extends Error {
  constructor(message, options) {
    super(message, options);
    this.name = 'RevisionError';
  }
}

function requireRevision(condition, message) {
  if (!condition) throw new RevisionError(message);
}

/** Find the beginning of the turn and the existing next-turn queue entry. */
export function planRevision(events, messageId, inheritedCount = 0) {
  const queues = { 'next-turn': [], 'next-step': [] };
  let turn;
  let selected;
  let checkpoint;
  let inputCopies = [];
  let index = 0;
  for (const event of events) {
    requireRevision(event.seq === index, '会话日志的事件顺序不完整。');
    const data = event.data;
    if (event.type === 'agent/inbox/spliced') {
      const queue = queues[data.target];
      const count = data.removedCount ?? 0;
      requireRevision(Array.isArray(queue) && Number.isSafeInteger(data.start) && data.start >= 0
        && Number.isSafeInteger(count) && count >= 0 && data.start + count <= queue.length
        && Array.isArray(data.inserted)
        && queue.length - count + data.inserted.length <= maxQueue, '无法安全复原会话的消息队列。');
      const inserted = data.inserted.map((message, offset) => {
        requireRevision(typeof message?.id === 'string' && message.id.length <= 2048, '无法安全复原会话的消息队列。');
        if (message.id !== messageId) return { id: message.id };
        requireRevision(inputCopies.length < maxCopies, '同一消息的历史入队记录超出检查上限。');
        const item = { message, index, offset };
        inputCopies.push(item);
        return { id: message.id, ...item };
      });
      queue.splice(data.start, count, ...inserted);
    } else if (event.type === 'turn/start') {
      requireRevision(turn === undefined && Number.isSafeInteger(data?.turn), '会话日志中存在未关闭的回合。');
      const candidate = queues['next-turn'][0];
      turn = {
        index,
        number: data.turn,
        time: event.time,
        input: candidate?.id === messageId ? candidate : undefined,
        queuedCount: queues['next-turn'].length,
        hasSteering: queues['next-step'].length > 0,
      };
    } else if (event.type === 'turn/end') {
      requireRevision(turn !== undefined && turn.number === data?.turn, '会话日志的回合边界不完整。');
      turn = undefined;
    } else if (event.type === 'user/message' && data?.id === messageId && data.source?.kind === 'user') {
      requireRevision(selected === undefined, '日志中找到了重复的用户消息。');
      selected = event;
      checkpoint = turn;
      if (checkpoint) inputCopies = inputCopies.filter(item => item.index < checkpoint.index);
    }
    index++;
  }
  requireRevision(turn === undefined, '回复生成中，请先停止或等待完成。');
  requireRevision(selected && checkpoint?.input?.message.id === messageId
    && checkpoint.index > inheritedCount && checkpoint.input.index >= inheritedCount
    && !checkpoint.hasSteering && checkpoint.queuedCount === 1,
  '此消息没有可独立回退的对话轮次。');
  requireRevision(queues['next-turn'].length === 0 && queues['next-step'].length === 0,
    '会话仍有排队消息，请等队列清空后再试。');
  const retainedInputs = inputCopies.filter(item => item.index < checkpoint.index);
  requireRevision(retainedInputs.every(item => item.index >= inheritedCount && item.message.source?.kind === 'user'),
    '无法替换继承历史中的同一条消息。');
  return {
    cut: checkpoint.index,
    retainedInputs,
    time: checkpoint.time,
    message: checkpoint.input.message,
  };
}

export function isRevisionAttachment(block) {
  return (block?.type === 'image' || block?.type === 'file') && typeof block.attachment === 'object' && block.attachment !== null;
}

export function retainedRevisionAttachments(message, removed = []) {
  requireRevision(Array.isArray(message.content), '无法读取该消息正文。');
  for (const index of removed) {
    requireRevision(isRevisionAttachment(message.content[index]), '无法确认待移除的附件。');
  }
  const ignored = new Set(removed);
  return message.content.filter((block, index) => isRevisionAttachment(block) && !ignored.has(index));
}

export function revisionTextParts(message) {
  return message.content.filter(block => block.type === 'text').map(block => block.text);
}

export function resolveRevisionText(message, { text, textParts }) {
  const original = revisionTextParts(message);
  if (textParts !== undefined) {
    requireRevision(Array.isArray(textParts) && original.length > 1 && textParts.length === original.length
      && textParts.every(part => typeof part === 'string') && textParts.join('') === text,
    '分段正文与原消息不匹配，请重新打开编辑框。');
    return textParts;
  }
  if (text === original.join('')) return original;
  requireRevision(original.length <= 1, '此消息包含多段正文，请使用分段编辑。');
  return [text];
}

export function rewriteMessage(message, input, removed, added) {
  const retained = retainedRevisionAttachments(message, removed);
  const textParts = resolveRevisionText(message, input);
  requireRevision(input.text.trim() !== '' || retained.length > 0 || added.length > 0, '请输入消息内容或至少保留一个附件。');
  const ignored = new Set(removed);
  const content = [];
  let part = 0;
  for (const [index, block] of message.content.entries()) {
    if (block.type === 'text') {
      const replacement = textParts[part++];
      if (replacement === block.text) content.push(block);
      else if (replacement !== '') content.push({ ...block, text: replacement });
    } else if (!ignored.has(index)) {
      content.push(block);
    }
  }
  if (part === 0 && input.text.trim() !== '') content.unshift({ type: 'text', text: input.text });
  return { ...message, content: [...content, ...added] };
}

function validateText(text) {
  requireRevision(typeof text === 'string' && text.length <= 200_000,
    '消息内容无效，最多支持 200,000 个字符。');
}

function checkLocation(snapshot) {
  const path = snapshot?.location?.path;
  requireRevision(snapshot?.location?.kind === 'jsonl' && typeof path === 'string'
    && isAbsolute(path) && logName.test(basename(path)), '当前会话存储格式不支持编辑。');
  return path;
}

async function checkedRolloutPath(dshPath) {
  const path = await resolveRolloutPath(dshPath);
  requireRevision(path !== undefined, '会话日志路径异常。');
  return path;
}

async function checkArtifact(path) {
  const identity = await lstat(path, { bigint: true });
  requireRevision(identity.isFile() && !identity.isSymbolicLink() && await isCanonicalPath(path),
    '会话日志路径异常。');
  requireRevision(identity.size <= maxBytes, '会话日志超出大小上限，无法编辑。');
  return identity;
}

async function openLocked(persistence, sessionId, snapshot) {
  const dshPath = checkLocation(snapshot);
  const path = await checkedRolloutPath(dshPath);
  const identity = await checkArtifact(path);
  const handle = await persistence.open(sessionId, 'write');
  try {
    const current = await persistence.stat(sessionId);
    requireRevision(handle.header.id === sessionId && isSamePath(checkLocation(current), dshPath)
      && current.revision === snapshot.revision && isSamePath(await resolveRolloutPath(dshPath), path), '会话状态已变更，请刷新后重试。');
    const { events } = await handle.read(0);
    return { handle, path, identity, events, snapshot: current };
  } catch (error) {
    await handle.close();
    throw error;
  }
}

async function encodeReplacement(source, plan, message, controls, restoredCatalog) {
  const compressed = source.path.endsWith('.zstd');
  const raw = await readFile(source.path);
  const decoded = compressed ? decompressZstdConcat(raw, { maxOutputLength: maxBytes }) : raw;
  const lines = utf8.decode(decoded).split('\n');
  requireRevision(lines.pop() === '' && lines.length === source.events.length + 1,
    '会话日志不完整，已取消编辑。');
  const header = JSON.parse(lines[0]);
  requireRevision(header.type === 'session' && header.version === 4 && header.id === source.handle.header.id,
    '会话日志文件头不匹配。');
  const prefix = lines.slice(1, plan.cut + 1);
  // A previous edit may have canceled the original queue occurrence and
  // enqueued the same message again. Keep native splice semantics and replace
  // the content in every retained occurrence of this specific user input.
  for (const { index, offset } of plan.retainedInputs) {
    const encodedInput = JSON.parse(prefix[index]);
    const occurrence = encodedInput.data?.inserted?.[offset];
    requireRevision(encodedInput.seq === index && encodedInput.type === 'agent/inbox/spliced'
      && occurrence?.id === message.id && occurrence.source?.kind === 'user', '无法定位原消息的入队记录。');
    encodedInput.data.inserted[offset] = { ...occurrence, content: message.content };
    prefix[index] = JSON.stringify(encodedInput);
  }
  for (const event of restoredCatalog) {
    prefix.push(JSON.stringify({ ...event, seq: prefix.length, time: plan.time }));
  }
  for (const control of controls) {
    prefix.push(JSON.stringify({ type: control.type, seq: prefix.length, time: Date.now(), data: control.data }));
  }
  const headerBytes = Buffer.from(`${lines[0]}\n`);
  const eventBytes = Buffer.from(`${prefix.join('\n')}\n`);
  if (!compressed) return { original: raw, buffers: [headerBytes, eventBytes], retained: prefix.length };
  const options = { params: { [constants.ZSTD_c_checksumFlag]: 1 } };
  const buffers = await Promise.all([compress(headerBytes, options), compress(eventBytes, options)]);
  return { original: raw, buffers, retained: prefix.length };
}

function catalogEntries(events) {
  return events.filter(event => event.type === 'subagent/catalog');
}

function verifyCatalogBirth(event) {
  const { childId, childCreatedAt, mode } = event.data;
  requireRevision(typeof childId === 'string' && childId.length > 0
    && ['continuable', 'one-shot', 'unknown', 'external'].includes(mode)
    && Number.isSafeInteger(childCreatedAt) && childCreatedAt >= 0 && childCreatedAt <= event.time,
  '无法确认子会话的登记或创建时间。');
}

/** Native migration can append an older child's creation fact at the log tail. */
export function splitChildCatalog(catalog, { cut, time }) {
  const retained = [];
  const withdrawn = [];
  const seen = new Set();
  for (const event of catalog) {
    verifyCatalogBirth(event);
    const { childId, childCreatedAt } = event.data;
    requireRevision(!seen.has(childId), '子会话登记重复，无法安全判断归属。');
    seen.add(childId);
    if (event.seq < cut) {
      requireRevision(childCreatedAt <= time, '子会话的创建时间与会话顺序不一致。');
      retained.push(event);
    } else {
      requireRevision(childCreatedAt !== time, '子会话的创建时间与改写点重合，无法安全判定归属。');
      (childCreatedAt < time ? retained : withdrawn).push(event);
    }
  }
  return { retained, withdrawn };
}

function verifyChildHeader(header, parent, { childId, childCreatedAt }) {
  requireRevision(header.id === childId && header.parentSession === parent.handle.header.id
    && header.origin === 'subagent' && header.createdAt === childCreatedAt,
  '子会话的实际归属或创建时间与登记不匹配。');
}

async function collectChildTree(persistence, parent, catalog, cutoff, locked, seen) {
  for (const event of catalog) {
    verifyCatalogBirth(event);
    const { childId, childCreatedAt, mode } = event.data;
    requireRevision(!seen.has(childId) && mode !== 'external', '无法安全核对子会话树。');
    seen.add(childId);
    requireRevision(childCreatedAt >= parent.handle.header.createdAt, '子会话早于其父会话，无法确认创建顺序。');
    const snapshot = await persistence.stat(childId);
    requireRevision(snapshot, '子会话记录不可用，无法核对其历史与后代。');
    verifyChildHeader(snapshot.header, parent, event.data);

    // The native writer lock makes the entire traversed subtree stable until the
    // parent commits. It also catches descendants that are live or opening.
    let child;
    try {
      child = await openLocked(persistence, childId, snapshot);
    } catch (error) {
      if (error.name === 'SessionAlreadyOwnedError') {
        throw new RevisionError('关联的子智能体会话或其后代仍处于打开状态，请关闭后重试。', { cause: error });
      }
      throw error;
    }
    locked.push(child);
    verifyChildHeader(child.handle.header, parent, event.data);
    const ownEvents = child.events.slice(child.handle.inheritedEventCount);
    if (cutoff !== undefined) {
      requireRevision(childCreatedAt < cutoff && ownEvents.every(item => item.time < cutoff),
        '保留的子智能体会话或其后代跨过改写点，无法编辑此消息。');
      let inTurn = false;
      for (const item of ownEvents) {
        if (item.type === 'turn/start') inTurn = true;
        if (item.type === 'turn/end') inTurn = false;
      }
      requireRevision(!inTurn, '保留的子智能体会话或其后代存在未关闭的回合。');
    }
    await collectChildTree(persistence, child, catalogEntries(ownEvents), cutoff, locked, seen);
  }
}

async function checkUnchanged(entry) {
  const now = await checkArtifact(entry.path);
  const initial = entry.identity;
  requireRevision(now.dev === initial.dev && now.ino === initial.ino && now.size === initial.size
    && now.mtimeNs === initial.mtimeNs && now.ctimeNs === initial.ctimeNs,
  '日志文件已变化，未继续替换。');
}

async function replaceArtifact(source, buffers, beforeReplace, onPublished, staging = dirname(source.path)) {
  const directory = dirname(source.path);
  const stage = join(staging, `.message-revision-${randomUUID()}.tmp`);
  let pending = false;
  try {
    const file = await open(stage, 'wx', Number(source.identity.mode & 0o777n));
    pending = true;
    try {
      for (const bytes of buffers) await file.writeFile(bytes);
      await file.sync();
    } finally {
      await file.close();
    }
    await checkUnchanged(source);
    await beforeReplace?.();
    await checkUnchanged(source);
    await replaceFileDurable(stage, source.path);
    pending = false;
    onPublished?.();
    if (process.platform !== 'win32') {
      const parent = await open(directory, 'r');
      try { await parent.sync(); } finally { await parent.close(); }
    }
  } finally {
    if (pending) await unlink(stage);
  }
}

async function recoverRewrite(source, journal, { canRollback, beforeCommit } = {}) {
  if (journal.committed) return;
  const revision = await matchJournalRevision(journal);
  requireRevision((canRollback?.() ?? true) && revision !== 'conflict',
    '会话已变化，无法自动恢复上次编辑。');
  if (revision === 'original') {
    await restoreWithdrawnChildren(journal);
    return;
  }
  const retained = journal.intent.candidate.records;
  requireRevision(source.events.length >= retained
    && source.events.slice(retained).every(event => isProvisionalResumeMarker(event, retained)),
    '更新后的会话已有新增内容，无法自动回滚。');
  const original = await readRewriteOriginal(journal);
  await replaceArtifact(source, [original], async () => {
    await beforeCommit?.([journal.intent.parentId, ...journal.intent.children.map(child => child.id)]);
    await restoreWithdrawnChildren(journal);
  }, undefined, journal.directory);
}

const recoveries = new Map();
const recoveryRetryDelays = [100, 250, 500, 1_000, 2_000, 4_000];

function recoverSessionRewrite(persistence, snapshot, { beforeCommit, canRollback, onError, restoreBoundedJournal, recoverDelivery } = {}) {
  const { id } = snapshot.header;
  const path = checkLocation(snapshot);
  const key = pathKey(path);
  const existing = recoveries.get(key);
  if (existing) return existing;
  const recovery = (async () => {
    const [journal] = await readRewriteJournals(snapshot);
    if (!journal) return false;
    if (journal.intent.format === 2) {
      if (journal.committed && journal.intent.delivery) {
        recoverDelivery?.(journal);
        return false;
      }
      const retire = () => retireRewriteJournal(journal, { deferCleanup: true, onCleanupError: error => onError?.(id, error) });
      if (journal.committed) {
        await retire();
        return false;
      }
      if (!restoreBoundedJournal) throw new RevisionError('当前无法恢复多会话编辑事务。');
      await restoreBoundedJournal(journal, {
        beforeCommit,
        canRollback: () => [...journal.intent.artifacts, ...journal.intent.children]
          .every(entry => canRollback?.(entry.id) ?? true),
        afterRestore: retire,
      });
      return true;
    }
    const source = await openLocked(persistence, id, snapshot);
    try {
      // Only act on the journal still present after obtaining the native lock.
      const [current] = await readRewriteJournals(source.snapshot);
      if (!current || current.intent.format !== 1) return false;
      await recoverRewrite(source, current, { beforeCommit, canRollback: () => canRollback?.(id) ?? true });
      await retireRewriteJournal(current);
      return !current.committed;
    } finally {
      await source.handle.close();
    }
  })().finally(() => recoveries.delete(key));
  recoveries.set(key, recovery);
  return recovery;
}

async function retryLockedRewrites(persistence, pending, options) {
  const { retrySignal, onError } = options;
  for (const delay of recoveryRetryDelays) {
    if (pending.size === 0 || retrySignal.aborted) return;
    try { await wait(delay, undefined, { signal: retrySignal, ref: false }); } catch (error) {
      if (retrySignal.aborted) return;
      throw error;
    }
    for (const id of pending) {
      if (retrySignal.aborted) return;
      try {
        const snapshot = await persistence.stat(id);
        if (snapshot) await recoverSessionRewrite(persistence, snapshot, options);
        pending.delete(id);
      } catch (error) {
        if (error.name === 'SessionAlreadyOwnedError') continue;
        pending.delete(id);
        onError?.(id, error);
      }
    }
  }
}

/** Recover transactions left by a previous Host before admitting new rewrites. */
export async function recoverStoredSessions(persistence, options = {}) {
  const { onError, retrySignal } = options;
  const locked = new Set();
  for (const snapshot of await persistence.list()) {
    if (retrySignal?.aborted) break;
    const { id } = snapshot.header;
    try {
      if (snapshot.location?.kind !== 'jsonl' || !isAbsolute(snapshot.location.path)
        || !logName.test(basename(snapshot.location.path))) continue;
      const dshPath = checkLocation(snapshot);
      const path = await checkedRolloutPath(dshPath);
      await cleanupRevisionCandidates(path, { retryBusy: true, onError: error => onError?.(id, error) })
        .catch(error => onError?.(id, error));
      await recoverSessionRewrite(persistence, snapshot, options);
      const { preparations, retired } = await listRewriteDebris(path);
      if (preparations.length > 0) {
        if (isSupportedPlatform(process.platform)) {
          try {
            // An unpublished directory has never modified the live session. Its
            // native writer lease protects metadata-only cleanup of any size.
            const lease = await acquireNativeLease(path, dshPath);
            try {
              const current = await persistence.stat(id);
              requireRevision(current?.header.id === id && isSamePath(checkLocation(current), dshPath), '会话状态已变更，请刷新后重试。');
              await lease.assertHeld();
              await cleanRewriteDebris((await listRewriteDebris(path)).preparations);
            } finally { await lease.close(); }
          } catch (error) { onError?.(id, error); }
        } else {
          const source = await openLocked(persistence, id, snapshot);
          try {
            await cleanRewriteDebris((await listRewriteDebris(source.path)).preparations);
          } finally {
            await source.handle.close();
          }
        }
      }
      await cleanRewriteDebris(retired);
    } catch (error) {
      if (retrySignal && error.name === 'SessionAlreadyOwnedError') locked.add(id);
      onError?.(id, error);
    }
  }
  if (locked.size > 0) {
    void retryLockedRewrites(persistence, locked, options).catch(error => onError?.('startup', error));
  }
}

export async function recoverPendingRewrite(persistence, snapshot, options) {
  if (snapshot?.location?.kind !== 'jsonl') return false;
  const path = await checkedRolloutPath(checkLocation(snapshot));
  await retrySettledRewriteRetirement(path);
  if (!await hasPendingRewrite(path)) return false;
  const recovered = await recoverSessionRewrite(persistence, snapshot, options);
  requireRevision(!await hasPendingRewrite(path), '上次编辑尚未完成恢复，请检查运行日志。');
  return recovered;
}

export async function assertNoPendingRewrite(snapshot) {
  if (snapshot?.location?.kind === 'jsonl') {
    const path = await checkedRolloutPath(checkLocation(snapshot));
    await retrySettledRewriteRetirement(path);
    requireRevision(!await hasPendingRewrite(path), '上次编辑尚未完成恢复，请检查运行日志。');
  }
}

/** Publish provisionally, then let DSH open the same ID before discarding any old data. */
export async function rewriteStoredSession(persistence, {
  sessionId, messageId, text, textParts, expectedRevision, removedAttachments = [],
}, { beforeCommit, verifyReload, canRollback, closeFailedReload, afterCommit, onCleanupError, addedAttachments = [], controls = [] } = {}) {
  validateText(text);
  const snapshot = await persistence.stat(sessionId);
  requireRevision(expectedRevision === undefined || snapshot?.revision === expectedRevision,
    '会话状态已变更，请重新打开编辑框。');
  const source = await openLocked(persistence, sessionId, snapshot);
  const retainedChildren = [];
  const withdrawnChildren = [];
  let journal;
  let published = false;
  let committed = false;
  let markerFailed = false;
  let handlesClosed = false;
  async function releaseHandles() {
    if (handlesClosed) return;
    handlesClosed = true;
    const results = await Promise.allSettled([
      ...retainedChildren.map(child => child.handle.close()),
      ...withdrawnChildren.map(child => child.handle.close()),
      source.handle.close(),
    ]);
    const failure = results.find(result => result.status === 'rejected');
    if (failure) throw failure.reason;
  }
  try {
    await assertNoPendingRewrite(source.snapshot);
    requireRevision(source.handle.header.origin !== 'subagent', '子智能体会话不支持直接编辑，请在主会话中操作。');
    const plan = planRevision(source.events, messageId, source.handle.inheritedEventCount);
    const catalog = catalogEntries(source.events).filter(event => event.seq >= source.handle.inheritedEventCount);
    const { retained, withdrawn } = splitChildCatalog(catalog, plan);
    const seen = new Set([sessionId]);
    await collectChildTree(persistence, source, retained, plan.time, retainedChildren, seen);
    await collectChildTree(persistence, source, withdrawn, undefined, withdrawnChildren, seen);
    const message = rewriteMessage(plan.message, { text, textParts }, removedAttachments, addedAttachments);
    const replacement = await encodeReplacement(source, plan, message, controls, retained.filter(event => event.seq >= plan.cut));
    const removedChildren = withdrawnChildren.map(child => child.handle.header.id);
    for (const child of [...retainedChildren, ...withdrawnChildren]) await checkUnchanged(child);
    await beforeCommit?.([sessionId, ...removedChildren]);
    journal = await prepareRewriteJournal(source, withdrawnChildren, replacement);
    await replaceArtifact(source, replacement.buffers, async () => {
      for (const child of [...retainedChildren, ...withdrawnChildren]) await checkUnchanged(child);
      await stageWithdrawnChildren(journal);
    }, () => { published = true; }, journal.directory);
    await releaseHandles();
    await verifyReload?.();
    try {
      await markRewriteCommitted(journal);
    } catch (error) {
      markerFailed = true;
      throw error;
    }
    committed = true;
    const result = { messageId, removedChildren };
    try {
      await afterCommit?.(result);
    } finally {
      // Dispatch happens first. Cleanup cannot turn a saved edit into a failed
      // request, and startup will retry committed or partially retired journals.
      try {
        await retireRewriteJournal(journal);
      } catch (error) {
        result.cleanupWarning = true;
        onCleanupError?.(error);
      }
    }
    return result;
  } catch (error) {
    if (committed || !journal) throw error;
    try {
      if (published) {
        await releaseHandles();
        if (markerFailed) await closeFailedReload?.();
        const current = await openLocked(persistence, sessionId, await persistence.stat(sessionId));
        try {
          requireRevision(isSamePath(current.path, source.path) && (canRollback?.() ?? true), '无法确认候选会话已释放。');
          if (markerFailed) await clearFailedCommitMarker(journal);
          await recoverRewrite(current, journal, { canRollback, beforeCommit });
        } finally {
          await current.handle.close();
        }
      } else {
        await restoreWithdrawnChildren(journal);
      }
      await retireRewriteJournal(journal);
    } catch (rollbackError) {
      throw new Error('编辑回滚失败，请刷新后检查会话。', { cause: rollbackError });
    }
    if (published) throw new RevisionError('无法加载更新后的会话，已恢复原记录。', { cause: error });
    throw error;
  } finally {
    forgetRewriteJournal(journal);
    await releaseHandles();
  }
}
