import {
  assertNoPendingRewrite, isRevisionAttachment, planRevision, recoverPendingRewrite, recoverStoredSessions,
  resolveRevisionText, retainedRevisionAttachments, RevisionError, revisionTextParts, rewriteMessage,
} from './revision.js';
import { prepareBoundedRevision, revalidateWork } from './bounded-revision.js';
import { commitBoundedRevision, restoreBoundedJournal } from './bounded-commit.js';
import {
  assertRewriteSetSpace, matchJournalRevision, readRewriteJournals, RewriteJournalSpaceError, scheduleSettledRewriteRetirement,
} from './rewrite-journal.js';
import { createRevisionNotifications, registerRevisionNotifications } from './revision-notifications.js';
import { isSamePath, resolveRolloutPath } from './platform.js';

export const name = 'message-revision';
export const inject = ['agents', 'connection', 'sessionController', 'sessionPersistence', 'sessions'];

const effectSymbol = Symbol.for('cordis.effect');
const originalSymbol = Symbol.for('cordis.original');
const headers = { 'Cache-Control': 'no-store' };
const BUSY = '回复生成中，请先停止或等待完成。';
const toolbarRevision = 1;
const activeRevision = Symbol.for('dsh-message-revision.active-rewrite');

function assertIdlePhase(agent) {
  // This rc.5 driver reports its private maintenance phase as public idle.
  if (agent.phase?.kind === 'maintenance') throw new RevisionError('会话正在维护或压缩上下文，请等操作完成后重试。');
  if (agent.status !== 'idle' || (agent.phase !== undefined && agent.phase?.kind !== 'idle')) throw new RevisionError(BUSY);
}

function assertIdle(agent) {
  assertIdlePhase(agent);
  if (agent.inbox.hasPending) throw new RevisionError('会话仍有排队消息，请等队列清空后再试。');
}

function effectDisposers(fiber, label) {
  const registered = fiber?.getEffects?.();
  if (!Array.isArray(registered) || typeof fiber?._disposables?.[Symbol.iterator] !== 'function') return [];
  const effects = registered.filter(effect => effect?.label === label);
  const disposers = [...fiber._disposables].filter(fn => typeof fn === 'function' && effects.includes(fn[effectSymbol]));
  return disposers.length === effects.length ? disposers : [];
}

function lifecycleDisposer(ctx, agent) {
  // The controller owns the outer effect, while the Agent loop owns its scope.
  // Cordis traces service.ctx to the caller, so use the actual controller owner.
  const owner = ctx.sessionController[originalSymbol]?.ctx?.fiber;
  const loop = agent.ctx?.fiber?.parent?.fiber;
  const [scope, duplicate] = effectDisposers(loop, `agentLoop.scope(${agent.id})`);
  const lifecycle = effectDisposers(owner, `agentLoop.lifecycle(${agent.id})`);
  const scopeMeta = agent.scope?.rawDispose?.[effectSymbol];
  const childEffects = scope?.[effectSymbol]?.children;
  const failure = () => new RevisionError('无法重新加载当前会话。');
  if (!scope || duplicate || lifecycle.length === 0 || !scopeMeta
    || !Array.isArray(childEffects) || !childEffects.includes(scopeMeta)) throw failure();

  return async () => {
    const ownerDisposers = new Set(owner._disposables);
    if (ctx.agents.get(agent.id) !== agent || ctx.sessions.get(agent.id) !== agent.session
      || ![...loop._disposables].includes(scope) || lifecycle.some(dispose => !ownerDisposers.has(dispose))) throw failure();
    // The latest controller registration is the live generation. Its native
    // teardown must also release the exact scope of the Agent we're replacing.
    const active = lifecycle.at(-1);
    assertIdlePhase(agent);
    await active();
    if ([...loop._disposables].includes(scope) || [...owner._disposables].includes(active)) throw failure();
    // Older revisions of this plugin could leave completed owner registrations.
    for (const dispose of lifecycle.slice(0, -1).reverse()) await dispose();
  };
}

async function assertNoActivity(ctx, sessionId) {
  const activities = await ctx.waterfall('workspace/session-activity', { sessionId }, async () => []);
  if (activities?.length) throw new RevisionError(BUSY);
}

function assertChildrenReleased(ctx, work) {
  for (const { header: { id } } of work.entries.slice(1)) {
    if (ctx.agents.get(id) || ctx.sessions.get(id)) {
      throw new RevisionError('关联的子智能体会话仍处于打开状态，请关闭后重试。');
    }
  }
}

function* nativeEvents(session, end = session.seq, start = 0) {
  // Use bounded ranges of the immutable native log instead of retaining a
  // second array of every event while the candidate is prepared or reopened.
  for (let offset = start; offset < end; offset += 512) {
    yield* session.snapshotEvents(offset, Math.min(end, offset + 512));
  }
}

function editableAgent(ctx, sessionId) {
  const agent = ctx.agents.get(sessionId);
  if (!agent || ctx.sessions.get(sessionId) !== agent.session) {
    throw new RevisionError('请先打开该会话。');
  }
  if (agent.session.header.origin === 'subagent') {
    throw new RevisionError('子智能体会话不支持直接编辑，请在主会话中操作。');
  }
  assertIdle(agent);
  return agent;
}

const permissionEvents = ['permission/preset', 'sandbox/mode', 'approval/policy'];

function lastPermissionEvents(events) {
  const latest = new Map();
  for (const event of events) if (permissionEvents.includes(event.type)) latest.set(event.type, event);
  return latest;
}

function foldControlState(events, cut) {
  const currentPermissions = new Map();
  const retainedPermissions = new Map();
  let pendingModel;
  let lastUsedModel;
  let retainedModel;
  let capturedPrefix = false;
  for (const event of events) {
    if (!capturedPrefix && event.seq >= cut) {
      retainedModel = pendingModel ?? lastUsedModel;
      capturedPrefix = true;
    }
    if (permissionEvents.includes(event.type)) {
      currentPermissions.set(event.type, event);
      if (event.seq < cut) retainedPermissions.set(event.type, event);
    } else if (event.type === 'model/selection') {
      pendingModel = event.data;
    } else if (event.type === 'request/header') {
      const { config, adapterDefaults } = event.data.header;
      const effort = adapterDefaults?.reasoningEffort === true ? undefined : config.reasoningEffort;
      lastUsedModel = { provider: config.provider, model: config.model,
        ...(effort === undefined ? {} : { reasoningEffort: effort }) };
      if (sameModel(pendingModel, lastUsedModel)) pendingModel = undefined;
    }
  }
  const currentModel = pendingModel ?? lastUsedModel;
  if (!capturedPrefix) retainedModel = currentModel;
  return { currentPermissions, retainedPermissions, currentModel, retainedModel };
}

function sameModel(first, second) {
  return first?.provider === second?.provider && first?.model === second?.model
    && first?.reasoningEffort === second?.reasoningEffort;
}

function preparePermissions(ctx, agent, current, retained, preset) {
  if (preset !== undefined) {
    const service = ctx.get('permissionPresets');
    if (!service || preset === 'custom') throw new RevisionError('不支持该权限预设。');
    // A detached native Session gives the real permission service its projection
    // and Auto admission gate, without changing the live Session or its revision.
    const seed = [...current.values()].map((event, seq) => ({ ...event, seq }));
    let detached;
    try {
      detached = agent.session.constructor.create(agent.id, seed);
      service.set(detached, preset);
      // Require the effect to be recognizable by the stock permission resolver.
      if (service.current(detached) !== preset) throw new Error('preset was not applied');
    } catch (error) {
      throw new RevisionError('所选权限预设不可用，请重新选择。', { cause: error });
    }
    for (const [type, event] of lastPermissionEvents(detached.snapshotEvents())) current.set(type, event);
  }
  return permissionEvents.flatMap(type => {
    const next = current.get(type);
    if (!next || JSON.stringify(next.data) === JSON.stringify(retained.get(type)?.data)) return [];
    return [{ type, data: next.data }];
  });
}

async function prepareRevisionInputs(ctx, agent, input, original, settings) {
  const retained = retainedRevisionAttachments(original, input.removedAttachments);
  const receiptIds = input.receiptIds ?? [];
  const uploads = receiptIds.length === 0 ? undefined : ctx.get('fileUploads');
  const staged = receiptIds.map(id => {
    const part = uploads?.resolve(agent, id);
    if (!isRevisionAttachment(part)) throw new RevisionError('附件上传已失效，请重新添加。');
    return part;
  });
  if (input.text.trim() === '' && retained.length + staged.length === 0) {
    throw new RevisionError('请输入消息内容或至少保留一个附件。');
  }
  const stagedImages = staged.some(part => part.type === 'image');
  const retainedImages = retained.some(part => part.type === 'image');
  // Native admission counts upload bytes. Historical image references only
  // contain the normalized size, even if no originalDimensions are present.
  if (stagedImages && retainedImages) {
    throw new RevisionError('旧图片的原始上传大小无法核对。请移除旧图片，或用原文件重新添加后再发送。');
  }
  const model = input.model ?? settings.model;
  const hasImages = stagedImages || retainedImages;
  const llm = input.model || hasImages ? ctx.get('llm') : undefined;
  if (input.model) {
    try {
      if (!llm?.listProviders().some(provider => provider.id === model.provider)
        || !(await llm.listModels(model.provider)).some(candidate => candidate.id === model.model)) {
        throw new Error('model missing from catalog');
      }
      await llm.resolveCallConfig(model);
    } catch (error) {
      throw new RevisionError('所选模型或思考强度不可用，请重新选择。', { cause: error });
    }
  }
  if (hasImages) {
    try {
      const info = await llm?.resolveModelInfo(model?.provider, model?.model);
      if (info?.inputModalities && !info.inputModalities.includes('image')) {
        throw new Error('model does not accept images');
      }
    } catch (error) {
      throw new RevisionError('所选模型不支持图片输入，请切换模型或移除图片。', { cause: error });
    }
  }
  let addedAttachments = [];
  if (staged.length > 0) {
    const attachments = ctx.get('attachments');
    if (!attachments) throw new RevisionError('当前环境不支持上传附件。');
    try {
      addedAttachments = await attachments.admitPromptContent(staged);
    } catch (error) {
      throw new RevisionError('附件校验未通过，请重新添加。', { cause: error });
    }
  }
  return { retained, addedAttachments, model };
}

function revisionControls(ctx, agent, input, cut, model) {
  const { currentPermissions, retainedPermissions, currentModel, retainedModel } = foldControlState(nativeEvents(agent.session), cut);
  const controls = preparePermissions(ctx, agent, currentPermissions, retainedPermissions, input.permissionPreset);
  if (model && !sameModel(model, retainedModel) && (input.model || currentModel)) {
    controls.push({ type: 'model/selection', data: model });
  }
  return controls;
}

function revisionSettings(ctx, agent) {
  return {
    model: agent.model.current(),
    permissionPreset: ctx.get('permissionPresets')?.current(agent.session),
  };
}

async function sameEffectiveModel(ctx, selected, original) {
  if (selected === undefined || sameModel(selected, original)) return true;
  if (selected.provider !== original?.provider || selected.model !== original?.model) return false;
  const llm = ctx.get('llm');
  if (!llm) return false;
  try {
    const [before, after] = await Promise.all([llm.resolveCallConfig(original), llm.resolveCallConfig(selected)]);
    return sameModel(before, after);
  } catch {
    // Validation at submission will explain unsupported settings.
    return false;
  }
}

async function isUnchangedRevision(ctx, input, original, settings) {
  const before = revisionTextParts(original);
  const after = resolveRevisionText(original, input);
  return before.length === after.length && before.every((part, index) => part === after[index])
    && !input.removedAttachments?.length && !input.receiptIds?.length
    && (input.permissionPreset === undefined || input.permissionPreset === settings.permissionPreset)
    && await sameEffectiveModel(ctx, input.model, settings.model);
}

async function recoverEditableSession(ctx, sessionId, recoverDelivery) {
  let restored;
  try {
    restored = await recoverPendingRewrite(ctx.sessionPersistence, await ctx.sessionPersistence.stat(sessionId), {
      beforeCommit: revisionCacheInvalidator(ctx),
      canRollback: id => !ctx.agents.get(id) && !ctx.sessions.get(id),
      restoreBoundedJournal,
      recoverDelivery: recoverDelivery && (journal => recoverDelivery(journal.intent.parentId)),
    });
  } catch (error) {
    if (error?.name === 'SessionAlreadyOwnedError') {
      throw new RevisionError('上次编辑仍在等待会话释放，稍后可重新打开编辑框。', { cause: error });
    }
    if (error?.name === 'RewriteRollbackConflictError') throw new RevisionError(error.message, { cause: error });
    throw error;
  }
  if (restored) await ctx.sessionController.resolveAgent(sessionId);
}

async function inspectRevision(ctx, { sessionId, messageId }, recoverDelivery) {
  await recoverEditableSession(ctx, sessionId, recoverDelivery);
  const agent = editableAgent(ctx, sessionId);
  await ctx.sessions.flush(agent.session);
  const snapshot = await ctx.sessionPersistence.stat(sessionId);
  if (editableAgent(ctx, sessionId) !== agent || typeof snapshot?.revision !== 'string') {
    throw new RevisionError('会话状态已变更，请刷新后重试。');
  }
  await assertNoPendingRewrite(snapshot);
  const { message } = planRevision(nativeEvents(agent.session), messageId, agent.session.inheritedEventCount);
  const textParts = revisionTextParts(message);
  return { ok: true, toolbarRevision, revision: snapshot.revision,
    text: textParts.join(''), textParts, settings: revisionSettings(ctx, agent),
    attachments: message.content.flatMap((block, index) => isRevisionAttachment(block) ? [{ index, block }] : []),
  };
}

function deliveredRevision(agent, messageId) {
  for (const event of nativeEvents(agent.session)) {
    if (event.type === 'user/message' && event.data.id === messageId) return true;
  }
  return false;
}

function committedPromptState(agent, journal) {
  const { messageId } = journal.intent.delivery;
  const begin = journal.intent.artifacts[0].candidate.records;
  if (agent.session.seq < begin) throw new Error('Native session does not contain the committed revision');
  // The committed candidate starts with exactly one pending next-turn item.
  let position = 0;
  let turn;
  let claim;
  for (const { type, data } of nativeEvents(agent.session, agent.session.seq, begin)) {
    if (type === 'user/message' && data.id === messageId) return 'settled';
    if (type === 'turn/start') {
      turn = data.turn;
    } else if (type === 'turn/end' && data.turn === turn) {
      if (claim?.turn === data.turn) {
        // A closed turn with recorded execution must not submit its input again.
        if (data.reason.kind !== 'interrupted' || claim.executed) return 'settled';
        claim.interrupted = true;
      }
      turn = undefined;
    }
    if (claim && turn === claim.turn && data.turn === claim.turn
      && (type === 'assistant/message' || type === 'assistant/attempt' || type === 'tool/call' || type === 'tool/result')) {
      claim.executed = true;
    }
    if (type !== 'agent/inbox/spliced' || data.target !== 'next-turn') continue;
    const replacement = data.inserted.findIndex(message => message.id === messageId);
    if (position === undefined) {
      if (replacement >= 0) {
        position = data.start + replacement;
        claim = undefined;
      }
      continue;
    }
    if (position < data.start) continue;
    const removed = data.removedCount ?? 0;
    if (position < data.start + removed) {
      if (replacement < 0) {
        if (data.outcome === 'canceled') return 'settled';
        if (turn === undefined) throw new Error('Native inbox claimed the committed input outside a turn');
        position = undefined;
        claim = { turn, executed: false, interrupted: false };
      } else position = data.start + replacement;
    } else position += data.inserted.length - removed;
  }
  if (position === undefined) return claim?.interrupted ? 'interrupted' : 'claimed';
  if (agent.inbox.nextTurn[position]?.id !== messageId) throw new Error('Cannot reconcile the committed input with the native inbox');
  return 'pending';
}

function findCommittedInput(session, journal) {
  const { messageId } = journal.intent.delivery;
  const begin = journal.intent.artifacts[0].candidate.records;
  for (let end = begin; end > 0; end -= 512) {
    const events = session.snapshotEvents(Math.max(0, end - 512), end);
    for (let index = events.length - 1; index >= 0; index--) {
      const { type, data } = events[index];
      if (type !== 'agent/inbox/spliced' || data.target !== 'next-turn') continue;
      const message = data.inserted.find(item => item.id === messageId);
      if (message) return message;
    }
  }
  throw new Error('Cannot find the revision input in the committed history');
}

async function deliverCommittedPrompt(ctx, agent, journal) {
  const { parentId } = journal.intent;
  if (ctx.agents.get(parentId) !== agent || ctx.sessions.get(parentId) !== agent.session) return 'waiting';
  const state = committedPromptState(agent, journal);
  if (state === 'settled') return await ctx.sessions.flush(agent.session) === true ? 'settled' : 'waiting';
  if (state === 'claimed') return 'started';
  if (agent.status !== 'idle' || agent.inbox.nextStep.length !== 0) return 'waiting';
  if (state === 'interrupted') {
    if (agent.inbox.nextTurn.length !== 0) return 'waiting';
    // Native recovery durably closed an earlier claim before its user/message.
    // Restore the same input only after confirming no request or tool ran.
    agent.followup(findCommittedInput(agent.session, journal));
  } else {
    if (agent.inbox.nextTurn.length !== 1) return 'waiting';
    // rc.5 can wake the existing queue item without another enqueue record.
    if (typeof agent.wakeDriver !== 'function') throw new Error('Native inbox wakeup is unavailable');
    agent.wakeDriver();
  }
  const after = committedPromptState(agent, journal);
  return after === 'settled' && await ctx.sessions.flush(agent.session) === true ? 'settled' : 'started';
}

async function recoverCommittedPrompt(ctx, journal) {
  const { parentId } = journal.intent;
  let agent = ctx.agents.get(parentId);
  const snapshot = agent ? undefined : await ctx.sessionPersistence.stat(parentId);
  if (await matchJournalRevision(journal) !== 'candidate') throw new Error('Committed revision no longer matches its journal');
  if (!agent) {
    if (!isSamePath(snapshot?.location?.path, journal.dshPath ?? journal.parentPath) || ctx.sessions.get(parentId)
      || !isSamePath(await resolveRolloutPath(snapshot.location.path), journal.parentPath)
      || (await ctx.sessionPersistence.stat(parentId))?.revision !== snapshot.revision) return 'waiting';
    const reopened = await ctx.sessionController.resolveAgent(parentId);
    if (!reopened.agent) throw reopened.error ?? new Error('Cannot reopen the committed session');
    agent = reopened.agent;
  }
  return deliverCommittedPrompt(ctx, agent, journal);
}

function installDeliveryRecovery(ctx) {
  const tracked = new Set();
  const pending = new Set();
  let timer;
  let started = false;
  let disposed = false;
  let running = false;
  let currentId;
  // Process outstanding deliveries serially after the initial journal scan.
  // A failed cold open waits for the session's next native activation.
  function schedule() {
    if (!started || timer || running || disposed || pending.size === 0) return;
    timer = setImmediate(() => { timer = undefined; void run(); });
    timer.unref?.();
  }
  async function run() {
    running = true;
    try {
      for (const id of pending) {
        if (disposed) break;
        pending.delete(id);
        currentId = id;
        try {
          const snapshot = await ctx.sessionPersistence.stat(id);
          const journal = (await readRewriteJournals(snapshot)).find(entry => entry.committed && entry.intent.delivery);
          if (!journal) {
            tracked.delete(id);
            pending.delete(id);
          } else if (await recoverCommittedPrompt(ctx, journal) === 'settled') {
            scheduleSettledRewriteRetirement(journal, { onCleanupError(error) {
              ctx.logger.warn('Saved message revision cleanup failed for %s: %s', id, error);
            } });
            tracked.delete(id);
            pending.delete(id);
          }
        } catch (error) {
          ctx.logger.warn('Pending message revision delivery for %s: %s', id, error);
        } finally { currentId = undefined; }
      }
    } finally {
      running = false;
      schedule();
    }
  }
  function track(id) { tracked.add(id); pending.add(id); schedule(); }
  function onAgent(agent) {
    if (agent.id !== currentId && tracked.has(agent.id)) { pending.add(agent.id); schedule(); }
  }
  ctx.effect(() => {
    const created = ctx.on('agent/created', ({ agent }) => onAgent(agent));
    const idle = ctx.on('agent/status', ({ agent, status }) => { if (status === 'idle') onAgent(agent); });
    const finish = ctx.on('session/event', (session, event) => {
      if (event.type !== 'user/message' && event.type !== 'turn/end') return;
      const agent = ctx.agents.get(session.id);
      if (agent?.session !== session || !tracked.has(agent.id)) return;
      // Delivery can finish while this very session is being checked. Revisit
      // it afterwards if that check did not already observe the final event.
      pending.add(agent.id);
      schedule();
    });
    return () => { disposed = true; clearImmediate(timer); created(); idle(); finish(); pending.clear(); tracked.clear(); };
  });
  return { track, start() { started = true; schedule(); } };
}

function revisionCacheInvalidator(ctx) {
  return async ids => {
    // Same ID and header means the native cache cannot detect a history rewrite.
    const cache = ctx.get('storageDomain')?.get('session_projcache')?.table('sessions');
    if (cache) for (const id of ids) await cache.delete(id);
  };
}

/** The Web route and isolated tests share this host operation. */
export async function reviseSession(ctx, input, options = {}) {
  const { sessionId, messageId } = input;
  await recoverEditableSession(ctx, sessionId, options.recoverDelivery);
  const agent = editableAgent(ctx, sessionId);
  await assertNoActivity(ctx, sessionId);
  await ctx.sessions.flush(agent.session);
  await assertNoActivity(ctx, sessionId);
  const snapshot = await ctx.sessionPersistence.stat(sessionId);
  await assertNoPendingRewrite(snapshot);
  if (input.expectedRevision !== undefined && snapshot?.revision !== input.expectedRevision) {
    throw new RevisionError('会话状态已变更，请重新打开编辑框。');
  }
  if (editableAgent(ctx, sessionId) !== agent) throw new RevisionError('会话状态已变更，请刷新后重试。');
  const seq = agent.session.seq;
  const { message: original } = planRevision(nativeEvents(agent.session, seq), messageId, agent.session.inheritedEventCount);
  const settings = revisionSettings(ctx, agent);
  // Provider lookups can take arbitrarily long. Complete them before claiming
  // the single resource slot used by the log worker and transaction.
  if (await isUnchangedRevision(ctx, input, original, settings)) return { ok: true, unchanged: true };
  const prepared = await prepareRevisionInputs(ctx, agent, input, original, settings);
  options.signal?.throwIfAborted();
  if (globalThis[activeRevision]) throw new RevisionError('已有会话正在编辑，请稍后重试。');
  globalThis[activeRevision] = true;
  try {
    return await reviseSessionExclusive(ctx, input, { agent, seq, snapshot, prepared }, options);
  } finally {
    globalThis[activeRevision] = false;
  }
}

async function reviseSessionExclusive(ctx, input, { agent, seq, snapshot, prepared },
  { onReopening, onCommitted, signal, recoverDelivery }) {
  const { sessionId, messageId } = input;
  const validateAgent = () => {
    if (editableAgent(ctx, sessionId) !== agent || agent.session.seq !== seq
      || (!input.model && !sameModel(agent.model.current(), prepared.model))) {
      throw new RevisionError('会话状态已变更，请重新提交。');
    }
  };
  validateAgent();
  if (typeof agent.wakeDriver !== 'function') throw new RevisionError('当前 DSH 版本不支持安全重发编辑后的消息。');
  const dispose = lifecycleDisposer(ctx, agent);
  const work = await prepareBoundedRevision(ctx.sessionPersistence, snapshot, messageId, plan => {
    validateAgent();
    const [record] = agent.session.snapshotEvents(plan.input.index, plan.input.index + 1);
    const queued = record?.type === 'agent/inbox/spliced' && record.data.inserted?.[plan.input.offset];
    if (queued?.id !== messageId || queued.source?.kind !== 'user') {
      throw new RevisionError('无法核对原消息的入队记录，请重新打开编辑框。');
    }
    const retained = retainedRevisionAttachments(queued, input.removedAttachments);
    if (JSON.stringify(retained) !== JSON.stringify(prepared.retained)) {
      throw new RevisionError('会话附件与原始消息不一致，请重新打开编辑框。');
    }
    const controls = revisionControls(ctx, agent, input, plan.cut, prepared.model);
    const { content } = rewriteMessage(queued, input, input.removedAttachments ?? [], prepared.addedAttachments);
    return { message: { id: messageId, content }, controls };
  }, { signal, eventCount: seq, inheritedCount: agent.session.inheritedEventCount });
  if (!work) return { ok: true, unchanged: true };

  const rewrittenChildren = work.artifacts.slice(1).map(artifact => artifact.header.id);
  let childChanges;
  let updated;
  let reopenedSeq;
  let released = false;
  try {
    await ctx.sessions.flush(agent.session);
    await assertNoActivity(ctx, sessionId);
    await revalidateWork(ctx.sessionPersistence, work);
    await assertNoPendingRewrite(snapshot);
    await assertRewriteSetSpace(work.parent, work.artifacts);
    validateAgent();
    assertChildrenReleased(ctx, work);
    signal?.throwIfAborted();
    onReopening?.();
    await dispose();
    released = true;
    if (ctx.sessions.get(sessionId) || ctx.agents.get(sessionId)) {
      throw new RevisionError('会话仍在释放中，请稍后重试。');
    }
    await commitBoundedRevision(ctx.sessionPersistence, work, {
      beforeCommit: revisionCacheInvalidator(ctx),
      messageId,
      onCleanupError(error) {
        ctx.logger.warn('Saved message revision cleanup failed for %s: %s', sessionId, error);
      },
      canRollback() {
        return work.entries.every(({ header }) => !ctx.agents.get(header.id) && !ctx.sessions.get(header.id));
      },
      async closeFailedReload() {
        if (!updated) return;
        await assertNoActivity(ctx, sessionId);
        if (ctx.agents.get(sessionId) === updated && ctx.sessions.get(sessionId) === updated.session
          && updated.status === 'idle' && updated.session.seq === reopenedSeq
          && updated.inbox.nextTurn.length === 1 && updated.inbox.nextTurn[0].id === messageId
          && updated.inbox.nextStep.length === 0) {
          await lifecycleDisposer(ctx, updated)();
        }
      },
      async verifyReload() {
        const reopened = await ctx.sessionController.resolveAgent(sessionId);
        if (!reopened.agent) throw reopened.error ?? new Error('无法重新打开会话。');
        updated = reopened.agent;
        reopenedSeq = updated.session.seq;
        if (typeof updated.wakeDriver === 'function' && (updated.inbox.nextTurn.some(message => message.id === messageId)
          || deliveredRevision(updated, messageId))) return;
        // A startup hook may discard the candidate input and still return normally.
        // Close that provisional Agent only if no other request owns it.
        if (updated.status === 'idle' && !updated.inbox.hasPending) {
          const seq = updated.session.seq;
          await assertNoActivity(ctx, sessionId);
          if (ctx.agents.get(sessionId) === updated && updated.session.seq === seq
            && updated.status === 'idle' && !updated.inbox.hasPending) {
            await lifecycleDisposer(ctx, updated)();
          }
        }
        throw new Error('重开会话时更新后的消息被移除。');
      },
      async afterCommit({ removedChildren }, journal) {
        childChanges = {
          ...(rewrittenChildren.length > 0 && { rewrittenChildren }),
          ...(removedChildren.length > 0 && { withdrawnChildren: removedChildren }),
          ...(work.affectedParentIds?.length > 0 && { affectedParentIds: work.affectedParentIds }),
        };
        onCommitted?.(childChanges);
        try {
          const delivery = await deliverCommittedPrompt(ctx, updated, journal);
          if (delivery === 'settled') return true;
          recoverDelivery?.(sessionId);
          if (delivery === 'started') return false;
          throw new Error('消息已保存，但尚未触发发送。');
        } catch (error) {
          recoverDelivery?.(sessionId);
          throw error;
        }
      },
    });
  } catch (error) {
    const rejected = error instanceof RewriteJournalSpaceError
      ? new RevisionError(error.message, { cause: error }) : error;
    if (released && (rejected instanceof RevisionError || rejected.name === 'SessionAlreadyOwnedError')) {
      await ctx.sessionController.resolveAgent(sessionId);
    }
    throw rejected;
  } finally {
    await work.dispose().catch(error => ctx.logger?.warn('Cannot clean up message revision staging: %s', error));
  }
  return { ok: true, reload: true, ...childChanges };
}

function validId(value) {
  return typeof value === 'string' && value.length > 0 && value.length <= 2048 && !/[\u0000-\u001f]/u.test(value);
}

function validDistinctList(value, item) {
  return Array.isArray(value) && value.length <= 128 && value.every(item) && new Set(value).size === value.length;
}

function parseSubmission(body) {
  const { sessionId, messageId, text, textParts, expectedRevision, removedAttachments = [], receiptIds = [], permissionPreset } = body;
  const model = body.model;
  const usesToolbar = permissionPreset !== undefined || model !== undefined
    || removedAttachments?.length > 0 || receiptIds?.length > 0;
  if ((usesToolbar || body.toolbarRevision !== undefined) && body.toolbarRevision !== toolbarRevision) return undefined;
  if (typeof text !== 'string' || text.length > 200_000
    || (textParts !== undefined && (!Array.isArray(textParts) || textParts.length > 2_000
      || !textParts.every(part => typeof part === 'string' && part.length <= text.length)
      || textParts.reduce((size, part) => size + part.length, 0) !== text.length))
    || typeof expectedRevision !== 'string' || expectedRevision.length > 1024
    || !validDistinctList(removedAttachments, index => Number.isSafeInteger(index) && index >= 0)
    || !validDistinctList(receiptIds, id => validId(id) && id.length <= 128)
    || (permissionPreset !== undefined && (!validId(permissionPreset) || permissionPreset.length > 256))
    || (model !== undefined && (model === null || typeof model !== 'object' || Array.isArray(model)
      || !validId(model.provider) || !validId(model.model)
      || (model.reasoningEffort !== undefined && (!validId(model.reasoningEffort) || model.reasoningEffort.length > 256))))) {
    return undefined;
  }
  return { sessionId, messageId, text, textParts, expectedRevision, removedAttachments, receiptIds, permissionPreset,
    ...(model && { model: { provider: model.provider, model: model.model,
      ...(model.reasoningEffort !== undefined && { reasoningEffort: model.reasoningEffort }) } }),
  };
}

export async function apply(ctx) {
  const notifications = createRevisionNotifications();
  const deliveryRecovery = installDeliveryRecovery(ctx);
  const recoverDelivery = deliveryRecovery.track;
  const recoveryLifetime = new AbortController();
  ctx.effect(() => () => recoveryLifetime.abort());
  if (typeof ctx.sessionPersistence.list === 'function') {
    await recoverStoredSessions(ctx.sessionPersistence, {
      retrySignal: recoveryLifetime.signal,
      beforeCommit: revisionCacheInvalidator(ctx),
      canRollback: id => !ctx.agents.get(id) && !ctx.sessions.get(id),
      restoreBoundedJournal,
      recoverDelivery: journal => { recoverDelivery(journal.intent.parentId); },
      onError: (id, error) => ctx.logger.warn('Pending message revision recovery for %s: %s', id, error),
    }).catch(error => ctx.logger.warn('Cannot scan pending message revisions: %s', error));
  }
  ctx.effect(() => () => notifications.close());
  ctx.inject?.(['typert'], scope => registerRevisionNotifications(scope, notifications));
  const requests = new Set();
  ctx.effect(() => ctx.connection.fetch.register({
    path: '/api/message-revision',
    methods: ['POST'],
    requestBody: 'buffered',
    async fetch(request) {
      const send = (value, status = 200) => Response.json(value, { status, headers });
      // Exact custom Connection routes use the host's owner admission policy.
      if (request.headers.get('content-type')?.split(';', 1)[0].trim() !== 'application/json') {
        return send({ ok: false, error: '请求格式无效。' }, 400);
      }
      const body = await request.json().catch(() => undefined);
      if (!validId(body?.sessionId) || !validId(body?.messageId)) {
        return send({ ok: false, toolbarRevision, error: '未找到该消息。' }, 400);
      }
      if (body.text === undefined) {
        try {
          return send(await inspectRevision(ctx, body, recoverDelivery));
        } catch (error) {
          if (error instanceof RevisionError) return send({ ok: false, error: error.message }, 409);
          ctx.logger.warn('Message revision inspection failed: %s', error);
          return send({ ok: false, error: '无法读取该消息，请刷新后重试。' }, 500);
        }
      }
      const submission = parseSubmission(body);
      if (!submission) {
        return send({ ok: false, error: '编辑内容或参数无效，请重新打开编辑框。' }, 400);
      }
      if (requests.has(body.sessionId)) return send({ ok: false, error: '当前会话正在处理编辑请求。' }, 409);
      requests.add(body.sessionId);
      let reopening = false;
      let childChanges;
      const failure = (error, status) => send({ ok: false, error, ...(reopening && { reconnect: true }), ...childChanges }, status);
      try {
        request.signal.throwIfAborted();
        return send(await reviseSession(ctx, submission, {
          signal: request.signal,
          recoverDelivery,
          onReopening: () => { reopening = true; },
          onCommitted(changes) {
            childChanges = changes;
            notifications.publish(changes);
          },
        }));
      } catch (error) {
        if (error instanceof RevisionError || error?.name === 'RewriteRollbackConflictError') return failure(error.message, 409);
        if (error?.name === 'SessionAlreadyOwnedError') {
          return failure('会话正被其他操作占用，请稍后重试。', 409);
        }
        ctx.logger.warn('Message revision failed: %s', error);
        return failure(reopening ? '编辑未完成，请先检查当前会话。' : '编辑失败，请重试。', 500);
      } finally {
        requests.delete(body.sessionId);
      }
    },
  }));
  deliveryRecovery.start();
}
