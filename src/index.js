import {
  assertNoPendingRewrite, isRevisionAttachment, planRevision, recoverStoredSessions, resolveRevisionText,
  retainedRevisionAttachments, RevisionError, revisionTextParts, rewriteStoredSession, splitChildCatalog,
} from './revision.js';

export const name = 'message-revision';
export const inject = ['agents', 'connection', 'sessionController', 'sessionPersistence', 'sessions'];

const effectSymbol = Symbol.for('cordis.effect');
const originalSymbol = Symbol.for('cordis.original');
const headers = { 'Cache-Control': 'no-store' };
const BUSY = '回复生成中，请先停止或等待完成。';
const toolbarRevision = 1;

function assertIdle(agent) {
  if (agent.status !== 'idle') throw new RevisionError(BUSY);
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

function assertChildrenIdle(ctx, agent, events, plan) {
  const catalog = events.slice(agent.session.inheritedEventCount)
    .filter(event => event.type === 'subagent/catalog');
  const { withdrawn } = splitChildCatalog(catalog, plan);
  const childIds = new Set(catalog.map(event => event.data.childId));
  if (ctx.agents.list().some(candidate => childIds.has(candidate.id) && candidate.status !== 'idle')) {
    throw new RevisionError('子智能体仍在运行，请等执行结束后再试。');
  }
  const withdrawnIds = new Set(withdrawn.map(event => event.data.childId));
  if (ctx.agents.list().some(candidate => withdrawnIds.has(candidate.id))) {
    throw new RevisionError('关联的子智能体会话仍处于打开状态，请关闭后重试。');
  }
}

function editableAgent(ctx, sessionId) {
  const agent = ctx.agents.get(sessionId);
  if (!agent || ctx.sessions.get(sessionId) !== agent.session) {
    throw new RevisionError('请先打开该会话。');
  }
  if (agent.session.header.origin === 'subagent' || agent.session.header.parentSession) {
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

function modelAt(events) {
  let pending;
  let lastUsed;
  for (const event of events) {
    if (event.type === 'model/selection') pending = event.data;
    if (event.type === 'request/header') {
      const { config, adapterDefaults } = event.data.header;
      const effort = adapterDefaults?.reasoningEffort === true ? undefined : config.reasoningEffort;
      lastUsed = { provider: config.provider, model: config.model,
        ...(effort === undefined ? {} : { reasoningEffort: effort }) };
      if (sameModel(pending, lastUsed)) pending = undefined;
    }
  }
  return pending ?? lastUsed;
}

function sameModel(first, second) {
  return first?.provider === second?.provider && first?.model === second?.model
    && first?.reasoningEffort === second?.reasoningEffort;
}

function preparePermissions(ctx, agent, events, prefix, preset) {
  const current = lastPermissionEvents(events);
  const retained = lastPermissionEvents(prefix);
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

async function prepareAmendments(ctx, agent, input, events, plan) {
  const retained = retainedRevisionAttachments(plan.message, input.removedAttachments);
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
  const prefix = events.slice(0, plan.cut);
  const model = input.model ?? agent.model.current();
  const hasImages = [...retained, ...staged].some(part => part.type === 'image');
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
      // Count retained images against the same combined native message limits.
      const images = retained.filter(part => part.type === 'image')
        .map(part => ({ ...part, encodedBytes: part.attachment.bytes }));
      addedAttachments = (await attachments.admitPromptContent([...images, ...staged])).slice(images.length);
    } catch (error) {
      throw new RevisionError('附件校验未通过，请重新添加。', { cause: error });
    }
  }
  const controls = preparePermissions(ctx, agent, events, prefix, input.permissionPreset);
  if (model && !sameModel(model, modelAt(prefix)) && (input.model || modelAt(events))) {
    controls.push({ type: 'model/selection', data: model });
  }
  return { addedAttachments, controls };
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

async function inspectRevision(ctx, { sessionId, messageId }) {
  const agent = editableAgent(ctx, sessionId);
  await ctx.sessions.flush(agent.session);
  const snapshot = await ctx.sessionPersistence.stat(sessionId);
  if (editableAgent(ctx, sessionId) !== agent || typeof snapshot?.revision !== 'string') {
    throw new RevisionError('会话状态已变更，请刷新后重试。');
  }
  await assertNoPendingRewrite(snapshot);
  const events = agent.session.snapshotEvents();
  const plan = planRevision(events, messageId, agent.session.inheritedEventCount);
  const textParts = revisionTextParts(plan.message);
  return { ok: true, toolbarRevision, revision: snapshot.revision,
    text: textParts.join(''), textParts, settings: revisionSettings(ctx, agent),
    attachments: plan.message.content.flatMap((block, index) => isRevisionAttachment(block) ? [{ index, block }] : []),
  };
}

function deliveredRevision(agent, messageId) {
  return agent.session.snapshotEvents().some(event => event.type === 'user/message' && event.data.id === messageId);
}

function revisionCacheInvalidator(ctx) {
  return async ids => {
    // Same ID and header means the native cache cannot detect a history rewrite.
    const cache = ctx.get('storageDomain')?.get('session_projcache')?.table('sessions');
    if (cache) for (const id of ids) await cache.delete(id);
  };
}

/** The Web route and isolated tests share this host operation. */
export async function reviseSession(ctx, input, { onReopening } = {}) {
  const { sessionId, messageId } = input;
  const agent = editableAgent(ctx, sessionId);
  await assertNoActivity(ctx, sessionId);
  await ctx.sessions.flush(agent.session);
  await assertNoActivity(ctx, sessionId);
  const snapshot = await ctx.sessionPersistence.stat(sessionId);
  await assertNoPendingRewrite(snapshot);
  if (input.expectedRevision !== undefined) {
    if (snapshot?.revision !== input.expectedRevision) {
      throw new RevisionError('会话状态已变更，请重新打开编辑框。');
    }
  }
  assertIdle(agent);
  if (ctx.agents.get(sessionId) !== agent || ctx.sessions.get(sessionId) !== agent.session) {
    throw new RevisionError('会话状态已变更，请刷新后重试。');
  }
  const events = agent.session.snapshotEvents();
  const plan = planRevision(events, messageId, agent.session.inheritedEventCount);
  if (await isUnchangedRevision(ctx, input, plan.message, revisionSettings(ctx, agent))) {
    return { ok: true, unchanged: true };
  }
  const dispose = lifecycleDisposer(ctx, agent);
  assertChildrenIdle(ctx, agent, events, plan);
  const amendments = await prepareAmendments(ctx, agent, input, events, plan);
  await ctx.sessions.flush(agent.session);
  await assertNoActivity(ctx, sessionId);
  if (editableAgent(ctx, sessionId) !== agent || agent.session.seq !== events.length) {
    throw new RevisionError('会话状态已变更，请刷新后重试。');
  }
  assertChildrenIdle(ctx, agent, events, plan);
  onReopening?.();
  await dispose();
  if (ctx.sessions.get(sessionId) || ctx.agents.get(sessionId)) {
    throw new RevisionError('会话仍在释放中，请稍后重试。');
  }

  let rewritten;
  let updated;
  let reopenedSeq;
  try {
    rewritten = await rewriteStoredSession(ctx.sessionPersistence, input, {
      ...amendments,
      beforeCommit: revisionCacheInvalidator(ctx),
      onCleanupError(error) {
        ctx.logger.warn('Saved message revision cleanup failed for %s: %s', sessionId, error);
      },
      canRollback() {
        return ctx.agents.get(sessionId) === undefined && ctx.sessions.get(sessionId) === undefined;
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
        if (updated.inbox.nextTurn.some(message => message.id === messageId) || deliveredRevision(updated, messageId)) return;
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
      async afterCommit({ messageId }) {
        const pending = updated.inbox.nextTurn;
        if (updated.status === 'idle' && pending.length === 1 && pending[0].id === messageId) {
          if (!updated.inbox.remove(pending[0].id)) throw new Error('消息已保存，但未能触发发送。');
          updated.followup(pending[0]);
          await ctx.sessions.flush(updated.session);
        } else if (!deliveredRevision(updated, messageId)) {
          throw new Error('消息已保存，但尚未触发发送。');
        }
      },
    });
  } catch (error) {
    if (error instanceof RevisionError) await ctx.sessionController.resolveAgent(sessionId);
    throw error;
  }
  return { ok: true, reload: true, ...(rewritten.cleanupWarning && { cleanupWarning: true }) };
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
  if (typeof ctx.sessionPersistence.list === 'function') {
    await recoverStoredSessions(ctx.sessionPersistence, {
      beforeCommit: revisionCacheInvalidator(ctx),
      canRollback: id => !ctx.agents.get(id) && !ctx.sessions.get(id),
      onError: (id, error) => ctx.logger.warn('Pending message revision recovery for %s: %s', id, error),
    }).catch(error => ctx.logger.warn('Cannot scan pending message revisions: %s', error));
  }
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
          return send(await inspectRevision(ctx, body));
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
      const failure = (error, status) => send({ ok: false, error, ...(reopening && { reconnect: true }) }, status);
      try {
        request.signal.throwIfAborted();
        return send(await reviseSession(ctx, submission, { onReopening: () => { reopening = true; } }));
      } catch (error) {
        if (error instanceof RevisionError) return failure(error.message, 409);
        if (error?.name === 'SessionAlreadyOwnedError') {
          return failure('会话正被其他操作占用，请稍后重试。', 409);
        }
        ctx.logger.warn('Message revision failed: %s', error);
        return failure('编辑失败，请重试。', 500);
      } finally {
        requests.delete(body.sessionId);
      }
    },
  }));
}
