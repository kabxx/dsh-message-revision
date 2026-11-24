import { lstat } from 'node:fs/promises';
import { basename, isAbsolute, join } from 'node:path';
import { RevisionError, splitChildCatalog } from './revision.js';
import { createRevisionCandidateDirectory } from './revision-candidates.js';
import { runRolloutWorker } from './rollout-process.js';
import { fileIdentity, legacyLogName, nativeLogName } from './rollout-stream.js';
import { MAX_NATIVE_LEASES } from './native-lease.js';
import { isCanonicalPath, isSamePath, isSupportedPlatform, resolveRolloutPath, rolloutStorageRoot } from './platform.js';

const maxSessions = MAX_NATIVE_LEASES;
let preparing = false;

function check(condition, message) {
  if (!condition) throw new RevisionError(message);
}

const stale = '会话状态已变更，请重新提交。';

function assertSession(snapshot, retainedChild) {
  const path = snapshot?.location?.path;
  check(typeof snapshot?.header?.id === 'string' && typeof snapshot.revision === 'string'
    && snapshot.location?.kind === 'jsonl' && typeof path === 'string' && isAbsolute(path),
  '当前会话存储格式不支持编辑。');
  const name = basename(path);
  const legacy = legacyLogName.test(name);
  check(nativeLogName.test(name) || (retainedChild === true && legacy), legacy && retainedChild === false
    ? '请在 DSH 中迁移需要撤回的旧版子会话，再试一次。' : '当前会话存储格式不支持编辑。');
  return { path, legacy };
}

export function sameIdentity(a, b) {
  return ['dev', 'ino', 'size', 'mtimeNs', 'ctimeNs'].every(key => a[key] === b[key]);
}

export async function artifactIdentity(path) {
  const st = await lstat(path, { bigint: true });
  check(st.isFile() && await isCanonicalPath(path), '会话日志路径异常。');
  return st;
}

async function entryFor(snapshot, retainedChild) {
  const { path: dshPath, legacy } = assertSession(snapshot, retainedChild);
  const path = await resolveRolloutPath(dshPath);
  check(path !== undefined, '会话日志路径异常。');
  return { path, dshPath, legacy, header: snapshot.header, revision: snapshot.revision, identity: await artifactIdentity(path) };
}

const lineageFields = ['id', 'parentSession', 'origin', 'createdAt', 'cwd', 'isSeeded', 'delegationDepth', 'agentPreset'];

function matchingHeader(first, second) {
  return first.version === second.version && lineageFields.every(key => first[key] === second[key]);
}

export async function revalidateWork(persistence, work, { signal } = {}) {
  const retainedLegacy = work.entries.filter(entry => entry.legacy);
  if (retainedLegacy.length > 0) {
    // A v3 revision includes the entire storage corpus because DSH may add
    // missing catalog entries during migration. These files stay at v3. Check
    // the selected physical source and the relevant parent edges instead.
    const expected = new Map(work.entries.map(entry => [entry.header.id, entry]));
    const legacyIds = new Set(retainedLegacy.map(entry => entry.header.id));
    const observedLegacy = new Set();
    for (const snapshot of await persistence.list({ signal })) {
      const { header } = snapshot;
      const entry = expected.get(header.id);
      if (header.origin === 'subagent' && legacyIds.has(header.parentSession)) {
        check(entry?.header.parentSession === header.parentSession, stale);
      }
      if (!entry?.legacy) continue;
      check(!observedLegacy.has(header.id) && matchingHeader(header, entry.header)
        && snapshot.location?.kind === 'jsonl' && isSamePath(snapshot.location.path, entry.dshPath), stale);
      observedLegacy.add(header.id);
    }
    check(observedLegacy.size === retainedLegacy.length, stale);
  }
  for (const entry of work.entries) {
    signal?.throwIfAborted();
    if (!entry.legacy) {
      const snapshot = await persistence.stat(entry.header.id, { signal });
      check(snapshot?.location?.kind === 'jsonl' && isSamePath(snapshot.location.path, entry.dshPath)
        && snapshot.revision === entry.revision, stale);
    }
    if (!isSamePath(entry.dshPath, entry.path)) {
      check(isSamePath(await resolveRolloutPath(entry.dshPath), entry.path), stale);
    }
    check(sameIdentity(entry.identity, await artifactIdentity(entry.path)), stale);
  }
}

function parseBirth(entry, parent, event) {
  const { header } = entry;
  check(header.origin === 'subagent' && header.parentSession === parent.header.id
    && Number.isSafeInteger(header.createdAt) && header.createdAt >= parent.header.createdAt,
  '子智能体会话的实际归属或创建时间与登记不匹配。');
  if (event) check(event.data.childCreatedAt === header.createdAt, '子智能体会话的创建时间与登记不匹配。');
}

function restoredCatalog(events, cut, time) {
  return events.filter(event => event.seq >= cut).map(event => ({ ...event, time }));
}

/** Prepare closed candidate files without acquiring a native writer or touching history. */
export async function prepareBoundedRevision(persistence, snapshot, messageId, amend, { signal, inheritedCount, eventCount } = {}) {
  check(isSupportedPlatform(process.platform), '历史消息编辑需要 DSH 宿主运行在 Linux、macOS 或 Windows 上。');
  const deadline = AbortSignal.timeout(180_000);
  signal = signal ? AbortSignal.any([signal, deadline]) : deadline;
  check(!preparing, '已有会话正在准备编辑，请稍后重试。');
  preparing = true;
  let staging;
  let ready = false;
  try {
    const parent = await entryFor(snapshot);
    const root = rolloutStorageRoot(parent.path);
    const dshRoot = rolloutStorageRoot(parent.dshPath);
    const worker = (op, input) => runRolloutWorker(op, input, { signal });
    const native = entry => ({ path: entry.path, sessionId: entry.header.id });
    const main = await worker('plan', { ...native(parent), messageId, inheritedCount });
    check(main.header.id === parent.header.id && main.header.origin !== 'subagent'
      && sameIdentity(fileIdentity(parent.identity), main.identity)
      && (eventCount === undefined || main.stats.records === eventCount), '会话日志与当前版本不匹配，请重新提交。');
    const { plan } = main;
    const amendment = await amend(plan);
    if (!amendment) return undefined;
    const keep = [];
    const withdrawn = [];
    const changed = [];
    const entries = [parent];
    const seen = new Set([parent.header.id]);
    // A child may have reached disk before its first catalog event. List
    // native headers even when the parent log has no child registrations.
    // The rc.5 persistence service offers no parent-filtered inventory. The
    // related-session limit is enforced while walking descendants below.
    const all = await persistence.list({ signal });
    const childrenByParent = new Map();
    for (const candidate of all) {
      const { origin, parentSession } = candidate.header;
      if (origin !== 'subagent' || !parentSession) continue;
      const children = childrenByParent.get(parentSession) ?? [];
      children.push(candidate);
      childrenByParent.set(parentSession, children);
    }
    check(!plan.timestampAmbiguous || (main.catalog.length === 0 && !childrenByParent.has(parent.header.id)),
      '主会话日志的时间顺序与编辑点冲突，无法安全回退子智能体会话。');
    const split = splitChildCatalog(main.catalog, plan);

    async function schedule(ancestor, catalog, retained, cutoff) {
      const physical = childrenByParent.get(ancestor.header.id) ?? [];
      const registered = new Map(catalog.map(event => [event.data.childId, event]));
      const physicalIds = new Set(physical.map(item => item.header.id));
      for (const event of registered.values()) {
        check(event.data.mode !== 'external', '存在外部子任务，无法安全回退关联记录。');
        check(physicalIds.has(event.data.childId), '子智能体会话记录不可用，无法核对其历史与后代。');
      }
      const keptIds = new Set(retained.map(event => event.data.childId));
      for (const child of physical) {
        const id = child.header.id;
        const registeredEvent = registered.get(id);
        // An unregistered file may belong to an earlier rejected/removed branch.
        // Refuse to implicitly remove or rewind it in a branch that is kept.
        if (cutoff !== undefined && !registeredEvent) {
          // Persisted parent links for unsubscribed descendants are still real.
          // Without an event we cannot distinguish prior branch state from a
          // current registration that was lost or is in flight.
          if (ancestor.legacy) {
            throw new RevisionError(`旧版子会话 ${ancestor.header.id} 缺少直属子会话的原生登记，请先在 DSH 中迁移这个直接父会话。`);
          }
          throw new RevisionError('发现没有原生登记的子智能体会话，无法安全确定其回退边界。');
        }
        check(entries.length < maxSessions && !seen.has(id), '本次编辑涉及的会话过多（上限 128 个）或归属不明确。');
        const retainedChild = cutoff !== undefined && keptIds.has(id);
        const entry = await entryFor(child, retainedChild);
        check(isSamePath(rolloutStorageRoot(entry.dshPath), dshRoot) && isSamePath(rolloutStorageRoot(entry.path), root),
          '子智能体会话与主会话不在同一个原生存储目录。');
        parseBirth(entry, ancestor, registeredEvent);
        seen.add(id);
        entries.push(entry);
        if (cutoff !== undefined && keptIds.has(id)) keep.push(entry);
        else withdrawn.push(entry);
      }
    }

    await schedule(parent, main.catalog, split.retained, plan.time);
    let keptCount = 0;
    let removedCount = 0;
    while (keptCount < keep.length || removedCount < withdrawn.length) {
      while (removedCount < withdrawn.length) await schedule(withdrawn[removedCount++], [], [], undefined);
      if (keptCount >= keep.length) continue;
      signal?.throwIfAborted();
      const batch = keep.slice(keptCount);
      keptCount += batch.length;
      const scans = await worker('scanMany', { items: batch.map(entry => ({ ...native(entry),
        cutoff: plan.time, allowLegacy: entry.legacy,
      })) });
      for (const [index, scan] of scans.entries()) {
        const child = batch[index];
        check(scan.header.id === child.header.id && sameIdentity(fileIdentity(child.identity), scan.identity) && !scan.inTurn,
          '子智能体会话的历史顺序或轮次状态无法安全核对。');
        if (child.legacy) {
          check(lineageFields.every(key => scan.header[key] === child.header[key]),
            '旧版子会话的物理日志头与 DSH 记录不匹配。');
          check(!scan.crosses && !scan.ambiguous, '旧版子会话跨过编辑点或边界不明确，请先在 DSH 中迁移该子会话。');
        }
        check(!scan.ambiguous, '无法确定子智能体会话事件与编辑点的先后顺序。');
        const edges = splitChildCatalog(scan.catalog, { cut: scan.cut, time: plan.time });
        if (scan.crosses) {
          check(!scan.inTurnAtCut && scan.cut > scan.inheritedCount,
            '子智能体会话有轮次横跨编辑点，找不到安全的截断位置。');
          changed.push({ entry: child, scan, appendEvents: restoredCatalog(edges.retained, scan.cut, plan.time) });
        }
        await schedule(child, scan.catalog, edges.retained, plan.time);
      }
    }

    await revalidateWork(persistence, { entries }, { signal });
    staging = await createRevisionCandidateDirectory(parent.path);
    const artifact = async (entry, parts, index) => ({ ...entry,
      ...await worker('prepare', { ...native(entry), identity: fileIdentity(entry.identity),
        output: join(staging.directory, String(index)), ...parts }),
    });
    const controls = amendment.controls ?? [];
    const appendEvents = [...restoredCatalog(split.retained, plan.cut, plan.time), ...controls];
    const files = [
      { entry: parent, parts: { cut: plan.cut, inputCopies: plan.retainedInputs, message: amendment.message,
        appendEvents, prefix: main.prefix } },
      ...changed.map(({ entry, scan, appendEvents: records }) => ({
        entry, parts: { cut: scan.cut, appendEvents: records, prefix: scan.prefix },
      })),
    ];
    const artifacts = [];
    for (const [index, { entry, parts }] of files.entries()) {
      const prepared = await artifact(entry, parts, index);
      artifacts.push({ ...prepared, candidatePath: prepared.path, path: entry.path });
    }
    const withdrawnIds = new Set(withdrawn.map(entry => entry.header.id));
    const affectedParentIds = [...new Set(withdrawn.map(entry => entry.header.parentSession)
      .filter(id => !withdrawnIds.has(id)))];
    ready = true;
    return { parent, entries, withdrawn, artifacts, affectedParentIds, dispose: staging.dispose };
  } catch (error) {
    if (staging) await staging.dispose().catch(() => {});
    if (error.name === 'RolloutWorkerError') {
      const message = error.reason !== undefined ? error.message : '处理会话日志时超过资源上限，或无法确认日志格式。';
      throw new RevisionError(message, { cause: error });
    }
    throw error;
  } finally {
    preparing = false;
    if (!ready && staging) await staging.dispose().catch(() => {});
  }
}
