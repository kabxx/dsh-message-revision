import { join, relative } from 'node:path';
import { RevisionError } from './revision.js';
import { rolloutStorageRoot } from './platform.js';
import { artifactIdentity, revalidateWork } from './bounded-revision.js';
import { acquireNativeLease } from './native-lease.js';
import { runRolloutWorker } from './rollout-process.js';
import {
  clearFailedCommitMarker, forgetRewriteJournal, markRewriteCommitted, prepareRewriteSetJournal,
  publishRewriteSetArtifacts, resolveRewriteSetChildPath, restoreRewriteSetArtifacts, retireRewriteJournal, rewriteSetArtifactPath,
  scheduleSettledRewriteRetirement, stageWithdrawnChildren,
} from './rewrite-journal.js';

class RewriteRollbackConflictError extends Error {
  constructor(options) {
    super('上次编辑后会话已有新增内容，未覆盖现有记录，请刷新后检查会话。', options);
    this.name = 'RewriteRollbackConflictError';
  }
}

async function withLeases(entries, operation) {
  const leases = [];
  try {
    const paths = new Map(entries.map(({ path, dshPath }) => [path, dshPath]));
    for (const [path, dshPath] of paths) leases.push(await acquireNativeLease(path, dshPath));
    return await operation(async () => {
      for (const lease of leases) await lease.assertHeld();
    });
  } finally {
    const result = await Promise.allSettled(leases.toReversed().map(lease => lease.close()));
    const failure = result.find(entry => entry.status === 'rejected');
    if (failure) throw failure.reason;
  }
}

function pathsForRestore(journal) {
  return journal.intent.artifacts.map((_, index) => rewriteSetArtifactPath(journal, index));
}

export async function restoreBoundedJournal(journal, { beforeCommit, canRollback, afterRestore } = {}) {
  const withdrawn = await Promise.all(journal.intent.children.map((_, index) => resolveRewriteSetChildPath(journal, index)));
  const root = rolloutStorageRoot(journal.parentPath);
  const dshRoot = rolloutStorageRoot(journal.dshPath ?? journal.parentPath);
  const entries = [...pathsForRestore(journal), ...withdrawn].map(path => ({ path, dshPath: join(dshRoot, relative(root, path)) }));
  await withLeases(entries, async assertHeld => {
    const verifyClosed = () => canRollback?.() ?? true;
    if (!verifyClosed()) throw new RevisionError('会话仍在使用，无法恢复上次编辑。');
    await beforeCommit?.(journal.intent.artifacts.map(entry => entry.id));
    await assertHeld();
    await restoreRewriteSetArtifacts(journal, {
      async canRestoreArtifact(artifact, match, index, path) {
        if (!verifyClosed()) return false;
        if (match !== 'candidate') return true;
        const current = await artifactIdentity(path);
        if (current.size === BigInt(artifact.candidate.size)) return true;
        const inspection = await runRolloutWorker('inspect', {
          path, sessionId: artifact.id, fromSeq: artifact.candidate.records,
          permitResumeMarker: artifact.id === journal.intent.parentId,
        });
        if (!inspection.provisional) throw new RewriteRollbackConflictError();
        return verifyClosed();
      },
    });
    await afterRestore?.();
  });
}

export async function commitBoundedRevision(persistence, work, {
  beforeCommit, verifyReload, canRollback, closeFailedReload, afterCommit, onCleanupError, messageId,
}) {
  let journal;
  let committed = false;
  let delivered = messageId === undefined;
  let markerFailed = false;
  const removedChildren = work.withdrawn.map(entry => entry.header.id);
  try {
    await withLeases(work.entries, async assertHeld => {
      await revalidateWork(persistence, work);
      journal = await prepareRewriteSetJournal(work.parent, work.withdrawn, work.artifacts, { messageId });
      await assertHeld();
      await revalidateWork(persistence, work);
      await beforeCommit?.([...work.artifacts.map(entry => entry.header.id), ...removedChildren]);
      await assertHeld();
      await stageWithdrawnChildren(journal);
      await publishRewriteSetArtifacts(journal);
    });
    await verifyReload();
    try {
      await markRewriteCommitted(journal);
    } catch (error) {
      markerFailed = true;
      throw error;
    }
    committed = true;
    const result = { removedChildren };
    const accepted = await afterCommit(result, journal);
    delivered ||= accepted === true;
    return result;
  } catch (error) {
    if (committed || !journal) throw error;
    try {
      if (markerFailed) {
        await closeFailedReload?.();
        // Only clear a partially persisted marker if no runtime still owns the
        // provisional state or has actually consumed the revised prompt.
        if (canRollback && !canRollback()) throw new Error('Cannot restore a running candidate');
        await clearFailedCommitMarker(journal);
      }
      await restoreBoundedJournal(journal, { beforeCommit, canRollback });
      await retireRewriteJournal(journal, { deferCleanup: true, onCleanupError });
    } catch (rollbackError) {
      const cause = new AggregateError([error, rollbackError]);
      if (rollbackError instanceof RewriteRollbackConflictError) throw new RewriteRollbackConflictError({ cause });
      throw new Error('编辑回滚失败，请刷新后检查会话。', { cause });
    }
    throw new RevisionError('无法完成编辑，已恢复原记录。', { cause: error });
  } finally {
    if (committed && delivered) scheduleSettledRewriteRetirement(journal, { onCleanupError });
    // A committed journal with an unsettled prompt remains available for recovery.
    forgetRewriteJournal(journal);
  }
}
