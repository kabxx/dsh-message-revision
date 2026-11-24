import assert from 'node:assert/strict';
import { appendFile, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import test from 'node:test';
import { setTimeout as wait } from 'node:timers/promises';
import { restoreBoundedJournal } from '../src/bounded-commit.js';
import { acquireNativeLease } from '../src/native-lease.js';
import { recoverPendingRewrite, recoverStoredSessions } from '../src/revision.js';
import { forgetRewriteJournal, prepareRewriteSetJournal, publishRewriteSetArtifacts, readRewriteJournals } from '../src/rewrite-journal.js';
import { childFixture } from './child-fixture.js';
import { skipWithoutNativeLocks } from './platform-fixture.js';

async function fixture(t) {
  const fixture = await childFixture(t, {
    parent: { createdAt: 1, events: [[5, 'system/context', { version: 'old parent' }]] },
    child: { createdAt: 2, parentSession: 'parent', events: [[5, 'system/context', { version: 'old child' }]] },
  });
  const { persistence } = fixture;
  const entries = await Promise.all(['parent', 'child'].map(async id => {
    const { header, location: { path } } = await persistence.stat(id);
    const candidatePath = join(path, '..', 'candidate.jsonl');
    const candidate = `${JSON.stringify(header)}\n${JSON.stringify({ seq: 0, time: 4, type: 'system/context', data: { version: id } })}\n`;
    await writeFile(candidatePath, candidate);
    return { header, path, candidatePath, retained: 1, original: await readFile(path), candidate };
  }));
  persistence.list = async () => [await persistence.stat('parent')];
  const [parent, child] = entries;
  const journal = await prepareRewriteSetJournal(parent, [], entries);
  try { await publishRewriteSetArtifacts(journal); } finally { forgetRewriteJournal(journal); }
  const snapshot = await persistence.stat('parent');
  const errors = [];
  const options = { restoreBoundedJournal, onError: (id, error) => errors.push({ id, error }) };
  return { persistence, parent, child, snapshot, errors, options };
}

async function until(condition) {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (await condition()) return;
    await wait(10);
  }
  assert.fail('Timed out waiting for write-lock recovery');
}

async function assertRestored({ persistence, parent, child, snapshot }) {
  assert.deepEqual(await readRewriteJournals(snapshot), []);
  assert.deepEqual(await persistence.stat('parent').then(result => readFile(result.location.path)), parent.original);
  assert.deepEqual(await readFile(child.path), child.original);
}

test('startup retries a held child write lock and restores the complete transaction after release', { skip: skipWithoutNativeLocks }, async t => {
  const state = await fixture(t);
  const { persistence, parent, child, snapshot, errors, options } = state;
  const retry = new AbortController();
  const held = await acquireNativeLease(child.path);
  try {
    await recoverStoredSessions(persistence, { ...options, retrySignal: retry.signal });
    assert.deepEqual(errors.map(({ id, error }) => [id, error.name]), [['parent', 'SessionAlreadyOwnedError']]);
    assert.equal((await readRewriteJournals(snapshot)).length, 1);
    assert.equal(await readFile(parent.path, 'utf8'), parent.candidate);
    assert.equal(await readFile(child.path, 'utf8'), child.candidate);
    await held.close();
    await until(async () => (await readRewriteJournals(snapshot)).length === 0);
    await assertRestored(state);
  } finally {
    retry.abort();
    await held.close();
  }
});

test('the next edit retries only its pending recovery without evicting an existing writer', { skip: skipWithoutNativeLocks }, async t => {
  const state = await fixture(t);
  const { persistence, parent, snapshot, options } = state;
  const held = await acquireNativeLease(parent.path);
  try {
    await assert.rejects(recoverPendingRewrite(persistence, snapshot, options), { name: 'SessionAlreadyOwnedError' });
    await held.assertHeld();
    assert.equal(await readFile(parent.path, 'utf8'), parent.candidate);
  } finally {
    await held.close();
  }
  assert.equal(await recoverPendingRewrite(persistence, snapshot, options), true);
  await assertRestored(state);
  assert.equal(await recoverPendingRewrite(persistence, snapshot, options), false);
});

test('a changed candidate is preserved and startup schedules no retry for a history conflict', { skip: skipWithoutNativeLocks }, async t => {
  const { persistence, parent, child, snapshot, errors, options } = await fixture(t);
  const retry = new AbortController();
  try {
    // Change committed bytes within the candidate so an actual history conflict
    // is observable without depending on the suffix classifier.
    const changed = parent.candidate.replace('"version":"parent"', '"version":"later"');
    assert.notEqual(changed, parent.candidate);
    await writeFile(parent.path, changed);
    let attempts = 0;
    await recoverStoredSessions(persistence, {
      ...options, retrySignal: retry.signal,
      restoreBoundedJournal(...args) {
        attempts++;
        return restoreBoundedJournal(...args);
      },
    });
    await wait(150);
    assert.equal(attempts, 1);
    assert.equal(errors.length, 1);
    assert.equal((await readRewriteJournals(snapshot)).length, 1);
    assert.equal(await readFile(parent.path, 'utf8'), changed);
    assert.equal(await readFile(child.path, 'utf8'), child.candidate);
  } finally { retry.abort(); }
});
