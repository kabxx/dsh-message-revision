import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { lstat, mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import {
  cleanupRevisionCandidates, createRevisionCandidateDirectory, withRevisionCandidateOutput,
} from '../src/revision-candidates.js';
import { assertTerminated, skipWithoutNativeLocks, spawnTestEnv } from './platform-fixture.js';

const candidateModule = new URL('../src/revision-candidates.js', import.meta.url).href;
const platformOptions = { skip: skipWithoutNativeLocks, timeout: 15_000 };

async function fixture(t) {
  const root = await mkdtemp(join(await realpath(tmpdir()), 'revision-candidates-test-'));
  t.after(async () => {
    assert.equal(await realpath(root), root);
    await rm(root, { recursive: true, force: true });
  });
  const rollout = join(root, 'session.v4.jsonl');
  await writeFile(rollout, 'original session log');
  return rollout;
}

function launch(script, argument) {
  const child = spawn(process.execPath, ['--input-type=module', '-e', script, argument], {
    stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
    env: spawnTestEnv(),
  });
  return { child, result: once(child, 'message'), closed: once(child, 'exit') };
}

async function terminate(proc) {
  if (proc.child.exitCode === null && proc.child.signalCode === null) proc.child.kill('SIGKILL');
  return proc.closed;
}

async function exists(path) {
  try { await lstat(path); return true; } catch (error) {
    if (error.code === 'ENOENT') return false;
    throw error;
  }
}

test('Candidate cleanup skips an active Host and normally disposes output files', platformOptions, async t => {
  const rollout = await fixture(t);
  const candidate = await createRevisionCandidateDirectory(rollout);
  const output = join(candidate.directory, '0');
  t.after(() => candidate.dispose());
  await withRevisionCandidateOutput(output, () => writeFile(output, 'candidate data'));
  assert.equal(await cleanupRevisionCandidates(rollout), 0);
  assert.equal(await readFile(output, 'utf8'), 'candidate data');
  assert.equal(await candidate.dispose(), true);
  assert.equal(await exists(candidate.directory), false);
  assert.equal(await readFile(rollout, 'utf8'), 'original session log');
});

test('Startup reclaims candidates after Host and detached worker exit', platformOptions, async t => {
  const rollout = await fixture(t);
  const host = launch(`
    import { createRevisionCandidateDirectory } from ${JSON.stringify(candidateModule)};
    const candidate = await createRevisionCandidateDirectory(process.argv[1]);
    process.send(candidate.directory);
    setInterval(() => {}, 1000);
  `, rollout);
  let worker;
  t.after(async () => {
    await terminate(host);
    if (worker) await terminate(worker);
  });
  const [directory] = await host.result;
  const output = join(directory, '0');
  worker = launch(`
    import { writeFile } from 'node:fs/promises';
    import { withRevisionCandidateOutput } from ${JSON.stringify(candidateModule)};
    await withRevisionCandidateOutput(process.argv[1], async () => {
      await writeFile(process.argv[1], 'unfinished candidate');
      await new Promise(() => {
        process.send('writing');
        setInterval(() => {}, 1000);
      });
    });
  `, output);
  assert.deepEqual(await worker.result, ['writing', undefined]);
  assertTerminated(...await terminate(host));
  const errors = [];
  assert.equal(await cleanupRevisionCandidates(rollout, { retryBusy: true, onError: error => errors.push(error) }), 0);
  assert.equal(await readFile(output, 'utf8'), 'unfinished candidate');
  assertTerminated(...await terminate(worker));
  for (let attempt = 0; attempt < 40 && await exists(directory); attempt++) await delay(50);
  assert.deepEqual(errors, []);
  assert.equal(await exists(directory), false);
  assert.equal(await cleanupRevisionCandidates(rollout), 0);
  let called = false;
  await assert.rejects(withRevisionCandidateOutput(output, () => { called = true; }), /disappeared/);
  assert.equal(called, false);
  assert.equal(await readFile(rollout, 'utf8'), 'original session log');
});

test('Cleanup preserves candidate lookalikes, symlinks and legacy ownerless directories', platformOptions, async t => {
  const rollout = await fixture(t);
  const candidate = await createRevisionCandidateDirectory(rollout);
  t.after(() => candidate.dispose());
  const namespace = dirname(candidate.directory);
  await candidate.dispose();
  const badContents = join(namespace, 'candidate-56f9d92c-2197-4ef6-9902-2962af45ffce');
  const linked = join(namespace, 'candidate-38171724-38f9-47fc-8c2f-aacf247ab5d2');
  const legacy = join(dirname(rollout), '.message-revision-candidate-abcd12');
  await mkdir(badContents);
  await mkdir(legacy);
  await writeFile(join(legacy, '0'), 'legacy ownerless preparation');
  let symlinksSupported = true;
  try {
    await symlink(rollout, join(badContents, '0'));
    await symlink(legacy, linked, process.platform === 'win32' ? 'junction' : undefined);
  } catch (error) {
    if (process.platform === 'win32' && error.code === 'EPERM') {
      symlinksSupported = false;
      await writeFile(join(badContents, 'unexpected.txt'), 'unexpected');
      await symlink(legacy, linked, 'junction');
    } else throw error;
  }
  await assert.rejects(cleanupRevisionCandidates(rollout), /Unexpected file/);
  if (symlinksSupported) {
    assert.equal(await readFile(join(badContents, '0'), 'utf8'), 'original session log');
  } else {
    assert.equal(await readFile(join(badContents, 'unexpected.txt'), 'utf8'), 'unexpected');
  }
  assert.equal(await readFile(join(linked, '0'), 'utf8'), 'legacy ownerless preparation');
});
