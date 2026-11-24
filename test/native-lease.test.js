import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { mkdir, mkdtemp, open, realpath, rename, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { acquireNativeLease, MAX_NATIVE_LEASES } from '../src/native-lease.js';
import {
  acquireWin32Semaphore, dshSessionLockName, loadHostFlock, releaseWin32Semaphore,
} from '../src/platform.js';
import { skipWithoutNativeLocks, spawnTestEnv } from './platform-fixture.js';

const leaseModule = new URL('../src/native-lease.js', import.meta.url).href;
const flock = '/usr/bin/flock';
const nativeArgs = ['--exclusive', '--nonblock', '--conflict-exit-code', '73'];
const skipFlockCli = process.platform !== 'linux' || spawnSync(flock, ['--version']).status !== 0;

async function fixture(t) {
  const directory = await mkdtemp(join(await realpath(tmpdir()), 'dsh-native-lease-'));
  t.after(async () => {
    assert.equal(await realpath(directory), directory);
    await rm(directory, { recursive: true, force: true });
  });
  const rollout = join(directory, 'session.v4.jsonl.zstd');
  const lock = join(directory, 'session.lock');
  // A lightweight lease needs the native layout, even when the bytes are
  // unreadable as a log. Testing malformed bytes detects accidental decoding.
  await writeFile(rollout, 'fixture has no rollout records');
  return { rollout, lock };
}

function flockOutcome(lock) {
  const acquired = spawnSync(flock, [...nativeArgs, lock, '/usr/bin/true']);
  assert.ifError(acquired.error);
  return acquired.status;
}

test('native flock and the Host lease exclude each other in both directions', { skip: skipFlockCli }, async t => {
  const { rollout, lock } = await fixture(t);
  const lease = await acquireNativeLease(rollout);
  try {
    await lease.assertHeld();
    assert.equal(flockOutcome(lock), 73, 'the Host retains flock after the helper exits');
    await assert.rejects(acquireNativeLease(rollout), { name: 'SessionAlreadyOwnedError' });
  } finally {
    await lease.close();
    await lease.close();
  }
  assert.equal(flockOutcome(lock), 0);

  // The kernel lease spans all commands in this short-lived fixture holder.
  const child = spawn(flock, [...nativeArgs, lock, '/usr/bin/sh', '-c', 'echo ready; read value'], {
    stdio: ['pipe', 'pipe', 'ignore'],
  });
  const closed = once(child, 'close');
  try {
    const ready = new Promise((accept, reject) => {
      child.stdout.once('data', accept);
      closed.then(() => reject(new Error('native fixture writer exited before acquiring flock')), reject);
    });
    assert.equal(String(await ready).trim(), 'ready');
    await assert.rejects(acquireNativeLease(rollout), { name: 'SessionAlreadyOwnedError' });
  } finally {
    child.stdin.end();
    await closed;
  }
  const reopened = await acquireNativeLease(rollout);
  await reopened.close();
});

test('cross-process native lease exclusion and DSH lock interop work on the active OS', {
  skip: skipWithoutNativeLocks,
}, async t => {
  const { rollout, lock } = await fixture(t);
  const lease = await acquireNativeLease(rollout);
  try {
    await lease.assertHeld();
    await assert.rejects(acquireNativeLease(rollout), { name: 'SessionAlreadyOwnedError' });
    if (process.platform === 'win32') {
      const probe = acquireWin32Semaphore(dshSessionLockName(lock), lock);
      assert.equal(probe.busy, true);
    } else {
      const hostFlock = await loadHostFlock();
      if (hostFlock) {
        const handle = await open(lock, 'w');
        try {
          await assert.rejects(hostFlock(handle.fd), error => error.code === 'EAGAIN' || error.code === 'EWOULDBLOCK');
        } finally {
          await handle.close();
        }
      }
    }
  } finally {
    await lease.close();
    await lease.close();
  }

  // Verify cross-process mutual exclusion and automatic kernel release on child exit.
  const holder = spawn(process.execPath, ['--input-type=module', '-e', `
    import { acquireNativeLease } from ${JSON.stringify(leaseModule)};
    const held = await acquireNativeLease(process.argv[1]);
    process.send('held');
    process.on('message', async () => { await held.close(); process.exit(0); });
  `, rollout], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'], env: spawnTestEnv() });
  const closed = once(holder, 'close');
  try {
    assert.equal((await once(holder, 'message'))[0], 'held');
    await assert.rejects(acquireNativeLease(rollout), { name: 'SessionAlreadyOwnedError' });
    if (process.platform === 'win32') {
      const probe = acquireWin32Semaphore(dshSessionLockName(lock), lock);
      assert.equal(probe.busy, true);
    }
    holder.send('release');
    await closed;
  } finally {
    if (holder.exitCode === null && holder.signalCode === null) holder.kill('SIGKILL');
  }

  if (process.platform === 'win32') {
    const dshSem = acquireWin32Semaphore(dshSessionLockName(lock), lock);
    assert.equal(dshSem.busy, false);
    try {
      await assert.rejects(acquireNativeLease(rollout), { name: 'SessionAlreadyOwnedError' });
    } finally {
      releaseWin32Semaphore(dshSem.handle);
    }
  }

  const reopened = await acquireNativeLease(rollout);
  await reopened.close();
});

test('a renamed lock invalidates the held path while a replaced rollout can keep the lock', {
  skip: skipWithoutNativeLocks,
}, async t => {
  const { rollout, lock } = await fixture(t);
  const lease = await acquireNativeLease(rollout);
  try {
    await writeFile(`${rollout}.new`, 'candidate');
    await rename(`${rollout}.new`, rollout);
    await lease.assertHeld();
    if (process.platform !== 'win32') {
      await rename(lock, `${lock}.old`);
      await writeFile(lock, '');
      await assert.rejects(lease.assertHeld(), /写锁已失效/u);
    }
  } finally {
    await lease.close();
  }
  const replacement = await acquireNativeLease(rollout);
  await replacement.close();
});

test('a symlink at the native lock path is rejected', {
  skip: process.platform === 'win32' || skipWithoutNativeLocks,
}, async t => {
  const { rollout, lock } = await fixture(t);
  await symlink(rollout, lock);
  await assert.rejects(acquireNativeLease(rollout), { code: 'ELOOP' });
});

test('lease capacity is bounded and returns after closing a lease', {
  skip: skipWithoutNativeLocks,
}, async t => {
  const { rollout } = await fixture(t);
  const root = dirname(rollout);
  const leases = [];
  try {
    for (let number = 0; number < MAX_NATIVE_LEASES; number++) {
      const directory = join(root, `fixture-${number}`);
      await mkdir(directory);
      const artifact = join(directory, 'session.v4.jsonl');
      await writeFile(artifact, 'unread log bytes');
      leases.push(await acquireNativeLease(artifact));
    }
    await assert.rejects(acquireNativeLease(rollout), /会话过多/u);
    await leases.pop().close();
    leases.push(await acquireNativeLease(rollout));
  } finally {
    await Promise.all(leases.map(lease => lease.close()));
  }
});
