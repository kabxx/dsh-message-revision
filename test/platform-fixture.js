import assert from 'node:assert/strict';
import { isSupportedPlatform, loadHostFlock, loadHostKoffi, workerEnv } from '../src/platform.js';

export const supportedPlatform = isSupportedPlatform(process.platform);

export const nativeLocksAvailable = process.platform === 'linux'
  ? true
  : process.platform === 'darwin'
    ? Boolean(await loadHostFlock())
    : process.platform === 'win32'
      ? Boolean(loadHostKoffi())
      : false;

export const skipWithoutNativeLocks = !nativeLocksAvailable
  ? `Native DSH lock primitives unavailable on standalone ${process.platform} without DSH host`
  : false;

export function spawnTestEnv(extra = {}) {
  return workerEnv(extra);
}

export function assertTerminated(code, signal, detail = '') {
  if (process.platform === 'win32') {
    assert.ok(signal === 'SIGKILL' || (code !== null && code !== 0),
      `Expected abrupt termination on win32, got code=${code} signal=${signal}\n${detail}`);
  } else {
    assert.equal(signal, 'SIGKILL', detail);
  }
}
