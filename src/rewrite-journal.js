import { createHash, randomUUID } from 'node:crypto';
import { lstat, mkdir, open, readFile, readdir, realpath, rename, rm, unlink } from 'node:fs/promises';
import { hostname } from 'node:os';
import { basename, dirname, join } from 'node:path';
import manifest from '../package.json' with { type: 'json' };

const release = /^v\d+\.\d+\.\d+(?:-[0-9a-z.-]+)?$/iu;
const sessionFile = /^session\.v4\.jsonl(?:\.zstd)?$/u;
const digestPattern = /^[a-f0-9]{64}$/u;
const garbageName = /^discard-[a-f0-9-]{36}$/u;
const preparationName = /^preparing-[a-f0-9-]{36}$/u;
const limit = 256 * 1024 * 1024;
const ownerHost = hostname();
const busyKey = Symbol.for('dsh-message-revision.active-transactions');
const active = globalThis[busyKey] ??= new Set();

function assert(condition, detail) {
  if (!condition) throw new Error(detail);
}

function optionalStat(path) {
  return lstat(path).catch(error => {
    if (error.code !== 'ENOENT') throw error;
    return undefined;
  });
}

async function assertDirectory(path) {
  const entry = await optionalStat(path);
  if (entry === undefined) return false;
  assert(entry.isDirectory() && await realpath(path) === path, 'Unsafe transaction directory');
  return true;
}

async function syncDirectory(path) {
  if (process.platform === 'win32') return;
  const handle = await open(path, 'r');
  try { await handle.sync(); } finally { await handle.close(); }
}

async function ensureDirectory(path) {
  await mkdir(path, { mode: 0o700 }).catch(error => {
    if (error.code !== 'EEXIST') throw error;
  });
  assert(await assertDirectory(path), 'Missing transaction directory');
  await syncDirectory(dirname(path));
}

async function writeFileDurably(path, bytes) {
  const file = await open(path, 'wx', 0o600);
  try {
    await file.writeFile(bytes);
    await file.sync();
  } finally { await file.close(); }
}

async function readRegular(path, max = limit) {
  const entry = await lstat(path);
  assert(entry.isFile() && entry.size <= max && await realpath(path) === path, 'Unsafe transaction file');
  return readFile(path);
}

const digest = data => createHash('sha256').update(data).digest('hex');

function safeComponent(value) {
  return typeof value === 'string' && value.length > 0 && value !== '.' && value !== '..' && !/[\\/\u0000]/u.test(value);
}

function namespace(parentPath) {
  return join(dirname(parentPath), 'plugins', ...manifest.name.split('/'));
}

function childPath(parentPath, child) {
  assert(safeComponent(child.project) && safeComponent(child.directory) && sessionFile.test(child.filename),
    'Unsafe recorded child path');
  const store = dirname(dirname(dirname(parentPath)));
  const path = join(store, child.project, child.directory, child.filename);
  assert(dirname(path) !== dirname(parentPath), 'Transaction cannot move its parent directory');
  return path;
}

async function moveDirectory(from, to) {
  assert(await assertDirectory(from), 'Transaction source directory disappeared');
  assert(await assertDirectory(dirname(to)), 'Transaction destination parent disappeared');
  assert(!await optionalStat(to), 'Transaction destination is already occupied');
  await rename(from, to);
  await syncDirectory(dirname(from));
  if (dirname(to) !== dirname(from)) await syncDirectory(dirname(to));
}

async function removeRetiredDirectory(directory) {
  if (!await assertDirectory(directory)) return;
  await rm(directory, { recursive: true });
  await syncDirectory(dirname(directory));
}

function assertOwnerReleased(owner) {
  assert(owner?.host === ownerHost && Number.isSafeInteger(owner.pid) && owner.pid > 0,
    'Transaction belongs to another host or has invalid ownership');
  if (owner.pid === process.pid) return;
  try { process.kill(owner.pid, 0); } catch (error) {
    if (error.code === 'ESRCH') return;
    throw error;
  }
  throw new Error('The process that started this rewrite is still running');
}

async function verifyChildFile(path, expected) {
  const bytes = await readRegular(path);
  assert(digest(bytes) === expected.digest, 'Recorded child data has changed');
}

export async function hasPendingRewrite(parentPath) {
  const root = namespace(parentPath);
  for (const path of [join(dirname(parentPath), 'plugins'), dirname(root), root]) {
    if (!await assertDirectory(path)) return false;
  }
  for (const name of await readdir(root)) {
    if (!release.test(name)) continue;
    const directory = join(root, name);
    if (await assertDirectory(directory) && await assertDirectory(join(directory, 'rewrite'))) return true;
  }
  return false;
}

/** Keep the intent immutable. Publish it and sync its directory before moving any session. */
export async function prepareRewriteJournal(parent, children, replacement) {
  const parentPath = parent.path;
  const parentId = parent.handle.header.id;
  const root = namespace(parentPath);
  const versionPath = join(root, `v${manifest.version}`);
  for (const path of [join(dirname(parentPath), 'plugins'), dirname(root), root, versionPath]) {
    await ensureDirectory(path);
  }
  assert(!active.has(parentPath) && !await hasPendingRewrite(parentPath), 'A previous rewrite still needs recovery');
  active.add(parentPath);
  const temporary = join(versionPath, `preparing-${randomUUID()}`);
  const directory = join(versionPath, 'rewrite');
  try {
    const records = [];
    for (const child of children) {
      const path = child.path;
      const project = basename(dirname(dirname(path)));
      const localDirectory = basename(dirname(path));
      const { id, parentSession, createdAt } = child.handle.header;
      const record = { id, parentSession, createdAt, project, directory: localDirectory, filename: basename(path),
        digest: digest(await readRegular(path)) };
      assert(childPath(parentPath, record) === path, 'Child and parent use different storage roots');
      records.push(record);
    }
    const candidate = createHash('sha256');
    let size = 0;
    for (const bytes of replacement.buffers) { candidate.update(bytes); size += bytes.length; }
    assert(size <= limit, 'Replacement transcript is too large');
    const intent = {
      format: 1, parentId, filename: basename(parentPath), owner: { host: ownerHost, pid: process.pid },
      original: digest(replacement.original), candidate: { digest: candidate.digest('hex'), size, records: replacement.retained },
      children: records,
    };
    await mkdir(temporary, { mode: 0o700 });
    await writeFileDurably(join(temporary, 'original'), replacement.original);
    await writeFileDurably(join(temporary, 'intent.json'), JSON.stringify(intent));
    await syncDirectory(temporary);
    await moveDirectory(temporary, directory);
    return { directory, intent, parentPath };
  } catch (error) {
    active.delete(parentPath);
    // The preparation directory never contains moved children. Keep an already
    // published intent intact so the next recovery can make the same decision.
    await removeRetiredDirectory(temporary).catch(() => {});
    throw error;
  }
}

export function forgetRewriteJournal(journal) {
  if (journal) active.delete(journal.parentPath);
}

function childLocations(journal, entry, index) {
  const path = childPath(journal.parentPath, entry);
  return { from: dirname(path), to: join(journal.directory, `child-${index}`), path };
}

export async function stageWithdrawnChildren(journal) {
  for (const [index, entry] of journal.intent.children.entries()) {
    const { from, to, path } = childLocations(journal, entry, index);
    await verifyChildFile(path, entry);
    await moveDirectory(from, to);
  }
}

export async function restoreWithdrawnChildren(journal) {
  const restore = [];
  // Validate every move before making any. A repeat after a crash accepts an
  // already restored directory only if it still holds the exact same rollout.
  for (const [index, entry] of journal.intent.children.entries()) {
    const { from, to, path } = childLocations(journal, entry, index);
    const original = await assertDirectory(from);
    const held = await assertDirectory(to);
    assert(original !== held, 'Cannot distinguish the original child directory from a conflict');
    if (original) {
      await verifyChildFile(path, entry);
    } else {
      await verifyChildFile(join(to, entry.filename), entry);
      restore.push({ from, to });
    }
  }
  for (const { from, to } of restore.toReversed()) await moveDirectory(to, from);
}

export async function markRewriteCommitted(journal) {
  // fsync an empty regular marker after the native Agent has accepted the
  // candidate. Once present, recovery can only finish cleanup.
  const temporary = join(journal.directory, `commit-${randomUUID()}.tmp`);
  await writeFileDurably(temporary, Buffer.alloc(0));
  await rename(temporary, join(journal.directory, 'committed'));
  await syncDirectory(journal.directory);
}

export async function clearFailedCommitMarker(journal) {
  try { await unlink(join(journal.directory, 'committed')); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  await syncDirectory(journal.directory);
}

export async function retireRewriteJournal(journal) {
  const garbage = join(dirname(journal.directory), `discard-${randomUUID()}`);
  // Rename first. If deletion terminates partway through, startup does not
  // mistake the partial removal for a journal that still needs rollback.
  await moveDirectory(journal.directory, garbage);
  await removeRetiredDirectory(garbage);
}

function validateIntent(intent, snapshot, path) {
  const { id } = snapshot.header;
  assert(intent?.format === 1 && intent.parentId === id && intent.filename === basename(path), 'Unknown transaction format or parent');
  assert(digestPattern.test(intent.original) && digestPattern.test(intent.candidate?.digest)
    && Number.isSafeInteger(intent.candidate.size) && intent.candidate.size > 0 && intent.candidate.size <= limit
    && Number.isSafeInteger(intent.candidate.records) && intent.candidate.records >= 0 && Array.isArray(intent.children),
  'Invalid transaction hashes or boundaries');
  const ids = new Set([id]);
  const paths = new Set([dirname(path)]);
  for (const child of intent.children) {
    const directory = dirname(childPath(path, child));
    assert(typeof child.id === 'string' && child.id.length > 0 && !ids.has(child.id)
      && ids.has(child.parentSession) && !paths.has(directory) && Number.isSafeInteger(child.createdAt)
      && digestPattern.test(child.digest), 'Invalid recorded child lineage');
    ids.add(child.id);
    paths.add(directory);
  }
}

/** Scan only artifacts located by the native persistence backend. */
export async function readRewriteJournals(snapshot) {
  const path = snapshot?.location?.path;
  if (snapshot?.location?.kind !== 'jsonl' || typeof path !== 'string' || !sessionFile.test(basename(path))
    || !await assertDirectory(dirname(path))) return [];
  const root = namespace(path);
  for (const directory of [join(dirname(path), 'plugins'), dirname(root), root]) {
    if (!await assertDirectory(directory)) return [];
  }
  if (active.has(path)) return [];
  const journals = [];
  for (const name of await readdir(root)) {
    if (!release.test(name)) continue;
    const versionPath = join(root, name);
    if (!await assertDirectory(versionPath)) continue;
    const directory = join(versionPath, 'rewrite');
    if (await assertDirectory(directory)) {
      const intent = JSON.parse(await readRegular(join(directory, 'intent.json'), 4 * 1024 * 1024));
      validateIntent(intent, snapshot, path);
      assertOwnerReleased(intent.owner);
      const marker = join(directory, 'committed');
      const committed = await optionalStat(marker) !== undefined;
      if (committed) assert((await readRegular(marker, 0)).length === 0, 'Invalid transaction commit marker');
      journals.push({ directory, intent, parentPath: path, committed });
    }
  }
  assert(journals.length <= 1, 'Multiple unresolved rewrites for the same session');
  return journals;
}

/** Enumerate only names this version uses for incomplete preparations or retired journals. */
export async function listRewriteDebris(path) {
  const root = namespace(path);
  const preparations = [];
  const retired = [];
  for (const directory of [join(dirname(path), 'plugins'), dirname(root), root]) {
    if (!await assertDirectory(directory)) return { preparations, retired };
  }
  for (const name of await readdir(root)) {
    if (!release.test(name)) continue;
    const versionPath = join(root, name);
    if (!await assertDirectory(versionPath)) continue;
    for (const entry of await readdir(versionPath)) {
      if (garbageName.test(entry)) retired.push(join(versionPath, entry));
      else if (preparationName.test(entry)) preparations.push(join(versionPath, entry));
    }
  }
  return { preparations, retired };
}

export async function cleanRewriteDebris(paths) {
  for (const path of paths) await removeRetiredDirectory(path);
}

export async function readRewriteOriginal(journal) {
  const original = await readRegular(join(journal.directory, 'original'));
  assert(digest(original) === journal.intent.original, 'The original transcript copy has changed');
  return original;
}

export async function matchJournalRevision(journal) {
  const raw = await readRegular(journal.parentPath);
  if (digest(raw) === journal.intent.original) return 'original';
  const { size, digest: hash } = journal.intent.candidate;
  if (raw.length >= size && digest(raw.subarray(0, size)) === hash) return 'candidate';
  return 'conflict';
}
