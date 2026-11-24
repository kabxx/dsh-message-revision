import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { lstat, mkdir, open, readFile, readdir, readlink, rm, statfs, unlink } from 'node:fs/promises';
import { hostname } from 'node:os';
import { basename, dirname, join } from 'node:path';
import manifest from '../package.json' with { type: 'json' };
import {
  isCanonicalPath, isSamePath, moveEntryDurable, pathKey, readDarwinProcessScope,
  readDarwinProcessStart, readWin32BootId, readWin32ProcessCreationTime, replaceFileDurable, resolveRolloutPath,
} from './platform.js';

const release = /^v\d+\.\d+\.\d+(?:-[0-9a-z.-]+)?$/iu;
const sessionFile = /^session\.v4\.jsonl(?:\.zstd)?$/u;
const digestPattern = /^[a-f0-9]{64}$/u;
const garbageName = /^discard-[a-f0-9-]{36}$/u;
const preparationName = /^preparing-[a-f0-9-]{36}$/u;
const limit = 256 * 1024 * 1024;
const streamLimit = 16 * 1024 * 1024 * 1024;
const chunkSize = 128 * 1024;
const intentLimit = 4 * 1024 * 1024;
const ownerHost = hostname();
const busyKey = Symbol.for('dsh-message-revision.active-transactions');
const active = globalThis[busyKey] ??= new Set();
const retirementKey = Symbol.for('dsh-message-revision.settled-retirements');
const settledRetirements = globalThis[retirementKey] ??= new Map();
const retirementDelays = [0, 100, 500, 2000];
const cleanupQueue = [];
let cleaning = false;

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
  let canonical;
  try { canonical = await isCanonicalPath(path); }
  catch (error) {
    if (error.code === 'ENOENT') return false;
    throw error;
  }
  assert(entry.isDirectory() && canonical, 'Unsafe transaction directory');
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
  assert(entry.isFile() && entry.size <= max && await isCanonicalPath(path), 'Unsafe transaction file');
  return readFile(path);
}

const digest = data => createHash('sha256').update(data).digest('hex');

async function regularIdentity(path) {
  const entry = await lstat(path, { bigint: true });
  assert(entry.isFile() && await isCanonicalPath(path), 'Unsafe transaction file');
  return entry;
}

/** Copy and hash with one bounded buffer, optionally reading just a prefix. */
async function streamFile(path, { destination, mode = 0o600, max = streamLimit, length, sourceIdentity } = {}) {
  const expected = await regularIdentity(path);
  requireGivenIdentity(sourceIdentity, expected);
  const source = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
  let target;
  try {
    assert(sameIdentity(fileIdentity(await source.stat({ bigint: true })), fileIdentity(expected)),
      'Transaction source changed while opening');
    if (destination !== undefined) target = await open(destination, 'wx', mode);
    const digest = createHash('sha256');
    const buffer = Buffer.allocUnsafe(chunkSize);
    let size = 0;
    while (length === undefined || size < length) {
      const count = length === undefined ? buffer.length : Math.min(buffer.length, length - size);
      const { bytesRead } = await source.read(buffer, 0, count, null);
      if (bytesRead === 0) break;
      size += bytesRead;
      assert(size <= max, 'Streamed transaction artifact exceeds size limit');
      digest.update(buffer.subarray(0, bytesRead));
      if (target !== undefined) {
        for (let offset = 0; offset < bytesRead;) {
          const { bytesWritten } = await target.write(buffer, offset, bytesRead - offset, null);
          assert(bytesWritten > 0, 'Unable to make progress writing rewrite artifact');
          offset += bytesWritten;
        }
      }
    }
    assert(length === undefined || length === size, 'Transaction artifact is shorter than its candidate');
    const after = await regularIdentity(path);
    assert(sameIdentity(fileIdentity(after), fileIdentity(expected)), 'Transaction source changed during streaming');
    if (target !== undefined) await target.sync();
    return { size, digest: digest.digest('hex') };
  } finally {
    try { await target?.close(); } finally { await source.close(); }
  }
}

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
  assert(!isSamePath(dirname(path), dirname(parentPath)), 'Transaction cannot move its parent directory');
  return path;
}

async function moveDirectory(from, to, expectedIdentity) {
  assert(await assertDirectory(from), 'Transaction source directory disappeared');
  assert(await assertDirectory(dirname(to)), 'Transaction destination parent disappeared');
  assert(!await optionalStat(to), 'Transaction destination is already occupied');
  const current = expectedIdentity && await transactionDirectoryIdentity(from);
  if (expectedIdentity && current !== expectedIdentity) return false;
  await moveEntryDurable(from, to);
  await syncDirectory(dirname(from));
  if (!isSamePath(dirname(to), dirname(from))) await syncDirectory(dirname(to));
  return true;
}

async function transactionDirectoryIdentity(path) {
  const entry = await lstat(path, { bigint: true }).catch(error => {
    if (error.code !== 'ENOENT') throw error;
    return undefined;
  });
  if (!entry) return undefined;
  let canonical;
  try { canonical = await isCanonicalPath(path); }
  catch (error) {
    if (error.code === 'ENOENT') return undefined;
    throw error;
  }
  assert(entry.isDirectory() && canonical, 'Unsafe transaction directory');
  return `${entry.dev}:${entry.ino}`;
}

async function removeRetiredDirectory(directory) {
  if (!await assertDirectory(directory)) return;
  await rm(directory, process.platform === 'win32'
    ? { recursive: true, maxRetries: 5, retryDelay: 50 }
    : { recursive: true });
  await syncDirectory(dirname(directory));
}

async function listedProcessStart(pid) {
  try {
    const stat = await readFile(`/proc/${pid}/stat`, 'utf8');
    // The comm field can contain spaces and parentheses. Field 22 is starttime.
    const close = stat.lastIndexOf(')');
    const ticks = stat.slice(close + 1).trim().split(/\s+/u)[19];
    assert(close > 0 && typeof ticks === 'string' && /^\d+$/u.test(ticks), 'Cannot identify transaction owner process');
    return ticks;
  } catch (error) {
    if (error.code === 'ENOENT' || error.code === 'ESRCH') return undefined;
    throw error;
  }
}

let processScope;
function readProcessScope() {
  if (process.platform !== 'linux') return undefined;
  return processScope ??= Promise.all([
    readFile('/proc/sys/kernel/random/boot_id', 'utf8'), readlink('/proc/self/ns/pid'),
  ]).then(([bootId, pidNamespace]) => ({ bootId: bootId.trim(), pidNamespace })).catch(() => undefined);
}

async function transactionOwner() {
  const owner = { host: ownerHost, pid: process.pid };
  if (process.platform === 'linux') {
    const scope = await readProcessScope();
    if (!scope) return owner;
    const startTicks = await listedProcessStart(process.pid);
    assert(startTicks !== undefined, 'Cannot identify current process for transaction recovery');
    return { ...owner, ...scope, startTicks };
  }
  if (process.platform === 'darwin') {
    const scope = await readDarwinProcessScope();
    const darwinStartLabel = await readDarwinProcessStart(process.pid);
    if (scope && darwinStartLabel) return { ...owner, ...scope, darwinStartLabel };
    return owner;
  }
  if (process.platform === 'win32') {
    const win32CreationTime = readWin32ProcessCreationTime(process.pid);
    const win32BootId = readWin32BootId();
    if (typeof win32CreationTime === 'string') {
      return { ...owner, win32CreationTime, ...(win32BootId ? { win32BootId } : {}) };
    }
    return owner;
  }
  return owner;
}

async function assertOwnerReleased(owner) {
  assert(owner?.host === ownerHost && Number.isSafeInteger(owner.pid) && owner.pid > 0,
    'Transaction belongs to another host or has invalid ownership');
  if (owner.bootId !== undefined || owner.pidNamespace !== undefined || owner.startTicks !== undefined) {
    assert(typeof owner.bootId === 'string' && typeof owner.pidNamespace === 'string'
      && typeof owner.startTicks === 'string' && /^\d+$/u.test(owner.startTicks), 'Invalid transaction process identity');
    const scope = await readProcessScope();
    assert(scope, 'Cannot verify transaction owner process identity');
    if (scope.bootId !== owner.bootId) return;
    assert(scope.pidNamespace === owner.pidNamespace, 'Transaction belongs to another PID namespace');
    const startTicks = await listedProcessStart(owner.pid);
    if (startTicks !== owner.startTicks || owner.pid === process.pid) return;
  } else if (owner.darwinBootId !== undefined || owner.darwinStartLabel !== undefined) {
    assert(typeof owner.darwinBootId === 'string' && owner.darwinBootId.length > 0
      && typeof owner.darwinStartLabel === 'string' && owner.darwinStartLabel.length > 0,
    'Invalid transaction process identity');
    const scope = await readDarwinProcessScope();
    assert(scope, 'Cannot verify transaction owner process identity');
    if (scope.darwinBootId !== owner.darwinBootId) return;
    const startLabel = await readDarwinProcessStart(owner.pid);
    if (startLabel === null || startLabel !== owner.darwinStartLabel || owner.pid === process.pid) return;
  } else if (owner.win32CreationTime !== undefined || owner.win32BootId !== undefined) {
    assert(typeof owner.win32CreationTime === 'string' && /^\d+$/u.test(owner.win32CreationTime)
      && (owner.win32BootId === undefined || (typeof owner.win32BootId === 'string' && /^[a-f0-9]{32}$/u.test(owner.win32BootId))),
    'Invalid transaction process identity');
    if (owner.pid === process.pid) return;
    if (owner.win32BootId !== undefined) {
      const currentBootId = readWin32BootId();
      if (currentBootId && currentBootId !== owner.win32BootId) return;
    }
    const creationTime = readWin32ProcessCreationTime(owner.pid);
    assert(creationTime !== undefined, 'Cannot verify transaction owner process identity');
    if (creationTime === null || creationTime !== owner.win32CreationTime) return;
  } else {
    // Older pending journals only contain a PID. A live numeric PID cannot
    // prove who owned one of those journals, so keep it recoverable on disk.
    if (owner.pid === process.pid) return;
    try { process.kill(owner.pid, 0); } catch (error) {
      if (error.code === 'ESRCH') return;
      throw error;
    }
  }
  throw new Error('The process that started this rewrite is still running');
}

async function verifyChildFile(path, expected) {
  if (expected.identity !== undefined) {
    const directory = await lstat(dirname(path), { bigint: true });
    const identity = await regularIdentity(path);
    assert(directory.isDirectory() && String(directory.dev) === expected.directoryIdentity.dev
      && String(directory.ino) === expected.directoryIdentity.ino
      && sameIdentity(fileIdentity(identity), expected.identity), 'Recorded child data has changed');
    return;
  }
  assert((await streamFile(path, { max: limit })).digest === expected.digest, 'Recorded child data has changed');
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

function fileIdentity(entry) {
  return {
    dev: String(entry.dev), ino: String(entry.ino), size: String(entry.size),
    mtimeNs: String(entry.mtimeNs), ctimeNs: String(entry.ctimeNs),
  };
}

function sameIdentity(left, right) {
  return left.dev === right.dev && left.ino === right.ino && left.size === right.size
    && left.mtimeNs === right.mtimeNs && left.ctimeNs === right.ctimeNs;
}

function entryRecord(entry) {
  const header = entry.header ?? entry.handle?.header;
  return {
    id: header?.id,
    parentSession: header?.parentSession,
    createdAt: header?.createdAt,
    project: basename(dirname(dirname(entry.path))),
    directory: basename(dirname(entry.path)),
    filename: basename(entry.path),
  };
}

function v2ArtifactPath(journal, artifact) {
  return artifact.index === 0 ? journal.parentPath : childPath(journal.parentPath, artifact);
}

function v2StoredPath(journal, artifact, kind) {
  return join(journal.directory, 'artifacts', `artifact-${artifact.index}.${kind}`);
}

function requireGivenIdentity(given, actual) {
  if (given !== undefined) {
    assert(sameIdentity(fileIdentity(given), fileIdentity(actual)), 'Rewrite source changed since preparation');
  }
}

export class RewriteJournalSpaceError extends Error {
  constructor(requiredBytes, availableBytes) {
    super('磁盘可用空间不足，无法暂存本次编辑的候选日志和备份。');
    this.name = 'RewriteJournalSpaceError';
    this.requiredBytes = requiredBytes;
    this.availableBytes = availableBytes;
  }
}

async function assertJournalSpace(destination, sizes, metadataBytes = intentLimit) {
  const filesystem = await statfs(destination, { bigint: true });
  const blockSize = filesystem.frsize > 0n ? filesystem.frsize : filesystem.bsize;
  assert(blockSize > 0n && filesystem.bavail >= 0n, 'Cannot verify available transaction disk space');
  const blocks = size => (size + blockSize - 1n) / blockSize;
  // Allow for the intent, enclosing directories and allocation bookkeeping.
  const overhead = blocks(BigInt(metadataBytes)) + BigInt(sizes.length + 4);
  const required = (sizes.reduce((total, size) => total + blocks(size), overhead)) * blockSize;
  const available = filesystem.bavail * blockSize;
  if (required > available) throw new RewriteJournalSpaceError(required, available);
}

async function planRewriteSetCopies(parent, artifacts) {
  const parentPath = parent.path;
  assert(Array.isArray(artifacts) && artifacts.length > 0 && isSamePath(artifacts[0].path, parentPath), 'Parent artifact must be first');
  const copies = [];
  for (const [index, artifact] of artifacts.entries()) {
    const record = { ...entryRecord(artifact), index };
    assert(isSamePath(v2ArtifactPath({ parentPath }, record), artifact.path), 'Rewrite artifact is outside the session storage root');
    assert(typeof artifact.candidatePath === 'string' && !isSamePath(artifact.candidatePath, artifact.path), 'Missing rewrite candidate');
    const originalIdentity = await regularIdentity(artifact.path);
    requireGivenIdentity(artifact.identity ?? (index === 0 ? parent.identity : undefined), originalIdentity);
    const candidateIdentity = await regularIdentity(artifact.candidatePath);
    assert(artifact.size === undefined || candidateIdentity.size === BigInt(artifact.size), 'Rewrite candidate changed since preparation');
    assert(originalIdentity.size <= BigInt(streamLimit) && candidateIdentity.size <= BigInt(streamLimit),
      'Streamed transaction artifact exceeds size limit');
    copies.push({ artifact, record, originalIdentity, candidateIdentity });
  }
  return copies;
}

async function assertRewriteSetCopiesFit(destination, copies) {
  const journalDev = (await lstat(destination, { bigint: true })).dev;
  assert(copies.every(({ originalIdentity }) => originalIdentity.dev === journalDev),
    'Rewrite artifacts must be on the transaction filesystem');
  // Space already used by the originals and prepared candidates is reflected
  // in bavail. Only their additional journal copies consume new file blocks.
  const sizes = copies.flatMap(({ originalIdentity, candidateIdentity }) => [originalIdentity.size, candidateIdentity.size]);
  await assertJournalSpace(destination, sizes);
  return journalDev;
}

/** Allow the Host to fail early before releasing the live Agent. */
export async function assertRewriteSetSpace(parent, artifacts) {
  const root = namespace(parent.path);
  let destination = dirname(parent.path);
  for (const path of [join(destination, 'plugins'), dirname(root), root, join(root, `v${manifest.version}`)]) {
    if (!await assertDirectory(path)) break;
    destination = path;
  }
  await assertRewriteSetCopiesFit(destination, await planRewriteSetCopies(parent, artifacts));
}

/** Prepare a durable multi-log journal under caller-held native writer locks. */
export async function prepareRewriteSetJournal(parent, children, artifacts, { messageId } = {}) {
  const parentPath = parent.path;
  const parentKey = pathKey(parentPath);
  const header = parent.header ?? parent.handle?.header;
  assert(typeof header?.id === 'string' && header.id.length > 0 && sessionFile.test(basename(parentPath)),
    'Missing parent session header or path');
  assert(Array.isArray(artifacts) && artifacts.length > 0 && isSamePath(artifacts[0].path, parentPath), 'Parent artifact must be first');
  const root = namespace(parentPath);
  const versionPath = join(root, `v${manifest.version}`);
  for (const path of [join(dirname(parentPath), 'plugins'), dirname(root), root, versionPath]) await ensureDirectory(path);
  assert(!active.has(parentKey) && !await hasPendingRewrite(parentPath), 'A previous rewrite still needs recovery');
  active.add(parentKey);
  const temporary = join(versionPath, `preparing-${randomUUID()}`);
  const directory = join(versionPath, 'rewrite');
  try {
    const copies = await planRewriteSetCopies(parent, artifacts);
    await mkdir(temporary, { mode: 0o700 });
    await mkdir(join(temporary, 'artifacts'), { mode: 0o700 });
    const journal = { directory: temporary, parentPath };
    const intent = {
      format: 2, parentId: header.id, filename: basename(parentPath), owner: await transactionOwner(),
      artifacts: [], children: [],
      ...(messageId === undefined ? {} : { delivery: { messageId } }),
    };
    const journalDev = await assertRewriteSetCopiesFit(journal.directory, copies);
    for (const { artifact, record, originalIdentity, candidateIdentity } of copies) {
      const candidatePath = v2StoredPath(journal, record, 'candidate');
      const candidate = await streamFile(artifact.candidatePath, {
        destination: candidatePath, mode: Number(originalIdentity.mode & 0o777n), sourceIdentity: candidateIdentity,
      });
      assert((artifact.size === undefined || candidate.size === artifact.size)
        && (artifact.digest === undefined || candidate.digest === artifact.digest), 'Rewrite candidate changed since preparation');
      const original = await streamFile(artifact.path, {
        destination: v2StoredPath(journal, record, 'original'), sourceIdentity: originalIdentity,
      });
      intent.artifacts.push({
        ...record,
        original: { ...original, mode: Number(originalIdentity.mode & 0o777n), identity: fileIdentity(originalIdentity) },
        candidate: { ...candidate, records: artifact.retained, identity: fileIdentity(await regularIdentity(candidatePath)) },
      });
    }
    for (const child of children) {
      const record = entryRecord(child);
      assert(isSamePath(childPath(parentPath, record), child.path), 'Child is outside the session storage root');
      const identity = await regularIdentity(child.path);
      requireGivenIdentity(child.identity, identity);
      const directoryIdentity = await lstat(dirname(child.path), { bigint: true });
      assert(directoryIdentity.isDirectory() && directoryIdentity.dev === journalDev,
        'Withdrawn child must be on the transaction filesystem');
      intent.children.push({ ...record, identity: fileIdentity(identity),
        directoryIdentity: { dev: String(directoryIdentity.dev), ino: String(directoryIdentity.ino) } });
    }
    validateIntent(intent, { header }, parentPath);
    const bytes = JSON.stringify(intent);
    assert(Buffer.byteLength(bytes) <= intentLimit, 'Rewrite transaction metadata exceeds size limit');
    await writeFileDurably(join(temporary, 'intent.json'), bytes);
    await syncDirectory(join(temporary, 'artifacts'));
    await syncDirectory(temporary);
    const directoryIdentity = await transactionDirectoryIdentity(temporary);
    await moveDirectory(temporary, directory);
    return { directory, directoryIdentity, intent, parentPath, dshPath: parent.dshPath };
  } catch (error) {
    active.delete(parentKey);
    await removeRetiredDirectory(temporary).catch(() => {});
    throw error;
  }
}

export function rewriteSetArtifactPath(journal, index = 0) {
  assert(journal.intent?.format === 2 && Number.isSafeInteger(index) && journal.intent.artifacts[index], 'Unknown rewrite artifact');
  return v2ArtifactPath(journal, journal.intent.artifacts[index]);
}

async function matchRewriteSetArtifact(journal, index = 0) {
  const path = rewriteSetArtifactPath(journal, index);
  const artifact = journal.intent.artifacts[index];
  const candidate = artifact.candidate;
  const original = artifact.original;
  const current = await regularIdentity(path);
  if (sameIdentity(fileIdentity(current), original.identity)) return { kind: 'original', current };
  // A prior recovery may have restored the bytes at a new inode.
  if (current.size === BigInt(original.size) && (await streamFile(path)).digest === original.digest) {
    return { kind: 'original', current };
  }
  if (current.size >= BigInt(candidate.size)
    && (await streamFile(path, { length: candidate.size })).digest === candidate.digest) {
    return { kind: 'candidate', current };
  }
  return { kind: 'conflict', current };
}

/** Publish child candidates first so recovery can identify a partial publication. */
export async function publishRewriteSetArtifacts(journal) {
  assert(journal.intent?.format === 2, 'Not a multi-log rewrite journal');
  const order = [...journal.intent.artifacts.slice(1), journal.intent.artifacts[0]];
  for (const artifact of order) {
    const path = v2ArtifactPath(journal, artifact);
    const source = v2StoredPath(journal, artifact, 'candidate');
    assert(sameIdentity(fileIdentity(await regularIdentity(path)), artifact.original.identity), 'Rewrite source changed before publish');
    assert(sameIdentity(fileIdentity(await regularIdentity(source)), artifact.candidate.identity), 'Rewrite candidate changed before publish');
    await replaceFileDurable(source, path);
    await syncDirectory(dirname(path));
    await syncDirectory(dirname(source));
  }
}

export async function restoreRewriteSetArtifacts(journal, { canRestoreArtifact } = {}) {
  assert(journal.intent?.format === 2 && !journal.committed, 'Not an uncommitted multi-log rewrite journal');
  const pending = [];
  for (const artifact of journal.intent.artifacts) {
    const path = v2ArtifactPath(journal, artifact);
    const { kind, current } = await matchRewriteSetArtifact(journal, artifact.index);
    assert(kind !== 'conflict', 'Rewrite artifact changed after publish');
    if (kind !== 'candidate') continue;
    const hasSuffix = current.size > BigInt(artifact.candidate.size);
    const allowed = await canRestoreArtifact?.(artifact, kind, artifact.index, path);
    assert(hasSuffix ? allowed === true : allowed !== false, 'Candidate artifact cannot be restored safely');
    const original = v2StoredPath(journal, artifact, 'original');
    const backup = await regularIdentity(original);
    const restored = await streamFile(original);
    assert(restored.size === artifact.original.size && restored.digest === artifact.original.digest, 'Original rewrite backup changed');
    pending.push({ artifact, path, original, backup: fileIdentity(backup), current: fileIdentity(current) });
  }
  for (const { path, current } of pending) {
    assert(sameIdentity(fileIdentity(await regularIdentity(path)), current), 'Rewrite artifact changed during recovery');
  }
  await restoreWithdrawnChildren(journal);
  for (const { artifact, path, original, backup } of pending.reverse()) {
    const openFlags = (process.platform === 'win32' ? constants.O_RDWR : constants.O_RDONLY) | (constants.O_NOFOLLOW ?? 0);
    const handle = await open(original, openFlags);
    try {
      assert(sameIdentity(fileIdentity(await handle.stat({ bigint: true })), backup), 'Original rewrite backup changed');
      if (process.platform !== 'win32') await handle.chmod(artifact.original.mode);
      await handle.sync();
    } finally { await handle.close(); }
    // A crash after this rename can resume using the original digest to
    // identify the already restored native file at its new inode.
    await replaceFileDurable(original, path);
    await syncDirectory(dirname(path));
    await syncDirectory(dirname(original));
  }
}

/** Keep the original journal API for version 1 callers and transactions. */
export async function prepareRewriteJournal(parent, children, replacement) {
  const parentPath = parent.path;
  const parentKey = pathKey(parentPath);
  const parentId = parent.handle.header.id;
  const root = namespace(parentPath);
  const versionPath = join(root, `v${manifest.version}`);
  for (const path of [join(dirname(parentPath), 'plugins'), dirname(root), root, versionPath]) {
    await ensureDirectory(path);
  }
  assert(!active.has(parentKey) && !await hasPendingRewrite(parentPath), 'A previous rewrite still needs recovery');
  active.add(parentKey);
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
        digest: (await streamFile(path, { max: limit })).digest };
      assert(isSamePath(childPath(parentPath, record), path), 'Child and parent use different storage roots');
      records.push(record);
    }
    const candidate = createHash('sha256');
    let size = 0;
    for (const bytes of replacement.buffers) { candidate.update(bytes); size += bytes.length; }
    assert(size <= limit, 'Replacement transcript is too large');
    const intent = {
      format: 1, parentId, filename: basename(parentPath), owner: await transactionOwner(),
      original: digest(replacement.original), candidate: { digest: candidate.digest('hex'), size, records: replacement.retained },
      children: records,
    };
    await mkdir(temporary, { mode: 0o700 });
    await writeFileDurably(join(temporary, 'original'), replacement.original);
    await writeFileDurably(join(temporary, 'intent.json'), JSON.stringify(intent));
    await syncDirectory(temporary);
    const directoryIdentity = await transactionDirectoryIdentity(temporary);
    await moveDirectory(temporary, directory);
    return { directory, directoryIdentity, intent, parentPath };
  } catch (error) {
    active.delete(parentKey);
    // The preparation directory never contains moved children. Keep an already
    // published intent intact so the next recovery can make the same decision.
    await removeRetiredDirectory(temporary).catch(() => {});
    throw error;
  }
}

export function forgetRewriteJournal(journal) {
  if (journal) active.delete(pathKey(journal.parentPath));
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

async function resolveChildLocation(journal, entry, index) {
  const { from, to, path } = childLocations(journal, entry, index);
  const original = await assertDirectory(from);
  const held = await assertDirectory(to);
  assert(original !== held, 'Cannot distinguish the original child directory from a conflict');
  const rollout = original ? path : join(to, entry.filename);
  await verifyChildFile(rollout, entry);
  return { from, to, rollout, original };
}

export async function resolveRewriteSetChildPath(journal, index) {
  const entry = journal.intent?.format === 2 && Number.isSafeInteger(index) && journal.intent.children[index];
  assert(entry, 'Unknown withdrawn child');
  return (await resolveChildLocation(journal, entry, index)).rollout;
}

export async function restoreWithdrawnChildren(journal) {
  const restore = [];
  // Validate every move before making any. A repeat after a crash accepts an
  // already restored directory only if it still holds the exact same rollout.
  for (const [index, entry] of journal.intent.children.entries()) {
    const location = await resolveChildLocation(journal, entry, index);
    if (!location.original) restore.push(location);
  }
  for (const { from, to } of restore.toReversed()) await moveDirectory(to, from);
}

export async function markRewriteCommitted(journal) {
  // fsync an empty regular marker after the native Agent has accepted the
  // candidate. Once present, recovery can only finish cleanup.
  const temporary = join(journal.directory, `commit-${randomUUID()}.tmp`);
  await writeFileDurably(temporary, Buffer.alloc(0));
  await replaceFileDurable(temporary, join(journal.directory, 'committed'));
  await syncDirectory(journal.directory);
  journal.committed = true;
}

export async function clearFailedCommitMarker(journal) {
  try { await unlink(join(journal.directory, 'committed')); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  await syncDirectory(journal.directory);
}

function reportCleanupFailure(onCleanupError, error) {
  try { onCleanupError?.(error); } catch { /* The durable directory remains recoverable. */ }
}

async function drainCleanup() {
  try {
    while (cleanupQueue.length > 0) {
      const { garbage, onCleanupError } = cleanupQueue.shift();
      await removeRetiredDirectory(garbage).catch(error => reportCleanupFailure(onCleanupError, error));
    }
  } finally {
    cleaning = false;
  }
}

export async function retireRewriteJournal(journal, { deferCleanup = false, onCleanupError } = {}) {
  const garbage = join(dirname(journal.directory), `discard-${randomUUID()}`);
  // Rename first. Startup will resume cleanup of any detached directory.
  if (!await moveDirectory(journal.directory, garbage, journal.directoryIdentity)) return;
  if (!deferCleanup) return removeRetiredDirectory(garbage);
  if (cleanupQueue.length >= 64) {
    reportCleanupFailure(onCleanupError, new Error('Deferred rewrite cleanup will resume on startup'));
    return;
  }
  cleanupQueue.push({ garbage, onCleanupError });
  if (!cleaning) {
    cleaning = true;
    setImmediate(() => { void drainCleanup(); });
  }
}

function completeSettledRetirement(path, state) {
  clearTimeout(state.timer);
  if (settledRetirements.get(path) === state) settledRetirements.delete(path);
}

function scheduleRetirementAttempt(path, state) {
  const delay = retirementDelays[state.attempts];
  if (state.timer || delay === undefined) return;
  state.timer = setTimeout(() => {
    state.timer = undefined;
    void retrySettledRewriteRetirement(path);
  }, delay);
  state.timer.unref();
}

/** Remember only durable commits whose native prompt is already settled. */
export function scheduleSettledRewriteRetirement(journal, { onCleanupError } = {}) {
  const { parentPath, directory, directoryIdentity, committed } = journal;
  assert(committed && directoryIdentity, 'Cannot retire an unverified or unsettled transaction');
  const key = pathKey(parentPath);
  const known = settledRetirements.get(key);
  if (known?.journal.directoryIdentity === directoryIdentity) return;
  if (known) completeSettledRetirement(key, known);
  const state = { journal: { directory, directoryIdentity }, onCleanupError, attempts: 0 };
  settledRetirements.set(key, state);
  scheduleRetirementAttempt(key, state);
}

/** Retry on edit admission as well as on the bounded background schedule. */
export async function retrySettledRewriteRetirement(path) {
  const key = pathKey(path);
  const state = settledRetirements.get(key);
  if (!state) return;
  if (state.running) return state.running;
  clearTimeout(state.timer);
  state.timer = undefined;
  state.attempts++;
  state.running = (async () => {
    try {
      const { journal, onCleanupError } = state;
      if (await transactionDirectoryIdentity(journal.directory) === journal.directoryIdentity) {
        assert((await readRegular(join(journal.directory, 'committed'), 0)).length === 0, 'Invalid transaction commit marker');
        await retireRewriteJournal(journal, { deferCleanup: true, onCleanupError });
      }
      completeSettledRetirement(key, state);
    } catch (error) {
      if (state.attempts >= retirementDelays.length) reportCleanupFailure(state.onCleanupError, error);
      if (settledRetirements.get(key) === state) scheduleRetirementAttempt(key, state);
    }
  })();
  try { await state.running; } finally { state.running = undefined; }
}

function validIdentity(identity, keys = ['dev', 'ino', 'size', 'mtimeNs', 'ctimeNs']) {
  return identity !== null && typeof identity === 'object'
    && keys.every(key => typeof identity[key] === 'string' && /^-?\d{1,40}$/u.test(identity[key]));
}

function validFile(record) {
  return Number.isSafeInteger(record?.size) && record.size > 0 && record.size <= streamLimit
    && typeof record.digest === 'string' && digestPattern.test(record.digest)
    && validIdentity(record.identity) && BigInt(record.identity.size) === BigInt(record.size);
}

function validChildRecord(record) {
  return typeof record?.id === 'string' && record.id.length > 0 && typeof record.parentSession === 'string'
    && record.parentSession.length > 0 && record.parentSession !== record.id && Number.isSafeInteger(record.createdAt)
    && record.createdAt >= 0;
}

function validateSetIntent(intent, path) {
  assert(Array.isArray(intent.artifacts) && intent.artifacts.length > 0 && Array.isArray(intent.children),
    'Invalid multi-log transaction artifacts');
  const ids = new Set();
  const paths = new Set();
  const withdrawnIds = new Set(intent.children.map(record => record?.id));
  const seenWithdrawn = new Set();
  for (const [index, artifact] of intent.artifacts.entries()) {
    assert(artifact?.index === index && validFile(artifact.original) && validFile(artifact.candidate)
      && Number.isInteger(artifact.original.mode) && artifact.original.mode >= 0 && artifact.original.mode <= 0o777
      && Number.isSafeInteger(artifact.candidate.records) && artifact.candidate.records >= 0,
    'Invalid multi-log artifact metadata');
    if (index === 0) {
      assert(artifact.id === intent.parentId && artifact.filename === basename(path), 'Invalid parent artifact metadata');
    } else {
      assert(validChildRecord(artifact) && !withdrawnIds.has(artifact.parentSession), 'Invalid child artifact metadata');
    }
    const directory = pathKey(dirname(index === 0 ? path : childPath(path, artifact)));
    assert(!ids.has(artifact.id) && !paths.has(directory), 'Duplicate transaction identifier or path');
    ids.add(artifact.id);
    paths.add(directory);
  }
  for (const child of intent.children) {
    assert(validChildRecord(child) && validIdentity(child.identity) && validIdentity(child.directoryIdentity, ['dev', 'ino'])
      && child.identity.dev === child.directoryIdentity.dev
      && (!withdrawnIds.has(child.parentSession) || seenWithdrawn.has(child.parentSession)), 'Invalid recorded child lineage');
    const directory = pathKey(dirname(childPath(path, child)));
    assert(!ids.has(child.id) && !paths.has(directory), 'Duplicate transaction identifier or path');
    ids.add(child.id);
    seenWithdrawn.add(child.id);
    paths.add(directory);
  }
}

function validateIntent(intent, snapshot, path) {
  const { id } = snapshot.header;
  assert((intent?.format === 1 || intent?.format === 2) && intent.parentId === id && intent.filename === basename(path), 'Unknown transaction format or parent');
  if (intent.format === 2) {
    if (intent.delivery !== undefined) {
      const { messageId } = intent.delivery ?? {};
      assert(typeof messageId === 'string' && messageId.length > 0 && messageId.length <= 2048,
        'Invalid pending delivery metadata');
    }
    return validateSetIntent(intent, path);
  }
  assert(digestPattern.test(intent.original) && digestPattern.test(intent.candidate?.digest)
    && Number.isSafeInteger(intent.candidate.size) && intent.candidate.size > 0 && intent.candidate.size <= limit
    && Number.isSafeInteger(intent.candidate.records) && intent.candidate.records >= 0 && Array.isArray(intent.children),
  'Invalid transaction hashes or boundaries');
  const ids = new Set([id]);
  const paths = new Set([pathKey(dirname(path))]);
  for (const child of intent.children) {
    const directory = pathKey(dirname(childPath(path, child)));
    assert(typeof child.id === 'string' && child.id.length > 0 && !ids.has(child.id)
      && ids.has(child.parentSession) && !paths.has(directory) && Number.isSafeInteger(child.createdAt)
      && digestPattern.test(child.digest), 'Invalid recorded child lineage');
    ids.add(child.id);
    paths.add(directory);
  }
}

/** Scan only artifacts located by the native persistence backend. */
export async function readRewriteJournals(snapshot) {
  const dshPath = snapshot?.location?.path;
  if (snapshot?.location?.kind !== 'jsonl' || typeof dshPath !== 'string' || !sessionFile.test(basename(dshPath))) return [];
  const path = await resolveRolloutPath(dshPath).catch(error => {
    if (error.code !== 'ENOENT') throw error;
    return undefined;
  });
  if (!path || !await assertDirectory(dirname(path))) return [];
  const root = namespace(path);
  for (const directory of [join(dirname(path), 'plugins'), dirname(root), root]) {
    if (!await assertDirectory(directory)) return [];
  }
  if (active.has(pathKey(path))) return [];
  const journals = [];
  for (const name of await readdir(root)) {
    if (!release.test(name)) continue;
    const versionPath = join(root, name);
    if (!await assertDirectory(versionPath)) continue;
    const directory = join(versionPath, 'rewrite');
    const directoryIdentity = await transactionDirectoryIdentity(directory);
    if (!directoryIdentity) continue;
    try {
      const intent = JSON.parse(await readRegular(join(directory, 'intent.json'), intentLimit));
      validateIntent(intent, snapshot, path);
      await assertOwnerReleased(intent.owner);
      const marker = join(directory, 'committed');
      const committed = await optionalStat(marker) !== undefined;
      if (committed) assert((await readRegular(marker, 0)).length === 0, 'Invalid transaction commit marker');
      if (await transactionDirectoryIdentity(directory) === directoryIdentity) {
        journals.push({ directory, directoryIdentity, intent, parentPath: path, dshPath, committed });
      }
    } catch (error) {
      // Retirement atomically moves the entire directory out of this path.
      // A missing file in a directory that still exists remains an error.
      if (error.code !== 'ENOENT' || await transactionDirectoryIdentity(directory) === directoryIdentity) throw error;
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

export async function matchJournalRevision(journal, index = 0) {
  if (journal.intent?.format === 2) return (await matchRewriteSetArtifact(journal, index)).kind;
  if ((await streamFile(journal.parentPath, { max: limit })).digest === journal.intent.original) return 'original';
  const { size, digest: hash } = journal.intent.candidate;
  const current = await regularIdentity(journal.parentPath);
  if (current.size >= BigInt(size) && (await streamFile(journal.parentPath, { length: size })).digest === hash) return 'candidate';
  return 'conflict';
}
