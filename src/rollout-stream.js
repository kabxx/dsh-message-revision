import { createHash } from 'node:crypto';
import { readSync } from 'node:fs';
import { lstat, open, unlink } from 'node:fs/promises';
import { basename, dirname, isAbsolute } from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { promisify } from 'node:util';
import { constants, createZstdDecompress, zstdCompress, zstdDecompressSync } from 'node:zlib';
import { isCanonicalPath, openReadOnlyFlags } from './platform.js';

const compress = promisify(zstdCompress);
const utf8 = new TextDecoder('utf-8', { fatal: true });
const chunkBytes = 128 * 1024;
const inlineZstdOutputBytes = 256 * 1024;
const frameBytes = 512 * 1024;
const maxCandidateBytes = 256 * 1024 * 1024;
const zstdMagic = 0xFD2FB528;
const zstdSkippableMask = 0xFFFFFFF0;
const zstdSkippableMagic = 0x184D2A50;
export const maxRecordBytes = 16 * 1024 * 1024;
export const nativeLogName = /^session\.v4\.jsonl(?:\.zstd)?$/u;
export const legacyLogName = /^session\.v3\.jsonl(?:\.zstd)?$/u;

export function requireRollout(condition, message) {
  if (!condition) throw new Error(message);
}

/** Compare opened files by native identity while keeping the wire representation JSON-safe. */
export function fileIdentity(stat) {
  return Object.fromEntries(['dev', 'ino', 'size', 'mtimeNs', 'ctimeNs'].map(key => [key, String(stat[key])]));
}

function sameIdentity(stat, expected) {
  return Object.entries(fileIdentity(stat)).every(([key, value]) => expected[key] === value);
}

export async function checkRolloutIdentity(path, expected) {
  const stat = await lstat(path, { bigint: true });
  requireRollout(stat.isFile() && await isCanonicalPath(path) && sameIdentity(stat, expected),
    '会话日志在检查期间已变化，请重新打开编辑框。');
}

/** Stream a single native event row at a time. Legacy generations are read-only. */
export async function* readRolloutLines(path, sessionId, { allowLegacy = false, trackZstdPrefix = false } = {}) {
  requireRollout(typeof path === 'string' && isAbsolute(path), '会话日志路径或格式异常。');
  const name = basename(path);
  const expectedVersion = nativeLogName.test(name) ? 4 : allowLegacy && legacyLogName.test(name) ? 3 : undefined;
  requireRollout(expectedVersion !== undefined && await isCanonicalPath(path), '会话日志路径或格式异常。');
  const file = await open(path, openReadOnlyFlags());
  try {
    const initial = await file.stat({ bigint: true });
    requireRollout(initial.isFile() && initial.size <= BigInt(Number.MAX_SAFE_INTEGER), '会话日志路径或大小异常。');
    const identity = fileIdentity(initial);
    await checkRolloutIdentity(path, identity);
    const fileSize = Number(initial.size);
    let fragments = [];
    let bytes = 0;
    let index = -1;
    let framePrefix;
    let reusable = true;
    function onFrame(offset, frame) {
      // A prefix can only end between records. The preceding frames have
      // already passed decoding and checksum verification in this scan.
      if (reusable && index > 0 && bytes === 0 && offset <= maxCandidateBytes) {
        framePrefix = { end: offset, startSeq: index };
      }
      reusable &&= frame.reusable === true;
    }
    const chunks = path.endsWith('.zstd')
      ? streamZstdChunks(file, 0, fileSize, trackZstdPrefix ? onFrame : undefined)
      : readFrameChunks(file, 0, fileSize);
    for await (const chunk of chunks) {
      let start = 0;
      while (start < chunk.length) {
        const newline = chunk.indexOf(0x0a, start);
        const end = newline < 0 ? chunk.length : newline + 1;
        const part = chunk.subarray(start, end);
        bytes += part.length;
        requireRollout(bytes <= maxRecordBytes, '会话含有超过 16 MiB 的单条记录，无法安全编辑。');
        if (newline === -1) {
          fragments.push(part);
        } else {
          const line = fragments.length ? Buffer.concat([...fragments, part], bytes) : part;
          fragments = [];
          bytes = 0;
          const record = JSON.parse(utf8.decode(line));
          requireRollout(record && typeof record === 'object' && !Array.isArray(record), '会话日志内含无效记录。');
          if (index === -1) {
            requireRollout(record.type === 'session' && record.version === expectedVersion && record.id === sessionId,
              '会话日志文件头不匹配。');
            if (expectedVersion === 3) {
              requireRollout(record.origin === 'subagent' && typeof record.parentSession === 'string'
                && typeof record.isSeeded === 'boolean' && Number.isSafeInteger(record.createdAt) && record.createdAt >= 0
                && Number.isSafeInteger(record.delegationDepth) && record.delegationDepth > 0,
              '无法核对旧版子会话日志的身份。');
            }
          } else {
            requireRollout(record.seq === index && typeof record.type === 'string' && Number.isSafeInteger(record.time)
              && record.time >= 0 && Object.hasOwn(record, 'data'), '会话日志的事件顺序或记录无效。');
          }
          yield { record, line, index, identity, framePrefix };
          index++;
        }
        start = end;
      }
    }
    requireRollout(index >= 0 && bytes === 0, '会话日志不完整，已取消编辑。');
    await checkRolloutIdentity(path, identity);
  } finally {
    await file.close();
  }
}

function readExactSync(fd, buf, len, pos) {
  let offset = 0;
  while (offset < len) {
    const bytesRead = readSync(fd, buf, offset, len - offset, pos + offset);
    if (bytesRead <= 0) return null;
    offset += bytesRead;
  }
  return buf.subarray(0, len);
}

function makeBufferedSlice(fd, fileSize) {
  const buf = Buffer.allocUnsafe(chunkBytes);
  let bufStart = 0;
  let bufEnd = 0;
  return function readSlice(pos, len) {
    if (len === 0) return buf.subarray(0, 0);
    if (pos < 0 || len > chunkBytes || pos + len > fileSize) return null;
    if (pos < bufStart || pos + len > bufEnd) {
      const want = Math.min(chunkBytes, fileSize - pos);
      let filled = 0;
      while (filled < len) {
        const bytesRead = readSync(fd, buf, filled, want - filled, pos + filled);
        if (bytesRead <= 0) return null;
        filled += bytesRead;
      }
      bufStart = pos;
      bufEnd = pos + filled;
    }
    return buf.subarray(pos - bufStart, pos - bufStart + len);
  };
}

function readZstdFrame(readSlice, start, fileSize, { strictDsh = true } = {}) {
  let pos = start;
  const magicBuf = readSlice(pos, 4);
  if (!magicBuf) return null;
  const magic = magicBuf.readUInt32LE(0);
  if (!strictDsh && ((magic & zstdSkippableMask) >>> 0) === zstdSkippableMagic) {
    const lenBuf = readSlice(pos + 4, 4);
    if (!lenBuf) return null;
    const end = pos + 8 + lenBuf.readUInt32LE(0);
    return end <= fileSize ? { end, skippable: true } : null;
  }
  if (magic !== zstdMagic) return null;
  const descBuf = readSlice(pos + 4, 1);
  if (!descBuf) return null;
  const descriptor = descBuf[0];
  const checksum = (descriptor & 0x04) !== 0;
  const reusable = checksum && (descriptor & 0x1b) === 0;
  if ((descriptor & 0x18) !== 0 || (strictDsh && !reusable)) return null;
  const dictFlag = descriptor & 0x03;
  const dictBytes = dictFlag === 3 ? 4 : dictFlag;
  const contentSizeFlag = descriptor >>> 6;
  const singleSegment = (descriptor & 0x20) !== 0;
  const fcsBytes = contentSizeFlag === 0 ? (singleSegment ? 1 : 0) : 1 << contentSizeFlag;
  const headerLen = 5 + (singleSegment ? 0 : 1) + dictBytes + fcsBytes;
  if (pos + headerLen > fileSize) return null;
  let contentSize;
  if (fcsBytes > 0) {
    const fcs = readSlice(pos + 5 + (singleSegment ? 0 : 1) + dictBytes, fcsBytes);
    if (!fcs) return null;
    if (fcsBytes === 1) contentSize = fcs[0];
    else if (fcsBytes === 2) contentSize = fcs.readUInt16LE(0) + 256;
    else if (fcsBytes === 4) contentSize = fcs.readUInt32LE(0);
    else {
      const value = fcs.readBigUInt64LE(0);
      if (value > BigInt(Number.MAX_SAFE_INTEGER)) return null;
      contentSize = Number(value);
    }
  }
  pos += headerLen;
  for (;;) {
    const block = readSlice(pos, 3);
    if (!block) return null;
    const raw = block[0] | (block[1] << 8) | (block[2] << 16);
    const lastBlock = (raw & 1) !== 0;
    const blockType = (raw >>> 1) & 0x03;
    const blockSize = raw >>> 3;
    if (blockType === 0x03 || blockSize > chunkBytes) return null;
    pos += 3 + (blockType === 0x01 ? 1 : blockSize);
    if (pos > fileSize) return null;
    if (lastBlock) break;
  }
  if (checksum) pos += 4;
  if (pos > fileSize) return null;
  return { end: pos, contentSize, reusable, skippable: false };
}

/**
 * Decode concatenated Zstandard frames from a file handle in bounded memory.
 * Node 24's built-in Zstd stream stops after the first frame in a concatenated
 * stream, so walking RFC 8878 frame boundaries keeps Electron (Node 24) and
 * CLI (Node 26) behavior identical while detecting torn trailing frames.
 */
async function* streamZstdChunks(file, startOffset, fileSize, onFrame) {
  const readSlice = makeBufferedSlice(file.fd, fileSize);
  let pos = startOffset;
  while (pos < fileSize) {
    const frame = readZstdFrame(readSlice, pos, fileSize, { strictDsh: false });
    requireRollout(frame !== null, '会话日志不完整，已取消编辑。');
    onFrame?.(pos, frame);
    const frameEnd = frame.end;
    if (frame.skippable) {
      pos = frameEnd;
      continue;
    }
    const frameLen = frameEnd - pos;
    if (frameLen <= chunkBytes && frame.contentSize !== undefined && frame.contentSize <= inlineZstdOutputBytes) {
      const raw = readSlice(pos, frameLen);
      requireRollout(raw !== null, '会话日志读取不完整。');
      const out = zstdDecompressSync(raw, {
        maxOutputLength: inlineZstdOutputBytes,
        params: { [constants.ZSTD_d_windowLogMax]: 27 },
      });
      if (out.length > 0) yield out;
    } else {
      const input = Readable.from(readFrameChunks(file, pos, frameEnd));
      const decoded = createZstdDecompress({ chunkSize: chunkBytes, params: { [constants.ZSTD_d_windowLogMax]: 27 } });
      const transport = pipeline(input, decoded).then(() => undefined, error => error);
      let done = false;
      try {
        for await (const chunk of decoded) {
          if (chunk.length > 0) yield chunk;
        }
        const error = await transport;
        if (error) throw error;
        done = true;
      } finally {
        if (!done) {
          decoded.destroy();
          input.destroy();
        }
        await transport;
      }
    }
    pos = frameEnd;
  }
}

/** Decode all concatenated Zstandard frames in an in-memory buffer with a total output cap. */
export function decompressZstdConcat(buffer, { maxOutputLength = maxCandidateBytes } = {}) {
  const readSlice = (pos, len) => (pos >= 0 && pos + len <= buffer.length ? buffer.subarray(pos, pos + len) : null);
  const parts = [];
  let total = 0;
  let pos = 0;
  while (pos < buffer.length) {
    const frame = readZstdFrame(readSlice, pos, buffer.length, { strictDsh: false });
    requireRollout(frame !== null, '会话日志不完整，已取消编辑。');
    if (!frame.skippable) {
      const remaining = maxOutputLength - total;
      requireRollout(remaining > 0 || frame.contentSize === undefined || frame.contentSize === 0, '会话日志超出解压大小上限。');
      let out;
      try {
        out = zstdDecompressSync(buffer.subarray(pos, frame.end), {
          maxOutputLength: Math.max(1, remaining),
          params: { [constants.ZSTD_d_windowLogMax]: 27 },
        });
      } catch (error) {
        if (error?.code === 'ERR_BUFFER_TOO_LARGE' || error instanceof RangeError) {
          throw new Error('会话日志超出解压大小上限。', { cause: error });
        }
        throw error;
      }
      total += out.length;
      requireRollout(total <= maxOutputLength, '会话日志超出解压大小上限。');
      if (out.length > 0) parts.push(out);
    }
    pos = frame.end;
  }
  return parts.length === 1 ? parts[0] : Buffer.concat(parts, total);
}

async function* readFrameChunks(file, start, end) {
  let pos = start;
  while (pos < end) {
    const size = Math.min(chunkBytes, end - pos);
    const buf = Buffer.allocUnsafe(size);
    const { bytesRead } = await file.read(buf, 0, size, pos);
    if (bytesRead <= 0) throw new Error('会话日志读取不完整。');
    yield bytesRead === size ? buf : buf.subarray(0, bytesRead);
    pos += bytesRead;
  }
}

async function readFirstRecordInZstdFrame(file, start, end, { singleLine = false } = {}) {
  const input = Readable.from(readFrameChunks(file, start, end));
  const decoded = createZstdDecompress({ chunkSize: chunkBytes, params: { [constants.ZSTD_d_windowLogMax]: 27 } });
  const transport = pipeline(input, decoded).then(() => undefined, error => error);
  let fragments = [];
  let bytes = 0;
  let line;
  let extra = false;
  let finished = false;
  try {
    for await (const chunk of decoded) {
      if (line !== undefined) {
        if (chunk.length > 0) { extra = true; break; }
        continue;
      }
      const newline = chunk.indexOf(0x0a);
      const part = newline < 0 ? chunk : chunk.subarray(0, newline + 1);
      bytes += part.length;
      if (bytes > maxRecordBytes) return null;
      if (newline === -1) {
        fragments.push(part);
      } else {
        line = fragments.length ? Buffer.concat([...fragments, part], bytes) : part;
        fragments = [];
        if (!singleLine) break;
        if (newline + 1 < chunk.length) { extra = true; break; }
      }
    }
    if (singleLine && !extra) {
      const error = await transport;
      if (error) return null;
      finished = true;
    }
  } catch {
    return null;
  } finally {
    if (!finished) {
      decoded.destroy();
      input.destroy();
    }
    await transport;
  }
  if (!line || extra) return null;
  try {
    const record = JSON.parse(utf8.decode(line));
    return record && typeof record === 'object' && !Array.isArray(record) ? record : null;
  } catch {
    return null;
  }
}

/** Locate the owned tail frame of a checksummed v4 Zstandard seeded subagent log. */
export async function probeSeededSubagentTail(path, sessionId, cutoff) {
  if (typeof path !== 'string' || !isAbsolute(path) || !path.endsWith('.zstd')
    || !nativeLogName.test(basename(path)) || !Number.isSafeInteger(cutoff) || cutoff < 0
    || !await isCanonicalPath(path)) return null;
  const file = await open(path, openReadOnlyFlags());
  try {
    const initial = await file.stat({ bigint: true });
    if (!initial.isFile() || initial.size > BigInt(Number.MAX_SAFE_INTEGER)) return null;
    const identity = fileIdentity(initial);
    await checkRolloutIdentity(path, identity);
    const fileSize = Number(initial.size);
    const scratch = Buffer.allocUnsafe(64);
    function readSmall(pos, len) {
      if (pos + len > fileSize || len > scratch.length) return null;
      return readExactSync(file.fd, scratch, len, pos);
    }
    const frame0 = readZstdFrame(readSmall, 0, fileSize);
    if (!frame0 || frame0.end >= fileSize) return null;
    const header = await readFirstRecordInZstdFrame(file, 0, frame0.end, { singleLine: true });
    if (header?.type !== 'session' || header.version !== 4 || header.id !== sessionId
      || header.isSeeded !== true || header.origin !== 'subagent' || typeof header.parentSession !== 'string'
      || !Number.isSafeInteger(header.createdAt) || header.createdAt < 0) return null;
    const frame1 = readZstdFrame(readSmall, frame0.end, fileSize);
    // Normal DSH one-shot seed frames always record an explicit positive Frame_Content_Size,
    // whereas v3->v4 streaming migrations omit it and must fall back to full scan.
    if (!frame1 || frame1.end >= fileSize || frame1.contentSize === undefined || frame1.contentSize <= 0) return null;
    const frame2 = readZstdFrame(readSmall, frame1.end, fileSize);
    if (!frame2) return null;
    const firstSeed = await readFirstRecordInZstdFrame(file, frame0.end, frame1.end);
    if (firstSeed?.seq !== 0 || typeof firstSeed.type !== 'string' || !Number.isSafeInteger(firstSeed.time)
      || firstSeed.time < 0 || !Object.hasOwn(firstSeed, 'data')) return null;
    const firstTail = await readFirstRecordInZstdFrame(file, frame1.end, frame2.end);
    if (!firstTail || !Number.isSafeInteger(firstTail.seq) || firstTail.seq < 1
      || typeof firstTail.type !== 'string' || !Number.isSafeInteger(firstTail.time)
      || firstTail.time < 0 || firstTail.time >= cutoff || !Object.hasOwn(firstTail, 'data')) return null;
    if (firstTail.type !== 'subagent/descriptor'
      && !(header.agentPreset !== undefined && firstTail.type === 'agent-preset/selected'
        && firstTail.data?.agentPreset === header.agentPreset)) return null;
    await checkRolloutIdentity(path, identity);
    return {
      header,
      identity,
      tailOffset: frame1.end,
      startSeq: firstTail.seq,
      inheritedCount: firstTail.seq - 1,
      seedFrameBytes: frame1.contentSize ?? 0,
    };
  } finally {
    await file.close();
  }
}

/** Stream compressed native events starting from a verified Zstandard frame boundary. */
export async function* readRolloutLinesFromOffset(path, startOffset, startSeq, expected) {
  requireRollout(typeof path === 'string' && isAbsolute(path) && path.endsWith('.zstd')
    && nativeLogName.test(basename(path)) && await isCanonicalPath(path), '会话日志路径或格式异常。');
  const file = await open(path, openReadOnlyFlags());
  try {
    const initial = await file.stat({ bigint: true });
    requireRollout(initial.isFile() && initial.size <= BigInt(Number.MAX_SAFE_INTEGER)
      && sameIdentity(initial, expected), '会话日志在检查期间已变化，请重新打开编辑框。');
    requireRollout(Number.isSafeInteger(startOffset) && startOffset > 0 && BigInt(startOffset) < initial.size,
      '会话日志偏移量异常。');
    await checkRolloutIdentity(path, expected);
    let fragments = [];
    let bytes = 0;
    let index = startSeq;
    for await (const chunk of streamZstdChunks(file, startOffset, Number(initial.size))) {
      let start = 0;
      while (start < chunk.length) {
        const newline = chunk.indexOf(0x0a, start);
        const end = newline < 0 ? chunk.length : newline + 1;
        const part = chunk.subarray(start, end);
        bytes += part.length;
        requireRollout(bytes <= maxRecordBytes, '会话含有超过 16 MiB 的单条记录，无法安全编辑。');
        if (newline === -1) {
          fragments.push(part);
        } else {
          const line = fragments.length ? Buffer.concat([...fragments, part], bytes) : part;
          fragments = [];
          bytes = 0;
          const record = JSON.parse(utf8.decode(line));
          requireRollout(record && typeof record === 'object' && !Array.isArray(record)
            && record.seq === index && typeof record.type === 'string' && Number.isSafeInteger(record.time)
            && record.time >= 0 && Object.hasOwn(record, 'data'), '会话日志的事件顺序或记录无效。');
          yield { record, line, index };
          index++;
        }
        start = end;
      }
    }
    requireRollout(bytes === 0, '会话日志不完整，已取消编辑。');
    await checkRolloutIdentity(path, expected);
  } finally {
    await file.close();
  }
}

/** Buffer one small native frame at a time and publish to a caller-chosen temporary file. */
export async function writeRolloutCandidate(path, compressed, records, prefixCopy) {
  requireRollout(typeof path === 'string' && isAbsolute(path) && await isCanonicalPath(dirname(path)),
    '候选日志路径异常。');
  const file = await open(path, 'wx', 0o600);
  let owner;
  const hash = createHash('sha256');
  const options = { params: { [constants.ZSTD_c_checksumFlag]: 1 } };
  let size = 0;
  let used = 0;
  let pieces = [];
  let complete = false;
  async function flush() {
    if (pieces.length === 0) return;
    const raw = pieces.length === 1 ? pieces[0] : Buffer.concat(pieces, used);
    pieces = [];
    used = 0;
    const bytes = compressed ? await compress(raw, options) : raw;
    requireRollout(size + bytes.length <= maxCandidateBytes, '候选日志超出磁盘写入上限。');
    await file.writeFile(bytes);
    size += bytes.length;
    hash.update(bytes);
  }
  try {
    owner = await file.stat({ bigint: true });
    if (prefixCopy) {
      requireRollout(compressed, '非压缩会话日志不支持压缩前缀透传。');
      const source = await open(prefixCopy.sourcePath, openReadOnlyFlags());
      try {
        const stat = await source.stat({ bigint: true });
        requireRollout(stat.isFile() && sameIdentity(stat, prefixCopy.expected), '会话日志在检查期间已变化，请重新打开编辑框。');
        await checkRolloutIdentity(prefixCopy.sourcePath, prefixCopy.expected);
        requireRollout(prefixCopy.end > 0 && prefixCopy.end <= Number(stat.size) && prefixCopy.end <= maxCandidateBytes,
          '候选日志超出磁盘写入上限。');
        const copyBuf = Buffer.allocUnsafe(chunkBytes);
        let offset = 0;
        while (offset < prefixCopy.end) {
          const { bytesRead } = await source.read(copyBuf, 0, Math.min(chunkBytes, prefixCopy.end - offset), offset);
          requireRollout(bytesRead > 0, '会话日志读取不完整，已取消编辑。');
          const slice = bytesRead === copyBuf.length ? copyBuf : copyBuf.subarray(0, bytesRead);
          await file.writeFile(slice);
          hash.update(slice);
          size += bytesRead;
          offset += bytesRead;
        }
        await checkRolloutIdentity(prefixCopy.sourcePath, prefixCopy.expected);
      } finally {
        await source.close();
      }
    }
    for await (const { line, index } of records) {
      requireRollout(line.length <= maxRecordBytes && line.at(-1) === 0x0a, '候选日志包含无效或过大的记录。');
      if (used + line.length > frameBytes) await flush();
      pieces.push(line);
      used += line.length;
      if (index === -1 || used >= frameBytes) await flush();
    }
    await flush();
    await file.sync();
    complete = true;
    return { size, digest: hash.digest('hex') };
  } finally {
    try { await file.close(); } finally {
      if (!complete) {
        const current = await lstat(path, { bigint: true }).catch(error => {
          if (error.code !== 'ENOENT') throw error;
          return undefined;
        });
        if (owner && current?.isFile() && current.dev === owner.dev && current.ino === owner.ino
          && await isCanonicalPath(path)) await unlink(path);
      }
    }
  }
}
