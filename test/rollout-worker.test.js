import assert from 'node:assert/strict';
import { mkdir, mkdtemp, open, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { constants, createZstdCompress, zstdCompressSync } from 'node:zlib';
import { runRolloutWorker } from '../src/rollout-process.js';
import { decompressZstdConcat } from '../src/rollout-stream.js';
import { supportedPlatform } from './platform-fixture.js';

const workerSupported = supportedPlatform;
const compress = data => zstdCompressSync(Buffer.from(data), { params: { [constants.ZSTD_c_checksumFlag]: 1 } });
const row = value => `${JSON.stringify(value)}\n`;
const header = (id, isSeeded = false) => ({ type: 'session', version: 4, id, isSeeded, createdAt: 1 });
const event = (seq, time, type, data) => ({ seq, time, type, data });
const text = (id, value) => ({ id, source: { kind: 'user' }, content: [{ type: 'text', text: value }] });

async function fixture(t) {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'dsh-rollout-test-')));
  t.after(async () => {
    assert.equal(await realpath(root), root);
    assert.ok(root.startsWith(join(await realpath(tmpdir()), 'dsh-rollout-test-')));
    await rm(root, { recursive: true });
  });
  return root;
}

for (const compressed of [false, true]) {
  test(`isolated worker prepares native ${compressed ? 'Zstandard' : 'plain'} logs retaining every inbox occurrence`, { skip: !workerSupported }, async t => {
    const root = await fixture(t);
    const file = join(root, compressed ? 'session.v4.jsonl.zstd' : 'session.v4.jsonl');
    const output = join(root, 'candidate');
    const id = 'fixture-parent';
    const message = text('fixture-message', 'before');
    const replacement = text(message.id, 'after');
    const rows = [header(id), event(0, 10, 'agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [message] }),
      event(1, 11, 'agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [] }),
      event(2, 12, 'agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [message] }),
      event(3, 13, 'turn/start', { turn: 1 }),
      event(4, 14, 'agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [] }),
      event(5, 14, 'user/message', message), event(6, 15, 'turn/end', { turn: 1 }),
    ];
    const bytes = compressed ? Buffer.concat(rows.map(value => compress(row(value)))) : Buffer.from(rows.map(row).join(''));
    await writeFile(file, bytes);
    const summary = await runRolloutWorker('plan', { path: file, sessionId: id, messageId: message.id });
    assert.deepEqual(summary.plan, { cut: 3, time: 13, input: { index: 2, offset: 0 },
      retainedInputs: [{ index: 0, offset: 0 }, { index: 2, offset: 0 }] });
    assert.equal(summary.stats.records, 7);
    assert.equal(summary.prefixStats.records, 3);
    const result = await runRolloutWorker('prepare', { path: file, sessionId: id, identity: summary.identity,
      cut: summary.plan.cut, inputCopies: summary.plan.retainedInputs, message: replacement, output });
    assert.equal(result.retained, 3);
    const candidate = await readFile(output);
    assert.equal(result.size, candidate.length);
    const parsed = (compressed ? decompressZstdConcat(candidate) : candidate).toString('utf8').trimEnd().split('\n').map(JSON.parse);
    assert.deepEqual(parsed, [rows[0], { ...rows[1], data: { ...rows[1].data, inserted: [replacement] } }, rows[2],
      { ...rows[3], data: { ...rows[3].data, inserted: [replacement] } }]);
    assert.deepEqual(await readFile(file), bytes);
  });
}

test('a legal inbox record larger than the IPC limit returns only its location', { skip: !workerSupported }, async t => {
  const root = await fixture(t);
  const path = join(root, 'session.v4.jsonl.zstd');
  const output = join(root, 'candidate');
  const id = 'large-prompt';
  const original = text('edit-me', 'x'.repeat(9 * 1024 * 1024));
  const intake = event(0, 10, 'agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [original] });
  await writeFile(path, Buffer.concat([
    header(id), intake,
    event(1, 11, 'turn/start', { turn: 1 }),
    event(2, 12, 'agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [] }),
    event(3, 13, 'user/message', original), event(4, 14, 'turn/end', { turn: 1 }),
  ].map(value => compress(row(value)))));
  const { plan, identity } = await runRolloutWorker('plan', { path, sessionId: id, messageId: original.id });
  assert.deepEqual(plan, { cut: 1, time: 11, input: { index: 0, offset: 0 }, retainedInputs: [{ index: 0, offset: 0 }] });
  const message = text(original.id, 'smaller message');
  await runRolloutWorker('prepare', { path, sessionId: id, identity, output,
    cut: plan.cut, inputCopies: plan.retainedInputs, message });
  assert.equal(decompressZstdConcat(await readFile(output)).toString(), row(header(id))
    + row({ ...intake, data: { ...intake.data, inserted: [message] } }));
});

test('parent planning flags clock inversions on either side of the child cutoff', { skip: !workerSupported }, async t => {
  const root = await fixture(t);
  const paths = [];
  const first = text('first', 'before');
  const target = text('target', 'edit me');
  const cases = [
    { id: 'retained-later', precedingEnd: 100, start: 71, trailingTime: 75, ambiguous: true },
    { id: 'discarded-earlier', precedingEnd: 70, start: 71, trailingTime: 69, ambiguous: true },
    { id: 'same-parent-millisecond', precedingEnd: 71, start: 71, trailingTime: 75, ambiguous: false },
  ];
  for (const { id, precedingEnd, start, trailingTime } of cases) {
    const path = join(root, id, 'session.v4.jsonl');
    await mkdir(join(root, id));
    paths.push(path);
    const rows = [header(id), event(0, 50, 'turn/start', { turn: 1 }),
      event(1, 60, 'user/message', first), event(2, precedingEnd, 'turn/end', { turn: 1 }),
      event(3, 70, 'agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [target] }),
      event(4, start, 'turn/start', { turn: 2 }),
      event(5, 72, 'agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [] }),
      event(6, 73, 'user/message', target), event(7, 74, 'turn/end', { turn: 2 }),
      event(8, trailingTime, 'other', {}),
    ];
    await writeFile(path, rows.map(row).join(''));
  }
  for (const [index, { id, ambiguous }] of cases.entries()) {
    const result = await runRolloutWorker('plan', { path: paths[index], sessionId: id, messageId: target.id });
    assert.equal(result.plan.cut, 4);
    assert.equal(result.plan.timestampAmbiguous === true, ambiguous, id);
  }
});

test('batch scan excludes inherited catalog entries and treats clock-order inversion as ambiguous', { skip: !workerSupported }, async t => {
  const root = await fixture(t);
  const path = join(root, 'session.v4.jsonl');
  const children = [event(0, 2, 'subagent/catalog', { childId: 'inherited' }),
    event(1, 3, 'session/end-seed', { inherited: true }), event(2, 4, 'subagent/catalog', { childId: 'owned' }),
    event(3, 5, 'turn/start', { turn: 1 }), event(4, 7, 'turn/end', { turn: 1 }),
    event(5, 4, 'other', {}),
  ];
  await writeFile(path, [header('child', true), ...children].map(row).join(''));
  const [summary, complete] = await runRolloutWorker('scanMany', { items: [
    { path, sessionId: 'child', cutoff: 6 }, { path, sessionId: 'child', cutoff: 8 },
  ] });
  assert.equal(summary.inheritedCount, 1);
  assert.deepEqual(summary.catalog.map(item => item.data.childId), ['owned']);
  assert.equal(summary.cut, 4);
  assert.equal(summary.inTurnAtCut, true);
  assert.equal(summary.ambiguous, true);
  assert.equal(complete.crosses, false);
});

test('cutoff timestamp ties are ambiguous only for events owned by the child', { skip: !workerSupported }, async t => {
  const root = await fixture(t);
  const path = join(root, 'session.v4.jsonl');
  const childEvents = [event(0, 6, 'inherited-event', {}),
    event(1, 6, 'session/end-seed', { inherited: true }),
    event(2, 8, 'agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [text('instruction', 'continue')] }),
  ];
  await writeFile(path, [header('seeded-child', true), ...childEvents].map(row).join(''));
  const [inheritedTie, ownedTie, noTie] = await runRolloutWorker('scanMany', { items: [
    { path, sessionId: 'seeded-child', cutoff: 6 }, { path, sessionId: 'seeded-child', cutoff: 8 },
    { path, sessionId: 'seeded-child', cutoff: 9 },
  ] });
  assert.equal(inheritedTie.ambiguous, false);
  assert.equal(ownedTie.ambiguous, true);
  assert.equal(ownedTie.cut, 2);
  assert.equal(ownedTie.inTurnAtCut, false);
  assert.equal(noTie.crosses, false);
});

test('isolated scan handles an expanded log larger than its V8 heap', { skip: !workerSupported, timeout: 30_000 }, async t => {
  const root = await fixture(t);
  const path = join(root, 'session.v4.jsonl.zstd');
  const log = await open(path, 'wx');
  const payload = 'x'.repeat(512 * 1024);
  try {
    await log.writeFile(compress(row(header('large'))));
    for (let seq = 0; seq < 640; seq++) await log.writeFile(compress(row(event(seq, seq, 'other', { payload }))));
  } finally {
    await log.close();
  }
  const result = await runRolloutWorker('scan', { path, sessionId: 'large', cutoff: 3 });
  assert.equal(result.stats.records, 640);
  assert.ok(result.stats.bytes > 320 * 1024 * 1024);
  assert.equal(result.cut, 3);
  assert.equal(result.prefixStats.records, 3);
});

test('oversized records and incomplete compressed tails fail in the worker', { skip: !workerSupported }, async t => {
  const root = await fixture(t);
  const path = join(root, 'session.v4.jsonl.zstd');
  const first = compress(row(header('broken')));
  const last = compress(row(event(0, 1, 'other', {})));
  await writeFile(path, Buffer.concat([first, last.subarray(0, -1)]));
  await assert.rejects(runRolloutWorker('scan', { path, sessionId: 'broken' }), { name: 'RolloutWorkerError' });
  const oversized = compress(row(event(0, 1, 'other', { text: 'x'.repeat(16 * 1024 * 1024) })));
  await writeFile(path, Buffer.concat([first, oversized]));
  await assert.rejects(runRolloutWorker('scan', { path, sessionId: 'broken' }), /16 MiB/u);
});

test('prepare reuses unchanged checksummed Zstandard prefix frames verbatim and falls back when unchecksummed', { skip: !workerSupported }, async t => {
  const root = await fixture(t);
  const id = 'zstd-passthrough';
  const rows = [header(id)];
  let target;
  for (let turn = 1; turn <= 8; turn++) {
    const prompt = text(`msg-${turn}`, `question ${turn}`);
    if (turn === 7) target = prompt;
    const base = rows.length - 1;
    rows.push(
      event(base, turn * 10, 'agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [prompt] }),
      event(base + 1, turn * 10 + 1, 'turn/start', { turn }),
      event(base + 2, turn * 10 + 2, 'agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [] }),
      event(base + 3, turn * 10 + 3, 'user/message', prompt),
      event(base + 4, turn * 10 + 4, 'turn/end', { turn }),
    );
  }
  const sourcePath = join(root, 'session.v4.jsonl.zstd');
  const frames = [compress(row(rows[0]))];
  for (let index = 1; index < rows.length; index += 5) {
    frames.push(compress(rows.slice(index, index + 5).map(row).join('')));
  }
  const sourceBytes = Buffer.concat(frames);
  await writeFile(sourcePath, sourceBytes);

  const summary = await runRolloutWorker('plan', { path: sourcePath, sessionId: id, messageId: target.id });
  const replacement = text(target.id, 'revised question 7');
  const outParent = join(root, 'candidate-parent');
  const parentResult = await runRolloutWorker('prepare', {
    path: sourcePath, sessionId: id, identity: summary.identity, output: outParent,
    cut: summary.plan.cut, inputCopies: summary.plan.retainedInputs, message: replacement, prefix: summary.prefix,
    appendEvents: [{ type: 'model/selection', time: 999, data: { provider: 'fixture', model: 'alt' } }],
  });
  const expectedPrefix = Buffer.concat(frames.slice(0, 7)).length;
  assert.deepEqual(summary.prefix, { startSeq: 30, end: expectedPrefix });
  assert.equal(parentResult.copiedPrefixBytes, expectedPrefix);
  const parentBytes = await readFile(outParent);
  assert.equal(parentBytes.subarray(0, expectedPrefix).equals(sourceBytes.subarray(0, expectedPrefix)), true);
  const parsedParent = decompressZstdConcat(parentBytes).toString('utf8').trimEnd().split('\n').map(JSON.parse);
  assert.equal(parsedParent.length, summary.plan.cut + 2);
  assert.deepEqual(parsedParent[summary.plan.input.index + 1].data.inserted[0], replacement);
  assert.equal(parsedParent.at(-1).type, 'model/selection');

  // Truncating with no swaps on a frame boundary copies the entire retained prefix verbatim.
  const outChild = join(root, 'candidate-child');
  const crossing = await runRolloutWorker('scan', { path: sourcePath, sessionId: id, cutoff: 70 });
  assert.equal(crossing.cut, 30);
  assert.deepEqual(crossing.prefix, summary.prefix);
  const childResult = await runRolloutWorker('prepare', {
    path: sourcePath, sessionId: id, identity: crossing.identity, output: outChild,
    cut: crossing.cut, prefix: crossing.prefix,
  });
  assert.equal(childResult.copiedPrefixBytes, expectedPrefix);
  assert.equal((await readFile(outChild)).equals(sourceBytes.subarray(0, expectedPrefix)), true);

  // Unchecksummed frames fall back cleanly to full streaming re-encode.
  const rawFrames = [zstdCompressSync(Buffer.from(row(rows[0])))];
  for (let index = 1; index < rows.length; index += 5) {
    rawFrames.push(zstdCompressSync(Buffer.from(rows.slice(index, index + 5).map(row).join(''))));
  }
  const fallbackSource = join(root, 'fallback');
  await mkdir(fallbackSource);
  const fallbackFile = join(fallbackSource, 'session.v4.jsonl.zstd');
  await writeFile(fallbackFile, Buffer.concat(rawFrames));
  const fallbackPlan = await runRolloutWorker('plan', { path: fallbackFile, sessionId: id, messageId: target.id });
  const outFallback = join(root, 'candidate-fallback');
  const fallbackResult = await runRolloutWorker('prepare', {
    path: fallbackFile, sessionId: id, identity: fallbackPlan.identity, output: outFallback,
    cut: fallbackPlan.plan.cut, inputCopies: fallbackPlan.plan.retainedInputs, message: replacement,
    prefix: fallbackPlan.prefix,
  });
  assert.equal(fallbackPlan.prefix, undefined);
  assert.equal(fallbackResult.copiedPrefixBytes, 0);
  const parsedFallback = decompressZstdConcat(await readFile(outFallback)).toString('utf8').trimEnd().split('\n').map(JSON.parse);
  assert.deepEqual(parsedFallback[fallbackPlan.plan.input.index + 1].data.inserted[0], replacement);

  // A frame ending in JSON whitespace can only be reused through the prior complete boundary.
  const paddedFrames = frames.slice();
  paddedFrames[6] = compress(`${rows.slice(26, 31).map(row).join('')} `);
  const paddedDir = join(root, 'padded');
  await mkdir(paddedDir);
  const paddedFile = join(paddedDir, 'session.v4.jsonl.zstd');
  await writeFile(paddedFile, Buffer.concat(paddedFrames));
  const paddedScan = await runRolloutWorker('scan', { path: paddedFile, sessionId: id, cutoff: 70 });
  const outPadded = join(root, 'candidate-padded');
  const paddedResult = await runRolloutWorker('prepare', {
    path: paddedFile, sessionId: id, identity: paddedScan.identity, output: outPadded,
    cut: paddedScan.cut, prefix: paddedScan.prefix,
  });
  assert.equal(paddedResult.copiedPrefixBytes, Buffer.concat(frames.slice(0, 6)).length);
  assert.equal(decompressZstdConcat(await readFile(outPadded)).toString('utf8'), rows.slice(0, 31).map(row).join(''));
});

test('candidate fallback stops at the last retained row before a discarded Zstandard frame', { skip: !workerSupported }, async t => {
  const root = await fixture(t);
  const path = join(root, 'session.v4.jsonl.zstd');
  const output = join(root, 'candidate');
  const sessionId = 'early-edit';
  const rows = [header(sessionId), event(0, 2, 'model/selection', { provider: 'fixture', model: 'test' })];
  const data = Buffer.concat([...rows.map(value => compress(row(value))), Buffer.from([0x28, 0xb5])]);
  await writeFile(path, data);
  // A normal planning scan still validates all frames and rejects this torn file.
  await assert.rejects(runRolloutWorker('scan', { path, sessionId }), /会话日志不完整/u);
  const result = await runRolloutWorker('prepare', { path, sessionId, output, cut: 1 });
  assert.equal(result.copiedPrefixBytes, 0);
  assert.equal(decompressZstdConcat(await readFile(output)).toString('utf8'), rows.map(row).join(''));
});

test('scan uses fast Zstandard tail path for retained seeded subagents and falls back on crossing or anomalies', { skip: !workerSupported }, async t => {
  const root = await fixture(t);
  const makeSeeded = async (name, { headerExtra = {}, seedFrames, tailFrames }) => {
    const dir = join(root, name);
    await mkdir(dir);
    const file = join(dir, 'session.v4.jsonl.zstd');
    const hdr = { type: 'session', version: 4, id: name, isSeeded: true, origin: 'subagent', parentSession: 'parent-1', createdAt: 10, ...headerExtra };
    await writeFile(file, Buffer.concat([compress(row(hdr)), ...seedFrames, ...tailFrames]));
    return file;
  };

  // Standard DSH seeded subagent layout: frame 0 header, frame 1 inherited seed + end-seed, frame 2+ child-owned events.
  const seedRows = [
    event(0, 10, 'subagent/catalog', { childId: 'parent-sibling', childCreatedAt: 10 }),
    event(1, 11, 'turn/start', { turn: 1 }),
    event(2, 12, 'turn/end', { turn: 1 }),
    event(3, 13, 'session/end-seed', { inherited: true }),
  ];
  const seedFrame = compress(seedRows.map(row).join(''));
  const childFrame = compress([
    event(4, 20, 'subagent/descriptor', { version: 3, provider: 'fork', mode: 'continuable' }),
    event(5, 21, 'turn/start', { turn: 2 }),
    event(6, 22, 'subagent/catalog', { childId: 'grandchild-1', childCreatedAt: 22 }),
    event(7, 25, 'turn/end', { turn: 2 }),
  ].map(row).join(''));
  const basicFile = await makeSeeded('seeded-basic', { seedFrames: [seedFrame], tailFrames: [childFrame] });

  // Retained (cutoff > all tail events): takes fast path and matches full scan semantics.
  const fast = await runRolloutWorker('scan', { path: basicFile, sessionId: 'seeded-basic', cutoff: 100 });
  assert.equal(fast.fastSeededTail, true);
  assert.equal(fast.crosses, false);
  assert.equal(fast.ambiguous, false);
  assert.equal(fast.inTurn, false);
  assert.equal(fast.inheritedCount, 3);
  assert.equal(fast.cut, 8);
  assert.equal(fast.stats.records, 8);
  assert.deepEqual(fast.catalog.map(entry => entry.data.childId), ['grandchild-1']);

  // Crossing or tie cutoff: automatically falls back to full scan.
  const crossing = await runRolloutWorker('scan', { path: basicFile, sessionId: 'seeded-basic', cutoff: 23 });
  assert.equal(crossing.fastSeededTail, undefined);
  assert.equal(crossing.crosses, true);
  assert.equal(crossing.cut, 7);
  assert.equal(crossing.inTurnAtCut, true);
  const tied = await runRolloutWorker('scan', { path: basicFile, sessionId: 'seeded-basic', cutoff: 25 });
  assert.equal(tied.fastSeededTail, undefined);
  assert.equal(tied.ambiguous, true);

  // Preset-configured seeded subagent: agent-preset/selected followed by subagent/descriptor also takes fast path.
  const presetTail = compress([
    event(4, 20, 'agent-preset/selected', { agentPreset: 'reviewer' }),
    event(5, 20, 'subagent/descriptor', { version: 3, provider: 'fork', mode: 'continuable' }),
  ].map(row).join(''));
  const presetFile = await makeSeeded('seeded-preset', {
    headerExtra: { agentPreset: 'reviewer' }, seedFrames: [seedFrame], tailFrames: [presetTail],
  });
  const presetScan = await runRolloutWorker('scan', { path: presetFile, sessionId: 'seeded-preset', cutoff: 100 });
  assert.equal(presetScan.fastSeededTail, true);
  assert.equal(presetScan.inheritedCount, 3);
  assert.equal(presetScan.cut, 6);

  // Open turn in tail falls back to full scan and reports inTurn: true.
  const openTurnTail = compress([
    event(4, 20, 'subagent/descriptor', { version: 3, provider: 'fork', mode: 'continuable' }),
    event(5, 21, 'turn/start', { turn: 2 }),
  ].map(row).join(''));
  const openTurnFile = await makeSeeded('seeded-open-turn', { seedFrames: [seedFrame], tailFrames: [openTurnTail] });
  const openTurnScan = await runRolloutWorker('scan', { path: openTurnFile, sessionId: 'seeded-open-turn', cutoff: 100 });
  assert.equal(openTurnScan.fastSeededTail, undefined);
  assert.equal(openTurnScan.inTurn, true);

  // Second inherited marker in tail falls back to full scan and updates inheritedCount/catalog.
  const reseededTail = compress([
    event(4, 20, 'subagent/descriptor', { version: 3, provider: 'fork', mode: 'continuable' }),
    event(5, 21, 'subagent/catalog', { childId: 'discarded-grandchild', childCreatedAt: 21 }),
    event(6, 22, 'session/end-seed', { inherited: true }),
    event(7, 23, 'subagent/catalog', { childId: 'kept-grandchild', childCreatedAt: 23 }),
  ].map(row).join(''));
  const reseededFile = await makeSeeded('seeded-reseeded', { seedFrames: [seedFrame], tailFrames: [reseededTail] });
  const reseededScan = await runRolloutWorker('scan', { path: reseededFile, sessionId: 'seeded-reseeded', cutoff: 100 });
  assert.equal(reseededScan.fastSeededTail, undefined);
  assert.equal(reseededScan.inheritedCount, 6);
  assert.deepEqual(reseededScan.catalog.map(entry => entry.data.childId), ['kept-grandchild']);

  // Non-descriptor frame 2 (e.g. split seed across frames) falls back cleanly.
  const splitFile = await makeSeeded('seeded-split', {
    seedFrames: [compress([seedRows[0], seedRows[1]].map(row).join('')), compress([seedRows[2], seedRows[3]].map(row).join(''))],
    tailFrames: [childFrame],
  });
  const splitScan = await runRolloutWorker('scan', { path: splitFile, sessionId: 'seeded-split', cutoff: 100 });
  assert.equal(splitScan.fastSeededTail, undefined);
  assert.equal(splitScan.inheritedCount, 3);
  assert.equal(splitScan.cut, 8);
  assert.deepEqual(splitScan.catalog.map(entry => entry.data.childId), ['grandchild-1']);

  // Streaming-migrated frame 1 (lacking Frame_Content_Size, as produced by DSH v3->v4 migration)
  // must always fall back to full scan so own events merged into frame 1 are never missed.
  const streamCompress = async payload => {
    const z = createZstdCompress({ params: { [constants.ZSTD_c_checksumFlag]: 1 } });
    const chunks = [];
    z.on('data', chunk => chunks.push(chunk));
    await new Promise((done, fail) => { z.once('end', done); z.once('error', fail); z.end(payload); });
    return Buffer.concat(chunks);
  };
  const migratedFrame1 = await streamCompress([
    ...seedRows,
    event(4, 150, 'user/message', text('migrated-crossing', 'later')),
  ].map(row).join(''));
  const migratedTail = compress([
    event(5, 20, 'subagent/descriptor', { version: 3, provider: 'fork', mode: 'continuable' }),
  ].map(row).join(''));
  const migratedFile = await makeSeeded('seeded-migrated', {
    seedFrames: [migratedFrame1], tailFrames: [migratedTail],
  });
  const migratedScan = await runRolloutWorker('scan', { path: migratedFile, sessionId: 'seeded-migrated', cutoff: 100 });
  assert.equal(migratedScan.fastSeededTail, undefined);
  assert.equal(migratedScan.crosses, true);
  assert.equal(migratedScan.cut, 4);
});
