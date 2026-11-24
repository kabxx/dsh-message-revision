import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { setImmediate } from 'node:timers/promises';
import test from 'node:test';
import { runInNewContext } from 'node:vm';

const source = await readFile(new URL('../src/client.js', import.meta.url), 'utf8');
const helpers = source.slice(source.indexOf('    function revisionDraftKey('), source.indexOf('    function openDraft('));
const { revisionDraftKey, snapshotRevisionDraft, canRestoreRevisionDraft, seedRevisionDraft } = runInNewContext(
  `${helpers}\n({ revisionDraftKey, snapshotRevisionDraft, canRestoreRevisionDraft, seedRevisionDraft });`,
);

const original = {
  text: 'original', attachments: [
    { index: 1, block: { type: 'file', attachment: { digest: 'first' } } },
    { index: 2, block: { type: 'file', attachment: { digest: 'second' } } },
  ],
};
const nativeAttachmentSlots = new Map([['old:1', { index: 1 }], ['old:2', { index: 2 }]]);

function snapshot(file) {
  const edit = {
    original: original.text,
    originalAttachments: JSON.stringify(original.attachments),
    settings: { model: { provider: 'fixture', model: 'profile' }, permissionPreset: 'default' },
    composer: {
      retained: nativeAttachmentSlots,
      conversation: {
        resolveDraftAttachments(ids) {
          assert.deepEqual([...ids], ['prior-agent-upload']);
          return file ? [{ id: 'prior-agent-upload', file }] : [];
        },
      },
    },
  };
  return snapshotRevisionDraft(edit, 'changed prompt', ['old:2', 'prior-agent-upload']);
}

test('a failed revision restores text, controls, removals and original File bytes with new native upload IDs', () => {
  const file = new File(['preserved content'], 'upload.txt');
  const saved = snapshot(file);
  const fresh = new Set();
  const uploads = [];
  const input = {
    setDraft(text) { this.text = text; },
    addAttachments(ids) { this.attachments = [...ids]; return true; },
  };
  assert.equal(canRestoreRevisionDraft(saved, { ...original, revision: 'after-rollback' }), true);
  assert.equal(saved.settings.model.model, 'profile');
  assert.equal(saved.settings.permissionPreset, 'default');
  seedRevisionDraft(input, nativeAttachmentSlots, fresh, {
    createDrafts(sessionId, files, uploadImages) {
      uploads.push([sessionId, files[0], uploadImages]);
      return [{ id: 'new-agent-upload' }];
    },
  }, 'original-session', 'ignored original text', saved);
  assert.equal(input.text, 'changed prompt');
  assert.deepEqual(input.attachments, ['old:2', 'new-agent-upload']);
  assert.deepEqual([...fresh], ['new-agent-upload']);
  assert.deepEqual(uploads, [['original-session', file, true]]);
});

test('message-address keys and the original prompt fingerprint isolate draft recovery', () => {
  const saved = snapshot(new File(['x'], 'x.txt'));
  assert.notEqual(revisionDraftKey('one', 'two:three'), revisionDraftKey('one:two', 'three'));
  assert.notEqual(revisionDraftKey('other-session', 'same'), revisionDraftKey('this-session', 'same'));
  assert.equal(canRestoreRevisionDraft(saved, { ...original, text: 'other tab edited it' }), false);
  assert.equal(canRestoreRevisionDraft(saved, { ...original, attachments: original.attachments.toReversed() }), false);
  assert.throws(() => snapshot(), /fileUnavailable/);
});

test('sending only a later text segment locks cancellation and duplicate submission until the response', async () => {
  const start = source.indexOf('          function cancel() {');
  const end = source.indexOf('          return h(Fragment, null,', start);
  const handlers = source.slice(start, end);
  const reply = Promise.withResolvers();
  const claim = Symbol();
  const pending = { current: false };
  const requests = [];
  const states = [];
  const dismissals = [];
  const edit = {
    original: 'old firstold second', originalTextParts: ['old first', 'old second'],
    originalSettings: {}, settings: {}, additionalText: ['new second'],
    composer: { retained: new Map() }, messageId: 'message', revision: 'before',
  };
  const { cancel, submit } = runInNewContext(`let submitting = false;\n${handlers}\n({ cancel, submit });`, {
    activeEditor: claim, editableRef: { current: true }, pending, draft: { current: edit.composer }, sessionId: 'parent',
    emptyUploads: {}, lifetime: { signal: { aborted: false } }, window: { location: { href: '/' } },
    errorMessage: error => error.message, t: key => key, notify() {},
    snapshotRevisionDraft,
    setRequestPending: state => states.push(state),
    dismiss: () => dismissals.push('closed'),
    sessionViews: { holdSessionView: () => ({ release() {} }) },
    requestRevision(payload) { requests.push(payload); return reply.promise; },
  });
  const request = submit(edit, '', [], claim);
  await setImmediate();
  assert.deepEqual(states, [true]);
  assert.equal(requests.length, 1);
  assert.deepEqual([...requests[0].textParts], ['', 'new second']);
  cancel();
  assert.deepEqual(dismissals, []);
  assert.equal((await submit(edit, '', [], claim)).kind, 'error');
  assert.equal(requests.length, 1);
  reply.reject(new Error('fixture rejected revision'));
  assert.equal((await request).kind, 'error');
  assert.deepEqual(states, [true, false]);
  assert.deepEqual(edit.additionalText, ['new second']);
  assert.deepEqual(dismissals, []);
  cancel();
  assert.deepEqual(dismissals, ['closed']);
});

test('a dropped submission response preserves the draft without retrying automatically', async () => {
  const start = source.indexOf('          function cancel() {');
  const end = source.indexOf('          return h(Fragment, null,', start);
  const claim = Symbol();
  const edit = {
    key: 'draft', original: 'old', originalTextParts: ['old'], originalSettings: {},
    settings: {}, additionalText: [], composer: { retained: new Map() }, messageId: 'message', revision: 'before',
  };
  const notices = [];
  const kept = [];
  let released = false;
  const editableRef = { current: true };
  const { submit } = runInNewContext(`let submitting = false;\n${source.slice(start, end)}\n({ submit });`, {
    activeEditor: claim, editableRef, pending: { current: false }, draft: { current: edit.composer }, sessionId: 'parent',
    emptyUploads: {}, lifetime: { signal: { aborted: false } }, window: { location: { href: '/' } },
    errorMessage: error => error.message, t: key => key, notify: message => notices.push(message),
    snapshotRevisionDraft, setRequestPending() {}, dismiss() {}, keepDraft: (key, value) => kept.push({ key, value }),
    sessionViews: { holdSessionView: () => ({ removed: false, release() { released = true; } }) },
    async requestRevision() { throw Object.assign(new Error('deliveryUnknown'), { deliveryUnknown: true }); },
  });
  assert.equal((await submit(edit, 'edited', [], claim)).kind, 'error');
  assert.equal(released, true);
  assert.deepEqual(notices, ['deliveryUnknown draftSaved']);
  assert.equal(kept[0].key, 'draft');
  assert.equal(kept[0].value.text, 'edited');
  assert.equal(kept[0].value.reopenInView, undefined);

  editableRef.current = false;
  assert.equal((await submit(edit, 'edited again', [], claim)).kind, 'error');
  assert.equal(kept.length, 1);
});

test('the draft model label drops previous effort and uses the selected model catalog', () => {
  const start = source.indexOf('    function modelDirectoryWithSelection(');
  const end = source.indexOf('    function NativeEditor(', start);
  const { modelDirectoryWithSelection } = runInNewContext(`${source.slice(start, end)}\n({ modelDirectoryWithSelection });`);
  const original = {
    current: { provider: 'provider', model: 'old', reasoningEffort: 'high' }, retainedEffort: 'High', routable: true,
    groups: [{ id: 'provider', models: [{ id: 'old' }, { id: 'no-reasoning' }, {
      id: 'reasoning', reasoning: { defaultEffort: 'low', efforts: [{ id: 'low', name: 'Low' }, { id: 'medium', name: 'Medium' }] },
    }] }],
  };
  const directory = { getSnapshot: () => original };
  assert.equal(modelDirectoryWithSelection(directory, { provider: 'provider', model: 'no-reasoning' }).getSnapshot().retainedEffort, undefined);
  assert.equal(modelDirectoryWithSelection(directory, { provider: 'provider', model: 'reasoning' }).getSnapshot().retainedEffort, 'Low');
  assert.equal(modelDirectoryWithSelection(directory, { provider: 'provider', model: 'reasoning', reasoningEffort: 'medium' }).getSnapshot().retainedEffort, 'Medium');
  assert.equal(directory.getSnapshot().retainedEffort, 'High');
});
