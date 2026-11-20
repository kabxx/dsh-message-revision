import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
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
