import assert from 'node:assert/strict';
import test from 'node:test';
import { childFixture, turn } from './child-fixture.js';
import { rewriteStoredSession } from '../src/revision.js';

const text = value => ({ type: 'text', text: value });
const image = name => ({ type: 'image', attachment: { name } });
const file = name => ({ type: 'file', attachment: { name } });

async function fixture(t) {
  const content = [text('before '), image('one.png'), text('after'), image('two.png')];
  const events = turn(1, 100);
  events[0][2].inserted[0].content = content;
  const world = await childFixture(t, { parent: { createdAt: 1, events } });
  const input = { sessionId: 'parent', messageId: 'question-1' };
  return { ...world, content, input };
}

async function pendingContent(world) {
  const log = (await world.bytes('parent')).toString().trimEnd().split('\n').map(JSON.parse);
  return log[1].data.inserted[0].content;
}

test('adding a file keeps every original text and image block in place', async t => {
  const world = await fixture(t);
  const added = file('new.txt');
  await rewriteStoredSession(world.persistence, { ...world.input, text: 'before after' }, { addedAttachments: [added] });
  assert.deepEqual(await pendingContent(world), [...world.content, added]);
});

test('explicit segment edits and attachment removal preserve the other blocks', async t => {
  const world = await fixture(t);
  const added = file('last.txt');
  await rewriteStoredSession(world.persistence, {
    ...world.input, text: 'before updated', textParts: ['before ', 'updated'], removedAttachments: [3],
  }, { addedAttachments: [added] });
  assert.deepEqual(await pendingContent(world), [world.content[0], world.content[1], text('updated'), added]);
});

test('ambiguous edits to interleaved text leave the original rollout intact', async t => {
  const world = await fixture(t);
  const original = await world.bytes('parent');
  for (const payload of [{ text: 'edited' }, { text: 'edited', textParts: ['edited'] }]) {
    await assert.rejects(rewriteStoredSession(world.persistence, { ...world.input, ...payload }), { name: 'RevisionError' });
    assert.deepEqual(await world.bytes('parent'), original);
  }
});
