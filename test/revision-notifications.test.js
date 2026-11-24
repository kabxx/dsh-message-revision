import assert from 'node:assert/strict';
import test from 'node:test';
import { createRevisionNotifications } from '../src/revision-notifications.js';

async function readEvent(reader) {
  const next = await reader.next();
  assert.equal(next.done, false);
  return next.value;
}

function subscribe(feed, cursor, signal = new AbortController().signal) {
  return feed.watch(cursor ?? null, signal);
}

test('independent subscribers receive both kinds of committed child changes', { timeout: 2000 }, async t => {
  const feed = createRevisionNotifications();
  t.after(() => feed.close());
  const a = await subscribe(feed);
  const b = await subscribe(feed);
  for (const listener of [a, b]) assert.equal((await readEvent(listener)).type, 'reset');
  await a.return();
  feed.publish({ rewrittenChildren: ['shortened'], withdrawnChildren: ['removed'], affectedParentIds: ['parent'] });
  const change = await readEvent(b);
  assert.equal(change.type, 'revision');
  assert.deepEqual(((({ type, cursor, ...changes }) => changes)(change)), {
    rewrittenChildren: ['shortened'], withdrawnChildren: ['removed'], affectedParentIds: ['parent'],
  });
});

test('reconnect replays changes after its cursor and resets after lost or foreign cursors', { timeout: 2000 }, async t => {
  const feed = createRevisionNotifications();
  const replacement = createRevisionNotifications();
  t.after(() => { feed.close(); replacement.close(); });
  const first = await subscribe(feed);
  const cursor = (await readEvent(first)).cursor;
  await first.return();
  feed.publish({ rewrittenChildren: ['first'] });
  feed.publish({ withdrawnChildren: ['second'] });
  const returning = await subscribe(feed, cursor);
  assert.deepEqual((await readEvent(returning)).rewrittenChildren, ['first']);
  assert.deepEqual((await readEvent(returning)).withdrawnChildren, ['second']);
  await returning.return();
  for (let index = 0; index < 65; index++) feed.publish({ withdrawnChildren: [String(index)] });
  const expired = await subscribe(feed, cursor);
  assert.equal((await readEvent(expired)).type, 'reset');
  const newHost = await subscribe(replacement, cursor);
  const reset = await readEvent(newHost);
  assert.equal(reset.type, 'reset');
  assert.notEqual(reset.cursor, cursor);
});

test('request abort and plugin disposal release streams and subscriber capacity', { timeout: 2000 }, async t => {
  const feed = createRevisionNotifications();
  t.after(() => feed.close());
  const abort = new AbortController();
  const readers = await Promise.all(Array.from({ length: 64 }, () => subscribe(feed, undefined, abort.signal)));
  await Promise.all(readers.map(reader => readEvent(reader)));
  await assert.rejects(subscribe(feed).next(), /subscriber limit/);
  abort.abort();
  assert.deepEqual(await readers[0].next(), { value: undefined, done: true });
  const remaining = await subscribe(feed);
  assert.equal((await readEvent(remaining)).type, 'reset');
  feed.close();
  assert.deepEqual(await remaining.next(), { value: undefined, done: true });
});
