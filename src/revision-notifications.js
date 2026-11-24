import { randomUUID } from 'node:crypto';

const packageName = '@kabxx/dsh-message-revision';
const serviceKey = 'messageRevisionEvents';
const historySize = 64;
const historyBytesLimit = 1024 * 1024;
const subscriberLimit = 64;
const cursorPattern = /^[\da-f-]{36}:\d{1,16}$/u;

function parseCursor(value) {
  if (value === null || (typeof value === 'string' && cursorPattern.test(value))) return value;
  throw new TypeError('Invalid message revision notification cursor');
}

function parseNotice(value) {
  const ids = items => Array.isArray(items) && items.length <= 127
    && items.every(id => typeof id === 'string' && id.length > 0 && id.length <= 2048);
  if (value && typeof value.cursor === 'string' && cursorPattern.test(value.cursor)
    && (value.type === 'reset' || (value.type === 'revision' && ids(value.rewrittenChildren)
      && ids(value.withdrawnChildren) && value.rewrittenChildren.length + value.withdrawnChildren.length <= 127
      && (value.affectedParentIds === undefined || ids(value.affectedParentIds))))) return value;
  throw new TypeError('Invalid message revision notification');
}

function codec(name, parse) {
  return { mode: 'strict', typeSymbol: `${packageName}#${name}`, create: () => ({ parse }) };
}

const descriptor = {
  id: `${packageName}#${serviceKey}/watch`, service: serviceKey, namespace: serviceKey, method: 'watch',
  invocation: { kind: 'direct' }, mode: 'stream', cancellation: { parameter: 'signal' },
  parameters: [{ name: 'cursor', wire: 'cursor', source: 'json', codec: codec('Cursor', parseCursor) }],
  result: codec('Notice', parseNotice),
};

export function registerRevisionNotifications(ctx, notifications) {
  const service = {
    ctx,
    [Symbol.for('cordis.tracker')]: { property: 'ctx' },
    watch(cursor, signal) {
      const peer = this.ctx.invocation?.peer;
      if (peer?.principal.kind !== 'owner' || peer.signal.aborted) throw new Error('Message revision notifications require an owner connection');
      return notifications.watch(cursor, signal);
    },
  };
  service.typertRemote = { service, serviceKey, namespace: serviceKey };
  ctx.provide(serviceKey, service);
  ctx.typert.register({
    package: packageName, face: 'host', schemas: [],
    model: { services: [], events: [], objects: [] }, invocations: [descriptor],
  });
}

export function createRevisionNotifications() {
  const epoch = randomUUID();
  const history = [];
  const waiters = new Set();
  let sequence = 0;
  let retainedBytes = 0;
  let subscribers = 0;
  let disposed = false;

  function publish({ rewrittenChildren = [], withdrawnChildren = [], affectedParentIds = [] }) {
    if (disposed || rewrittenChildren.length + withdrawnChildren.length + affectedParentIds.length === 0) return;
    const notice = {
      type: 'revision', cursor: `${epoch}:${++sequence}`, rewrittenChildren, withdrawnChildren,
      ...(affectedParentIds.length > 0 && { affectedParentIds }),
    };
    const bytes = Buffer.byteLength(JSON.stringify(notice));
    history.push({ notice, bytes });
    retainedBytes += bytes;
    while (history.length > historySize || retainedBytes > historyBytesLimit) {
      retainedBytes -= history.shift().bytes;
    }
    for (const wake of waiters) wake();
  }

  function cursorFor(id) {
    if (!id?.startsWith(`${epoch}:`)) return undefined;
    const cursor = Number(id.slice(epoch.length + 1));
    return Number.isSafeInteger(cursor) && cursor >= 0 && cursor <= sequence && id === `${epoch}:${cursor}`
      ? cursor : undefined;
  }

  async function* watch(lastCursor, signal) {
    if (disposed || signal.aborted) return;
    if (subscribers >= subscriberLimit) throw new Error('Message revision notification subscriber limit reached');
    subscribers++;
    let cursor = cursorFor(lastCursor);
    try {
      while (!disposed && !signal.aborted) {
        const oldestCursor = sequence - history.length;
        if (cursor === undefined || cursor < oldestCursor) {
          cursor = sequence;
          yield { type: 'reset', cursor: `${epoch}:${cursor}` };
        } else if (cursor < sequence) {
          yield history[cursor++ - oldestCursor].notice;
        } else {
          const { promise, resolve } = Promise.withResolvers();
          waiters.add(resolve);
          signal.addEventListener('abort', resolve, { once: true });
          try { await promise; } finally {
            waiters.delete(resolve);
            signal.removeEventListener('abort', resolve);
          }
        }
      }
    } finally {
      subscribers--;
    }
  }

  return {
    watch, publish,
    close() {
      disposed = true;
      for (const wake of waiters) wake();
      history.length = 0;
      retainedBytes = 0;
    },
  };
}
