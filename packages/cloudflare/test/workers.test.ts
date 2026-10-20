import { describe, expect, it, vi } from 'vitest';

import type { ExecutionEventRow } from '../src/workers/drain';
import { ExecutionEventsObjectBase, type ExecutionEventsObjectDeps } from '../src/workers/execution-events-object';
import { createPostgresStore, type SqlTag } from '../src/workers/postgres-store';
import { defineExecutionEventsObject, fetchEventsAfterSql } from '../src/workers';

// ---- a fake SQL tag that records statements and answers from a queue -------------

function fakeSql(answers: unknown[][] = []) {
  const calls: { text: string; values: unknown[] }[] = [];
  const sql = ((strings: TemplateStringsArray, ...values: unknown[]) => {
    calls.push({ text: strings.join('?').replaceAll(/\s+/g, ' ').trim(), values });
    return Promise.resolve(answers.shift() ?? []);
  }) as SqlTag;
  return { sql, calls };
}

describe('createPostgresStore', () => {
  it('inserts an event idempotently and nudges the notifier', async () => {
    const { sql, calls } = fakeSql();
    const notify = vi.fn(async () => {});
    const store = createPostgresStore({ sql, notify });
    await store.emitExecutionEvent('exec-1', 3, 'node_completed', { output: 1 }, 'n1');
    expect(calls[0]?.text).toContain('ON CONFLICT (execution_id, sequence) DO NOTHING');
    expect(calls[0]?.values).toContain(3);
    expect(notify).toHaveBeenCalledWith('exec-1');
  });

  it('never lets a notifier failure fail the write', async () => {
    const { sql } = fakeSql();
    const store = createPostgresStore({ sql, notify: async () => { throw new Error('DO unavailable'); } });
    await expect(store.emitExecutionEvent('exec-1', 1, 'execution_started')).resolves.toBeUndefined();
  });

  it('guards terminal statuses from being replaced', async () => {
    const { sql, calls } = fakeSql();
    const store = createPostgresStore({ sql });
    await store.updateExecutionStatus('exec-1', 'running');
    const guard = calls[0]?.values.find((v) => Array.isArray(v)) as string[];
    expect(guard).toEqual(expect.arrayContaining(['completed', 'incomplete', 'failed', 'cancelled', 'cancelling']));
    await store.updateExecutionStatus('exec-1', 'completed', undefined, { value: 'ok', resolvedBy: 'pat' });
    const guard2 = calls[1]?.values.find((v) => Array.isArray(v)) as string[];
    expect(guard2).not.toContain('cancelling');
    expect(calls[1]?.values).toContain('ok');
  });

  it('records a cancellation as MAX+1 event then status', async () => {
    const { sql, calls } = fakeSql();
    const notify = vi.fn(async () => {});
    const store = createPostgresStore({ sql, notify });
    await store.recordCancellation('exec-1');
    expect(calls[0]?.text).toContain('COALESCE(MAX(sequence), 0) + 1');
    expect(calls[0]?.values).toContain('execution_cancelled');
    expect(calls[1]?.text).toContain("status = 'cancelled'");
    expect(notify).toHaveBeenCalledOnce();
  });

  it('derives the wait state from the node events', async () => {
    const waiting = createPostgresStore({ sql: fakeSql([[{ type: 'node_waiting' }]]).sql });
    expect(await waiting.getNodeWaitState('e', 'n')).toBe('waiting');
    const resolved = createPostgresStore({ sql: fakeSql([[{ type: 'node_completed' }], [{ n: 1 }]]).sql });
    expect(await resolved.getNodeWaitState('e', 'n')).toBe('resolved');
    const never = createPostgresStore({ sql: fakeSql([[{ type: 'node_completed' }], [{ n: 0 }]]).sql });
    expect(await never.getNodeWaitState('e', 'n')).toBe('not_waiting');
    const unknown = createPostgresStore({ sql: fakeSql([[]]).sql });
    expect(await unknown.getNodeWaitState('e', 'n')).toBe('not_waiting');
  });
});

describe('fetchEventsAfterSql', () => {
  it('maps snake_case rows to the drain shape', async () => {
    const { sql, calls } = fakeSql([
      [{ execution_id: 'e', sequence: 2, timestamp: new Date(0), type: 'node_started', node_id: 'n', path_id: null, payload_json: {} }],
    ]);
    const rows = await fetchEventsAfterSql(sql)('e', 1);
    expect(rows[0]).toMatchObject({ executionId: 'e', sequence: 2, nodeId: 'n' });
    expect(calls[0]?.values).toEqual(['e', 1]);
  });
});

// ---- the Durable Object, driven through an in-memory event table -----------------

function row(sequence: number, type: string, nodeId: string | null = null): ExecutionEventRow {
  return { executionId: 'exec-1', sequence, timestamp: new Date(0), type, nodeId, pathId: null, payloadJson: null };
}

function makeObject(table: ExecutionEventRow[], status = 'running') {
  const deps: ExecutionEventsObjectDeps = {
    fetchEventsAfter: async (_id, after) => table.filter((r) => Number(r.sequence) > after),
    fetchStatus: async () => status,
    heartbeatMs: 20,
  };
  const Cls = defineExecutionEventsObject<unknown>(() => deps);
  return new Cls({} as unknown as DurableObjectState, {}) as ExecutionEventsObjectBase<unknown>;
}

async function readFrames(response: Response, count: number, timeoutMs = 1000): Promise<unknown[]> {
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  const frames: unknown[] = [];
  let buffer = '';
  const deadline = Date.now() + timeoutMs;
  while (frames.length < count && Date.now() < deadline) {
    const { value, done } = await Promise.race([
      reader.read(),
      new Promise<{ value: undefined; done: true }>((resolve) => setTimeout(() => resolve({ value: undefined, done: true }), 50)),
    ]);
    if (value) buffer += decoder.decode(value);
    let index: number;
    while ((index = buffer.indexOf('\n\n')) !== -1) {
      const frame = buffer.slice(0, index);
      buffer = buffer.slice(index + 2);
      if (frame.startsWith('event: heartbeat')) continue;
      frames.push(JSON.parse(frame.replace(/^data: /, '')));
    }
    if (done && value === undefined && frames.length >= count) break;
  }
  reader.releaseLock();
  return frames;
}

describe('ExecutionEventsObject', () => {
  it('sends a snapshot, then live events on notify, and closes on the terminal event', async () => {
    const table = [row(1, 'execution_started'), row(2, 'node_started', 'n1')];
    const object = makeObject(table);
    const response = await object.stream('exec-1');
    expect(response.headers.get('content-type')).toBe('text/event-stream');

    const [snapshot] = await readFrames(response, 1);
    expect(snapshot).toMatchObject({ type: 'execution_snapshot', status: 'running', lastSequence: 2 });

    table.push(row(3, 'node_completed', 'n1'), row(4, 'execution_completed'));
    await object.notify();
    const live = await readFrames(response, 2);
    expect(live.map((f) => (f as { type: string }).type)).toEqual(['node_completed', 'execution_completed']);

    // The stream ends once the terminal event has been written.
    const reader = response.body!.getReader();
    const { done } = await reader.read();
    expect(done).toBe(true);
  });

  it('derives a terminal status from the last event and closes without subscribing', async () => {
    const object = makeObject([row(1, 'execution_started'), row(2, 'execution_failed')], 'running');
    const response = await object.stream('exec-1');
    const [snapshot] = await readFrames(response, 1);
    expect(snapshot).toMatchObject({ status: 'failed', lastSequence: 2 });
    const { done } = await response.body!.getReader().read();
    expect(done).toBe(true);
  });

  it('catches up on rows written between snapshot and subscribe, and polls on heartbeat', async () => {
    const table = [row(1, 'execution_started')];
    const object = makeObject(table);
    const response = await object.stream('exec-1');
    const [snapshot] = await readFrames(response, 1);
    expect(snapshot).toMatchObject({ lastSequence: 1 });
    // No notify at all: the heartbeat's poll must still deliver it.
    table.push(row(2, 'execution_completed'));
    const [terminal] = await readFrames(response, 1);
    expect(terminal).toMatchObject({ type: 'execution_completed', sequence: 2 });
  });
});
