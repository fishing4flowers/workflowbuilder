// The Workers replacement for the reference backend's LISTEN/NOTIFY fan-out
// (apps/backend/src/events/execution-event-bus.ts): one Durable Object per execution
// holds that run's open SSE streams. The store calls `notify()` after each write; the
// object drains `execution_events` past each stream's cursor and writes the rows out.
//
// Same guarantees as the Node route, by the same mechanism: rows are read in
// ascending `sequence` with a forward-only cursor (the workflow serializes its writes
// so no row ever commits below the cursor), the snapshot derives the run's status from
// its last event rather than the row, a catch-up drain covers the gap between
// snapshot and subscribe, and a heartbeat keeps proxies from closing idle streams.
//
// The object does not keep events itself: Postgres stays the record and the object is
// a cache of connections. That keeps a reconnecting client, a second object instance
// after eviction, and the catch-up path all reading one source.
import { DurableObject } from 'cloudflare:workers';

import { type DrainResult, drainEventsSince, isTerminalEventType, terminalStatusFor } from './drain';
import type { ExecutionEventRow } from './drain';

export type EventRowFetcher = (executionId: string, afterSequence: number) => Promise<ExecutionEventRow[]>;
export type ExecutionStatusFetcher = (executionId: string) => Promise<string | undefined>;

export type ExecutionEventsObjectDeps = {
  fetchEventsAfter: EventRowFetcher;
  fetchStatus: ExecutionStatusFetcher;
  heartbeatMs?: number;
};

const TERMINAL_STATUSES = new Set(['completed', 'incomplete', 'failed', 'cancelled']);
const DEFAULT_HEARTBEAT_MS = 15_000;

type Stream = {
  executionId: string;
  write: (chunk: string) => Promise<void>;
  close: () => void;
  cursor: number;
  draining: Promise<void>;
  pending: boolean;
  done: boolean;
};

export abstract class ExecutionEventsObjectBase<TEnv> extends DurableObject<TEnv> {
  private readonly streams = new Set<Stream>();
  private heartbeat: ReturnType<typeof setInterval> | undefined;

  // Built per instance from `this.env`, the same way the Workflow's deps are.
  protected abstract deps(): ExecutionEventsObjectDeps;

  // Called by the store after every event write (and the terminal status write) via
  // the stub. The object is addressed by execution id already, so there is nothing to
  // pass; each open stream is nudged to drain past its own cursor.
  async notify(): Promise<void> {
    for (const stream of this.streams) this.schedule(stream);
  }

  // Opens an SSE stream for `executionId` and returns the Response. The caller (a Hono
  // route in the backend Worker) has already authorised the request and checked the
  // execution exists; this only streams.
  async stream(executionId: string): Promise<Response> {
    const { fetchEventsAfter, fetchStatus, heartbeatMs = DEFAULT_HEARTBEAT_MS } = this.deps();

    const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
    const writer = writable.getWriter();
    const encoder = new TextEncoder();

    const stream: Stream = {
      executionId,
      write: (chunk) => writer.write(encoder.encode(chunk)),
      close: () => {
        writer.close().catch(() => {});
      },
      cursor: 0,
      draining: Promise.resolve(),
      pending: false,
      done: false,
    };

    const response = new Response(readable, {
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
        'X-Accel-Buffering': 'no',
      },
    });

    // Snapshot, then live. Not awaited: the Response must return for bytes to flow.
    void (async () => {
      const existing = await fetchEventsAfter(executionId, 0);
      const last = existing.at(-1);
      stream.cursor = last ? Number(last.sequence) : 0;
      const status = isTerminalEventType(last?.type)
        ? terminalStatusFor(last!.type)
        : ((await fetchStatus(executionId)) ?? 'pending');

      try {
        await stream.write(
          sse({
            type: 'execution_snapshot',
            executionId,
            status,
            lastSequence: stream.cursor,
            events: existing.map(formatEvent),
          }),
        );
      } catch {
        return;
      }

      if (TERMINAL_STATUSES.has(status)) {
        stream.close();
        return;
      }

      this.streams.add(stream);
      this.ensureHeartbeat(heartbeatMs);
      // Catch-up: a write between the snapshot read and this add nudged nobody.
      this.schedule(stream);
    })();

    return response;
  }

  // Coalesces: one drain in flight per stream, at most one queued behind it.
  private schedule(stream: Stream): void {
    if (stream.done || stream.pending) return;
    stream.pending = true;
    const { fetchEventsAfter } = this.deps();
    stream.draining = stream.draining.then(async () => {
      stream.pending = false;
      if (stream.done) return;
      const result: DrainResult = await drainEventsSince(stream.executionId, stream.cursor, fetchEventsAfter, (row) =>
        stream.write(sse(formatEvent(row))),
      );
      stream.cursor = result.lastSequence;
      if (result.reachedTerminal || result.writeFailed) this.finish(stream);
    });
  }

  private finish(stream: Stream): void {
    stream.done = true;
    this.streams.delete(stream);
    stream.close();
    if (this.streams.size === 0 && this.heartbeat) {
      clearInterval(this.heartbeat);
      this.heartbeat = undefined;
    }
  }

  private ensureHeartbeat(ms: number): void {
    if (this.heartbeat) return;
    this.heartbeat = setInterval(() => {
      for (const stream of this.streams) {
        stream.write('event: heartbeat\ndata: \n\n').catch(() => this.finish(stream));
        // A heartbeat doubles as a poll, so a lost nudge delays an event by one
        // interval instead of losing it.
        this.schedule(stream);
      }
    }, ms);
  }
}

function sse(data: unknown): string {
  return `data: ${JSON.stringify(data)}\n\n`;
}

export function formatEvent(event: ExecutionEventRow) {
  return {
    executionId: event.executionId,
    sequence: event.sequence,
    timestamp: event.timestamp instanceof Date ? event.timestamp.toISOString() : String(event.timestamp),
    type: event.type,
    nodeId: event.nodeId,
    pathId: event.pathId,
    payload: event.payloadJson,
  };
}
