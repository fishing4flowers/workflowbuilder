// ExecutionStore over Postgres, for a Worker with a Hyperdrive binding. The SQL is the
// reference worker's (apps/execution-worker/src/database.ts) with one change: where
// that store ran `pg_notify('execution_events', id)` for the backend's LISTEN loop,
// this one nudges the execution's Durable Object, which holds the SSE connections.
//
// `sql` is any tagged-template client (`postgres`, `@neondatabase/serverless`
// backed by Hyperdrive's connection string, …). Taken as a function so the package
// depends on neither; the consumer opens the connection from `env.DB.connectionString`.
import type { ExecutionEventType, ExecutionStatus } from '../core-contract';
import type { ExecutionStore, NodeWaitState, WaitStateStore } from '../store';

// Only the tagged-template form is used (array parameters go through `= ANY($1::text[])`
// rather than a client-specific list helper), so postgres.js and neon both fit.
export type SqlTag = <Row = Record<string, unknown>>(strings: TemplateStringsArray, ...values: unknown[]) => PromiseLike<Row[]>;

// Where "new events" notifications go. On Workers this is the ExecutionEventsObject
// stub; in tests, anything. Failures are swallowed: a notification is a hint to drain
// sooner, never the record (the SSE drain reads the table, and heartbeats re-poll).
export type EventNotifier = (executionId: string) => Promise<void>;

const TERMINAL_STATUSES: readonly ExecutionStatus[] = ['completed', 'incomplete', 'failed', 'cancelled'];

function statusesNotReplacedBy(status: ExecutionStatus): readonly ExecutionStatus[] {
  return TERMINAL_STATUSES.includes(status) ? TERMINAL_STATUSES : [...TERMINAL_STATUSES, 'cancelling'];
}

export type PostgresStoreOptions = {
  sql: SqlTag;
  notify?: EventNotifier;
};

export type PostgresExecutionStore = ExecutionStore &
  WaitStateStore & {
    // What CloudflareWorkflowEngine's `recordCancellation` needs: the terminal event under
    // the next free sequence, then the status. Nothing else writes to a terminated run,
    // so MAX+1 is safe here where it was not for live writes.
    recordCancellation(executionId: string): Promise<void>;
  };

export function createPostgresStore({ sql, notify }: PostgresStoreOptions): PostgresExecutionStore {
  async function ping(executionId: string): Promise<void> {
    if (!notify) return;
    try {
      await notify(executionId);
    } catch {
      // See EventNotifier.
    }
  }

  return {
    async emitExecutionEvent(executionId, sequence, type, payload, nodeId) {
      await sql`
        INSERT INTO execution_events (id, execution_id, sequence, timestamp, type, node_id, path_id, payload_json, tenant_id, created_at)
        VALUES (
          gen_random_uuid(),
          ${executionId},
          ${sequence},
          now(),
          ${type},
          ${nodeId ?? null},
          ${null},
          ${payload ? JSON.stringify(payload) : null}::jsonb,
          (SELECT tenant_id FROM executions WHERE id = ${executionId}),
          now()
        )
        ON CONFLICT (execution_id, sequence) DO NOTHING
      `;
      await ping(executionId);
    },

    async updateExecutionStatus(executionId, status, errorMessage, outcome) {
      const isTerminal = TERMINAL_STATUSES.includes(status);
      await sql`
        UPDATE executions SET
          status = ${status},
          started_at = CASE WHEN ${status} = 'running' AND started_at IS NULL THEN now() ELSE started_at END,
          finished_at = CASE WHEN ${isTerminal} THEN now() ELSE finished_at END,
          error_message = ${errorMessage ?? null},
          outcome = COALESCE(${outcome?.value ?? null}, outcome),
          resolved_by = COALESCE(${outcome?.resolvedBy ?? null}, resolved_by),
          updated_at = now()
        WHERE id = ${executionId}
          AND NOT (status = ANY(${[...statusesNotReplacedBy(status)]}::text[]))
      `;
      if (isTerminal) await ping(executionId);
    },

    async recordCancellation(executionId) {
      await sql`
        INSERT INTO execution_events (id, execution_id, sequence, timestamp, type, node_id, path_id, payload_json, tenant_id, created_at)
        SELECT gen_random_uuid(), ${executionId}, COALESCE(MAX(sequence), 0) + 1, now(),
               ${'execution_cancelled' satisfies ExecutionEventType}, NULL, NULL,
               ${JSON.stringify({ reason: 'user_request' })}::jsonb,
               (SELECT tenant_id FROM executions WHERE id = ${executionId}), now()
        FROM execution_events WHERE execution_id = ${executionId}
      `;
      await sql`
        UPDATE executions SET status = 'cancelled', finished_at = now(), updated_at = now()
        WHERE id = ${executionId} AND NOT (status = ANY(${[...TERMINAL_STATUSES]}::text[]))
      `;
      await ping(executionId);
    },

    // A node is waiting when its latest wait has no completion after it. Reads the same
    // rows the run view does, so what the engine refuses matches what the user sees.
    async getNodeWaitState(executionId, nodeId): Promise<NodeWaitState> {
      const rows = await sql<{ type: string }>`
        SELECT type FROM execution_events
        WHERE execution_id = ${executionId} AND node_id = ${nodeId}
          AND type IN ('node_waiting', 'node_completed', 'node_failed')
        ORDER BY sequence DESC LIMIT 1
      `;
      const latest = rows[0]?.type;
      if (latest === 'node_waiting') return 'waiting';
      if (latest === undefined) return 'not_waiting';
      // A completion or failure after a wait: the verdict (or a timeout) already landed.
      const waited = await sql<{ n: number }>`
        SELECT count(*)::int AS n FROM execution_events
        WHERE execution_id = ${executionId} AND node_id = ${nodeId} AND type = 'node_waiting'
      `;
      return (waited[0]?.n ?? 0) > 0 ? 'resolved' : 'not_waiting';
    },
  };
}
