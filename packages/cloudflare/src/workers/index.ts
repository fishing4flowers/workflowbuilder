// Runtime pieces for a Worker: the Postgres store, the per-execution Durable Object
// that fans events out over SSE, and the small helpers that connect them.
//
//   // src/index.ts (the backend Worker)
//   export const ExecutionEventsObject = defineExecutionEventsObject<Env>((env) => ({
//     fetchEventsAfter: fetchEventsAfterSql(openSql(env)),
//     fetchStatus: fetchStatusSql(openSql(env)),
//   }));
//
//   const store = createPostgresStore({ sql, notify: notifyExecutionEventsObject(env.EXECUTION_EVENTS) });
//   const engine = new CloudflareWorkflowEngine({ workflow: env.WORKFLOW, recordCancellation: store.recordCancellation, waitState: store });
//
//   app.get('/executions/:id/stream', (c) => streamExecutionEvents(c.env.EXECUTION_EVENTS, c.req.param('id')));
import {
  type EventRowFetcher,
  ExecutionEventsObjectBase,
  type ExecutionEventsObjectDeps,
  type ExecutionStatusFetcher,
} from './execution-events-object';
import type { EventNotifier, SqlTag } from './postgres-store';

export { createPostgresStore } from './postgres-store';
export type { EventNotifier, PostgresExecutionStore, PostgresStoreOptions, SqlTag } from './postgres-store';

export { ExecutionEventsObjectBase, formatEvent } from './execution-events-object';
export type { EventRowFetcher, ExecutionEventsObjectDeps, ExecutionStatusFetcher } from './execution-events-object';

export { drainEventsSince, isTerminalEventType, terminalStatusFor } from './drain';
export type { DrainResult, ExecutionEventRow } from './drain';

// The namespace binding's stub, typed by the methods the store and route call.
export type ExecutionEventsStub = {
  notify(): Promise<void>;
  stream(executionId: string): Promise<Response>;
};

export type ExecutionEventsNamespace = DurableObjectNamespace<ExecutionEventsObjectBase<unknown>>;

// Explicit constructor type, as for defineExecutionWorkflow: an inferred anonymous class
// with protected members cannot be emitted to .d.ts (TS4094).
export type ExecutionEventsObjectClass<TEnv> = new (
  state: DurableObjectState,
  env: TEnv,
) => ExecutionEventsObjectBase<TEnv>;

export function defineExecutionEventsObject<TEnv>(
  deps: (env: TEnv) => ExecutionEventsObjectDeps,
): ExecutionEventsObjectClass<TEnv> {
  return class ExecutionEventsObject extends ExecutionEventsObjectBase<TEnv> {
    protected deps(): ExecutionEventsObjectDeps {
      return deps(this.env);
    }
  };
}

// One object per execution, addressed by name so the store and the route agree.
function stubFor(namespace: ExecutionEventsNamespace, executionId: string): ExecutionEventsStub {
  return namespace.get(namespace.idFromName(executionId)) as unknown as ExecutionEventsStub;
}

export function notifyExecutionEventsObject(namespace: ExecutionEventsNamespace): EventNotifier {
  return (executionId) => stubFor(namespace, executionId).notify();
}

export function streamExecutionEvents(namespace: ExecutionEventsNamespace, executionId: string): Promise<Response> {
  return stubFor(namespace, executionId).stream(executionId);
}

export function fetchEventsAfterSql(sql: SqlTag): EventRowFetcher {
  return async (executionId, afterSequence) =>
    sql<{
      execution_id: string;
      sequence: number;
      timestamp: Date | string;
      type: string;
      node_id: string | null;
      path_id: string | null;
      payload_json: unknown;
    }>`
      SELECT execution_id, sequence, timestamp, type, node_id, path_id, payload_json
      FROM execution_events
      WHERE execution_id = ${executionId} AND sequence > ${afterSequence}
      ORDER BY sequence
    `.then((rows) =>
      rows.map((row) => ({
        executionId: row.execution_id,
        sequence: row.sequence,
        timestamp: row.timestamp,
        type: row.type,
        nodeId: row.node_id,
        pathId: row.path_id,
        payloadJson: row.payload_json,
      })),
    );
}

export function fetchStatusSql(sql: SqlTag): ExecutionStatusFetcher {
  return async (executionId) => {
    const rows = await sql<{ status: string }>`SELECT status FROM executions WHERE id = ${executionId}`;
    return rows[0]?.status;
  };
}
