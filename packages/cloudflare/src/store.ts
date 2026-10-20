// Where execution events and status transitions are persisted. Same contract as the
// Temporal adapter's store, because the reference backend's SSE drain depends on it:
// `sequence` is assigned inside the workflow, dense and ascending per execution, and a
// store that enforces uniqueness on (executionId, sequence) turns a replayed or
// retried write into a no-op.
//
// On Workers the implementation usually sits behind a Hyperdrive binding (Postgres)
// or D1. Because the Workflow and the store share one Worker, the store is built from
// `env` on each `run()` — see `createExecutionRunner`.
import type { ExecutionEventType, ExecutionOutcome, ExecutionStatus } from './core-contract';

export interface ExecutionStore {
  emitExecutionEvent(
    executionId: string,
    sequence: number,
    type: ExecutionEventType,
    payload?: unknown,
    nodeId?: string,
  ): Promise<void>;
  updateExecutionStatus(
    executionId: string,
    status: ExecutionStatus,
    errorMessage?: string,
    outcome?: ExecutionOutcome,
  ): Promise<void>;
}

// What the backend-side engine needs to know about a parked node before it forwards a
// verdict. Temporal validated this inside the workflow (an update validator); Cloudflare's
// `sendEvent` has no such hook, so the check moves to the caller and reads the same event
// rows the run view does. `apps/backend/src/events/count-node-waits.ts` already derives it.
export type NodeWaitState = 'waiting' | 'resolved' | 'not_waiting';

export interface WaitStateStore {
  getNodeWaitState(executionId: string, nodeId: string): Promise<NodeWaitState>;
}
