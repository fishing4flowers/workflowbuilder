# @workflowbuilder/cloudflare

Run Workflow Builder diagrams as durable [Cloudflare Workflows](https://developers.cloudflare.com/workflows/).

The same three ports the Temporal adapter implements, mapped onto Cloudflare's primitives:

| Port | Temporal adapter | This adapter |
| --- | --- | --- |
| `ActivityRunnerPort.executeNode` | `proxyActivities` → Activity on a Worker | `step.do(name, profile, executor)` in the same Worker |
| `ActivityRunnerPort.awaitResolution` | Update handler + `condition()` | `step.waitForEvent({ type: 'resolve:<nodeId>' })` |
| `EventEmitterPort` | Database activities | `step.do` around the store write |
| `WorkflowEnginePort.submit` | `client.workflow.start` | `env.WORKFLOW.create({ id, params })` |
| `WorkflowEnginePort.cancel` | `handle.cancel()` | `instance.terminate()` + the engine records the cancellation |
| `WorkflowEnginePort.resolveNode` | `handle.executeUpdate` (validated in-workflow) | `instance.sendEvent` (validated in the engine and on wake) |

Graph traversal is the core's `runGraph`, unchanged: it was already deterministic for Temporal's replay, and Cloudflare replays `run()` the same way. Independent branches still run in parallel (`Promise.all` of `step.do`), decisions still route, per-node-type timeouts and retries still apply, and a parked node holds no isolate while it waits.

## Worker side

```ts
// src/execution-workflow.ts
import { defineExecutionWorkflow } from '@workflowbuilder/cloudflare/workflow';

import { executors } from './executors'; // one per node type, as for Temporal
import { createStore } from './store';   // ExecutionStore over Hyperdrive or D1

export const ExecutionWorkflow = defineExecutionWorkflow<Env>((env) => ({
  executors,
  store: createStore(env),
  nodeStepProfiles: {
    'ai/generate': { timeout: '15 minutes', retries: { limit: 1, delay: '10 seconds', backoff: 'exponential' } },
  },
  decisionTimeout: '30 days',
}));
```

```jsonc
// wrangler.jsonc
{
  "workflows": [{ "name": "workflow-builder-execution", "binding": "WORKFLOW", "class_name": "ExecutionWorkflow" }],
  "hyperdrive": [{ "binding": "DB", "id": "<hyperdrive id>" }]
}
```

To keep full control of the class, call `createExecutionRunner(deps)` from your own `WorkflowEntrypoint.run()` instead.

## Backend side

```ts
import { CloudflareWorkflowEngine } from '@workflowbuilder/cloudflare/client';
import { createPostgresStore, notifyExecutionEventsObject } from '@workflowbuilder/cloudflare/workers';

const store = createPostgresStore({ sql, notify: notifyExecutionEventsObject(env.EXECUTION_EVENTS) });
const engine = new CloudflareWorkflowEngine({
  workflow: env.WORKFLOW,
  recordCancellation: (id) => store.recordCancellation(id),
  waitState: store, // refuses verdicts for nodes that are not waiting
});
```

`recordCancellation` appends `execution_cancelled` and sets the status, because `terminate()` runs no code inside the instance.

## Live run view: the Durable Object

The reference backend streams a run over SSE and learns about new rows through Postgres `LISTEN/NOTIFY`. Workers have no long-lived listener, so `@workflowbuilder/cloudflare/workers` replaces it with one Durable Object per execution:

```ts
import { defineExecutionEventsObject, fetchEventsAfterSql, fetchStatusSql, streamExecutionEvents } from '@workflowbuilder/cloudflare/workers';

export const ExecutionEventsObject = defineExecutionEventsObject<Env>((env) => ({
  fetchEventsAfter: fetchEventsAfterSql(sql(env)),
  fetchStatus: fetchStatusSql(sql(env)),
}));

app.get('/executions/:id/stream', (c) => streamExecutionEvents(c.env.EXECUTION_EVENTS, c.req.param('id')));
```

The store calls the object's `notify()` after each write; the object drains `execution_events` past each open stream's cursor. Postgres stays the record and the object is only a cache of connections, so a reconnect, a second object after eviction, and the catch-up path all read one source. Semantics match the Node route: a snapshot whose status derives from the last event, a forward-only cursor, a catch-up drain for the window between snapshot and subscribe, and a heartbeat that doubles as a poll so a lost nudge delays an event rather than losing it.

A complete Worker — Workflow, Durable Object, store, engine and a minimal Hono surface — is in [`examples/worker`](./examples/worker).

## What differs from Temporal, and why

- **Verdict validation moves to the caller.** Cloudflare's `sendEvent` has no validator hook. The engine checks the verdict's shape and, through `WaitStateStore`, whether the node is waiting; the instance re-checks the shape when it wakes and keeps waiting if the payload is not a verdict. First verdict wins.
- **Cancellation is recorded by the engine**, not the instance (see above).
- **Permanent node failures are returned from the step, not thrown.** A throw would be retried, and the error that reaches `run()` after retries keeps only its message. Returning the envelope keeps the `code` for the `node_failed` event. Transient failures are thrown so the platform retries them within the profile's limit.
- **Every store write is a step.** Replay then never touches the store, and the sequence counter (shared with status writes) gives each step a unique name.
- **No separate worker process.** Executors run inside the Workflow's Worker. Node types are developer-defined, so a static `[[workflows]]` binding suffices; Dynamic Workflows are only needed if end users supply executor code.

## Reference backend changes this implies

Outside this package, moving `apps/backend` onto Workers means:

- `@hono/node-server` → the Worker's `fetch` handler (Hono itself is unchanged).
- Postgres through Hyperdrive, or D1 with a Drizzle dialect change.
- `apps/backend/src/engine/index.ts` returns a `CloudflareWorkflowEngine`; the routes are unchanged.
- The `/executions/:id/stream` route delegates to `streamExecutionEvents` after its own auth and tenant checks.

## Platform limits (checked October 2026)

- `waitForEvent`: 1 second to 365 days, default 24 hours. `decisionTimeout` defaults to 30 days.
- Steps per instance: 1,024 (Workers Free), 10,000 (Paid, configurable to 25,000). Budget about four steps per node.
- Step return value and event payload: 1 MiB each. An executor producing more should store it (R2, a table) and return a key.
- CPU time per step: 10 ms (Free), 30 s (Paid). LLM and HTTP calls are wall-clock, which is unlimited.
- `sendEvent` for a type nothing is waiting on is **buffered** until a matching `waitForEvent` runs. Good for the race where a verdict lands just before the node parks; it also means a verdict sent after a node resolved would be consumed if that node parks again, so wire `waitState` in production. (Upstream tracks the same for Temporal as `decision-attempt-in-engine`.)

## Before `wrangler dev`

1. Apply the reference backend's migrations (`apps/backend`, Drizzle) to the Postgres that Hyperdrive fronts.
2. Add `localConnectionString` to the `hyperdrive` binding for local development.
3. `examples/worker` is outside the root `pnpm-workspace.yaml` globs; add `packages/*/examples/*` or run it standalone against a built tarball.
