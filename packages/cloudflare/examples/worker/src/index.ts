import { Hono } from 'hono';

import { CloudflareWorkflowEngine } from '@workflowbuilder/cloudflare/client';
import {
  createPostgresStore,
  defineExecutionEventsObject,
  fetchEventsAfterSql,
  fetchStatusSql,
  notifyExecutionEventsObject,
  streamExecutionEvents,
} from '@workflowbuilder/cloudflare/workers';
import { defineExecutionWorkflow } from '@workflowbuilder/cloudflare/workflow';

import type { Env } from './env';
import { type SdlcNode, executors } from './executors';
import { openSql } from './sql';

function store(env: Env) {
  return createPostgresStore({ sql: openSql(env), notify: notifyExecutionEventsObject(env.EXECUTION_EVENTS) });
}

// The Workflow: one instance per execution, executors in this same Worker.
export const ExecutionWorkflow = defineExecutionWorkflow<Env, SdlcNode>((env) => ({
  executors,
  store: store(env),
  nodeStepProfiles: {
    'sdlc/implement': { timeout: '30 minutes', retries: { limit: 2, delay: '30 seconds', backoff: 'exponential' } },
  },
  decisionTimeout: '14 days',
}));

// The per-execution SSE fan-out.
export const ExecutionEventsObject = defineExecutionEventsObject<Env>((env) => {
  const sql = openSql(env);
  return { fetchEventsAfter: fetchEventsAfterSql(sql), fetchStatus: fetchStatusSql(sql) };
});

// The engine the backend routes call — the one thing apps/backend/src/engine/index.ts
// would return instead of TemporalWorkflowEngine once the backend runs here.
function engine(env: Env) {
  const s = store(env);
  return new CloudflareWorkflowEngine<SdlcNode>({
    workflow: env.WORKFLOW,
    recordCancellation: (executionId) => s.recordCancellation(executionId),
    waitState: s,
  });
}

// A minimal HTTP surface. The real backend mounts its existing Hono routes here; auth,
// tenancy and the workflow CRUD are unchanged, only the engine and the stream differ.
const app = new Hono<{ Bindings: Env }>();

app.post('/executions', async (c) => {
  const input = await c.req.json();
  await engine(c.env).submit(input);
  return c.json({ executionId: input.executionId }, 202);
});

app.get('/executions/:id/stream', (c) => streamExecutionEvents(c.env.EXECUTION_EVENTS, c.req.param('id')));

app.post('/executions/:id/nodes/:nodeId/decision', async (c) => {
  const result = await engine(c.env).resolveNode({
    executionId: c.req.param('id'),
    nodeId: c.req.param('nodeId'),
    resolution: await c.req.json(),
  });
  return result.error ? c.json(result.error, 409) : c.body(null, 204);
});

app.delete('/executions/:id', async (c) => {
  await engine(c.env).cancel(c.req.param('id'));
  return c.body(null, 204);
});

export default app;
