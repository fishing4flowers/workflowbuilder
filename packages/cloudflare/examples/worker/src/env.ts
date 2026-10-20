import type { ExecutionEventsNamespace } from '@workflowbuilder/cloudflare/workers';

export type Env = {
  WORKFLOW: Workflow;
  EXECUTION_EVENTS: ExecutionEventsNamespace;
  DB: Hyperdrive;
};
