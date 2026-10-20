// One Cloudflare Workflow instance per Workflow Builder execution row. Deterministic,
// so cancel and resolveNode can address a run knowing only the execution id. Instance
// ids are limited to 100 characters by the platform; execution ids are UUIDs.
export function executionInstanceId(executionId: string): string {
  return `execution-${executionId}`;
}

// The event type a verdict for one node arrives on. `step.waitForEvent` filters by
// type, so each parked node listens on its own; a verdict for node A can never wake B.
export function resolveEventType(nodeId: string): string {
  return `resolve:${nodeId}`;
}

// How long a parked node waits for a person before the wait itself fails the node
// (which then goes through the node's errorPolicy like any other failure). Cloudflare
// caps a single wait; check `step.waitForEvent` limits before raising this.
export const DEFAULT_DECISION_TIMEOUT = '30 days';
