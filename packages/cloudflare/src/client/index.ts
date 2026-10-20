// Backend-side engine: starting, cancelling and delivering verdicts through a Workflow
// binding. Mirrors packages/temporal/src/client/index.ts, with the differences that
// Cloudflare forces:
//
//   - A verdict is an event, not an update with a validator. The engine validates the
//     shape here and asks the store whether the node is really waiting; the instance
//     re-checks the shape when it wakes (see run-workflow.ts).
//   - Cancel is `terminate()`, which runs no cleanup inside the instance. The engine
//     records the cancellation itself, through the two hooks in the options.
import { executionInstanceId, resolveEventType } from '../constants';
import type { BaseNode, ResolveNodeInput, ResolveNodeResult, WorkflowEnginePort, WorkflowExecutionInput } from '../core-contract';
import type { WaitStateStore } from '../store';
import { rejection, validateResolution } from '../validate-resolution';

export type CloudflareWorkflowEngineOptions = {
  // The `[[workflows]]` binding from wrangler.jsonc, as it arrives on `env`.
  workflow: Workflow;
  // Marks the execution row cancelled, and appends the execution_cancelled event under
  // a sequence number the store assigns. Both run after `terminate()` succeeds.
  recordCancellation: (executionId: string) => Promise<void>;
  // When present, verdicts for nodes that are not waiting, or already resolved, are
  // refused before `sendEvent`. Without it only shape and instance existence are checked.
  waitState?: WaitStateStore;
};

// What `instance.status()` reports once a run can no longer take a verdict.
const CLOSED: ReadonlySet<InstanceStatus['status']> = new Set(['complete', 'errored', 'terminated']);

export class CloudflareWorkflowEngine<TNode extends BaseNode = BaseNode> implements WorkflowEnginePort<TNode> {
  constructor(private readonly options: CloudflareWorkflowEngineOptions) {}

  async submit(input: WorkflowExecutionInput<TNode>): Promise<void> {
    await this.options.workflow.create({ id: executionInstanceId(input.executionId), params: input });
  }

  async cancel(executionId: string): Promise<void> {
    const instance = await this.getInstance(executionId);
    if (instance === undefined) return; // Idempotent: not found means already gone.
    const { status } = await instance.status();
    if (!CLOSED.has(status)) {
      await instance.terminate();
    }
    await this.options.recordCancellation(executionId);
  }

  async resolveNode({ executionId, nodeId, resolution }: ResolveNodeInput): Promise<ResolveNodeResult> {
    if (typeof nodeId !== 'string' || nodeId.length === 0) return { error: rejection('node_id_blank') };
    const malformed = validateResolution(resolution);
    if (malformed) return { error: malformed };

    const instance = await this.getInstance(executionId);
    if (instance === undefined) {
      return { error: { code: 'run_not_found', message: `no run '${executionId}'` } };
    }

    if (this.options.waitState) {
      const state = await this.options.waitState.getNodeWaitState(executionId, nodeId);
      if (state === 'resolved') return { error: rejection('already_delivered', nodeId) };
      if (state === 'not_waiting') return { error: rejection('not_waiting', nodeId) };
    }

    const { status } = await instance.status();
    if (CLOSED.has(status)) {
      return { error: rejection('not_waiting', nodeId) };
    }

    // First verdict wins: the instance consumes one event per wait. A second one for the
    // same node is refused above when `waitState` is wired; otherwise it sits unread.
    await instance.sendEvent({ type: resolveEventType(nodeId), payload: resolution });
    return {};
  }

  private async getInstance(executionId: string): Promise<WorkflowInstance | undefined> {
    try {
      return await this.options.workflow.get(executionInstanceId(executionId));
    } catch (error) {
      if (isNotFound(error)) return undefined;
      throw error;
    }
  }
}

// The binding reports an unknown id as an error rather than a value. Matched on the
// message, since the runtime exports no error class for it — pinned by a test.
function isNotFound(error: unknown): boolean {
  return error instanceof Error && /not found|does not exist|instance\.not_found/i.test(error.message);
}
