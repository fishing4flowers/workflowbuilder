// Workflow-side entry: what the consumer's Worker exports as its Workflow class.
//
//   // src/execution-workflow.ts
//   export class ExecutionWorkflow extends WorkflowEntrypoint<Env> {
//     run(event, step) {
//       return createExecutionRunner({ executors, store: createStore(this.env) })(event, step);
//     }
//   }
//
// or, with nothing to override, `export const ExecutionWorkflow = defineExecutionWorkflow(env => ({...}))`.
import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from 'cloudflare:workers';

import type { BaseNode, WorkflowExecutionInput } from '../core-contract';
import { type ExecutionRunnerDeps, type RunResult, createExecutionRunner } from './run-workflow';

export { createExecutionRunner } from './run-workflow';
export type { ExecutionRunnerDeps, RunResult } from './run-workflow';

export {
  DEFAULT_NODE_STEP_PROFILE,
  DEFAULT_STORE_STEP_PROFILE,
  assertNodeStepProfiles,
  nodeStepName,
  resolveStepProfile,
} from './step-profiles';
export type { DurationString, NodeStepProfiles, StepProfile } from './step-profiles';

export { createSequencedEventEmitter } from './sequenced-event-emitter';

// Builds the class `wrangler.jsonc` points at. `deps` runs on every `run()` with the
// Worker's bindings, so the store can wrap a Hyperdrive or D1 binding from `env`.
// The explicit constructor type keeps the emitted .d.ts valid: an anonymous class
// with WorkflowEntrypoint's protected members cannot be exported inferred (TS4094).
export type ExecutionWorkflowClass<TEnv, TNode extends BaseNode> = new (
  context: ExecutionContext,
  env: TEnv,
) => WorkflowEntrypoint<TEnv, WorkflowExecutionInput<TNode>>;

export function defineExecutionWorkflow<TEnv, TNode extends BaseNode = BaseNode>(
  deps: (env: TEnv) => ExecutionRunnerDeps<TNode>,
): ExecutionWorkflowClass<TEnv, TNode> {
  return class ExecutionWorkflow extends WorkflowEntrypoint<TEnv, WorkflowExecutionInput<TNode>> {
    run(event: WorkflowEvent<WorkflowExecutionInput<TNode>>, step: WorkflowStep): Promise<RunResult> {
      return createExecutionRunner(deps(this.env))(event, step);
    }
  };
}
