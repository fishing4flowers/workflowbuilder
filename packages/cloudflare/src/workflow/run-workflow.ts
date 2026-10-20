// The Cloudflare Workflow side. Runs inside `WorkflowEntrypoint.run()`, which the
// platform replays from the top after every hibernation or restart, so everything
// here is deterministic: graph traversal is the core's pure `runGraph`, and every
// effect — a node executor, a store write, a wait for a person — goes through `step`.
//
// Port mapping, next to the Temporal adapter it mirrors:
//   ActivityRunnerPort.executeNode   proxyActivities + activity   →  step.do(name, profile, executor)
//   ActivityRunnerPort.awaitResolution  update handler + condition  →  step.waitForEvent(type: resolve:<nodeId>)
//   EventEmitterPort                 database activities          →  step.do(name, storeProfile, store write)
import type { WorkflowEvent, WorkflowStep } from 'cloudflare:workers';
import { NonRetryableError } from 'cloudflare:workflows';

import { DEFAULT_DECISION_TIMEOUT, resolveEventType } from '../constants';
import {
  type ActivityRunnerPort,
  type BaseNode,
  type CompletedNodeExecution,
  type ExecutionContext,
  type NodeErrorEnvelope,
  type NodeExecutionResult,
  type NodeExecutorRegistry,
  PermanentNodeExecutionError,
  type WorkflowExecutionInput,
  classifyNodeError,
  resolveExecutor,
  runGraph,
} from '../core-contract';
import type { ExecutionStore } from '../store';
import { isCompletedNodeExecution } from '../validate-resolution';
import { createSequencedEventEmitter } from './sequenced-event-emitter';
import {
  DEFAULT_STORE_STEP_PROFILE,
  type DurationString,
  type NodeStepProfiles,
  type StepProfile,
  freezeNodeStepProfiles,
  nodeStepName,
  resolveStepProfile,
} from './step-profiles';

export type ExecutionRunnerDeps<TNode extends BaseNode> = {
  // One executor per node type; a missing or mistyped key fails to compile.
  executors: NodeExecutorRegistry<TNode>;
  store: ExecutionStore;
  nodeStepProfiles?: NodeStepProfiles;
  storeStepProfile?: StepProfile;
  // How long a parked node waits for a verdict before the wait fails the node.
  decisionTimeout?: DurationString;
};

// What a `step.do` for a node hands back across the checkpoint boundary. A permanent
// failure is *returned*, not thrown: thrown, the platform would retry it, and after
// the retries the error that reaches `run()` has been flattened to a message, losing
// the code the node_failed event carries. Transient failures are thrown so the
// platform retries them; once exhausted, the error reaching `run()` keeps its message
// but not its code, and the node fails through its errorPolicy like any other throw.
type StepResult =
  | { ok: true; result: NodeExecutionResult }
  | { ok: false; permanent: NodeErrorEnvelope & { message: string; name: string } };

// Restated, not `RunGraphOutcome['status']`: the core names @workflow-builder/types,
// which would leak into dist. 'failed' never returns; it throws.
export type RunResult = { status: 'completed' | 'incomplete' };

// Built once per `run()` call from `env`, because the store needs the Worker's
// bindings. Returns the function to delegate `run(event, step)` to.
export function createExecutionRunner<TNode extends BaseNode>(deps: ExecutionRunnerDeps<TNode>) {
  const profiles = freezeNodeStepProfiles(deps.nodeStepProfiles ?? {});
  const storeProfile = deps.storeStepProfile ?? DEFAULT_STORE_STEP_PROFILE;
  const decisionTimeout = deps.decisionTimeout ?? DEFAULT_DECISION_TIMEOUT;

  return async function runExecution(
    event: WorkflowEvent<WorkflowExecutionInput<TNode>>,
    step: WorkflowStep,
  ): Promise<RunResult> {
    const input = event.payload;

    const events = createSequencedEventEmitter({
      emitEvent: (executionId, sequence, type, payload, nodeId) =>
        step.do(`event ${sequence} ${type}`, storeProfile, () =>
          deps.store.emitExecutionEvent(executionId, sequence, type, payload, nodeId),
        ),
      updateStatus: (executionId, sequence, status, errorMessage, outcome) =>
        step.do(`status ${sequence} ${status}`, storeProfile, () =>
          deps.store.updateExecutionStatus(executionId, status, errorMessage, outcome),
        ),
    });

    const runner: ActivityRunnerPort<TNode> = {
      async executeNode(node, context) {
        // Node outputs are JSON by contract (they land in event rows), which is what the
        // platform's Serializable bound asks for; `unknown` just cannot say so.
        const result = (await step.do(
          nodeStepName(node),
          resolveStepProfile(node, profiles),
          () => executeInsideStep(deps.executors, node, context) as Promise<Rpc.Serializable<StepResult>>,
        )) as StepResult;
        if (result.ok) return result.result;
        const { message, name, ...envelope } = result.permanent;
        const error = new PermanentNodeExecutionError(envelope.code, message);
        error.name = name;
        throw error;
      },

      async awaitResolution(nodeId) {
        // Anyone holding the instance id can sendEvent; a payload that is not a verdict
        // is ignored and the node keeps waiting. The attempt index keeps step names
        // unique, which replay requires.
        for (let attempt = 1; ; attempt += 1) {
          const received = await step.waitForEvent(`verdict ${nodeId} #${attempt}`, {
            type: resolveEventType(nodeId),
            timeout: decisionTimeout,
          });
          if (isCompletedNodeExecution(received.payload)) {
            return received.payload as CompletedNodeExecution;
          }
        }
      },
    };

    const outcome = await runGraph(input, runner, events);

    // runGraph already emitted execution_failed and wrote the status; this only tells
    // the platform to close the instance as errored rather than complete. Non-retryable,
    // because re-running `run()` would re-execute every node that has no checkpoint.
    if (outcome.status === 'failed') {
      throw new NonRetryableError(outcome.error.message, outcome.error.code ?? 'WorkflowExecutionFailed');
    }
    return { status: outcome.status };
  };
}

async function executeInsideStep<TNode extends BaseNode>(
  executors: NodeExecutorRegistry<TNode>,
  node: TNode,
  context: ExecutionContext,
): Promise<StepResult> {
  const executor = resolveExecutor(executors, node);
  try {
    return { ok: true, result: await executor(node, context) };
  } catch (error) {
    const classification = classifyNodeError(error);
    if (classification === 'permanent') {
      const classified = error as Error & { code: string };
      return {
        ok: false,
        permanent: { wbNodeError: 1, classification, code: classified.code, attempt: 1, message: classified.message, name: classified.name },
      };
    }
    throw error;
  }
}
