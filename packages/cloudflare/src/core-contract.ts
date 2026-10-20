// The single seam between this package and the private, source-only workspace
// packages it is built on. Same rule as packages/temporal/src/core-contract.ts:
// relative imports into sibling src/ on purpose, so declarations inline into dist
// instead of referencing `@workflow-builder/*`, which is never published.
//
// Unlike Temporal there is no sandbox/activity split: a Cloudflare Workflow's
// `run()` and the node executors it calls live in one Worker, so a single seam
// serves both the workflow and the client halves. `runGraph` is still taken from
// the core's sandbox-safe entry — it is the half that is deterministic, which
// Cloudflare's replay of `run()` needs just as much as Temporal's did.
export { runGraph } from '../../execution-core/src/workflow';
export type { ActivityRunnerPort, RunGraphOutcome, VerdictRejection } from '../../execution-core/src/workflow';

export {
  NodeExecutionError,
  PermanentNodeExecutionError,
  RESOLVE_NODE_REJECTIONS,
  TransientNodeExecutionError,
  classifyNodeError,
  resolveExecutor,
} from '../../execution-core/src/index';
export type { NodeErrorClassification, NodeErrorEnvelope } from '../../execution-core/src/index';

export type { ExecutionContext } from '../../execution-core/src/execution-context';
export type { LogBindings, LoggerPort } from '../../execution-core/src/ports/logger.port';

export type {
  BaseNode,
  NodeErrorPolicy,
  WorkflowDefinition,
  WorkflowEdgeDefinition,
} from '../../types/src/workflow-execution/execution-model';

export type {
  ExecutionEventType,
  ExecutionOutcome,
  ExecutionOutcomeRecord,
  ExecutionStatus,
} from '../../types/src/workflow-execution/execution-events';

import type { ExecutionContext } from '../../execution-core/src/execution-context';
import type {
  ExecutionEventType,
  ExecutionOutcome,
  ExecutionStatus,
} from '../../types/src/workflow-execution/execution-events';
import type { BaseNode, WorkflowDefinition } from '../../types/src/workflow-execution/execution-model';

// Restated rather than re-exported, for the reason given in the Temporal seam: the
// core names `@workflow-builder/types` by package name, which would survive into dist.
export type WorkflowExecutionInput<TNode extends BaseNode> = {
  workflowId: string;
  executionId: string;
  definition: WorkflowDefinition<TNode>;
  triggerPayload: Record<string, unknown>;
  variables: Record<string, unknown>;
  global: Record<string, unknown>;
};

export type CompletedNodeExecution = {
  output: unknown;
  nextPort?: string;
  outcome?: ExecutionOutcome;
  waiting?: never;
};

export type WaitingNodeExecution = {
  waiting: true;
};

export type NodeExecutionResult = CompletedNodeExecution | WaitingNodeExecution;

export type ResolveNodeInput = {
  executionId: string;
  nodeId: string;
  resolution: CompletedNodeExecution;
};

export type ResolveNodeRejection =
  | 'verdict_malformed'
  | 'verdict_for_unknown_node'
  | 'verdict_already_delivered'
  | 'node_not_waiting'
  | 'run_not_found'
  | 'delivery_timeout';

export type ResolveNodeResult = { error?: undefined } | { error: { code: ResolveNodeRejection; message: string } };

export interface WorkflowEnginePort<TNode extends BaseNode> {
  submit(input: WorkflowExecutionInput<TNode>): Promise<void>;
  cancel(executionId: string): Promise<void>;
  resolveNode(input: ResolveNodeInput): Promise<ResolveNodeResult>;
}

export interface EventEmitterPort {
  emitEvent(executionId: string, type: ExecutionEventType, payload?: unknown, nodeId?: string): Promise<void>;
  updateStatus(
    executionId: string,
    status: ExecutionStatus,
    errorMessage?: string,
    outcome?: ExecutionOutcome,
  ): Promise<void>;
}

export type NodeExecutor<TNode extends BaseNode> = (
  node: TNode,
  context: ExecutionContext,
) => Promise<NodeExecutionResult> | NodeExecutionResult;

export type NodeExecutorRegistry<TNode extends BaseNode> = {
  [K in TNode['type']]: NodeExecutor<Extract<TNode, { type: K }>>;
};
