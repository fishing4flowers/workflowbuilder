import type { WorkflowEvent } from 'cloudflare:workers';
import { describe, expect, it } from 'vitest';

import { PermanentNodeExecutionError, TransientNodeExecutionError } from '../src/core-contract';
import type {
  BaseNode,
  ExecutionEventType,
  ExecutionStatus,
  NodeExecutorRegistry,
  WorkflowExecutionInput,
} from '../src/core-contract';
import { resolveEventType } from '../src/constants';
import type { ExecutionStore } from '../src/store';
import { createExecutionRunner } from '../src/workflow/run-workflow';
import { FakeStep, asStep } from './fake-step';

type TestNode = BaseNode & ({ type: 'plan' } | { type: 'approve' } | { type: 'deploy' });

type Recorded = { sequence: number; type: ExecutionEventType; nodeId?: string; payload?: unknown };

function makeStore() {
  const events: Recorded[] = [];
  const statuses: ExecutionStatus[] = [];
  const store: ExecutionStore = {
    async emitExecutionEvent(_id, sequence, type, payload, nodeId) {
      events.push({ sequence, type, nodeId, payload });
    },
    async updateExecutionStatus(_id, status) {
      statuses.push(status);
    },
  };
  return { store, events, statuses };
}

function input(nodes: TestNode[], edges: [string, string][]): WorkflowExecutionInput<TestNode> {
  return {
    workflowId: 'wf',
    executionId: 'exec-1',
    definition: {
      workflowId: 'wf',
      nodes,
      edges: edges.map(([from, to]) => ({ id: `${from}->${to}`, sourceNodeId: from, targetNodeId: to })),
    },
    triggerPayload: {},
    variables: {},
    global: {},
  };
}

const pipeline = input(
  [
    { id: 'n1', type: 'plan', config: {}, role: 'start', label: 'Plan the change' },
    { id: 'n2', type: 'approve', config: {} },
    { id: 'n3', type: 'deploy', config: {} },
  ],
  [
    ['n1', 'n2'],
    ['n2', 'n3'],
  ],
);

function event(payload: WorkflowExecutionInput<TestNode>): WorkflowEvent<WorkflowExecutionInput<TestNode>> {
  return { payload, timestamp: new Date(), instanceId: 'execution-exec-1', workflowName: 'ExecutionWorkflow' };
}

describe('createExecutionRunner', () => {
  it('runs each node as a step, parks on waitForEvent and resumes on a verdict', async () => {
    const { store, events, statuses } = makeStore();
    const step = new FakeStep();
    const run = createExecutionRunner<TestNode>({
      store,
      executors: {
        plan: async () => ({ output: { plan: 'add feature' } }),
        approve: async () => ({ waiting: true }),
        deploy: async (_node, context) => ({ output: { deployed: context.nodeOutputs.n2 } }),
      },
    });

    const running = run(event(pipeline), asStep(step));
    await step.whenWaitingFor(resolveEventType('n2'));
    expect(statuses).toContain('waiting');

    step.sendEvent(resolveEventType('n2'), { output: 'approved', outcome: { value: 'approved', resolvedBy: 'pat' } });
    await expect(running).resolves.toEqual({ status: 'completed' });

    expect(events.map((recorded) => recorded.type)).toEqual([
      'execution_started',
      'node_started',
      'node_completed',
      'node_started',
      'node_waiting',
      'node_completed',
      'node_started',
      'node_completed',
      'execution_completed',
    ]);
    // Dense, ascending, and shared with status writes.
    const sequences = events.map((recorded) => recorded.sequence);
    const sorted = [...sequences].sort((a, b) => a - b);
    expect(sequences).toEqual(sorted);
    expect(events.at(-2)?.payload).toEqual({ output: { deployed: 'approved' } });
    expect(step.stepNames).toContain('Plan the change (n1)');
    expect(step.stepNames.filter((n) => n.startsWith('verdict n2'))).toEqual(['verdict n2 #1']);
  });

  it('ignores an event that is not a verdict and keeps waiting', async () => {
    const { store } = makeStore();
    const step = new FakeStep();
    const run = createExecutionRunner<TestNode>({
      store,
      executors: {
        plan: async () => ({ output: 1 }),
        approve: async () => ({ waiting: true }),
        deploy: async () => ({ output: 2 }),
      },
    });
    const running = run(event(pipeline), asStep(step));
    await step.whenWaitingFor(resolveEventType('n2'));
    step.sendEvent(resolveEventType('n2'), { bogus: true });
    await step.whenWaitingFor(resolveEventType('n2'));
    step.sendEvent(resolveEventType('n2'), { output: 'ok' });
    await expect(running).resolves.toEqual({ status: 'completed' });
    expect(step.stepNames.filter((n) => n.startsWith('verdict n2'))).toEqual(['verdict n2 #1', 'verdict n2 #2']);
  });

  it('replays from checkpoints without re-running executors or store writes', async () => {
    const { store, events } = makeStore();
    let planRuns = 0;
    const executors: NodeExecutorRegistry<TestNode> = {
      plan: async () => {
        planRuns += 1;
        return { output: 'p' };
      },
      approve: async () => ({ waiting: true }),
      deploy: async () => ({ output: 'd' }),
    };
    const first = new FakeStep();
    const run = createExecutionRunner<TestNode>({ store, executors });
    const firstRun = run(event(pipeline), asStep(first));
    await first.whenWaitingFor(resolveEventType('n2'));
    const writesBeforeReplay = events.length;

    // The platform hibernates here and later replays run() on a fresh isolate.
    const replayed = first.replay();
    const secondRun = run(event(pipeline), asStep(replayed));
    await replayed.whenWaitingFor(resolveEventType('n2'));
    expect(planRuns).toBe(1);
    expect(events.length).toBe(writesBeforeReplay);

    replayed.sendEvent(resolveEventType('n2'), { output: 'go' });
    await expect(secondRun).resolves.toEqual({ status: 'completed' });
    first.sendEvent(resolveEventType('n2'), { output: 'go' });
    await firstRun;
  });

  it('does not retry a permanent failure and fails the run with its code', async () => {
    const { store, events, statuses } = makeStore();
    const step = new FakeStep();
    let attempts = 0;
    const run = createExecutionRunner<TestNode>({
      store,
      executors: {
        plan: async () => {
          attempts += 1;
          throw new PermanentNodeExecutionError('bad_config', 'model name rejected');
        },
        approve: async () => ({ waiting: true }),
        deploy: async () => ({ output: 'd' }),
      },
      nodeStepProfiles: { plan: { timeout: '1 minute', retries: { limit: 3, delay: '1 second' } } },
    });
    // NonRetryableError(message, name): the code rides as the name, as it would in the dashboard.
    await expect(run(event(pipeline), asStep(step))).rejects.toMatchObject({ name: 'bad_config', message: 'model name rejected' });
    expect(attempts).toBe(1);
    const failed = events.find((recorded) => recorded.type === 'node_failed');
    expect(failed?.payload).toMatchObject({ error: { code: 'bad_config', message: 'model name rejected' } });
    expect(statuses.at(-1)).toBe('failed');
  });

  it('retries a transient failure within the profile limit', async () => {
    const { store } = makeStore();
    const step = new FakeStep();
    let attempts = 0;
    const run = createExecutionRunner<TestNode>({
      store,
      executors: {
        plan: async () => {
          attempts += 1;
          if (attempts < 3) throw new TransientNodeExecutionError('rate_limited', '429');
          return { output: 'ok' };
        },
        approve: async () => ({ output: 'skip' }),
        deploy: async () => ({ output: 'd' }),
      },
      nodeStepProfiles: { plan: { timeout: '1 minute', retries: { limit: 3, delay: '1 second' } } },
    });
    await expect(run(event(pipeline), asStep(step))).resolves.toEqual({ status: 'completed' });
    expect(attempts).toBe(3);
  });
});
