import { describe, expect, it, vi } from 'vitest';

import { CloudflareWorkflowEngine } from '../src/client';
import { executionInstanceId, resolveEventType } from '../src/constants';

function makeBinding(status: string = 'waiting') {
  const instance = {
    id: executionInstanceId('exec-1'),
    status: vi.fn(async () => ({ status })),
    terminate: vi.fn(async () => {}),
    sendEvent: vi.fn(async () => {}),
    pause: vi.fn(),
    resume: vi.fn(),
    restart: vi.fn(),
  };
  const workflow = {
    create: vi.fn(async () => instance),
    createBatch: vi.fn(),
    get: vi.fn(async (id: string) => {
      if (id !== instance.id) throw new Error(`instance.not_found: ${id}`);
      return instance;
    }),
  };
  return { workflow: workflow as unknown as Workflow, instance };
}

const input = {
  workflowId: 'wf',
  executionId: 'exec-1',
  definition: { workflowId: 'wf', nodes: [], edges: [] },
  triggerPayload: {},
  variables: {},
  global: {},
};

describe('CloudflareWorkflowEngine', () => {
  it('submits one instance per execution, addressed by execution id', async () => {
    const { workflow } = makeBinding();
    const engine = new CloudflareWorkflowEngine({ workflow, recordCancellation: async () => {} });
    await engine.submit(input);
    expect(workflow.create).toHaveBeenCalledWith({ id: 'execution-exec-1', params: input });
  });

  it('delivers a well-formed verdict as a typed event', async () => {
    const { workflow, instance } = makeBinding();
    const engine = new CloudflareWorkflowEngine({ workflow, recordCancellation: async () => {} });
    const result = await engine.resolveNode({ executionId: 'exec-1', nodeId: 'n2', resolution: { output: 'yes' } });
    expect(result).toEqual({});
    expect(instance.sendEvent).toHaveBeenCalledWith({ type: resolveEventType('n2'), payload: { output: 'yes' } });
  });

  it('refuses a malformed verdict before it becomes an event', async () => {
    const { workflow, instance } = makeBinding();
    const engine = new CloudflareWorkflowEngine({ workflow, recordCancellation: async () => {} });
    const result = await engine.resolveNode({
      executionId: 'exec-1',
      nodeId: 'n2',
      resolution: { output: 1, nextPort: 'errorRoute' },
    });
    expect(result.error?.code).toBe('verdict_malformed');
    expect(instance.sendEvent).not.toHaveBeenCalled();
  });

  it('answers run_not_found for an unknown execution', async () => {
    const { workflow } = makeBinding();
    const engine = new CloudflareWorkflowEngine({ workflow, recordCancellation: async () => {} });
    const result = await engine.resolveNode({ executionId: 'nope', nodeId: 'n2', resolution: { output: 1 } });
    expect(result.error?.code).toBe('run_not_found');
  });

  it('consults the wait state store when one is wired', async () => {
    const { workflow, instance } = makeBinding();
    const engine = new CloudflareWorkflowEngine({
      workflow,
      recordCancellation: async () => {},
      waitState: { getNodeWaitState: async () => 'resolved' },
    });
    const result = await engine.resolveNode({ executionId: 'exec-1', nodeId: 'n2', resolution: { output: 1 } });
    expect(result.error?.code).toBe('verdict_already_delivered');
    expect(instance.sendEvent).not.toHaveBeenCalled();
  });

  it('refuses a verdict for a closed instance', async () => {
    const { workflow } = makeBinding('complete');
    const engine = new CloudflareWorkflowEngine({ workflow, recordCancellation: async () => {} });
    const result = await engine.resolveNode({ executionId: 'exec-1', nodeId: 'n2', resolution: { output: 1 } });
    expect(result.error?.code).toBe('node_not_waiting');
  });

  it('terminates on cancel and records the cancellation itself', async () => {
    const { workflow, instance } = makeBinding('running');
    const recordCancellation = vi.fn(async () => {});
    const engine = new CloudflareWorkflowEngine({ workflow, recordCancellation });
    await engine.cancel('exec-1');
    expect(instance.terminate).toHaveBeenCalledOnce();
    expect(recordCancellation).toHaveBeenCalledWith('exec-1');
  });

  it('treats cancel of an unknown execution as already gone', async () => {
    const { workflow } = makeBinding();
    const recordCancellation = vi.fn(async () => {});
    const engine = new CloudflareWorkflowEngine({ workflow, recordCancellation });
    await expect(engine.cancel('nope')).resolves.toBeUndefined();
    expect(recordCancellation).not.toHaveBeenCalled();
  });
});
