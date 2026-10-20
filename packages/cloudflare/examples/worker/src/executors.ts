// One executor per node type. These are the hooks an agentic SDLC would fill in:
// `plan` and `implement` call Claude (or a Managed Agents session), `review` parks the
// run for a person, `deploy` calls your pipeline. Every node is your code.
import {
  type BaseNode,
  type NodeExecutorRegistry,
  PermanentNodeExecutionError,
  TransientNodeExecutionError,
} from '@workflowbuilder/cloudflare';

export type SdlcNode = BaseNode &
  (
    | { type: 'sdlc/plan'; config: { prompt: string } }
    | { type: 'sdlc/implement'; config: { repo: string } }
    | { type: 'sdlc/review'; config: Record<string, never> }
    | { type: 'sdlc/deploy'; config: { environment: string } }
  );

export const executors: NodeExecutorRegistry<SdlcNode> = {
  'sdlc/plan': async (node) => {
    if (!node.config.prompt) throw new PermanentNodeExecutionError('empty_prompt', 'plan node has no prompt');
    return { output: { plan: `TODO: call Claude with ${JSON.stringify(node.config.prompt)}` } };
  },
  'sdlc/implement': async (node, context) => {
    const response = await fetch(`https://example.invalid/implement`, {
      method: 'POST',
      body: JSON.stringify({ repo: node.config.repo, plan: context.nodeOutputs }),
    });
    if (response.status === 429 || response.status >= 500) {
      throw new TransientNodeExecutionError(`http_${response.status}`, await response.text());
    }
    return { output: await response.json() };
  },
  // Parks the run. The person's decision arrives as the node's resolution, so the
  // verdict's `nextPort` ('approve' | 'reject') routes the graph from here.
  'sdlc/review': async () => ({ waiting: true }),
  'sdlc/deploy': async (node) => ({ output: { deployed: node.config.environment } }),
};
