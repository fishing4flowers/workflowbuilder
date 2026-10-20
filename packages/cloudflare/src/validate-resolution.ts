// Shape check for a verdict, ported from the Temporal adapter's update validator.
// Two callers: the client engine, which refuses a malformed verdict before it ever
// becomes an event, and the workflow runner, which re-checks the payload it was woken
// with (anything with the instance id can call sendEvent) and keeps waiting if it is
// not a verdict. Pure: no platform imports, so it runs in both.
import type { CompletedNodeExecution, VerdictRejection } from './core-contract';

export type VerdictRejected = { code: VerdictRejection; message: string };

export const VERDICT_REJECTION_MESSAGES = {
  not_an_object: { code: 'verdict_malformed', message: 'verdict must be a { nodeId, resolution } object' },
  node_id_blank: { code: 'verdict_malformed', message: 'nodeId must be a non-empty string' },
  resolution_not_an_object: { code: 'verdict_malformed', message: 'resolution must be an object' },
  unknown_resolution_key: { code: 'verdict_malformed', message: "unknown resolution key '{value}'" },
  next_port_invalid: {
    code: 'verdict_malformed',
    message: "nextPort must be a non-empty string other than the reserved 'errorRoute'",
  },
  outcome_invalid: {
    code: 'verdict_malformed',
    message: 'outcome must be an object carrying exactly value and resolvedBy, both non-empty strings',
  },
  unknown_node: { code: 'verdict_for_unknown_node', message: "no node '{value}' in this run" },
  already_delivered: { code: 'verdict_already_delivered', message: "node '{value}' already has a verdict" },
  not_waiting: { code: 'node_not_waiting', message: "node '{value}' is not waiting for a verdict" },
} as const satisfies Record<string, VerdictRejected>;

export function rejection(key: keyof typeof VERDICT_REJECTION_MESSAGES, value?: string): VerdictRejected {
  const { code, message } = VERDICT_REJECTION_MESSAGES[key];
  return { code, message: message.replace('{value}', () => value ?? '') };
}

// Returns the first rejection, or undefined when `resolution` is a well-formed
// CompletedNodeExecution.
export function validateResolution(resolution: unknown): VerdictRejected | undefined {
  if (typeof resolution !== 'object' || resolution === null || Array.isArray(resolution)) {
    return rejection('resolution_not_an_object');
  }
  for (const key of Object.keys(resolution)) {
    if (key !== 'output' && key !== 'nextPort' && key !== 'outcome') {
      return rejection('unknown_resolution_key', key);
    }
  }
  const { nextPort, outcome } = resolution as { nextPort?: unknown; outcome?: unknown };
  if (nextPort !== undefined && (typeof nextPort !== 'string' || nextPort.length === 0 || nextPort === 'errorRoute')) {
    return rejection('next_port_invalid');
  }
  if (outcome !== undefined && !isOutcome(outcome)) {
    return rejection('outcome_invalid');
  }
  return undefined;
}

export function isCompletedNodeExecution(candidate: unknown): candidate is CompletedNodeExecution {
  return validateResolution(candidate) === undefined;
}

function isOutcome(candidate: unknown): boolean {
  if (typeof candidate !== 'object' || candidate === null || Array.isArray(candidate)) return false;
  const keys = Object.keys(candidate);
  if (keys.length !== 2 || !keys.includes('value') || !keys.includes('resolvedBy')) return false;
  const { value, resolvedBy } = candidate as { value: unknown; resolvedBy: unknown };
  return isFilled(value) && isFilled(resolvedBy);
}

function isFilled(text: unknown): text is string {
  return typeof text === 'string' && text.trim().length > 0;
}
