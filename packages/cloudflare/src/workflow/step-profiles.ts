// Timeout and retry shape for one `step.do` call: the Cloudflare analogue of the
// Temporal adapter's ActivityProfile. Structurally a `WorkflowStepConfig`, so a profile
// is handed to `step.do` as-is, but our own type so @cloudflare/workers-types stays out
// of the published surface.
import type { BaseNode } from '../core-contract';

type DurationLabel = 'second' | 'minute' | 'hour' | 'day' | 'week' | 'month' | 'year';
export type DurationString = `${number} ${DurationLabel}${'s' | ''}`;

export type StepProfile = {
  timeout: DurationString;
  retries: {
    // Retries after the first attempt, so `limit: 1` means two attempts in all —
    // one more than Temporal's `maximumAttempts: 2` reads, which is the same thing.
    limit: number;
    delay: DurationString;
    backoff?: 'constant' | 'linear' | 'exponential';
  };
};

type ReadonlyStepProfile = {
  readonly timeout: DurationString;
  readonly retries: { readonly limit: number; readonly delay: DurationString; readonly backoff?: StepProfile['retries']['backoff'] };
};

// Node steps may call LLMs (minutes): generous timeout, one retry to bound cost.
export const DEFAULT_NODE_STEP_PROFILE: ReadonlyStepProfile = Object.freeze({
  timeout: '10 minutes',
  retries: Object.freeze({ limit: 1, delay: '5 seconds', backoff: 'exponential' as const }),
});

// Store writes: fast, idempotent — short timeout, aggressive retries.
export const DEFAULT_STORE_STEP_PROFILE: ReadonlyStepProfile = Object.freeze({
  timeout: '30 seconds',
  retries: Object.freeze({ limit: 4, delay: '1 second', backoff: 'exponential' as const }),
});

// Keyed by `node.type`; no entry means DEFAULT_NODE_STEP_PROFILE. Whole profiles, not
// partials, so an omitted `retries` cannot silently inherit the platform default.
export type NodeStepProfiles = Readonly<Record<string, StepProfile>>;

const DURATION = /^\d+ (second|minute|hour|day|week|month|year)s?$/;

function assertDuration(value: unknown, where: string): asserts value is DurationString {
  if (typeof value !== 'string' || !DURATION.test(value)) {
    throw new Error(`${where}: expected a duration like "10 minutes", got ${JSON.stringify(value)}`);
  }
}

export function assertNodeStepProfiles(profiles: NodeStepProfiles): void {
  for (const [type, profile] of Object.entries(profiles)) {
    assertDuration(profile?.timeout, `nodeStepProfiles[${JSON.stringify(type)}].timeout`);
    const retries = profile.retries;
    if (typeof retries !== 'object' || retries === null) {
      throw new Error(`nodeStepProfiles[${JSON.stringify(type)}].retries: expected an object`);
    }
    if (!Number.isInteger(retries.limit) || retries.limit < 0) {
      throw new Error(`nodeStepProfiles[${JSON.stringify(type)}].retries.limit: expected a non-negative integer`);
    }
    assertDuration(retries.delay, `nodeStepProfiles[${JSON.stringify(type)}].retries.delay`);
  }
}

export function freezeNodeStepProfiles(profiles: NodeStepProfiles): NodeStepProfiles {
  assertNodeStepProfiles(profiles);
  const frozen: Record<string, StepProfile> = {};
  for (const [type, profile] of Object.entries(profiles)) {
    frozen[type] = Object.freeze({ timeout: profile.timeout, retries: Object.freeze({ ...profile.retries }) });
  }
  return Object.freeze(frozen);
}

export function resolveStepProfile(node: BaseNode, profiles: NodeStepProfiles): StepProfile {
  const profile = Object.hasOwn(profiles, node.type) ? profiles[node.type] : DEFAULT_NODE_STEP_PROFILE;
  return { timeout: profile.timeout, retries: { ...profile.retries } };
}

// Step names show in the dashboard's run view and its auto-generated diagram, so they
// carry the node's label when it has one. Kept short: the platform bounds them.
const MAX_STEP_NAME = 120;

export function nodeStepName(node: BaseNode): string {
  const label = typeof node.label === 'string' ? node.label.replaceAll(/\s+/g, ' ').trim() : '';
  const name = label === '' ? `${node.type} ${node.id}` : `${label} (${node.id})`;
  return name.length > MAX_STEP_NAME ? name.slice(0, MAX_STEP_NAME) : name;
}
