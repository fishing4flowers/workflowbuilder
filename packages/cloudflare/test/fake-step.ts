// A WorkflowStep that behaves like the platform's as far as the runner can tell:
// `do` checkpoints by step name and replays from the checkpoint, retries throws up to
// the configured limit, and `waitForEvent` blocks until a matching event is sent.
import type { WorkflowStep, WorkflowStepConfig, WorkflowStepEvent } from 'cloudflare:workers';

export function asStep(fake: FakeStep): WorkflowStep {
  return fake as unknown as WorkflowStep;
}

type Checkpoint = { ok: true; value: unknown } | { ok: false; error: unknown };

// Structurally a WorkflowStep for the runner; not declared as one, since the platform's
// `do` overloads carry a Serializable bound this fake has no reason to restate.
export class FakeStep {
  readonly stepNames: string[] = [];
  readonly attempts = new Map<string, number>();
  private readonly checkpoints: Map<string, Checkpoint>;
  private readonly pendingEvents = new Map<string, unknown[]>();
  private readonly waiters = new Map<string, Array<(payload: unknown) => void>>();
  readonly waitingFor = new Set<string>();

  constructor(checkpoints = new Map<string, Checkpoint>()) {
    this.checkpoints = checkpoints;
  }

  // Hand the checkpoints to a fresh instance to simulate the platform replaying `run()`.
  replay(): FakeStep {
    return new FakeStep(new Map(this.checkpoints));
  }

  async do<T>(name: string, configOrCallback: WorkflowStepConfig | (() => Promise<T>), maybeCallback?: () => Promise<T>): Promise<T> {
    const config = typeof configOrCallback === 'function' ? {} : configOrCallback;
    const callback = (typeof configOrCallback === 'function' ? configOrCallback : maybeCallback) as () => Promise<T>;
    if (this.stepNames.includes(name)) throw new Error(`duplicate step name: ${name}`);
    this.stepNames.push(name);

    const cached = this.checkpoints.get(name);
    if (cached) {
      if (cached.ok) return cached.value as T;
      throw cached.error;
    }

    const limit = config.retries?.limit ?? 0;
    let lastError: unknown;
    for (let attempt = 1; attempt <= limit + 1; attempt += 1) {
      this.attempts.set(name, attempt);
      try {
        const value = await callback();
        this.checkpoints.set(name, { ok: true, value });
        return value;
      } catch (error) {
        lastError = error;
        if ((error as Error)?.name === 'NonRetryableError') break;
      }
    }
    this.checkpoints.set(name, { ok: false, error: lastError });
    throw lastError;
  }

  sleep = async (): Promise<void> => {};
  sleepUntil = async (): Promise<void> => {};

  async waitForEvent<T>(name: string, options: { type: string; timeout?: unknown }): Promise<WorkflowStepEvent<T>> {
    if (this.stepNames.includes(name)) throw new Error(`duplicate step name: ${name}`);
    this.stepNames.push(name);
    const cached = this.checkpoints.get(name);
    if (cached?.ok) return cached.value as WorkflowStepEvent<T>;

    const queued = this.pendingEvents.get(options.type);
    const payload =
      queued && queued.length > 0
        ? queued.shift()
        : await new Promise<unknown>((resolve) => {
            this.waitingFor.add(options.type);
            const list = this.waiters.get(options.type) ?? [];
            list.push(resolve);
            this.waiters.set(options.type, list);
          });
    this.waitingFor.delete(options.type);
    const event = { type: options.type, payload, timestamp: new Date() } as WorkflowStepEvent<T>;
    this.checkpoints.set(name, { ok: true, value: event });
    return event;
  }

  sendEvent(type: string, payload: unknown): void {
    const waiter = this.waiters.get(type)?.shift();
    if (waiter) {
      waiter(payload);
      return;
    }
    const list = this.pendingEvents.get(type) ?? [];
    list.push(payload);
    this.pendingEvents.set(type, list);
  }

  // Resolves once the runner is parked on `type`.
  async whenWaitingFor(type: string): Promise<void> {
    for (let index = 0; index < 100 && !this.waitingFor.has(type); index += 1) {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    if (!this.waitingFor.has(type)) throw new Error(`never waited for ${type}`);
  }
}
