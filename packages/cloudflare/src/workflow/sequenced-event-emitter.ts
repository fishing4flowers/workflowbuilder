// Pure counter + serialized delegation, lifted from the Temporal adapter unchanged in
// behaviour: numbers follow call order, writes land one at a time, and a failed write
// never poisons the chain. The reference backend's SSE drain depends on both
// (see packages/temporal/src/workflow/sequenced-event-emitter.ts for the full why).
//
// On Cloudflare the persistence functions wrap `step.do`, so on replay each returns its
// cached result and the chain drains instantly without touching the store.
import type { EventEmitterPort, ExecutionEventType, ExecutionOutcome, ExecutionStatus } from '../core-contract';

export type EventPersistence = {
  emitEvent(
    executionId: string,
    sequence: number,
    type: ExecutionEventType,
    payload?: unknown,
    nodeId?: string,
  ): Promise<void>;
  updateStatus(
    executionId: string,
    sequence: number,
    status: ExecutionStatus,
    errorMessage?: string,
    outcome?: ExecutionOutcome,
  ): Promise<void>;
};

export function createSequencedEventEmitter(persistence: EventPersistence): EventEmitterPort {
  let sequence = 0;
  let tail: Promise<void> = Promise.resolve();

  return {
    emitEvent(executionId, type, payload, nodeId) {
      sequence += 1;
      const assigned = sequence;
      const write = tail.then(() => persistence.emitEvent(executionId, assigned, type, payload, nodeId));
      tail = write.catch(() => {});
      return write;
    },
    // Status writes are also numbered: a `step.do` needs a name that is unique per
    // instance across replays, and the counter is the one thing that is both
    // deterministic and unique here.
    updateStatus(executionId, status, errorMessage, outcome) {
      sequence += 1;
      const assigned = sequence;
      const write = tail.then(() => persistence.updateStatus(executionId, assigned, status, errorMessage, outcome));
      tail = write.catch(() => {});
      return write;
    },
  };
}
