// The cursor drain, lifted from apps/backend/src/events/drain-events.ts so the Durable
// Object streams rows with exactly the Node route's semantics. Pure; no bindings.
export type ExecutionEventRow = {
  executionId: string;
  sequence: number | string;
  timestamp: Date | string;
  type: string;
  nodeId: string | null;
  pathId: string | null;
  payloadJson: unknown;
};

const TERMINAL_EVENT_TO_STATUS: Record<string, string> = {
  execution_completed: 'completed',
  execution_incomplete: 'incomplete',
  execution_failed: 'failed',
  execution_cancelled: 'cancelled',
};

export function isTerminalEventType(type: string | undefined): type is keyof typeof TERMINAL_EVENT_TO_STATUS {
  return type !== undefined && Object.hasOwn(TERMINAL_EVENT_TO_STATUS, type);
}

export function terminalStatusFor(type: string): string {
  return TERMINAL_EVENT_TO_STATUS[type] ?? 'failed';
}

export type DrainResult = { lastSequence: number; reachedTerminal: boolean; writeFailed: boolean };

// Rows must become visible in ascending `sequence`: the cursor only moves forward.
// The workflow's sequenced emitter upholds that; gaps are fine, reordering is not.
export async function drainEventsSince(
  executionId: string,
  afterSequence: number,
  fetch: (executionId: string, afterSequence: number) => Promise<ExecutionEventRow[]>,
  write: (row: ExecutionEventRow) => Promise<void>,
): Promise<DrainResult> {
  const rows = await fetch(executionId, afterSequence);
  let lastSequence = afterSequence;
  for (const row of rows) {
    try {
      await write(row);
    } catch {
      return { lastSequence, reachedTerminal: false, writeFailed: true };
    }
    lastSequence = Number(row.sequence);
  }
  return { lastSequence, reachedTerminal: isTerminalEventType(rows.at(-1)?.type), writeFailed: false };
}
