// Node-side stand-in for the workerd-only module.
export class WorkflowEntrypoint<TEnv = unknown, TParams = unknown> {
  // Declared to keep the class shape; never read by a stub.
  declare readonly params?: TParams;
  constructor(
    public context: unknown,
    public env: TEnv,
  ) {}
}

export class DurableObject<TEnv = unknown> {
  constructor(
    public context: unknown,
    public env: TEnv,
  ) {}
}
