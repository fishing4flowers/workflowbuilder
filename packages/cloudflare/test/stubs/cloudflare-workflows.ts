// Node-side stand-in for the workerd-only module.
export class NonRetryableError extends Error {
  constructor(message: string, name?: string) {
    super(message);
    this.name = name ?? 'NonRetryableError';
  }
}
