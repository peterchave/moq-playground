/**
 * AdapterError — typed error for adapter-level failures.
 *
 * Carries structured context about where the error originated and
 * whether it's fatal, so the player can classify without string matching.
 *
 * Fatality defaults are grounded in the spec:
 * - Control stream errors → fatal (§3.2: MUST NOT close during session)
 * - Data stream resets → non-fatal (§10.4: publisher MAY reset streams)
 * - Datagram decode errors → non-fatal (datagrams are unreliable)
 * - Transport-level errors → fatal (connection lost)
 *
 * @see draft-ietf-moq-transport-16 §3.2 (Control stream lifecycle)
 * @see draft-ietf-moq-transport-16 §10.4 (Data stream reset)
 * @see draft-ietf-moq-transport-16 §13.4 (IANA error code registries)
 * @module
 */

/** Where the error originated within the adapter. */
export type AdapterErrorSource = 'control' | 'data' | 'datagram' | 'transport';

/** Options for constructing an AdapterError. */
export interface AdapterErrorOptions {
  /** Where the error originated. */
  readonly errorSource: AdapterErrorSource;

  /** MOQT protocol error code from the wire, if available. @see §13.4 */
  readonly protocolCode?: number;

  /** Data stream ID, for stream-level errors. */
  readonly streamId?: bigint;

  /**
   * Whether this error is fatal (playback cannot continue).
   * If not specified, defaults based on errorSource:
   * - 'control' → true
   * - 'transport' → true
   * - 'data' → false
   * - 'datagram' → false
   */
  readonly isFatal?: boolean;

  /** Underlying cause for error chaining. */
  readonly cause?: Error;
}

/**
 * Typed adapter error with structured source, fatality, and protocol context.
 *
 * Extends `Error` so it's backward-compatible with `onError: (error: Error) => void`.
 */
export class AdapterError extends Error {
  /** Where the error originated. */
  readonly errorSource: AdapterErrorSource;

  /** MOQT protocol error code from the wire, if available. @see §13.4 */
  readonly protocolCode: number | undefined;

  /** Data stream ID, for stream-level errors. */
  readonly streamId: bigint | undefined;

  /** Whether this error is fatal (playback cannot continue). */
  readonly isFatal: boolean;

  constructor(message: string, options: AdapterErrorOptions) {
    super(message, options.cause ? { cause: options.cause } : undefined);
    this.name = 'AdapterError';
    this.errorSource = options.errorSource;
    this.protocolCode = options.protocolCode;
    this.streamId = options.streamId;
    this.isFatal = options.isFatal ?? defaultFatality(options.errorSource);
  }
}

/** Default fatality based on error source. */
function defaultFatality(source: AdapterErrorSource): boolean {
  switch (source) {
    case 'control':
    case 'transport':
      return true;
    case 'data':
    case 'datagram':
      return false;
  }
}
