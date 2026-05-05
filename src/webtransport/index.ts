/**
 * @moqt/webtransport — I/O adapter bridging WebTransport to @moqt/transport.
 * @module
 */

export { MoqtAdapter } from './adapter.js';
export type { DeliveredObject, RawSubscription, RawSubscribeOptions } from './adapter.js';
export { ControlStreamFramer } from './framer.js';
export type { FramedMessage } from './framer.js';
export type {
  WebTransportLike,
  WebTransportBidirectionalStream,
  WebTransportCloseInfo,
} from './types.js';
export { AdapterError } from './adapter-error.js';
export type { AdapterErrorSource, AdapterErrorOptions } from './adapter-error.js';

// Re-export version types from @moqt/transport for convenience
export { createControlCodec } from '../transport';
export type { DraftVersion, ControlCodec } from '../transport';
