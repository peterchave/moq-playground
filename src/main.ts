/**
 * MOQT Playground — browser-based test harness for exercising
 * all draft-ietf-moq-transport-16 control messages against a MOQT relay.
 *
 * Uses @moqt/transport and @moqt/webtransport directly (no player layer).
 */

import { MoqtAdapter } from '@moqt/webtransport';
import { varint, ForwardState, PublishDoneCode } from '@moqt/transport';
import type { ControlMessage, DraftVersion, Varint } from '@moqt/transport';
import { log, clearLog } from './log.js';
import { initViz, setConnectionState, resetMessages, addMessage } from './connection-viz.js';

const logPub = (msg: string, level: Parameters<typeof log>[1] = 'info', plane: 'ctrl' | 'data' = 'ctrl') =>
  log(`[PUB-${plane}] ${msg}`,
    level === 'sent' ? `pub-${plane}-sent` as Parameters<typeof log>[1]
    : level === 'recv' ? `pub-${plane}-recv` as Parameters<typeof log>[1]
    : level);
const logSub = (msg: string, level: Parameters<typeof log>[1] = 'info', plane: 'ctrl' | 'data' = 'ctrl') =>
  log(`[SUB-${plane}] ${msg}`,
    level === 'sent' ? `sub-${plane}-sent` as Parameters<typeof log>[1]
    : level === 'recv' ? `sub-${plane}-recv` as Parameters<typeof log>[1]
    : level);

// ─── State ──────────────────────────────────────────────────────────

let pubAdapter: MoqtAdapter | null = null;
let pubTransport: WebTransport | null = null;
let subAdapter: MoqtAdapter | null = null;
let subTransport: WebTransport | null = null;

/** Track active subscriptions: requestId → { namespace, track } */
const activeSubscriptions = new Map<bigint, { ns: string; track: string; alias?: bigint }>();

/** Track namespace subscriptions: requestId → prefix */
const activeNamespaceSubs = new Map<bigint, string>();

/** Track active fetches: requestId → { ns, track } */
const activeFetches = new Map<bigint, { ns: string; track: string }>();

/** Track published namespaces: requestId → namespace */
const publishedNamespaces = new Map<bigint, string>();

/** Track incoming subscribe requests: requestId → { ns, track } */
const incomingSubscribes = new Map<bigint, { ns: string; track: string }>();

/** Track incoming fetch requests: requestId → { ns, track, range } */
const incomingFetches = new Map<bigint, { ns: string; track: string; sg: bigint; so: bigint; eg: bigint; eo: bigint }>();

/** Accepted track aliases: alias → { ns, track, reqId, canPublishDone } */
const acceptedAliases = new Map<bigint, { ns: string; track: string; reqId: bigint; canPublishDone: boolean }>();

/** Sent objects log */
interface SentObject {
  alias: bigint;
  ns: string;
  track: string;
  groupId: bigint;
  subgroupId: bigint;
  objectId: bigint;
  streamId: bigint;
  payloadPreview: string;
  payload: Uint8Array;
}
const sentObjects: SentObject[] = [];


/** Track open outgoing subgroup streams: synthetic stream ID */
const openSubgroups = new Set<bigint>();

/** Stream IDs for incoming fetch data streams */
const fetchStreamIds = new Set<bigint>();

/** Maps fetch stream ID → { ns, track } for object viewer labels */
const fetchStreamInfo = new Map<bigint, { ns: string; track: string }>();

/** Announced namespaces from SUBSCRIBE_NAMESPACE responses: full-ns → reqId */
const announcedNamespaces = new Map<string, bigint>();

/** Object counts for object viewers */
let subObjectCount = 0;
let fetchObjectCount = 0;

/** Queue for publisher data-stream hex bytes; flushed after each logPub call */
const pubHexQueue: Uint8Array[] = [];
function flushPubHex(): void {
  for (const b of pubHexQueue) log(toHex(b), 'pub-data-hex');
  pubHexQueue.length = 0;
}

/** Pending hex bytes for an incoming NAMESPACE/NAMESPACE_DONE; flushed after label */
let subNamespaceHexPending: Uint8Array | null = null;

// ─── DOM References ─────────────────────────────────────────────────

// Publisher connection
const $pubRelayUrl = document.getElementById('pub-relay-url') as HTMLInputElement;
const $pubCertHash = document.getElementById('pub-cert-hash') as HTMLInputElement;
const $pubDraftVersion = document.getElementById('pub-draft-version') as HTMLSelectElement;
const $pubMaxRequestId = document.getElementById('pub-max-request-id') as HTMLInputElement;
const $pubBtnConnect = document.getElementById('pub-btn-connect') as HTMLButtonElement;
const $pubBtnDisconnect = document.getElementById('pub-btn-disconnect') as HTMLButtonElement;
const $pubConnStatus = document.getElementById('pub-conn-status') as HTMLSpanElement;

// Subscriber connection
const $subRelayUrl = document.getElementById('sub-relay-url') as HTMLInputElement;
const $subCertHash = document.getElementById('sub-cert-hash') as HTMLInputElement;
const $subDraftVersion = document.getElementById('sub-draft-version') as HTMLSelectElement;
const $subMaxRequestId = document.getElementById('sub-max-request-id') as HTMLInputElement;
const $subBtnConnect = document.getElementById('sub-btn-connect') as HTMLButtonElement;
const $subBtnDisconnect = document.getElementById('sub-btn-disconnect') as HTMLButtonElement;
const $subConnStatus = document.getElementById('sub-conn-status') as HTMLSpanElement;

// Publisher
const $pubNs = document.getElementById('pub-ns') as HTMLInputElement;
const $btnPubNs = document.getElementById('btn-pub-ns') as HTMLButtonElement;
const $publishedNs = document.getElementById('published-ns') as HTMLDivElement;
const $incomingSubs = document.getElementById('incoming-subs') as HTMLDivElement;
const $incomingFetches = document.getElementById('incoming-fetches') as HTMLDivElement;
const $pubTrackAlias = document.getElementById('pub-track-alias') as HTMLInputElement;
const $pubGroupId = document.getElementById('pub-group-id') as HTMLInputElement;
const $pubSubgroupId = document.getElementById('pub-subgroup-id') as HTMLInputElement;
const $pubObjectId = document.getElementById('pub-object-id') as HTMLInputElement;
const $pubStreamId = document.getElementById('pub-stream-id') as HTMLInputElement;
const $pubPayload = document.getElementById('pub-payload') as HTMLTextAreaElement;
const $btnSendObjectInSubgroup = document.getElementById('btn-send-object-in-subgroup') as HTMLButtonElement;
const $btnOpenSubgroup = document.getElementById('btn-open-subgroup') as HTMLButtonElement;
const $btnSendObject = document.getElementById('btn-send-object') as HTMLButtonElement;
const $btnCloseSubgroup = document.getElementById('btn-close-subgroup') as HTMLButtonElement;
const $trackAliasTableWrap = document.getElementById('track-alias-table-wrap') as HTMLDivElement;
const $pubPublishNs = document.getElementById('pub-publish-ns') as HTMLInputElement;
const $pubPublishTrack = document.getElementById('pub-publish-track') as HTMLInputElement;
const $pubPublishAlias = document.getElementById('pub-publish-alias') as HTMLInputElement;
const $btnPublish = document.getElementById('btn-publish') as HTMLButtonElement;
const $sentObjectsTableWrap = document.getElementById('sent-objects-table-wrap') as HTMLDivElement;
const $subNs = document.getElementById('sub-ns') as HTMLInputElement;
const $subTrack = document.getElementById('sub-track') as HTMLInputElement;
const $subFilter = document.getElementById('sub-filter') as HTMLSelectElement;
const $btnSubscribe = document.getElementById('btn-subscribe') as HTMLButtonElement;
const $activeSubs = document.getElementById('active-subs') as HTMLDivElement;
const $absStartFields = document.getElementById('abs-start-fields') as HTMLDivElement;
const $absRangeFields = document.getElementById('abs-range-fields') as HTMLDivElement;
const $subStartGroup = document.getElementById('sub-start-group') as HTMLInputElement;
const $subStartObject = document.getElementById('sub-start-object') as HTMLInputElement;
const $subEndGroup = document.getElementById('sub-end-group') as HTMLInputElement;

const $subNsPrefix = document.getElementById('sub-ns-prefix') as HTMLInputElement;
const $btnSubNs = document.getElementById('btn-sub-ns') as HTMLButtonElement;
const $activeNsSubs = document.getElementById('active-ns-subs') as HTMLDivElement;
const $announcedNs = document.getElementById('announced-ns') as HTMLDivElement;

const $fetchNs = document.getElementById('fetch-ns') as HTMLInputElement;
const $fetchTrack = document.getElementById('fetch-track') as HTMLInputElement;
const $fetchStartGroup = document.getElementById('fetch-start-group') as HTMLInputElement;
const $fetchStartObject = document.getElementById('fetch-start-object') as HTMLInputElement;
const $fetchEndGroup = document.getElementById('fetch-end-group') as HTMLInputElement;
const $fetchEndObject = document.getElementById('fetch-end-object') as HTMLInputElement;
const $btnFetch = document.getElementById('btn-fetch') as HTMLButtonElement;
const $btnFetchCancel = document.getElementById('btn-fetch-cancel') as HTMLButtonElement;

// Object viewers
const $subObjects = document.getElementById('sub-objects') as HTMLDivElement;
const $subObjCount = document.getElementById('sub-obj-count') as HTMLSpanElement;
const $btnClearSubObjects = document.getElementById('btn-clear-sub-objects') as HTMLButtonElement;
const $fetchObjects = document.getElementById('fetch-objects') as HTMLDivElement;
const $fetchObjCount = document.getElementById('fetch-obj-count') as HTMLSpanElement;
const $btnClearFetchObjects = document.getElementById('btn-clear-fetch-objects') as HTMLButtonElement;

const $tsNs = document.getElementById('ts-ns') as HTMLInputElement;
const $tsTrack = document.getElementById('ts-track') as HTMLInputElement;
const $btnTrackStatus = document.getElementById('btn-track-status') as HTMLButtonElement;

const $btnClearLog = document.getElementById('btn-clear-log') as HTMLButtonElement;
const $chkShowHex = document.getElementById('chk-show-hex') as HTMLInputElement;
const $chkShowNsOps = document.getElementById('chk-show-ns-ops') as HTMLInputElement;
const $chkShowFetchOps = document.getElementById('chk-show-fetch-ops') as HTMLInputElement;
const $logEl = document.getElementById('log')!;
const $pubIdleTimer = document.getElementById('pub-idle-timer') as HTMLSpanElement;
const $subIdleTimer = document.getElementById('sub-idle-timer') as HTMLSpanElement;;

const enc = new TextEncoder();
const dec = new TextDecoder();

// ─── Hex helper ─────────────────────────────────────────────────────

function toHex(bytes: Uint8Array): string {
  return '    ' + Array.from(bytes, b => (b ?? 0).toString(16).padStart(2, '0')).join(' ');
}

// ─── Connection lost flags ──────────────────────────────────────────
// Set to true before a deliberate disconnect so cleanupPub/Sub
// know the loss wasn't unexpected.
let pubUserDisconnecting = false;
let subUserDisconnecting = false;

// ─── Idle timeout timer ──────────────────────────────────────────────
const IDLE_TIMEOUT_S = 60;
let pubLastActivity = 0;
let subLastActivity = 0;
let pubIdleIntervalId: ReturnType<typeof setInterval> | null = null;
let subIdleIntervalId: ReturnType<typeof setInterval> | null = null;

function bumpPubIdle(): void { pubLastActivity = Date.now(); }
function bumpSubIdle(): void { subLastActivity = Date.now(); }

function startPubIdleTimer(): void {
  bumpPubIdle();
  $pubIdleTimer.classList.remove('hidden');
  if (pubIdleIntervalId !== null) clearInterval(pubIdleIntervalId);
  pubIdleIntervalId = setInterval(() => {
    const remaining = Math.max(0, IDLE_TIMEOUT_S - (Date.now() - pubLastActivity) / 1000);
    $pubIdleTimer.textContent = `Idle: ${Math.ceil(remaining)}s`;
    $pubIdleTimer.classList.toggle('idle-timer-warn', remaining < 10);
  }, 500);
}

function stopPubIdleTimer(): void {
  if (pubIdleIntervalId !== null) { clearInterval(pubIdleIntervalId); pubIdleIntervalId = null; }
  $pubIdleTimer.classList.add('hidden');
}

function startSubIdleTimer(): void {
  bumpSubIdle();
  $subIdleTimer.classList.remove('hidden');
  if (subIdleIntervalId !== null) clearInterval(subIdleIntervalId);
  subIdleIntervalId = setInterval(() => {
    const remaining = Math.max(0, IDLE_TIMEOUT_S - (Date.now() - subLastActivity) / 1000);
    $subIdleTimer.textContent = `Idle: ${Math.ceil(remaining)}s`;
    $subIdleTimer.classList.toggle('idle-timer-warn', remaining < 10);
  }, 500);
}

function stopSubIdleTimer(): void {
  if (subIdleIntervalId !== null) { clearInterval(subIdleIntervalId); subIdleIntervalId = null; }
  $subIdleTimer.classList.add('hidden');
}

// ─── Object Viewer Helpers ───────────────────────────────────────────

function appendToObjectViewer(
  container: HTMLDivElement,
  countEl: HTMLSpanElement,
  count: number,
  label: string,
  text: string,
): number {
  const isFirst = container.querySelector('em') !== null;
  if (isFirst) container.innerHTML = '';
  const div = document.createElement('div');
  div.className = 'sub-item';
  div.innerHTML = `<span class="sub-info">[${count + 1}] ${label} | payload=${text}</span>`;
  container.appendChild(div);
  container.scrollTop = container.scrollHeight;
  const next = count + 1;
  countEl.textContent = `${next} object${next === 1 ? '' : 's'}`;
  return next;
}

function clearObjectViewer(container: HTMLDivElement, countEl: HTMLSpanElement): number {
  container.innerHTML = '<em>No objects received yet</em>';
  countEl.textContent = '0 objects';
  return 0;
}

// ─── Helpers ────────────────────────────────────────────────────────

function nsToBytes(ns: string): Uint8Array[] {
  return ns.split('/').map(s => enc.encode(s));
}

function bytesToNs(parts: Uint8Array[]): string {
  return parts.map(p => dec.decode(p)).join('/');
}

function parseCertHash(hex: string): ArrayBuffer | undefined {
  const clean = hex.replace(/[^0-9a-fA-F]/g, '');
  if (!clean) return undefined;
  if (clean.length % 2 !== 0) {
    throw new Error(`Invalid cert hash: odd number of hex chars`);
  }
  const bytes = new Uint8Array(clean.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes.buffer;
}

// SUBSCRIPTION_FILTER byte[0] type code → name
const FILTER_TYPE_NAMES: Record<number, string> = {
  1: 'NextGroupStart', 2: 'LatestObject', 3: 'AbsoluteStart', 4: 'AbsoluteRange',
};

function formatKvpValue(name: string, val: unknown): string {
  if (val instanceof Uint8Array) {
    // SUBSCRIPTION_FILTER is a binary blob; first byte is the filter type varint
    if (name === 'SUBSCRIPTION_FILTER' && val.length > 0) {
      const typeCode = val[0] ?? 0;
      return FILTER_TYPE_NAMES[typeCode] ?? `0x${typeCode.toString(16)}`;
    }
    // Other byte values: try to decode as text, fall back to hex
    try { return dec.decode(val); } catch { return `[${val.length}B]`; }
  }
  // Varint (branded bigint) — just stringify
  return String(val);
}

function formatMessage(msg: ControlMessage): string {
  const parts: string[] = [msg.type];
  const m = msg as unknown as Record<string, unknown>;

  if ('requestId' in m)     parts.push(`REQ_ID=${m['requestId']}`);
  if ('trackAlias' in m)    parts.push(`TRACK_ALIAS=${m['trackAlias']}`);
  if ('trackNamespace' in m && Array.isArray(m['trackNamespace'])) {
    parts.push(`NAMESPACE=${bytesToNs(m['trackNamespace'] as Uint8Array[])}`);
  }
  if ('trackName' in m && m['trackName'] instanceof Uint8Array) {
    parts.push(`TRACK=${dec.decode(m['trackName'] as Uint8Array)}`);
  }
  if ('trackNamespacePrefix' in m && Array.isArray(m['trackNamespacePrefix'])) {
    parts.push(`PREFIX=${bytesToNs(m['trackNamespacePrefix'] as Uint8Array[])}`);
  }
  if ('trackNamespaceSuffix' in m && Array.isArray(m['trackNamespaceSuffix'])) {
    const suffix = bytesToNs(m['trackNamespaceSuffix'] as Uint8Array[]);
    parts.push(`SUFFIX=${suffix || '(empty)'}`);
  }
  if ('errorCode' in m)     parts.push(`ERROR_CODE=0x${BigInt(m['errorCode'] as any).toString(16)}`);
  if ('errorReason' in m && m['errorReason']) parts.push(`REASON="${m['errorReason']}"`);
  if ('statusCode' in m)    parts.push(`STATUS=${m['statusCode']}`);
  if ('newSessionUri' in m) parts.push(`URI="${m['newSessionUri']}"`);
  if ('maxRequestId' in m && msg.type === 'MAX_REQUEST_ID') parts.push(`MAX_REQUEST_ID=${m['maxRequestId']}`);
  if ('existingRequestId' in m) parts.push(`EXISTING_REQ_ID=${m['existingRequestId']}`);
  if ('streamCount' in m)   parts.push(`STREAMS=${m['streamCount']}`);
  if ('retryInterval' in m) parts.push(`RETRY_MS=${m['retryInterval']}`);

  // FETCH: namespace/track/range are nested under msg.fetch
  if (msg.type === 'FETCH' && 'fetch' in m && m['fetch'] && typeof m['fetch'] === 'object') {
    const f = m['fetch'] as Record<string, unknown>;
    if (f['fetchType'] === 0x1) {
      // StandaloneFetch
      if (Array.isArray(f['trackNamespace'])) parts.push(`NAMESPACE=${bytesToNs(f['trackNamespace'] as Uint8Array[])}`);
      if (f['trackName'] instanceof Uint8Array) parts.push(`TRACK=${dec.decode(f['trackName'] as Uint8Array)}`);
      if (f['startLocation'] && typeof f['startLocation'] === 'object') {
        const s = f['startLocation'] as { group: unknown; object: unknown };
        parts.push(`START=${s.group}:${s.object}`);
      }
      if (f['endLocation'] && typeof f['endLocation'] === 'object') {
        const e = f['endLocation'] as { group: unknown; object: unknown };
        parts.push(`END=${e.group}:${e.object}`);
      }
    } else {
      // JoiningFetch
      parts.push(`FETCH_TYPE=${f['fetchType'] === 0x2 ? 'joining-rel' : 'joining-abs'}`);
      if ('joiningRequestId' in f) parts.push(`JOINING_REQ_ID=${f['joiningRequestId']}`);
      if ('joiningStart' in f) parts.push(`JOINING_START=${f['joiningStart']}`);
    }
  }

  // FETCH_OK: end location
  if (msg.type === 'FETCH_OK' && 'endLocation' in m && m['endLocation'] && typeof m['endLocation'] === 'object') {
    const e = m['endLocation'] as { group: unknown; object: unknown };
    parts.push(`END_LOCATION=${e.group}:${e.object}`);
    if ('endOfTrack' in m) parts.push(`END_OF_TRACK=${m['endOfTrack']}`);
  }

  // Decode known parameters maps (setup + message params)
  if ('parameters' in m && m['parameters'] instanceof Map) {
    const params = m['parameters'] as Map<bigint, unknown[]>;
    const setupParamNames: Record<string, string> = {
      '1': 'PATH', '2': 'MAX_REQUEST_ID', '3': 'AUTHORIZATION_TOKEN',
      '4': 'MAX_AUTH_TOKEN_CACHE_SIZE', '5': 'AUTHORITY', '7': 'MOQT_IMPLEMENTATION',
    };
    const msgParamNames: Record<string, string> = {
      '2': 'DELIVERY_TIMEOUT', '3': 'AUTHORIZATION_TOKEN', '8': 'EXPIRES',
      '9': 'LARGEST_OBJECT', '16': 'FORWARD', '32': 'SUBSCRIBER_PRIORITY',
      '33': 'SUBSCRIPTION_FILTER', '34': 'GROUP_ORDER', '50': 'NEW_GROUP_REQUEST',
    };
    const nameMap = (msg.type === 'CLIENT_SETUP' || msg.type === 'SERVER_SETUP')
      ? setupParamNames : msgParamNames;
    for (const [key, values] of params) {
      const name = nameMap[String(key)] ?? `PARAM_0x${key.toString(16)}`;
      const val = (values as unknown[])[0];
      parts.push(`${name}=${formatKvpValue(name, val)}`);
    }
  }

  return parts.join(' | ');
}

function setPubConnected(state: 'disconnected' | 'connecting' | 'connected' | 'lost'): void {
  const labels: Record<string, string> = {
    disconnected: 'Disconnected', connecting: 'Connecting...', connected: 'Connected', lost: 'Lost',
  };
  $pubConnStatus.textContent = labels[state] ?? state;
  $pubConnStatus.className = `status ${state}`;
  const connected = state === 'connected';
  setConnectionState('pub', connected);
  $pubBtnConnect.disabled = state === 'connecting' || state === 'connected';
  $pubBtnDisconnect.disabled = !connected;

  const pubButtons = [
    $btnPubNs,
    $btnPublish,
    $btnSendObjectInSubgroup, $btnOpenSubgroup, $btnSendObject, $btnCloseSubgroup,
  ];
  for (const btn of pubButtons) {
    btn.disabled = !connected;
  }
}

function setSubConnected(state: 'disconnected' | 'connecting' | 'connected' | 'lost'): void {
  const labels: Record<string, string> = {
    disconnected: 'Disconnected', connecting: 'Connecting...', connected: 'Connected', lost: 'Lost',
  };
  $subConnStatus.textContent = labels[state] ?? state;
  $subConnStatus.className = `status ${state}`;
  const connected = state === 'connected';
  setConnectionState('sub', connected);
  $subBtnConnect.disabled = state === 'connecting' || state === 'connected';
  $subBtnDisconnect.disabled = !connected;

  const subButtons = [
    $btnSubscribe, $btnSubNs,
    $btnFetch, $btnFetchCancel,
    $btnTrackStatus,
  ];
  for (const btn of subButtons) {
    btn.disabled = !connected;
  }
}

function renderActiveNsSubs(): void {
  if (activeNamespaceSubs.size === 0) {
    $activeNsSubs.innerHTML = '<em>No active namespace subscriptions</em>';
    return;
  }
  $activeNsSubs.innerHTML = '';
  for (const [reqId, prefix] of activeNamespaceSubs) {
    const div = document.createElement('div');
    div.className = 'sub-item';
    div.innerHTML = `<span class="sub-info">reqId=${reqId} | prefix=${prefix}</span><button class="btn-cancel-ns-item" data-req-id="${reqId}">Cancel SUBSCRIBE_NAMESPACE</button>`;
    $activeNsSubs.appendChild(div);
  }
  for (const btn of $activeNsSubs.querySelectorAll('.btn-cancel-ns-item')) {
    btn.addEventListener('click', () => doCancelNamespace(BigInt(btn.getAttribute('data-req-id')!)));
  }
}

function renderActiveSubs(): void {
  if (activeSubscriptions.size === 0) {
    $activeSubs.innerHTML = '<em>No active subscriptions</em>';
    return;
  }
  $activeSubs.innerHTML = '';
  for (const [reqId, info] of activeSubscriptions) {
    const div = document.createElement('div');
    div.className = 'sub-item';
    div.innerHTML = `
      <span class="sub-info">reqId=${reqId} ns=${info.ns} track=${info.track}${info.alias !== undefined ? ` alias=${info.alias}` : ''}</span>
      <button data-req-id="${reqId}" class="btn-unsub">Unsubscribe</button>
      <button data-req-id="${reqId}" class="btn-pause">Pause</button>
      <button data-req-id="${reqId}" class="btn-resume">Resume</button>
    `;
    $activeSubs.appendChild(div);
  }

  // Wire up buttons
  for (const btn of $activeSubs.querySelectorAll('.btn-unsub')) {
    btn.addEventListener('click', () => doUnsubscribe(BigInt(btn.getAttribute('data-req-id')!)));
  }
  for (const btn of $activeSubs.querySelectorAll('.btn-pause')) {
    btn.addEventListener('click', () => doPauseResume(BigInt(btn.getAttribute('data-req-id')!), 0));
  }
  for (const btn of $activeSubs.querySelectorAll('.btn-resume')) {
    btn.addEventListener('click', () => doPauseResume(BigInt(btn.getAttribute('data-req-id')!), 1));
  }
}

function renderIncomingSubs(): void {
  if (incomingSubscribes.size === 0) {
    $incomingSubs.innerHTML = '<em>No incoming subscriptions</em>';
    return;
  }
  $incomingSubs.innerHTML = '';
  for (const [reqId, info] of incomingSubscribes) {
    const div = document.createElement('div');
    div.className = 'sub-item';
    div.innerHTML = `<span class="sub-info">reqId=${reqId} ns=${info.ns} track=${info.track}</span><button data-req-id="${reqId}" class="btn-accept primary">Accept</button><button data-req-id="${reqId}" class="btn-reject danger">Reject</button>`;
    $incomingSubs.appendChild(div);
  }

  for (const btn of $incomingSubs.querySelectorAll('.btn-accept')) {
    btn.addEventListener('click', () => doAcceptSubscribe(BigInt(btn.getAttribute('data-req-id')!)));
  }
  for (const btn of $incomingSubs.querySelectorAll('.btn-reject')) {
    btn.addEventListener('click', () => doRejectSubscribe(BigInt(btn.getAttribute('data-req-id')!)));
  }
}

function renderIncomingFetches(): void {
  if (incomingFetches.size === 0) {
    $incomingFetches.innerHTML = '<em>No incoming fetches</em>';
    return;
  }
  $incomingFetches.innerHTML = '';
  for (const [reqId, info] of incomingFetches) {
    const div = document.createElement('div');
    div.className = 'sub-item';
    div.innerHTML = `<span class="sub-info">reqId=${reqId} ns=${info.ns} track=${info.track} range=${info.sg}:${info.so}–${info.eg}:${info.eo}</span><button data-req-id="${reqId}" class="btn-accept-fetch primary">Accept</button><button data-req-id="${reqId}" class="btn-reject-fetch danger">Reject</button>`;
    $incomingFetches.appendChild(div);
  }
  for (const btn of $incomingFetches.querySelectorAll('.btn-accept-fetch')) {
    btn.addEventListener('click', () => doAcceptFetch(BigInt(btn.getAttribute('data-req-id')!)));
  }
  for (const btn of $incomingFetches.querySelectorAll('.btn-reject-fetch')) {
    btn.addEventListener('click', () => doRejectFetch(BigInt(btn.getAttribute('data-req-id')!)));
  }
}

function renderPublishedNs(): void {
  if (publishedNamespaces.size === 0) {
    $publishedNs.innerHTML = '<em>No published namespaces</em>';
    return;
  }
  $publishedNs.innerHTML = '';
  for (const [reqId, ns] of publishedNamespaces) {
    const div = document.createElement('div');
    div.className = 'sub-item';
    div.innerHTML = `<span class="sub-info">reqId=${reqId} | ns=${ns}</span><button class="btn-remove-ns" data-req-id="${reqId}">Publish Namespace Done</button>`;
    $publishedNs.appendChild(div);
  }
  for (const btn of $publishedNs.querySelectorAll('.btn-remove-ns')) {
    btn.addEventListener('click', () => doRemovePublishedNs(BigInt(btn.getAttribute('data-req-id')!)));
  }
}

function renderAnnouncedNs(): void {
  if (announcedNamespaces.size === 0) {
    $announcedNs.innerHTML = '<em>No announcements received</em>';
    return;
  }
  $announcedNs.innerHTML = '';
  for (const [ns, reqId] of announcedNamespaces) {
    const div = document.createElement('div');
    div.className = 'sub-item';
    div.innerHTML = `<span class="sub-info" title="reqId=${reqId}">${ns}</span>`;
    $announcedNs.appendChild(div);
  }
}

function renderTrackAliasTable(): void {
  if (acceptedAliases.size === 0) {
    $trackAliasTableWrap.innerHTML = '<em class="empty-note">No accepted subscriptions yet</em>';
    return;
  }
  $trackAliasTableWrap.innerHTML = '';
  for (const [alias, info] of acceptedAliases) {
    const div = document.createElement('div');
    div.className = 'sub-item';
    div.innerHTML = `<span class="sub-info">alias=${alias} ns=${info.ns} track=${info.track}</span><button class="btn-use-alias" data-alias="${alias}">Use</button>${info.canPublishDone ? `<button class="btn-publish-done" data-req-id="${info.reqId}">Publish Done</button>` : ''}`;
    $trackAliasTableWrap.appendChild(div);
  }
  for (const btn of $trackAliasTableWrap.querySelectorAll('.btn-use-alias')) {
    btn.addEventListener('click', () => {
      $pubTrackAlias.value = btn.getAttribute('data-alias')!;
    });
  }
  for (const btn of $trackAliasTableWrap.querySelectorAll('.btn-publish-done')) {
    btn.addEventListener('click', () => doPublishDone(BigInt(btn.getAttribute('data-req-id')!)));
  }
}

function renderSentObjectsTable(): void {
  if (sentObjects.length === 0) {
    $sentObjectsTableWrap.innerHTML = '<em class="empty-note">No objects sent yet</em>';
    return;
  }
  $sentObjectsTableWrap.innerHTML = '';
  sentObjects.forEach((obj, i) => {
    const div = document.createElement('div');
    div.className = 'sub-item';
    const trackPart = obj.track ? ` (${obj.track})` : '';
    const label = `[${i + 1}] alias=${obj.alias}${trackPart} | grp=${obj.groupId} | sub=${obj.subgroupId} | obj=${obj.objectId} | payload=${obj.payloadPreview}`;
    div.innerHTML = `<span class="sub-info">${label}</span><button class="btn-copy-fetch" data-i="${i}">Copy to Fetch</button>`;
    $sentObjectsTableWrap.appendChild(div);
  });
  for (const btn of $sentObjectsTableWrap.querySelectorAll('.btn-copy-fetch')) {
    btn.addEventListener('click', () => {
      const obj = sentObjects[parseInt(btn.getAttribute('data-i')!)];
      if (!obj) return;
      $fetchNs.value = obj.ns;
      $fetchTrack.value = obj.track;
      $fetchStartGroup.value = obj.groupId.toString();
      $fetchStartObject.value = obj.objectId.toString();
      // End is exclusive — set to one past the last object
      $fetchEndGroup.value = obj.groupId.toString();
      $fetchEndObject.value = (obj.objectId + 1n).toString();
    });
  }
}

// ─── URL Params (pre-fill from query string) ────────────────────────

function loadFromParams(): void {
  const params = new URLSearchParams(window.location.search);
  const url = params.get('url');
  const ns = params.get('ns');
  const track = params.get('track');
  const hash = params.get('hash');
  const v = params.get('v');
  const mode = params.get('mode');
  if (mode === 'pub') {
    document.getElementById('subscriber-panel')!.classList.add('hidden');
    document.getElementById('panels')!.classList.add('single-panel');
  } else if (mode === 'sub') {
    document.getElementById('publisher-panel')!.classList.add('hidden');
    document.getElementById('panels')!.classList.add('single-panel');
  }

  if (url) {
    $pubRelayUrl.value = url;
    $subRelayUrl.value = url;
  }
  if (hash) {
    $pubCertHash.value = hash;
    $subCertHash.value = hash;
  }
  if (v) {
    $pubDraftVersion.value = v;
    $subDraftVersion.value = v;
  }
  if (ns) {
    $pubNs.value = ns;
    $pubPublishNs.value = ns;
    $subNs.value = ns;
    $subNsPrefix.value = ns;
    $fetchNs.value = ns;
    $tsNs.value = ns;
  }
  if (track) {
    $pubPublishTrack.value = track;
    $subTrack.value = track;
    $fetchTrack.value = track;
    $tsTrack.value = track;
  }
}

// ─── Connection ─────────────────────────────────────────────────────

async function doPubConnect(): Promise<void> {
  const url = $pubRelayUrl.value.trim();
  if (!url) {
    logPub('Relay URL is required', 'error');
    return;
  }

  const version = parseInt($pubDraftVersion.value) as DraftVersion;
  const maxReqId = parseInt($pubMaxRequestId.value) || 100;

  setPubConnected('connecting');
  logPub(`Connecting to ${url} (draft-${version})...`);

  try {
    let certHashBuf: ArrayBuffer | undefined;
    const hashStr = $pubCertHash.value.trim();
    if (hashStr) {
      certHashBuf = parseCertHash(hashStr);
    }

    const opts: WebTransportOptions = {};
    if (certHashBuf) {
      opts.serverCertificateHashes = [{ algorithm: 'sha-256', value: certHashBuf }];
    }
    (opts as any).protocols = [`moqt-${version}`];

    pubTransport = new WebTransport(url, opts);
    await pubTransport.ready;
    logPub('WebTransport connected', 'info');

    pubAdapter = new MoqtAdapter(version);

    pubAdapter.onMessage = (_msg: ControlMessage) => {};

    pubAdapter.onRawMessage = (msg: ControlMessage, direction, bytes) => {
      bumpPubIdle();
      const level = direction === 'sent' ? 'sent' : 'recv';
      const prefix = direction === 'sent' ? '→ ' : '← ';
      addMessage('pub', direction as 'sent' | 'recv', 'ctrl', `${prefix}${msg.type}`);
      // For PUBLISH_NAMESPACE_DONE, embed the namespace in parens since the
      // wire message only carries the request ID.
      let label = formatMessage(msg);
      if (msg.type === 'PUBLISH_NAMESPACE_DONE' && 'requestId' in msg) {
        const ns = publishedNamespaces.get(msg.requestId as bigint);
        if (ns) label = `${label} (${ns})`;
      }
      logPub(`${prefix}${label}`, level);
      log(toHex(bytes), 'hex');
    };

    pubAdapter.onSentDataBytes = (_streamId, bytes) => {
      bumpPubIdle();
      pubHexQueue.push(bytes);
      addMessage('pub', 'sent', 'data', `data ${bytes.byteLength}B`);
    };

    pubAdapter.onClose = (error, reason) => {
      logPub(`Session closed: error=${error ?? 'none'} reason=${reason ?? ''}`, 'error');
      cleanupPub();
    };

    pubAdapter.onError = (error) => {
      logPub(`Session error: ${error.message}`, 'error');
      cleanupPub();
    };

    pubAdapter.onNamespaceMessage = (requestId, msg) => {
      logPub(`Namespace[${requestId}]: ${formatMessage(msg)}`, 'recv');
    };

    pubAdapter.onSubscribe = (requestId, namespace, trackName, _params) => {
      const ns = bytesToNs(namespace);
      const track = dec.decode(trackName);
      incomingSubscribes.set(BigInt(requestId), { ns, track });
      renderIncomingSubs();
    };

    pubAdapter.onFetch = (requestId, namespace, trackName, sg, so, eg, eo) => {
      const ns = bytesToNs(namespace);
      const track = dec.decode(trackName);
      incomingFetches.set(BigInt(requestId), {
        ns, track,
        sg: BigInt(sg), so: BigInt(so),
        eg: BigInt(eg), eo: BigInt(eo),
      });
      renderIncomingFetches();
    };

    await pubAdapter.connect(pubTransport, {
      maxRequestId: varint(BigInt(maxReqId)),
    });

    setPubConnected('connected');
    startPubIdleTimer();
    logPub(`Session established (draft-${pubAdapter.draftVersion})`, 'info');

    pubTransport.closed.then(({ closeCode, reason }) => {
      logPub(`Transport closed: code=${closeCode ?? 'none'} reason=${reason ?? ''}`, 'info');
      cleanupPub();
    }).catch(() => {});

  } catch (err) {
    logPub(`Connection failed: ${(err as Error).message}`, 'error');
    cleanupPub();
  }
}

async function doSubConnect(): Promise<void> {
  const url = $subRelayUrl.value.trim();
  if (!url) {
    logSub('Relay URL is required', 'error');
    return;
  }

  const version = parseInt($subDraftVersion.value) as DraftVersion;
  const maxReqId = parseInt($subMaxRequestId.value) || 100;

  setSubConnected('connecting');
  logSub(`Connecting to ${url} (draft-${version})...`);

  try {
    let certHashBuf: ArrayBuffer | undefined;
    const hashStr = $subCertHash.value.trim();
    if (hashStr) {
      certHashBuf = parseCertHash(hashStr);
    }

    const opts: WebTransportOptions = {};
    if (certHashBuf) {
      opts.serverCertificateHashes = [{ algorithm: 'sha-256', value: certHashBuf }];
    }
    (opts as any).protocols = [`moqt-${version}`];

    subTransport = new WebTransport(url, opts);
    await subTransport.ready;
    logSub('WebTransport connected', 'info');

    subAdapter = new MoqtAdapter(version);

    subAdapter.onMessage = (msg: ControlMessage) => {
      handleIncomingMessage(msg);
    };

    subAdapter.onRawMessage = (msg: ControlMessage, direction, bytes) => {
      bumpSubIdle();
      const level = direction === 'sent' ? 'sent' : 'recv';
      const arrow = direction === 'sent' ? '→ ' : '← ';
      addMessage('sub', direction as 'sent' | 'recv', 'ctrl', `${arrow}${msg.type}`);
      if (msg.type === 'NAMESPACE' || msg.type === 'NAMESPACE_DONE') {
        // Queue hex; label + hex are emitted in onNamespaceMessage (after the label)
        subNamespaceHexPending = bytes;
        return;
      }
      let label = formatMessage(msg);
      if (msg.type === 'REQUEST_OK' && 'requestId' in msg) {
        const ns = activeNamespaceSubs.get(msg.requestId as bigint);
        if (ns) label = `${label} (ns[${msg.requestId}]=${ns})`;
      }
      logSub(`${arrow}${label}`, level);
      log(toHex(bytes), 'hex');
    };

    subAdapter.onReceivedDataBytes = (_streamId, bytes) => {
      bumpSubIdle();
      log(toHex(bytes), 'sub-data-hex');
      addMessage('sub', 'recv', 'data', `data ${bytes.byteLength}B`);
    };

    subAdapter.onClose = (error, reason) => {
      logSub(`Session closed: error=${error ?? 'none'} reason=${reason ?? ''}`, 'error');
      cleanupSub();
    };

    subAdapter.onError = (error) => {
      logSub(`Session error: ${error.message}`, 'error');
      cleanupSub();
    };

    subAdapter.onObject = (streamId, obj) => {
      const isFetch = fetchStreamIds.has(streamId);
      if (obj.kind === 'gap') {
        logSub(`Object gap: TRACK_ALIAS=${obj.trackAlias} GROUP_ID=${obj.groupId} OBJ_ID=${obj.objectId} STATUS=${obj.status}`, 'recv', 'data');
        const label = `gap g=${obj.groupId} obj=${obj.objectId} status=${obj.status}`;
        if (isFetch) {
          fetchObjectCount = appendToObjectViewer($fetchObjects, $fetchObjCount, fetchObjectCount, label, '[gap]');
        } else {
          subObjectCount = appendToObjectViewer($subObjects, $subObjCount, subObjectCount, label, '[gap]');
        }
      } else {
        const payload = obj.payload;
        const HEX_LIMIT = 100;
        const text = payload
          ? (payload.byteLength <= 200
              ? dec.decode(payload)
              : `[${payload.byteLength} bytes]`)
          : '[empty]';
        logSub(`Object: TRACK_ALIAS=${obj.trackAlias} GROUP_ID=${obj.groupId} SUBGROUP_ID=${obj.subgroupId} OBJ_ID=${obj.objectId} PAYLOAD=${text}`, 'recv', 'data');
        const trackName = activeSubscriptions.get(BigInt(obj.trackAlias as bigint))?.track
          ?? acceptedAliases.get(BigInt(obj.trackAlias as bigint))?.track;
        const label = isFetch
          ? (() => { const fi = fetchStreamInfo.get(streamId); return `${fi ? `ns=${fi.ns} | track=${fi.track} | ` : ''}grp=${obj.groupId} | sub=${obj.subgroupId} | obj=${obj.objectId}`; })()
          : `alias=${obj.trackAlias}${trackName ? ` (${trackName})` : ''} | grp=${obj.groupId} | sub=${obj.subgroupId} | obj=${obj.objectId}`;
        if (isFetch) {
          fetchObjectCount = appendToObjectViewer($fetchObjects, $fetchObjCount, fetchObjectCount, label, text);
        } else {
          subObjectCount = appendToObjectViewer($subObjects, $subObjCount, subObjectCount, label, text);
        }
      }
    };

    subAdapter.onDataStream = (streamId, header) => {
      logSub(`Data stream: TYPE=${header.type}${header.type === 'fetch' ? ` REQ_ID=${header.header.requestId}` : ` TRACK_ALIAS=${header.header.trackAlias} GROUP_ID=${header.header.groupId}`}`, 'recv', 'data');
      if (header.type === 'fetch') {
        fetchStreamIds.add(streamId);
        const reqId = BigInt(header.header.requestId as bigint);
        const info = activeFetches.get(reqId);
        if (info) fetchStreamInfo.set(streamId, info);
      }
    };

    subAdapter.onStreamClosed = (streamId, errorCode) => {
      const isFetch = fetchStreamIds.has(streamId);
      if (errorCode !== undefined) {
        // Stream was reset (RESET_STREAM) — relay aborted delivery
        logSub(`Data stream closed: STREAM_ID=${streamId}${isFetch ? ' (fetch)' : ''} RESET_CODE=${errorCode}`, 'error', 'data');
      }
      fetchStreamIds.delete(streamId);
      fetchStreamInfo.delete(streamId);
    };

    subAdapter.onDatagram = (datagram) => {
      logSub(`Datagram: TRACK_ALIAS=${datagram.trackAlias} GROUP_ID=${datagram.groupId} OBJ_ID=${datagram.objectId}`, 'recv', 'data');
    };

    subAdapter.onNamespaceMessage = (requestId, msg) => {
      const m = msg as unknown as Record<string, unknown>;
      if ((msg.type === 'NAMESPACE' || msg.type === 'NAMESPACE_DONE') && Array.isArray(m['trackNamespaceSuffix'])) {
        const suffix = bytesToNs(m['trackNamespaceSuffix'] as Uint8Array[]);
        const nsPrefix = activeNamespaceSubs.get(BigInt(requestId)) ?? '';
        const full = nsPrefix && suffix ? `${nsPrefix}/${suffix}` : nsPrefix || suffix;
        logSub(`← ${formatMessage(msg)} (ns[${requestId}]=${full})`, 'recv');
        if (subNamespaceHexPending) { log(toHex(subNamespaceHexPending), 'hex'); subNamespaceHexPending = null; }
        if (msg.type === 'NAMESPACE') {
          announcedNamespaces.set(full, BigInt(requestId));
          renderAnnouncedNs();
        } else {
          announcedNamespaces.delete(full);
          renderAnnouncedNs();
        }
      }
      // REQUEST_OK/REQUEST_ERROR are already logged with ns context in onRawMessage
    };

    await subAdapter.connect(subTransport, {
      maxRequestId: varint(BigInt(maxReqId)),
    });

    setSubConnected('connected');
    startSubIdleTimer();
    logSub(`Session established (draft-${subAdapter.draftVersion})`, 'info');

    subTransport.closed.then(({ closeCode, reason }) => {
      logSub(`Transport closed: code=${closeCode ?? 'none'} reason=${reason ?? ''}`, 'info');
      cleanupSub();
    }).catch(() => {});

  } catch (err) {
    logSub(`Connection failed: ${(err as Error).message}`, 'error');
    cleanupSub();
  }
}

async function doPubDisconnect(): Promise<void> {
  pubUserDisconnecting = true;
  if (pubAdapter) {
    logPub('Closing session...', 'sent');
    try {
      await pubAdapter.close();
    } catch {
      // Ignore close errors
    }
  }
  cleanupPub();
}

async function doSubDisconnect(): Promise<void> {
  subUserDisconnecting = true;
  if (subAdapter) {
    logSub('Closing session...', 'sent');
    try {
      await subAdapter.close();
    } catch {
      // Ignore close errors
    }
  }
  cleanupSub();
}

function cleanupPub(): void {
  if (!pubAdapter && !pubTransport) return; // already cleaned up
  // Silence all further callbacks on the old adapter before discarding it.
  // The three background loops each fire onError independently on drop,
  // so without this the same error would log 3+ times.
  if (pubAdapter) {
    pubAdapter.onError = undefined;
    pubAdapter.onClose = undefined;
  }
  const lost = !pubUserDisconnecting;
  pubUserDisconnecting = false;
  pubAdapter = null;
  pubTransport = null;
  publishedNamespaces.clear();
  renderPublishedNs();
  incomingSubscribes.clear();
  incomingFetches.clear();
  openSubgroups.clear();
  acceptedAliases.clear();
  sentObjects.length = 0;
  renderIncomingSubs();
  renderIncomingFetches();
  renderTrackAliasTable();
  renderSentObjectsTable();
  stopPubIdleTimer();
  setPubConnected(lost ? 'lost' : 'disconnected');
}

function cleanupSub(): void {
  if (!subAdapter && !subTransport) return; // already cleaned up
  if (subAdapter) {
    subAdapter.onError = undefined;
    subAdapter.onClose = undefined;
  }
  const lost = !subUserDisconnecting;
  subUserDisconnecting = false;
  subAdapter = null;
  subTransport = null;
  activeSubscriptions.clear();
  activeNamespaceSubs.clear();
  activeFetches.clear();
  fetchStreamIds.clear();
  fetchStreamInfo.clear();
  announcedNamespaces.clear();
  renderActiveSubs();
  renderActiveNsSubs();
  renderAnnouncedNs();
  stopSubIdleTimer();
  setSubConnected(lost ? 'lost' : 'disconnected');
}

// ─── Control Message Handlers ───────────────────────────────────────

function handleIncomingMessage(msg: ControlMessage): void {
  switch (msg.type) {
    case 'SUBSCRIBE_OK': {
      const reqId = BigInt(msg.requestId);
      const sub = activeSubscriptions.get(reqId);
      if (sub) {
        sub.alias = BigInt(msg.trackAlias);
        renderActiveSubs();
      }
      break;
    }
    case 'REQUEST_ERROR': {
      const reqId = BigInt(msg.requestId);
      activeSubscriptions.delete(reqId);
      activeFetches.delete(reqId);
      activeNamespaceSubs.delete(reqId);
      renderActiveSubs();
      renderActiveNsSubs();
      break;
    }
    case 'PUBLISH_DONE': {
      const reqId = BigInt(msg.requestId);
      activeSubscriptions.delete(reqId);
      renderActiveSubs();
      break;
    }
    case 'REQUEST_OK': {
      // Could be response to publish namespace, subscribe namespace, etc.
      break;
    }
    case 'GOAWAY': {
      log(`Server sent GOAWAY: uri="${msg.newSessionUri}"`, 'error');
      break;
    }
  }
}

// ─── Publisher Actions ──────────────────────────────────────────────

async function doPublishNamespace(): Promise<void> {
  if (!pubAdapter) return;
  const ns = $pubNs.value.trim();
  if (!ns) { logPub('Namespace is required', 'error'); return; }

  try {
    const reqId = await pubAdapter.publishNamespace(nsToBytes(ns));
    publishedNamespaces.set(BigInt(reqId), ns);
    renderPublishedNs();
  } catch (err) {
    logPub(`PUBLISH_NAMESPACE failed: ${(err as Error).message}`, 'error');
  }
}

async function doRemovePublishedNs(reqId: bigint): Promise<void> {
  const ns = publishedNamespaces.get(reqId);
  if (!ns) return;
  if (pubAdapter) {
    try {
      await pubAdapter.publishNamespaceDone(varint(reqId));
    } catch (err) {
      logPub(`PUBLISH_NAMESPACE_DONE failed: ${(err as Error).message}`, 'error');
    }
  }
  publishedNamespaces.delete(reqId);
  renderPublishedNs();
}

async function doPublish(): Promise<void> {
  if (!pubAdapter) return;
  const ns = $pubPublishNs.value.trim();
  const track = $pubPublishTrack.value.trim();
  if (!ns || !track) { logPub('Namespace and track name are required for PUBLISH', 'error'); return; }
  const alias = varint(BigInt($pubPublishAlias.value));
  try {
    const reqId = await pubAdapter.publish(nsToBytes(ns), enc.encode(track), alias);
    // Auto-register in accepted aliases so Send Object can use it right away
    acceptedAliases.set(BigInt(alias), { ns, track, reqId: BigInt(reqId), canPublishDone: true });
    renderTrackAliasTable();
    $pubTrackAlias.value = alias.toString();
  } catch (err) {
    logPub(`PUBLISH failed: ${(err as Error).message}`, 'error');
  }
}

async function doAcceptSubscribe(requestId: bigint): Promise<void> {
  if (!pubAdapter) return;
  try {
    // Use request ID as track alias for simplicity
    const alias = requestId;
    await pubAdapter.acceptSubscribe(varint(requestId), varint(alias));
    const info = incomingSubscribes.get(requestId);
    incomingSubscribes.delete(requestId);
    renderIncomingSubs();
    // Track the alias and auto-fill the Track Alias field
    if (info) {
      acceptedAliases.set(alias, { ns: info.ns, track: info.track, reqId: requestId, canPublishDone: true });
      renderTrackAliasTable();
    }
    $pubTrackAlias.value = alias.toString();
  } catch (err) {
    logPub(`Accept subscribe failed: ${(err as Error).message}`, 'error');
  }
}

async function doRejectSubscribe(requestId: bigint): Promise<void> {
  if (!pubAdapter) return;
  try {
    await pubAdapter.rejectSubscribe(varint(requestId), varint(0x1n), 'Rejected by user');
    logPub(`REQUEST_ERROR sent: reqId=${requestId} reason="Rejected by user"`, 'sent');
    incomingSubscribes.delete(requestId);
    renderIncomingSubs();
  } catch (err) {
    logPub(`Reject subscribe failed: ${(err as Error).message}`, 'error');
  }
}

async function doPublishDone(requestId: bigint): Promise<void> {
  if (!pubAdapter) return;
  try {
    await pubAdapter.publishDone(varint(requestId), PublishDoneCode.TRACK_ENDED, 'Track ended by publisher');
    // Remove from accepted aliases (keyed by alias, which equals requestId for accepted subs)
    for (const [alias, info] of acceptedAliases) {
      if (info.reqId === requestId) { acceptedAliases.delete(alias); break; }
    }
    renderTrackAliasTable();
  } catch (err) {
    logPub(`PUBLISH_DONE failed: ${(err as Error).message}`, 'error');
  }
}

async function doAcceptFetch(requestId: bigint): Promise<void> {
  if (!pubAdapter) return;
  const info = incomingFetches.get(requestId);
  if (!info) return;
  try {
    await pubAdapter.acceptFetch(varint(requestId));
    incomingFetches.delete(requestId);
    renderIncomingFetches();
    logPub(`FETCH_OK sent: reqId=${requestId} ns=${info.ns} track=${info.track} range=${info.sg}:${info.so}–${info.eg}:${info.eo}`, 'sent');

    // Filter stored objects that fall within the requested range for this track
    const matching = sentObjects.filter(o =>
      o.ns === info.ns && o.track === info.track &&
      (o.groupId > info.sg || (o.groupId === info.sg && o.objectId >= info.so)) &&
      (o.groupId < info.eg || (o.groupId === info.eg && o.objectId <= info.eo)),
    );

    if (matching.length === 0) {
      logPub(`No cached objects match fetch range — opening empty fetch stream`, 'info');
    }

    const streamId = await pubAdapter.openFetchStream(varint(requestId));
    for (const obj of matching) {
      await pubAdapter.sendFetchObject(
        streamId,
        varint(obj.groupId),
        varint(obj.subgroupId),
        varint(obj.objectId),
        obj.payload,
      );
      logPub(`Fetch object sent: reqId=${requestId} grp=${obj.groupId} sub=${obj.subgroupId} obj=${obj.objectId} bytes=${obj.payload.byteLength}`, 'sent', 'data');
    }
    await pubAdapter.closeFetchStream(streamId);
  } catch (err) {
    logPub(`Accept fetch failed: ${(err as Error).message}`, 'error');
  }
}

async function doRejectFetch(requestId: bigint): Promise<void> {
  if (!pubAdapter) return;
  try {
    await pubAdapter.rejectFetch(varint(requestId), varint(0x1n), 'Rejected by user');
    logPub(`REQUEST_ERROR sent for FETCH: reqId=${requestId}`, 'sent');
    incomingFetches.delete(requestId);
    renderIncomingFetches();
  } catch (err) {
    logPub(`Reject fetch failed: ${(err as Error).message}`, 'error');
  }
}

async function doOpenSubgroup(): Promise<void> {
  if (!pubAdapter) return;
  try {
    const trackAlias = varint(BigInt($pubTrackAlias.value));
    const groupId = varint(BigInt($pubGroupId.value));
    const subgroupId = varint(BigInt($pubSubgroupId.value));
    const streamId = await pubAdapter.openSubgroup(trackAlias, groupId, subgroupId);
    openSubgroups.add(streamId);
    $pubStreamId.value = streamId.toString();
    logPub(`Subgroup stream opened: STREAM_ID=${streamId} TRACK_ALIAS=${trackAlias} GROUP_ID=${groupId} SUBGROUP_ID=${subgroupId}`, 'sent', 'data');
    flushPubHex();
  } catch (err) {
    logPub(`Open subgroup failed: ${(err as Error).message}`, 'error', 'data');
  }
}

async function doSendObject(): Promise<void> {
  if (!pubAdapter) return;
  try {
    const streamId = BigInt($pubStreamId.value);
    const alias = BigInt($pubTrackAlias.value);
    const groupId = BigInt($pubGroupId.value);
    const subgroupId = BigInt($pubSubgroupId.value);
    const objectId = varint(BigInt($pubObjectId.value));
    const payload = enc.encode($pubPayload.value);
    await pubAdapter.sendObject(streamId, objectId, payload);
    logPub(`Object sent: STREAM_ID=${streamId} OBJ_ID=${objectId} BYTES=${payload.byteLength}`, 'sent', 'data');
    flushPubHex();
    const aliasInfo = acceptedAliases.get(alias);
    sentObjects.push({
      alias,
      ns: aliasInfo?.ns ?? '',
      track: aliasInfo?.track ?? '',
      groupId,
      subgroupId,
      objectId: BigInt($pubObjectId.value),
      streamId,
      payloadPreview: $pubPayload.value.length <= 40 ? $pubPayload.value : $pubPayload.value.slice(0, 40) + '…',
      payload,
    });
    renderSentObjectsTable();
    // Auto-increment object ID
    $pubObjectId.value = (BigInt($pubObjectId.value) + 1n).toString();
  } catch (err) {
    logPub(`Send object failed: ${(err as Error).message}`, 'error', 'data');
  }
}

async function doSendObjectInSubgroup(): Promise<void> {
  if (!pubAdapter) return;
  try {
    const trackAlias = varint(BigInt($pubTrackAlias.value));
    const groupId = varint(BigInt($pubGroupId.value));
    const subgroupId = varint(BigInt($pubSubgroupId.value));
    const streamId = await pubAdapter.openSubgroup(trackAlias, groupId, subgroupId);
    openSubgroups.add(streamId);
    $pubStreamId.value = streamId.toString();
    logPub(`Subgroup stream opened: STREAM_ID=${streamId} TRACK_ALIAS=${trackAlias} GROUP_ID=${groupId} SUBGROUP_ID=${subgroupId}`, 'sent', 'data');
    flushPubHex();
    const objectId = varint(BigInt($pubObjectId.value));
    const payload = enc.encode($pubPayload.value);
    await pubAdapter.sendObject(streamId, objectId, payload);
    logPub(`Object sent: STREAM_ID=${streamId} OBJ_ID=${objectId} BYTES=${payload.byteLength}`, 'sent', 'data');
    flushPubHex();
    await pubAdapter.closeSubgroup(streamId);
    openSubgroups.delete(streamId);
    logPub(`Subgroup stream closed: STREAM_ID=${streamId}`, 'sent', 'data');
    const alias = BigInt($pubTrackAlias.value);
    const aliasInfo = acceptedAliases.get(alias);
    sentObjects.push({
      alias,
      ns: aliasInfo?.ns ?? '',
      track: aliasInfo?.track ?? '',
      groupId: BigInt($pubGroupId.value),
      subgroupId: BigInt($pubSubgroupId.value),
      objectId: BigInt($pubObjectId.value),
      streamId,
      payloadPreview: $pubPayload.value.length <= 40 ? $pubPayload.value : $pubPayload.value.slice(0, 40) + '…',
      payload,
    });
    renderSentObjectsTable();
    $pubObjectId.value = (BigInt($pubObjectId.value) + 1n).toString();
  } catch (err) {
    logPub(`Send object in subgroup failed: ${(err as Error).message}`, 'error', 'data');
  }
}

async function doCloseSubgroup(): Promise<void> {
  if (!pubAdapter) return;
  try {
    const streamId = BigInt($pubStreamId.value);
    await pubAdapter.closeSubgroup(streamId);
    openSubgroups.delete(streamId);
    logPub(`Subgroup stream closed: STREAM_ID=${streamId}`, 'sent', 'data');
  } catch (err) {
    logPub(`Close subgroup failed: ${(err as Error).message}`, 'error', 'data');
  }
}

// ─── Subscriber Actions ─────────────────────────────────────────────

async function doSubscribe(): Promise<void> {
  if (!subAdapter) return;
  const ns = $subNs.value.trim();
  const track = $subTrack.value.trim();
  if (!ns || !track) { logSub('Namespace and track are required', 'error'); return; }

  const filterValue = $subFilter.value;
  let filter: any;
  if (filterValue === 'LatestObject') {
    filter = { type: 'LatestObject' as const };
  } else if (filterValue === 'NextGroupStart') {
    filter = { type: 'NextGroupStart' as const };
  } else if (filterValue === 'AbsoluteStart') {
    filter = {
      type: 'AbsoluteStart' as const,
      startGroup: varint(BigInt($subStartGroup.value)),
      startObject: varint(BigInt($subStartObject.value)),
    };
  } else if (filterValue === 'AbsoluteRange') {
    filter = {
      type: 'AbsoluteRange' as const,
      startGroup: varint(BigInt($subStartGroup.value)),
      startObject: varint(BigInt($subStartObject.value)),
      endGroup: varint(BigInt($subEndGroup.value)),
    };
  }

  try {
    const opts = filter ? { subscriptionFilter: filter } : {};
    const reqId = await subAdapter.subscribe(nsToBytes(ns), enc.encode(track), opts);
    const reqIdBig = BigInt(reqId);
    activeSubscriptions.set(reqIdBig, { ns, track });
    renderActiveSubs();
  } catch (err) {
    logSub(`SUBSCRIBE failed: ${(err as Error).message}`, 'error');
  }
}

async function doUnsubscribe(requestId: bigint): Promise<void> {
  if (!subAdapter) return;
  try {
    await subAdapter.unsubscribe(varint(requestId));
    activeSubscriptions.delete(requestId);
    renderActiveSubs();
  } catch (err) {
    logSub(`UNSUBSCRIBE failed: ${(err as Error).message}`, 'error');
  }
}

async function doPauseResume(requestId: bigint, forward: 0 | 1): Promise<void> {
  if (!subAdapter) return;
  try {
    await subAdapter.requestUpdate(varint(requestId), { forward: forward as 0 | 1 });
  } catch (err) {
    logSub(`REQUEST_UPDATE failed: ${(err as Error).message}`, 'error');
  }
}

async function doSubscribeNamespace(): Promise<void> {
  if (!subAdapter) return;
  const prefix = $subNsPrefix.value.trim();
  if (!prefix) { logSub('Namespace prefix is required', 'error'); return; }

  try {
    // Subscribe Options 0x01 = NAMESPACE: relay sends NAMESPACE messages on the
    // bidi response stream for each matching PUBLISH_NAMESPACE (§9.25).
    const reqId = await subAdapter.subscribeNamespace(nsToBytes(prefix), varint(1n));
    activeNamespaceSubs.set(BigInt(reqId), prefix);
    renderActiveNsSubs();
  } catch (err) {
    logSub(`SUBSCRIBE_NAMESPACE failed: ${(err as Error).message}`, 'error');
  }
}

async function doCancelNamespace(reqId: bigint): Promise<void> {
  if (!subAdapter) return;
  const prefix = activeNamespaceSubs.get(reqId);
  if (!prefix) { logSub('No active namespace subscription for that ID', 'error'); return; }
  try {
    await subAdapter.cancelNamespace(varint(reqId));
    activeNamespaceSubs.delete(reqId);
    renderActiveNsSubs();
    logSub(`→ SUBSCRIBE_NAMESPACE cancelled (${prefix}) | REQ_ID=${reqId}`, 'sent');
    for (const [ns, nsReqId] of announcedNamespaces) {
      if (nsReqId === reqId) announcedNamespaces.delete(ns);
    }
    renderAnnouncedNs();
  } catch (err) {
    logSub(`Cancel namespace failed: ${(err as Error).message}`, 'error');
  }
}

async function doFetch(): Promise<void> {
  if (!subAdapter) return;
  const ns = $fetchNs.value.trim();
  const track = $fetchTrack.value.trim();
  if (!ns || !track) { logSub('Namespace and track are required', 'error'); return; }

  try {
    const reqId = await subAdapter.fetch(nsToBytes(ns), enc.encode(track), {
      startGroup: varint(BigInt($fetchStartGroup.value)),
      startObject: varint(BigInt($fetchStartObject.value)),
      endGroup: varint(BigInt($fetchEndGroup.value)),
      endObject: varint(BigInt($fetchEndObject.value)),
    });
    activeFetches.set(BigInt(reqId), { ns, track });
  } catch (err) {
    logSub(`FETCH failed: ${(err as Error).message}`, 'error');
  }
}

async function doFetchCancel(): Promise<void> {
  if (!subAdapter) return;
  const first = activeFetches.entries().next().value;
  if (!first) { logSub('No active fetches', 'error'); return; }
  const [reqId, info] = first;
  try {
    await subAdapter.fetchCancel(varint(reqId));
    activeFetches.delete(reqId);
  } catch (err) {
    logSub(`FETCH_CANCEL failed: ${(err as Error).message}`, 'error');
  }
}

async function doTrackStatus(): Promise<void> {
  if (!subAdapter) return;
  const ns = $tsNs.value.trim();
  const track = $tsTrack.value.trim();
  if (!ns || !track) { logSub('Namespace and track are required', 'error'); return; }

  try {
    const reqId = await subAdapter.trackStatus(nsToBytes(ns), enc.encode(track));
  } catch (err) {
    logSub(`TRACK_STATUS failed: ${(err as Error).message}`, 'error');
  }
}

// ─── Filter UI Toggle ───────────────────────────────────────────────

function updateFilterFields(): void {
  const filter = $subFilter.value;
  $absStartFields.classList.toggle('hidden', filter !== 'AbsoluteStart' && filter !== 'AbsoluteRange');
  $absRangeFields.classList.toggle('hidden', filter !== 'AbsoluteRange');
}

// ─── Event Wiring ───────────────────────────────────────────────────

$pubBtnConnect.addEventListener('click', () => { resetMessages(); doPubConnect(); });
$pubBtnDisconnect.addEventListener('click', () => { resetMessages(); doPubDisconnect(); });
$subBtnConnect.addEventListener('click', () => { resetMessages(); doSubConnect(); });
$subBtnDisconnect.addEventListener('click', () => { resetMessages(); doSubDisconnect(); });

$btnPubNs.addEventListener('click', () => { resetMessages(); doPublishNamespace(); });
$btnPublish.addEventListener('click', () => { resetMessages(); doPublish(); });
$btnSendObjectInSubgroup.addEventListener('click', () => { resetMessages(); doSendObjectInSubgroup(); });
$btnOpenSubgroup.addEventListener('click', () => { resetMessages(); doOpenSubgroup(); });
$btnSendObject.addEventListener('click', () => { resetMessages(); doSendObject(); });
$btnCloseSubgroup.addEventListener('click', () => { resetMessages(); doCloseSubgroup(); });

$btnSubscribe.addEventListener('click', () => { resetMessages(); doSubscribe(); });
$btnSubNs.addEventListener('click', () => { resetMessages(); doSubscribeNamespace(); });

$btnFetch.addEventListener('click', () => { resetMessages(); doFetch(); });
$btnFetchCancel.addEventListener('click', () => { resetMessages(); doFetchCancel(); });

$btnTrackStatus.addEventListener('click', () => { resetMessages(); doTrackStatus(); });

$subFilter.addEventListener('change', updateFilterFields);
$btnClearLog.addEventListener('click', clearLog);
$chkShowHex.addEventListener('change', () => {
  $logEl.classList.toggle('hex-hidden', !$chkShowHex.checked);
});
// Apply initial state (checkbox starts checked per HTML)
$logEl.classList.toggle('hex-hidden', !$chkShowHex.checked);

function applyNsOpsVisibility(): void {
  const show = $chkShowNsOps.checked;
  for (const el of document.querySelectorAll('.ns-ops-section')) {
    el.classList.toggle('hidden', !show);
  }
}
$chkShowNsOps.addEventListener('change', applyNsOpsVisibility);

function applyFetchOpsVisibility(): void {
  const show = $chkShowFetchOps.checked;
  for (const el of document.querySelectorAll('.fetch-ops-section')) {
    el.classList.toggle('hidden', !show);
  }
}
$chkShowFetchOps.addEventListener('change', applyFetchOpsVisibility);
$btnClearSubObjects.addEventListener('click', () => {
  subObjectCount = clearObjectViewer($subObjects, $subObjCount);
});
$btnClearFetchObjects.addEventListener('click', () => {
  fetchObjectCount = clearObjectViewer($fetchObjects, $fetchObjCount);
});

// ─── Draggable log panel ─────────────────────────────────────────────
{
  const logPanel = document.getElementById('log-panel')!;
  const logHeader = logPanel.querySelector('.log-header') as HTMLElement;
  let dragging = false;
  let startY = 0;
  let startH = 0;

  logHeader.addEventListener('mousedown', (e) => {
    // Don't capture clicks on buttons / checkboxes inside the header
    if ((e.target as HTMLElement).closest('button,input,label')) return;
    dragging = true;
    startY = e.clientY;
    startH = logPanel.offsetHeight;
    document.body.style.cursor = 'ns-resize';
    document.body.style.userSelect = 'none';
  });

  window.addEventListener('mousemove', (e) => {
    if (!dragging) return;
    // Dragging up (smaller Y) → taller panel → subtract delta
    const delta = startY - e.clientY;
    const newH = Math.max(80, Math.min(window.innerHeight * 0.8, startH + delta));
    logPanel.style.height = `${newH}px`;
  });

  window.addEventListener('mouseup', () => {
    if (!dragging) return;
    dragging = false;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  });
}

// ─── Initialize ─────────────────────────────────────────────────────

loadFromParams();
applyNsOpsVisibility();
applyFetchOpsVisibility();
updateFilterFields();
initViz();

// Viz panel collapse toggle + ew-resize drag
{
  const vizPanel = document.getElementById('connection-viz-panel')!;
  const vizToggle = document.getElementById('viz-toggle')!;
  const handle = document.getElementById('viz-resize-handle')!;
  const logPanel = document.getElementById('log-panel')!;

  // Collapse / expand
  let savedWidth = '';
  vizToggle.addEventListener('click', () => {
    const isCollapsed = vizPanel.classList.toggle('viz-collapsed');
    if (isCollapsed) {
      savedWidth = vizPanel.style.width || '';
    } else if (savedWidth) {
      vizPanel.style.width = savedWidth;
    }
  });

  // Horizontal resize
  let dragging = false;
  let startX = 0;
  let startW = 0;

  handle.addEventListener('mousedown', (e) => {
    if (vizPanel.classList.contains('viz-collapsed')) return;
    dragging = true;
    startX = e.clientX;
    startW = vizPanel.offsetWidth;
    handle.classList.add('viz-dragging');
    document.body.style.cursor = 'ew-resize';
    document.body.style.userSelect = 'none';
    e.preventDefault();
  });

  window.addEventListener('mousemove', (e) => {
    if (!dragging) return;
    const delta = startX - e.clientX; // drag left = bigger panel
    const containerW = logPanel.offsetWidth;
    const newW = Math.max(200, Math.min(containerW * 0.8, startW + delta));
    vizPanel.style.width = `${newW}px`;
    vizPanel.style.maxWidth = 'none';
  });

  window.addEventListener('mouseup', () => {
    if (!dragging) return;
    dragging = false;
    handle.classList.remove('viz-dragging');
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  });
}

if (!('WebTransport' in window)) {
  log('WebTransport is not available. Chrome 97+ or Edge 97+ required.', 'error');
  $pubBtnConnect.disabled = true;
  $subBtnConnect.disabled = true;
} else {
  log('MOQT Playground ready (draft-ietf-moq-transport-16)', 'info');
  log('Connect Publisher and/or Subscriber independently to the same relay.', 'info');
}
