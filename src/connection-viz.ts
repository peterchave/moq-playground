/**
 * Connection Visualization for MOQT Playground.
 * Shows QUIC connections and streams (control + data) for Publisher and Subscriber,
 * with animated message flow on user-initiated actions.
 */

export type StreamDirection = 'bidi' | 'uni-send' | 'uni-recv';
export type StreamKind = 'ctrl' | 'data';
export type MessageSide = 'pub' | 'sub';
export type MessageDirection = 'sent' | 'recv';

interface StreamEntry {
  id: string;
  kind: StreamKind;
  direction: StreamDirection;
  label: string;
}

interface MessageEntry {
  side: MessageSide;
  direction: MessageDirection;
  stream: StreamKind;
  label: string;
  timestamp: number;
}

const STREAMS_PUB: StreamEntry[] = [
  { id: 'pub-ctrl', kind: 'ctrl', direction: 'bidi', label: 'Control (bidi)' },
  { id: 'pub-data', kind: 'data', direction: 'uni-send', label: 'Data (uni →)' },
];

const STREAMS_SUB: StreamEntry[] = [
  { id: 'sub-ctrl', kind: 'ctrl', direction: 'bidi', label: 'Control (bidi)' },
  { id: 'sub-data', kind: 'data', direction: 'uni-recv', label: 'Data (uni ←)' },
];

let messages: MessageEntry[] = [];
let pubConnected = false;
let subConnected = false;

let $root: HTMLElement | null = null;

export function initViz(): void {
  $root = document.getElementById('connection-viz-content');
  if (!$root) return;
  render();
}

export function setConnectionState(side: MessageSide, connected: boolean): void {
  if (side === 'pub') pubConnected = connected;
  else subConnected = connected;
  render();
}

/** Called before each user-initiated action to reset the message list and animations */
export function resetMessages(): void {
  messages = [];
  render();
}

/** Record a message on a stream and animate it */
export function addMessage(side: MessageSide, direction: MessageDirection, stream: StreamKind, label: string): void {
  messages.push({ side, direction, stream, label, timestamp: Date.now() });
  // Keep last 12 messages
  if (messages.length > 12) messages = messages.slice(-12);
  render();
}

function render(): void {
  if (!$root) return;

  $root.innerHTML = `
    <div class="viz-row">
      <div class="viz-col viz-pub">
        <div class="viz-side-label">Publisher</div>
        ${renderConnection('pub', pubConnected, STREAMS_PUB)}
      </div>
      <div class="viz-relay">
        <div class="viz-relay-label">Relay</div>
        <div class="viz-relay-streams">
          <div class="viz-relay-stream viz-relay-ctrl" title="Control: bidirectional">&#x21C4; ctrl</div>
          <div class="viz-relay-stream viz-relay-data" title="Data: publisher → subscriber">→ data</div>
        </div>
      </div>
      <div class="viz-col viz-sub">
        <div class="viz-side-label">Subscriber</div>
        ${renderConnection('sub', subConnected, STREAMS_SUB)}
      </div>
    </div>
  `;
}

function renderConnection(side: MessageSide, connected: boolean, streams: StreamEntry[]): string {
  const stateClass = connected ? 'viz-connected' : 'viz-disconnected';
  const stateLabel = connected ? 'Connected' : 'Disconnected';

  const sideMessages = messages.filter(m => m.side === side);

  return `
    <div class="viz-connection ${stateClass}">
      <div class="viz-conn-header">
        <span class="viz-conn-dot"></span>
        <span class="viz-conn-label">WebTransport Connection</span>
        <span class="viz-conn-state">${stateLabel}</span>
      </div>
      <div class="viz-streams">
        ${streams.map(s => renderStream(s, sideMessages.filter(m => m.stream === s.kind), side)).join('')}
      </div>
    </div>
  `;
}

function renderStream(stream: StreamEntry, msgs: MessageEntry[], side: MessageSide): string {
  const dirIcon = stream.direction === 'bidi' ? '⇄' : stream.direction === 'uni-send' ? '→' : '←';
  const dirClass = `viz-stream-${stream.direction}`;
  const kindClass = `viz-stream-${stream.kind === 'ctrl' ? 'control' : stream.kind}`;

  return `
    <div class="viz-stream ${dirClass} ${kindClass}">
      <div class="viz-stream-header">
        <span class="viz-stream-icon">${dirIcon}</span>
        <span class="viz-stream-label">${stream.label}</span>
      </div>
      <div class="viz-stream-pipe">
        <div class="viz-pipe-track">
          ${stream.direction === 'bidi' ? `
            <span class="viz-arrow viz-arrow-right">▸</span>
            <div class="viz-pipe-line viz-pipe-line-top"></div>
            <div class="viz-pipe-line viz-pipe-line-bottom"></div>
            <span class="viz-arrow viz-arrow-left">◂</span>
          ` : stream.direction === 'uni-send' ? `
            <span class="viz-arrow viz-arrow-right">▸</span>
            <div class="viz-pipe-line"></div>
          ` : `
            <div class="viz-pipe-line"></div>
            <span class="viz-arrow viz-arrow-left">◂</span>
          `}
        </div>
      </div>
      ${msgs.length > 0 ? `
        <div class="viz-messages">
          ${msgs.map(m => {
            const arrow = m.direction === 'sent' ? '▶' : '◀';
            const cls = `viz-msg-${side}-${stream.kind}-${m.direction}`;
            return `<div class="viz-msg ${cls} viz-msg-animate">${arrow} ${escapeHtml(m.label)}</div>`;
          }).join('')}
        </div>
      ` : ''}
    </div>
  `;
}

function escapeHtml(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
