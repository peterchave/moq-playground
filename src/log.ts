/**
 * Debug log utility for the MOQT Playground.
 * Appends timestamped, color-coded lines to the log panel.
 */

type LogLevel = 'sent' | 'recv' | 'error' | 'info' | 'data' | 'hex'
             | 'pub-ctrl-sent' | 'pub-ctrl-recv'
             | 'pub-data-sent' | 'pub-data-recv' | 'pub-data-hex'
             | 'sub-ctrl-sent' | 'sub-ctrl-recv'
             | 'sub-data-sent' | 'sub-data-recv' | 'sub-data-hex';

const el = document.getElementById('log')!;

export function log(msg: string, level: LogLevel = 'info'): void {
  const ts = new Date().toISOString().slice(11, 23);
  const line = document.createElement('span');
  line.className = `log-${level}`;

  const arrows: Record<LogLevel, string> = {
    sent: '\u25B6',      // ▶
    recv: '\u25C0',      // ◀
    error: '\u2716',     // ✖
    info: '\u2022',      // •
    data: '\u25CF',      // ●
    hex: ' ',
    'pub-ctrl-sent': '\u25B6',
    'pub-ctrl-recv': '\u25C0',
    'pub-data-sent': '\u25B6',
    'pub-data-recv': '\u25C0',
    'pub-data-hex':  ' ',
    'sub-ctrl-sent': '\u25B6',
    'sub-ctrl-recv': '\u25C0',
    'sub-data-sent': '\u25B6',
    'sub-data-recv': '\u25C0',
    'sub-data-hex':  ' ',
  };

  if (level === 'hex' || level === 'pub-data-hex' || level === 'sub-data-hex') {
    line.textContent = `${msg}\n`;
  } else {
    line.textContent = `[${ts}] ${arrows[level]} ${msg}\n`;
  }
  el.appendChild(line);
  el.scrollTop = el.scrollHeight;
}

export function clearLog(): void {
  el.textContent = '';
}
