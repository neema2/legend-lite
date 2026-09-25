// An alert: a window that says one thing and offers what to do about
// it -- upstream's DataCubeAlert. A type (the icon and its colour), a
// message, a longer text under it, and action buttons along the
// bottom, the FIRST focused, since upstream makes the first action the
// default. Every action closes the alert after it runs.

export type AlertType = 'error' | 'info' | 'success' | 'warning';

export interface AlertAction {
  readonly label: string;
  readonly handler: () => void;
}

export interface AlertOptions {
  readonly type: AlertType;
  readonly message: string;
  readonly text?: string;
  readonly actions?: readonly AlertAction[];
  /** The window title; upstream's alerts are untitled unless an error. */
  readonly title?: string;
}

/** Upstream's DEFAULT_ALERT_WINDOW_CONFIG. */
export const ALERT_WINDOW = {
  width: 500,
  height: 200,
  minWidth: 200,
  minHeight: 80,
  center: true,
} as const;

const ICONS: Readonly<Record<AlertType, string>> = {
  error: '⨂',
  info: 'ⓘ',
  success: '✓',
  warning: '⚠',
};

/** Fill `host` with the alert; `close` removes its window. */
export function buildAlert(
  host: HTMLElement,
  options: AlertOptions,
  close: () => void,
): void {
  const doc = host.ownerDocument;
  host.classList.add('dc-alert', `dc-alert-${options.type}`);
  host.setAttribute('role', options.type === 'error' || options.type === 'warning'
    ? 'alertdialog' : 'dialog');

  const body = doc.createElement('div');
  body.className = 'dc-alert-body';
  const icon = doc.createElement('div');
  icon.className = 'dc-alert-icon';
  icon.textContent = ICONS[options.type];
  icon.setAttribute('aria-hidden', 'true');
  const words = doc.createElement('div');
  const message = doc.createElement('div');
  message.className = 'dc-alert-message';
  message.textContent = options.message;
  words.append(message);
  if (options.text) {
    const text = doc.createElement('div');
    text.className = 'dc-alert-text';
    text.textContent = options.text;
    words.append(text);
  }
  body.append(icon, words);
  host.append(body);

  const actions = options.actions ?? [];
  if (actions.length === 0) return;
  const footer = doc.createElement('div');
  footer.className = 'dc-alert-actions';
  const buttons = actions.map((action) => {
    const b = doc.createElement('button');
    b.type = 'button';
    b.className = 'dc-button dc-alert-action';
    b.textContent = action.label;
    b.addEventListener('click', () => {
      action.handler();
      close();
    });
    return b;
  });
  footer.append(...buttons);
  host.append(footer);
  buttons[0]?.focus();
}

// -- the two alerts that carry a query --------------------------------

/** Upstream's execution-error window: room for the debug info. */
export const EXECUTION_ERROR_WINDOW = {
  width: 600,
  height: 250,
  minWidth: 500,
  minHeight: 200,
  center: true,
} as const;

/** Upstream's code-check window. */
export const CODE_CHECK_WINDOW = {
  width: 500,
  height: 400,
  minWidth: 300,
  minHeight: 300,
  center: true,
} as const;

export interface ExecutionErrorOptions {
  readonly message: string;
  readonly text?: string;
  /** The Pure that failed: upstream's `queryCode`. */
  readonly pure?: string;
  /** The SQL it planned to, when planning got that far. */
  readonly sql?: string;
  /** Hand a file to the host; absent, there is no download button. */
  readonly download?: (name: string, mime: string, text: string) => void;
}

/**
 * Upstream's DataCubeExecutionErrorAlert: the error, and behind "Show
 * debug info?" the query code (ours adds the SQL it planned to) and a
 * download of all of it for a bug report. OK closes it.
 */
export function buildExecutionErrorAlert(
  host: HTMLElement,
  options: ExecutionErrorOptions,
  close: () => void,
): void {
  const doc = host.ownerDocument;
  buildAlert(host, { type: 'error', message: options.message,
    ...(options.text ? { text: options.text } : {}) }, close);
  host.classList.add('dc-alert-execution');

  const debug = doc.createElement('div');
  debug.className = 'dc-alert-debug';
  debug.hidden = true;
  const prompt = doc.createElement('div');
  prompt.className = 'dc-alert-prompt';
  prompt.textContent = options.sql !== undefined
    ? 'Check the query code and the SQL below to debug or report issue'
    : 'Check the query code below to debug or report issue';
  debug.append(prompt);
  const code = (title: string, text: string): void => {
    const label = doc.createElement('div');
    label.className = 'dc-alert-code-title';
    label.textContent = title;
    const pre = doc.createElement('pre');
    pre.className = 'dc-alert-code';
    pre.textContent = text;
    debug.append(label, pre);
  };
  if (options.pure !== undefined) code('Query Code', options.pure);
  if (options.sql !== undefined) code('SQL', options.sql);
  host.append(debug);

  const footer = doc.createElement('div');
  footer.className = 'dc-alert-actions dc-alert-actions-split';
  const toggle = doc.createElement('label');
  toggle.className = 'dc-check';
  const box = doc.createElement('input');
  box.type = 'checkbox';
  box.className = 'dc-check-box';
  const label = doc.createElement('span');
  label.className = 'dc-check-label';
  label.textContent = 'Show debug info?';
  toggle.append(box, label);
  const right = doc.createElement('div');
  right.className = 'dc-alert-buttons';
  const ok = doc.createElement('button');
  ok.type = 'button';
  ok.className = 'dc-button dc-alert-action';
  ok.textContent = 'OK';
  ok.addEventListener('click', close);
  right.append(ok);
  const hasDebug = options.pure !== undefined || options.sql !== undefined;
  let save: HTMLButtonElement | null = null;
  if (hasDebug && options.download) {
    const download = options.download;
    save = doc.createElement('button');
    save.type = 'button';
    save.className = 'dc-button dc-alert-action';
    save.textContent = 'Download Debug Info';
    save.hidden = true;
    save.addEventListener('click', () => {
      download(`DEBUG__Query__${stamp(new Date())}.json`, 'application/json',
        JSON.stringify({
          error: options.text ?? options.message,
          ...(options.pure !== undefined ? { queryCode: options.pure } : {}),
          ...(options.sql !== undefined ? { sql: options.sql } : {}),
        }, null, 2));
    });
    right.append(save);
  }
  box.disabled = !hasDebug;
  box.addEventListener('change', () => {
    debug.hidden = !box.checked;
    if (save) save.hidden = !box.checked;
  });
  footer.append(toggle, right);
  host.append(footer);
  ok.focus();
}

/**
 * Upstream's code-check alert: the query the compiler refused, with the
 * place it named marked -- here a caret line under it, when the refusal
 * carries a `[line:col]`.
 */
export function buildCodeCheckAlert(
  host: HTMLElement,
  options: { readonly message: string; readonly text?: string; readonly code: string },
  close: () => void,
): void {
  const doc = host.ownerDocument;
  buildAlert(host, { type: 'error', message: options.message,
    ...(options.text ? { text: options.text } : {}) }, close);
  const pre = doc.createElement('pre');
  pre.className = 'dc-alert-code dc-alert-codecheck';
  pre.textContent = markPosition(options.code, options.text ?? options.message);
  host.append(pre);
}

/**
 * The code with a caret line under the `[line:col]` a message names;
 * the code alone when it names none, or one outside the code.
 */
export function markPosition(code: string, message: string): string {
  const at = /\[(\d+):(\d+)\]/.exec(message);
  if (!at) return code;
  const line = Number(at[1]);
  const col = Number(at[2]);
  const lines = code.split('\n');
  if (line < 1 || line > lines.length || col < 1) return code;
  lines.splice(line, 0, `${' '.repeat(col - 1)}^`);
  return lines.join('\n');
}

function stamp(at: Date): string {
  const two = (n: number): string => String(n).padStart(2, '0');
  return `${at.getFullYear()}-${two(at.getMonth() + 1)}-${two(at.getDate())}`
    + `T${two(at.getHours())}${two(at.getMinutes())}${two(at.getSeconds())}`;
}
