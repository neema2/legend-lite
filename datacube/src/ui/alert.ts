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
