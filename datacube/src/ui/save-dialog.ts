// SAVE and SAVE AS, as a window of their own (the user, 2026-10-01: "make the Save dialog box also
// really nice and pretty too"): the name, where it goes, what it keeps and what it does not -- the
// rows are never saved -- and, when saving over the copy this was opened from would drop
// something, that said before anything is written. The HOST saves (it owns the store and the
// file's identity); this window asks and shows. Styled as the source picker (ui/source-picker.ts).

/** What the window says about the cube being saved. */
export interface SaveDialogOptions {
  /** Save (over the copy this was opened from, when there is one) or Save As (always a new cube). */
  readonly purpose: 'save' | 'saveAs';
  /** The name to offer. */
  readonly name: string;
  /** The saved copy this cube was opened from: Save goes over it. */
  readonly over?: { readonly name: string; readonly savedAt?: number };
  /** Where it is kept, in words: "in this browser", "for this visit only (storage was refused)". */
  readonly where: string;
  /** What is saved, a line each: "Grouped by region, desk", "2 charts and the page's layout". */
  readonly keeps: readonly string[];
  /** What is not: the rows, and what reopening needs. */
  readonly leaves: string;
  /** Why saving OVER the saved copy loses something; undefined when it does not. */
  readonly warning?: string;
  /** Save it; a refusal is shown in the window, which stays open. */
  save(name: string, asNew: boolean): Promise<void>;
  /** The clock "saved 2 hours ago" is read against: the system's, or a test's own (Bazel workplan P3-16). */
  readonly now?: () => number;
}

function el<K extends keyof HTMLElementTagNameMap>(doc: Document, tag: K, cls: string, parent?: Element, text?: string): HTMLElementTagNameMap[K] {
  const e = doc.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  parent?.append(e);
  return e;
}

/** "2 minutes ago", "yesterday": when the copy was saved. */
function ago(at: number, now = Date.now()): string {
  const s = Math.max(0, Math.round((now - at) / 1000));
  if (s < 60) return 'just now';
  const m = Math.round(s / 60);
  if (m < 60) return `${m} minute${m === 1 ? '' : 's'} ago`;
  const h = Math.round(m / 60);
  if (h < 24) return `${h} hour${h === 1 ? '' : 's'} ago`;
  const d = Math.round(h / 24);
  return d === 1 ? 'yesterday' : `${d} days ago`;
}

const CHECK = 'M5 12.5l4.5 4.5L19 7.5';
const CROSS = 'M7 7l10 10M17 7L7 17';

function mark(doc: Document, path: string, cls: string): SVGSVGElement {
  const NS = 'http://www.w3.org/2000/svg';
  const svg = doc.createElementNS(NS, 'svg');
  svg.setAttribute('viewBox', '0 0 24 24');
  svg.setAttribute('aria-hidden', 'true');
  svg.setAttribute('class', cls);
  const p = doc.createElementNS(NS, 'path');
  p.setAttribute('d', path);
  svg.append(p);
  return svg;
}

/**
 * Ask for the name and save. Resolves with what was saved, or undefined when the window was
 * closed without saving.
 */
export function saveDialog(doc: Document, o: SaveDialogOptions): Promise<{ readonly name: string; readonly asNew: boolean } | undefined> {
  return new Promise((resolve) => {
    const backdrop = el(doc, 'div', 'dc-picker-backdrop', doc.body);
    const win = el(doc, 'div', 'dc-picker dc-save dc-app-floating', backdrop);
    win.setAttribute('role', 'dialog');
    win.setAttribute('aria-modal', 'true');
    win.setAttribute('aria-labelledby', 'dc-save-title');
    const before = doc.activeElement as HTMLElement | null;
    let busy = false;
    let done = false;
    const finish = (value: { name: string; asNew: boolean } | undefined): void => {
      if (done) return;
      done = true;
      doc.removeEventListener('keydown', onKey, true);
      backdrop.remove();
      before?.focus?.();
      resolve(value);
    };
    const onKey = (e: KeyboardEvent): void => {
      if (e.key === 'Escape' && !busy) { e.preventDefault(); finish(undefined); }
    };
    doc.addEventListener('keydown', onKey, true);
    backdrop.addEventListener('mousedown', (e) => { if (e.target === backdrop && !busy) finish(undefined); });

    // over the saved copy -- or, Save As or never saved, a new one
    const overIt = o.purpose === 'save' && o.over !== undefined;

    const head = el(doc, 'div', 'dc-picker-head', win);
    const titles = el(doc, 'div', 'dc-picker-titles', head);
    const title = el(doc, 'h2', 'dc-picker-title', titles, o.purpose === 'saveAs' ? 'Save as' : 'Save');
    title.id = 'dc-save-title';
    el(doc, 'p', 'dc-picker-subtitle', titles, `Kept ${o.where}: the cube's settings, never its rows.`);
    const close = el(doc, 'button', 'dc-picker-close', head, '×') as HTMLButtonElement;
    close.type = 'button';
    close.setAttribute('aria-label', 'Close');
    close.addEventListener('click', () => { if (!busy) finish(undefined); });

    const body = el(doc, 'div', 'dc-save-body', win);
    const field = el(doc, 'label', 'dc-picker-field dc-save-name', body);
    el(doc, 'span', 'dc-picker-field-label', field, 'Name');
    const name = el(doc, 'input', 'dc-picker-input dc-save-input', field) as HTMLInputElement;
    name.type = 'text';
    name.value = o.name;
    name.setAttribute('aria-describedby', 'dc-save-where');
    const where = el(doc, 'p', 'dc-save-where', body,
      overIt
        ? `Saves over “${o.over!.name}”${o.over!.savedAt !== undefined ? `, saved ${ago(o.over!.savedAt, (o.now ?? Date.now)())}` : ''}.`
        : o.over ? `A new saved cube, beside “${o.over.name}”.` : 'A new saved cube.');
    where.id = 'dc-save-where';

    const card = el(doc, 'div', 'dc-save-card', body);
    el(doc, 'div', 'dc-save-card-title', card, 'What is saved');
    const list = el(doc, 'ul', 'dc-save-list', card);
    for (const line of o.keeps) {
      const li = el(doc, 'li', 'dc-save-keep', list);
      li.append(mark(doc, CHECK, 'dc-save-mark dc-yes'));
      el(doc, 'span', '', li, line);
    }
    const left = el(doc, 'li', 'dc-save-leave', list);
    left.append(mark(doc, CROSS, 'dc-save-mark dc-no'));
    el(doc, 'span', '', left, o.leaves);

    // SAVING OVER would drop what this file cannot show: said first, with both ways on
    let warned = false;
    const warnBox = el(doc, 'div', 'dc-save-warning', body);
    warnBox.setAttribute('role', 'alert');
    warnBox.hidden = true;

    const status = el(doc, 'div', 'dc-picker-status', win);
    status.setAttribute('role', 'status');
    const foot = el(doc, 'div', 'dc-save-foot', win);
    const cancel = el(doc, 'button', 'dc-picker-button dc-quiet', foot, 'Cancel') as HTMLButtonElement;
    cancel.type = 'button';
    cancel.addEventListener('click', () => { if (!busy) finish(undefined); });
    const spacer = el(doc, 'span', 'dc-save-spacer', foot);
    spacer.setAttribute('aria-hidden', 'true');
    const asNewButton = overIt ? el(doc, 'button', 'dc-picker-button', foot, 'Save as new') as HTMLButtonElement : undefined;
    if (asNewButton) asNewButton.type = 'button';
    const primary = el(doc, 'button', 'dc-picker-button dc-primary', foot, 'Save') as HTMLButtonElement;
    primary.type = 'button';

    const run = async (asNew: boolean): Promise<void> => {
      if (busy) return;
      const chosen = name.value.trim();
      if (!chosen) {
        status.className = 'dc-picker-status dc-failed';
        status.textContent = 'Give the cube a name.';
        name.focus();
        return;
      }
      if (!asNew && overIt && o.warning !== undefined && !warned) {
        warned = true;
        warnBox.replaceChildren();
        el(doc, 'div', 'dc-save-warning-title', warnBox, 'Saving over it drops something');
        el(doc, 'p', 'dc-save-warning-text', warnBox, o.warning);
        primary.textContent = 'Save over it anyway';
        if (asNewButton) asNewButton.textContent = 'Save as new instead';
        warnBox.hidden = false;
        return;
      }
      busy = true;
      win.classList.add('dc-busy');
      status.className = 'dc-picker-status dc-working';
      status.textContent = `Saving “${chosen}”…`;
      try {
        await o.save(chosen, asNew);
        finish({ name: chosen, asNew });
      } catch (e) {
        busy = false;
        win.classList.remove('dc-busy');
        status.className = 'dc-picker-status dc-failed';
        status.textContent = e instanceof Error ? e.message : String(e);
      }
    };
    primary.addEventListener('click', () => void run(!overIt));
    asNewButton?.addEventListener('click', () => void run(true));
    name.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') { e.preventDefault(); void run(!overIt); }
    });
    name.focus();
    if (!overIt) name.select();
  });
}
