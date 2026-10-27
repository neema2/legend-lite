// A TABLE TYPED IN (docs/DATACUBE_FLOW_MODE_2026_10_24.md M7's In tools: a flow's Text Input, Alteryx's): a few rows
// written in a window -- its columns named in its head row, a cell each value, a block of cells pasted from a sheet
// (tabs between values, a line a row) filling the table from the cell it is pasted in. What it makes is a CSV file's
// text: the box over it is an Input over a file, as a file dropped on the canvas is (its types the database's reading
// of it). Fetched with the first Text Input.

import { focusedElement } from '../focus.ts';
import { closeMark } from './tile-icons.ts';

/** What the window was given back: the table's name, its columns' names, and its rows, a value a column each. */
export interface TypedTable {
  readonly name: string;
  readonly columns: readonly string[];
  readonly rows: readonly (readonly string[])[];
}

export interface TextInputOptions {
  /** Where the table starts: its name, and the cells it starts with. */
  readonly start?: TypedTable;
  /** The table made: null when it was, else why not (the window stays, saying it). */
  apply(table: TypedTable): Promise<string | null>;
}

/** The table's text as a CSV file holds it: a head row of its columns, a line a row, a value quoted when it must be. */
export function tableCsv(table: TypedTable): string {
  const cell = (v: string): string => (/[",\r\n]/.test(v) || v !== v.trim() ? `"${v.replaceAll('"', '""')}"` : v);
  return [table.columns, ...table.rows].map((r) => r.map(cell).join(',')).join('\n') + '\n';
}

/** What is wrong with a table, if anything: a column with no name or a name twice, no row with a value. */
export function tableProblem(table: TypedTable): string | undefined {
  if (table.name.trim() === '') return 'it needs a name';
  if (table.columns.length === 0) return 'it needs a column';
  const blank = table.columns.findIndex((c) => c.trim() === '');
  if (blank >= 0) return `column ${blank + 1} needs a name`;
  const seen = new Set<string>();
  for (const c of table.columns) {
    if (seen.has(c.trim())) return `'${c.trim()}' names two columns`;
    seen.add(c.trim());
  }
  if (!table.rows.some((r) => r.some((v) => v !== ''))) return 'it needs a row with a value';
  return undefined;
}

/** A block of cells as a sheet copies it: a line a row (a last empty line none), tabs between its values. */
export function pastedCells(text: string): string[][] {
  const lines = text.replace(/\r\n?/g, '\n').split('\n');
  if (lines.at(-1) === '') lines.pop();
  return lines.map((l) => l.split('\t'));
}

/** Open the window; resolves once it is closed, with whether a table was made. */
export function textInputDialog(doc: Document, o: TextInputOptions): Promise<boolean> {
  return new Promise((resolve) => {
    const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent?: Element, text?: string): HTMLElementTagNameMap[K] => {
      const e = doc.createElement(tag);
      if (cls) e.className = cls;
      if (text !== undefined) e.textContent = text;
      parent?.append(e);
      return e;
    };
    const backdrop = el('div', 'dc-picker-backdrop', doc.body);
    const win = el('div', 'dc-picker dc-save dc-text-input dc-app-floating', backdrop);
    win.setAttribute('role', 'dialog');
    win.setAttribute('aria-modal', 'true');
    win.setAttribute('aria-labelledby', 'dc-text-input-title');
    const before = focusedElement(doc) as HTMLElement | null;
    let busy = false;
    let done = false;
    let made = false;
    const finish = (): void => {
      if (done) return;
      done = true;
      doc.removeEventListener('keydown', onKey, true);
      backdrop.remove();
      before?.focus?.();
      resolve(made);
    };
    const onKey = (e: KeyboardEvent): void => {
      if (e.key === 'Escape' && !busy) {
        e.preventDefault();
        finish();
      }
    };
    doc.addEventListener('keydown', onKey, true);
    backdrop.addEventListener('mousedown', (e) => {
      if (e.target === backdrop && !busy) finish();
    });

    const head = el('div', 'dc-picker-head', win);
    const titles = el('div', 'dc-picker-titles', head);
    el('h2', 'dc-picker-title', titles, 'Text Input').id = 'dc-text-input-title';
    el('p', 'dc-picker-subtitle', titles, 'A small table of your own: type its values, or paste them from a sheet.');
    const close = closeMark(el('button', 'dc-picker-close', head));
    close.type = 'button';
    close.setAttribute('aria-label', 'Close');
    close.addEventListener('click', () => {
      if (!busy) finish();
    });

    const body = el('div', 'dc-save-body', win);
    const nameLabel = el('label', 'dc-text-input-name', body);
    el('span', 'dc-picker-field-label', nameLabel, 'Name');
    const name = el('input', 'dc-picker-input', nameLabel);
    name.value = o.start?.name ?? 'Text Input';
    name.setAttribute('aria-label', 'Its name');

    // THE TABLE: its head row its columns' names, a cell a value; its own "+" after its last column and its last row
    let columns: string[] = [...(o.start?.columns ?? ['Column1', 'Column2'])];
    let rows: string[][] = (o.start?.rows ?? [['', ''], ['', '']]).map((r) => columns.map((_, i) => r[i] ?? ''));
    const wrap = el('div', 'dc-text-input-wrap', body);
    const table = el('table', 'dc-text-input-table', wrap);
    const addRow = el('button', 'dc-picker-button dc-quiet dc-text-input-add-row', body, '+ Row');
    addRow.type = 'button';
    el('p', 'dc-sample-hint', body, 'Paste a block of cells (Ctrl+V) into a cell to fill the table from there. Each column\'s type is read from its values.');
    const status = el('p', 'dc-sample-status', body);
    status.setAttribute('role', 'status');

    /** Each cell, kept as typed; the table drawn again. `focus`: the cell to put the cursor in, after. */
    const draw = (focus?: { r: number; c: number }): void => {
      table.replaceChildren();
      const thead = el('thead', '', table);
      const hr = el('tr', '', thead);
      el('th', 'dc-text-input-corner', hr);
      columns.forEach((c, i) => {
        const th = el('th', '', hr);
        const field = el('input', 'dc-text-input-head', th);
        field.value = c;
        field.setAttribute('aria-label', `Column ${i + 1}'s name`);
        field.addEventListener('input', () => {
          columns[i] = field.value;
          check();
        });
        field.addEventListener('paste', (e) => pasted(e, -1, i));
        if (columns.length > 1) {
          const drop = closeMark(el('button', 'dc-text-input-drop', th), 'Take the column off');
          drop.type = 'button';
          drop.title = `Take column ${c || i + 1} off`;
          drop.setAttribute('aria-label', drop.title);
          drop.addEventListener('click', () => {
            columns = columns.filter((_, k) => k !== i);
            rows = rows.map((r) => r.filter((_, k) => k !== i));
            draw();
          });
        }
      });
      const plus = el('th', 'dc-text-input-plus', hr);
      const addColumn = el('button', 'dc-text-input-add-column', plus, '+');
      addColumn.type = 'button';
      addColumn.title = 'Add a column';
      addColumn.setAttribute('aria-label', 'Add a column');
      addColumn.addEventListener('click', () => {
        columns.push(freeName(columns));
        rows = rows.map((r) => [...r, '']);
        draw({ r: -1, c: columns.length - 1 });
      });
      const tbody = el('tbody', '', table);
      rows.forEach((row, r) => {
        const tr = el('tr', '', tbody);
        const n = el('th', 'dc-text-input-n', tr, String(r + 1));
        if (rows.length > 1) {
          n.title = 'Take this row off';
          n.addEventListener('click', () => {
            rows = rows.filter((_, k) => k !== r);
            draw();
          });
        }
        row.forEach((v, c) => {
          const td = el('td', '', tr);
          const field = el('input', 'dc-text-input-cell', td);
          field.value = v;
          field.setAttribute('aria-label', `Row ${r + 1}, ${columns[c] || `column ${c + 1}`}`);
          field.addEventListener('input', () => {
            rows[r]![c] = field.value;
            check();
          });
          field.addEventListener('paste', (e) => pasted(e, r, c));
          field.addEventListener('keydown', (e) => {
            // Enter goes down a row, a new one at the last
            if (e.key !== 'Enter') return;
            e.preventDefault();
            if (r === rows.length - 1) rows.push(columns.map(() => ''));
            draw({ r: r + 1, c });
          });
        });
        el('td', '', tr);
      });
      if (focus !== undefined) {
        const field = focus.r < 0
          ? table.querySelectorAll<HTMLInputElement>('.dc-text-input-head')[focus.c]
          : table.querySelectorAll('tbody tr')[focus.r]?.querySelectorAll<HTMLInputElement>('.dc-text-input-cell')[focus.c];
        field?.focus();
        field?.select();
      }
      check();
    };

    /** A block of cells pasted at row `r` (-1: the head row, its columns' names), column `c`: the table grown to hold it. */
    const pasted = (e: ClipboardEvent, r: number, c: number): void => {
      const text = e.clipboardData?.getData('text/plain') ?? '';
      const cells = pastedCells(text);
      // one value: the field's own paste
      if (cells.length <= 1 && (cells[0]?.length ?? 0) <= 1) return;
      e.preventDefault();
      const width = Math.max(...cells.map((l) => l.length));
      while (columns.length < c + width) {
        columns.push(freeName(columns));
        rows = rows.map((row) => [...row, '']);
      }
      cells.forEach((line, i) => {
        const at = r + i;
        if (at < 0) {
          line.forEach((v, k) => { columns[c + k] = v; });
          return;
        }
        while (rows.length <= at) rows.push(columns.map(() => ''));
        line.forEach((v, k) => { rows[at]![c + k] = v; });
      });
      draw();
    };

    const foot = el('div', 'dc-save-foot', win);
    const cancel = el('button', 'dc-picker-button dc-quiet', foot, 'Cancel');
    cancel.type = 'button';
    cancel.addEventListener('click', () => {
      if (!busy) finish();
    });
    el('span', 'dc-save-spacer', foot).setAttribute('aria-hidden', 'true');
    const primary = el('button', 'dc-picker-button dc-primary', foot, 'Add Input');
    primary.type = 'button';
    const typed = (): TypedTable => ({
      name: name.value.trim(),
      columns: columns.map((c) => c.trim()),
      // the rows with a value, as typed
      rows: rows.filter((r) => r.some((v) => v !== '')),
    });
    const check = (): void => {
      const problem = tableProblem(typed());
      primary.disabled = busy || problem !== undefined;
      primary.title = problem === undefined ? '' : `Not yet: ${problem}`;
    };
    name.addEventListener('input', check);
    addRow.addEventListener('click', () => {
      rows.push(columns.map(() => ''));
      draw({ r: rows.length - 1, c: 0 });
    });
    primary.addEventListener('click', () => void (async () => {
      busy = true;
      check();
      status.textContent = '';
      const refused = await o.apply(typed()).finally(() => { busy = false; });
      if (refused === null) {
        made = true;
        finish();
        return;
      }
      status.textContent = refused;
      check();
    })());
    draw({ r: 0, c: 0 });
  });
}

/** A column name the table has not got: Column1, Column2… */
function freeName(columns: readonly string[]): string {
  for (let i = columns.length + 1; ; i += 1) if (!columns.includes(`Column${i}`)) return `Column${i}`;
}
