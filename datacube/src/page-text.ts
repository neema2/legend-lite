// A TEXT TILE'S WORDS (Power BI's text box, Tableau's text object; docs/DATACUBE_BI_EXCEL_PARITY_2026_10_24.md P4): a
// page's title, a note beside a chart, a sheet's explanation -- written in GitHub's Markdown (the user, 2026-10-25:
// headings, lists nested and of tasks, quotes, code, tables, rules; bold, italic, struck, code, links), read here into
// blocks that say what they are and runs that say how they read, and drawn as elements whose every word is text (never
// HTML), a link only http(s) or mailto -- anything else is its words, and an image its words and a link to it.
// Its editor too: a toolbar that writes the marks, the words, and what they will look like beside them. A chunk of its
// own, fetched with the first text tile drawn; its rule (page-text-rule.ts) is the page's.

import { textProblem } from './page-text-rule.ts';
import { drawIcon } from './ui/tile-icons.ts';
import type { TextFileFormat } from './text-files.ts';
import { UI_LOCALE } from '../../engine-client/src/locale.ts';

export type TextRun =
  | { readonly kind: 'text'; readonly text: string }
  | { readonly kind: 'bold' | 'italic' | 'strike'; readonly runs: readonly TextRun[] }
  | { readonly kind: 'code'; readonly text: string }
  | { readonly kind: 'link'; readonly href: string; readonly runs: readonly TextRun[] }
  | { readonly kind: 'break' };

/** A list's item: its words, whether it is a task and done (`- [x]`), and what is under it (a list in a list). */
export interface TextItem {
  readonly runs: readonly TextRun[];
  readonly checked?: boolean;
  readonly blocks?: readonly TextBlock[];
}

/** A table column's alignment, as its delimiter row says it (`:--`, `:-:`, `--:`); none, the reader's. */
export type TextAlign = 'left' | 'center' | 'right' | null;

export type TextBlock =
  | { readonly kind: 'heading'; readonly level: 1 | 2 | 3 | 4 | 5 | 6; readonly runs: readonly TextRun[] }
  | { readonly kind: 'paragraph'; readonly runs: readonly TextRun[] }
  | { readonly kind: 'list'; readonly ordered: boolean; readonly start?: number; readonly items: readonly TextItem[] }
  | { readonly kind: 'quote'; readonly blocks: readonly TextBlock[] }
  | { readonly kind: 'code'; readonly language?: string; readonly text: string }
  | { readonly kind: 'rule' }
  | {
    readonly kind: 'table'; readonly align: readonly TextAlign[];
    readonly head: readonly (readonly TextRun[])[]; readonly rows: readonly (readonly (readonly TextRun[])[])[];
  };

const HEADING = /^ {0,3}(#{1,6})(?:\s+(.*?))?(?:\s+#+)?\s*$/;
const SETEXT = /^ {0,3}(=+|-+)\s*$/;
const FENCE = /^( {0,3})(`{3,}|~{3,})\s*([^`\s]*)[^`]*$/;
const RULE = /^ {0,3}([-*_])(?:\s*\1){2,}\s*$/;
const QUOTE = /^ {0,3}>\s?(.*)$/;
const ITEM = /^(\s*)([-*+]|(\d{1,9})[.)])(?:\s+(.*)|$)/;
const TASK = /^\[([ xX])\](?:\s+(.*)|$)/;
const DELIMITER = /^\s*\|?\s*:?-+:?\s*(?:\|\s*:?-+:?\s*)*\|?\s*$/;

/** How far a line is indented, a tab as four. */
function indentOf(line: string): number {
  let n = 0;
  for (const c of line) {
    if (c === ' ') n += 1;
    else if (c === '\t') n += 4 - (n % 4);
    else break;
  }
  return n;
}

/** `line` with `n` columns of its indent taken off (fewer, if it has fewer). */
function dedent(line: string, n: number): string {
  let i = 0;
  let col = 0;
  while (i < line.length && col < n && (line[i] === ' ' || line[i] === '\t')) {
    col += line[i] === '\t' ? 4 - (col % 4) : 1;
    i += 1;
  }
  return line.slice(i);
}

/** A table row's cells: split on each `|` not escaped and not in code, its outer pipes dropped. */
function cellsOf(line: string): string[] {
  const cells: string[] = [];
  let cell = '';
  let code = false;
  for (let i = 0; i < line.length; i += 1) {
    const c = line[i]!;
    if (c === '\\' && line[i + 1] === '|') {
      cell += '|';
      i += 1;
    } else if (c === '`') {
      code = !code;
      cell += c;
    } else if (c === '|' && !code) {
      cells.push(cell);
      cell = '';
    } else {
      cell += c;
    }
  }
  cells.push(cell);
  const t = line.trim();
  if (t.startsWith('|')) cells.shift();
  if (t.endsWith('|') && !t.endsWith('\\|') && cells.length > 0) cells.pop();
  return cells.map((c) => c.trim());
}

/** A table starts at `lines[at]`: a row of cells, then a delimiter row of as many. */
function tableAt(lines: readonly string[], at: number): boolean {
  const head = lines[at];
  const next = lines[at + 1];
  if (head === undefined || next === undefined || !head.includes('|') || !DELIMITER.test(next)) return false;
  return cellsOf(head).length === cellsOf(next).length;
}

/** Whether `line` starts a block of its own, ending a paragraph (or a list item's words) before it. */
function startsBlock(lines: readonly string[], at: number): boolean {
  const line = lines[at]!;
  return HEADING.test(line) || FENCE.test(line) || RULE.test(line) || QUOTE.test(line) || ITEM.test(line) || tableAt(lines, at);
}

/**
 * The text's blocks, as GitHub reads them: blank lines part paragraphs (a line break within one is kept); `#` to
 * `######` a heading (or a line of `===` or `---` under one); `- `, `* `, `+ ` or `1. ` a list's item, `- [ ] ` a task,
 * indented under one another a list in it; `> ` a quote; ```` ``` ```` or `~~~` fenced code; `---` a rule; a row of
 * cells over a row of `---` a table.
 */
export function readText(text: string): TextBlock[] {
  return readLines(text.replace(/\r\n?/g, '\n').split('\n'), 0);
}

/** How deep blocks (a quote in a quote, a list in a list) and marks (bold in a link in bold) are read: deeper, their
 *  words are text (the 2026-10-25 audit: a file of 3,000 nested quotes overflowed the stack; 2,000 nested lists took
 *  17 s). */
const DEEPEST = 16;

function readLines(lines: readonly string[], depth: number): TextBlock[] {
  if (depth > DEEPEST) {
    return [{ kind: 'paragraph', runs: lines.filter((l) => l.trim() !== '')
      .flatMap((l, i): TextRun[] => [...(i > 0 ? [{ kind: 'break' } as const] : []), { kind: 'text', text: l.trim() }]) }];
  }
  const out: TextBlock[] = [];
  let paragraph: string[] = [];
  const flush = (): void => {
    if (paragraph.length === 0) return;
    out.push({ kind: 'paragraph', runs: linesRuns(paragraph) });
    paragraph = [];
  };
  let i = 0;
  while (i < lines.length) {
    const line = lines[i]!.replace(/\s+$/, '');
    if (line.trim() === '') {
      flush();
      i += 1;
      continue;
    }
    // a line of = or - under a paragraph: the paragraph is a heading
    const setext = paragraph.length > 0 ? SETEXT.exec(line) : null;
    if (setext) {
      out.push({ kind: 'heading', level: setext[1]!.startsWith('=') ? 1 : 2, runs: linesRuns(paragraph) });
      paragraph = [];
      i += 1;
      continue;
    }
    const fence = FENCE.exec(line);
    if (fence) {
      flush();
      const [, lead = '', marks = '', language = ''] = fence;
      const body: string[] = [];
      i += 1;
      while (i < lines.length) {
        const l = lines[i]!;
        const close = /^ {0,3}(`{3,}|~{3,})\s*$/.exec(l);
        if (close && close[1]![0] === marks[0] && close[1]!.length >= marks.length) {
          i += 1;
          break;
        }
        body.push(dedent(l, lead.length));
        i += 1;
      }
      out.push({ kind: 'code', ...(language !== '' ? { language } : {}), text: body.join('\n') });
      continue;
    }
    const heading = HEADING.exec(line);
    if (heading) {
      flush();
      out.push({ kind: 'heading', level: heading[1]!.length as 1 | 2 | 3 | 4 | 5 | 6, runs: readRuns(heading[2] ?? '') });
      i += 1;
      continue;
    }
    if (RULE.test(line)) {
      flush();
      out.push({ kind: 'rule' });
      i += 1;
      continue;
    }
    if (QUOTE.test(line)) {
      flush();
      const inner: string[] = [];
      while (i < lines.length) {
        const q = QUOTE.exec(lines[i]!);
        // a quote's line, or one carried on under it (a paragraph's, not a new block or a blank)
        if (q) inner.push(q[1]!);
        else if (lines[i]!.trim() !== '' && inner.length > 0 && inner.at(-1)!.trim() !== '' && !startsBlock(lines, i)) inner.push(lines[i]!);
        else break;
        i += 1;
      }
      out.push({ kind: 'quote', blocks: readLines(inner, depth + 1) });
      continue;
    }
    if (tableAt(lines, i)) {
      flush();
      const head = cellsOf(line);
      const align = cellsOf(lines[i + 1]!).map((d): TextAlign => {
        const left = d.startsWith(':');
        const right = d.endsWith(':');
        return left && right ? 'center' : right ? 'right' : left ? 'left' : null;
      });
      const rows: TextRun[][][] = [];
      i += 2;
      while (i < lines.length && lines[i]!.trim() !== '' && lines[i]!.includes('|') && !startsBlock(lines, i)) {
        const cells = cellsOf(lines[i]!);
        // as many cells as the head: the rest dropped, the missing blank
        rows.push(head.map((_h, c) => readRuns(cells[c] ?? '')));
        i += 1;
      }
      out.push({ kind: 'table', align, head: head.map((h) => readRuns(h)), rows });
      continue;
    }
    if (ITEM.test(line)) {
      flush();
      const { block, next } = readList(lines, i, depth);
      out.push(block);
      i = next;
      continue;
    }
    paragraph.push(line.trim());
    i += 1;
  }
  flush();
  return out;
}

/** The list starting at `lines[at]`, and the line after it. */
function readList(lines: readonly string[], at: number, depth: number): { block: TextBlock; next: number } {
  const first = ITEM.exec(lines[at]!)!;
  const indent = indentOf(first[1]!);
  const ordered = first[3] !== undefined;
  const items: { words: string[]; checked?: boolean; under: string[]; at: number }[] = [];
  let i = at;
  while (i < lines.length) {
    const line = lines[i]!;
    if (line.trim() === '') {
      // a blank line: the list goes on if what follows is one of its items, or indented under its last
      let j = i + 1;
      while (j < lines.length && lines[j]!.trim() === '') j += 1;
      const next = lines[j];
      const item = next !== undefined ? ITEM.exec(next) : null;
      if (next !== undefined && items.length > 0 && (indentOf(next) > indent
        || (item && indentOf(item[1]!) === indent && (item[3] !== undefined) === ordered))) {
        items.at(-1)!.under.push('');
        i = j;
        continue;
      }
      break;
    }
    const item = ITEM.exec(line);
    const lead = indentOf(line);
    if (item && lead === indent) {
      if ((item[3] !== undefined) !== ordered) break;
      const words = item[4] ?? '';
      const task = TASK.exec(words);
      items.push({ words: [task ? task[2] ?? '' : words], ...(task ? { checked: task[1] !== ' ' } : {}), under: [], at: 0 });
      // what is under it is indented past its mark
      items.at(-1)!.at = indent + item[2]!.length + 1;
      i += 1;
      continue;
    }
    const last = items.at(-1);
    if (!last) break;
    if (lead > indent) {
      // indented under it: its words carried on, or a block of its own (a list in it) and all after
      if (last.under.length === 0 && !item && !startsBlock(lines, i)) last.words.push(line.trim());
      else last.under.push(line);
      i += 1;
      continue;
    }
    // a line carried on, not indented: its words, unless a block starts
    if (last.under.length === 0 && !startsBlock(lines, i)) {
      last.words.push(line.trim());
      i += 1;
      continue;
    }
    break;
  }
  const start = ordered ? Number(first[3]) : 1;
  return {
    block: {
      kind: 'list', ordered, ...(ordered && start !== 1 ? { start } : {}),
      items: items.map((it): TextItem => {
        const under = it.under.map((l) => dedent(l, it.at));
        while (under.length > 0 && under.at(-1)!.trim() === '') under.pop();
        const blocks = under.length > 0 ? readLines(under, depth + 1) : [];
        return {
          runs: linesRuns(it.words),
          ...(it.checked !== undefined ? { checked: it.checked } : {}),
          ...(blocks.length > 0 ? { blocks } : {}),
        };
      }),
    },
    next: i,
  };
}

/** Lines as one run of words, a break between each. */
function linesRuns(lines: readonly string[]): TextRun[] {
  return lines.flatMap((line, i) => [...(i > 0 ? [{ kind: 'break' } as const] : []), ...readRuns(line)]);
}

/** A link's address a page follows: http, https or mailto; anything else is no link (its words are kept as text). */
export function followable(href: string): boolean {
  return /^(https?:\/\/|mailto:)\S+$/i.test(href);
}

const WORD = /[\p{L}\p{N}]/u;
/** A bare address at the index it is tried from (`lastIndex`): sticky, so no copy of the line's rest is made for it. */
const BARE_LINK = /https?:\/\/[^\s<]+/iy;

/**
 * Each `open` of `line` paired with its `close` at the same depth (`\` keeps the next character as text): a link's `]`
 * past the brackets inside its words, its address's `)` past the parentheses inside it (`.../Foo_(bar)`). One pass.
 */
function pairs(line: string, open: string, close: string): Map<number, number> {
  const out = new Map<number, number>();
  const stack: number[] = [];
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '\\') i++;
    else if (c === open) stack.push(i);
    else if (c === close && stack.length > 0) out.set(stack.pop()!, i);
  }
  return out;
}

/** Runs with their links let go, their words kept: a link's words hold no link (`[https://a](https://b)`). */
function unlinked(list: readonly TextRun[]): TextRun[] {
  return list.flatMap((r): TextRun[] => {
    if (r.kind === 'link') return unlinked(r.runs);
    if (r.kind === 'bold' || r.kind === 'italic' || r.kind === 'strike') return [{ ...r, runs: unlinked(r.runs) }];
    return [r];
  });
}

/**
 * A line's runs: `**bold**` or `__bold__`, `*italic*` or `_italic_` (an `_` inside a word is the word's: `trade_id`),
 * `***both***`, `~~struck~~`, `` `code` ``, `[words](address)`, `<address>` and a bare `https://...`; an image
 * (`![words](address)`) its words linked to it. A `\` keeps the next mark as text; a mark with no end is text.
 */
export function readRuns(line: string, depth = 0): TextRun[] {
  if (depth > DEEPEST) return line === '' ? [] : [{ kind: 'text', text: line }];
  const out: TextRun[] = [];
  let text = '';
  const keep = (): void => {
    if (text !== '') out.push({ kind: 'text', text });
    text = '';
  };
  // LINEAR IN THE LINE (the 2026-10-25 audit: a file's line of 60 KB of unclosed `*a ` took 4 s): what each search
  // found is remembered -- the first `needle` at or after an index, and the first close of a mark (whether one closes
  // depends on it alone, never on its opener) -- so the opener after reads it, not the line's rest again
  const searched = new Map<string, { from: number; at: number }>();
  const remembered = (key: string, from: number, find: () => number): number => {
    const known = searched.get(key);
    if (known && from >= known.from && (known.at < 0 || from <= known.at)) return known.at;
    const at = find();
    searched.set(key, { from, at });
    return at;
  };
  const next = (needle: string, from: number): number => remembered(`=${needle}`, from, () => line.indexOf(needle, from));
  /** A span from `i`: `mark` ... `mark`, its inside read as runs; `word` marks (`_`) neither open nor close inside a
   *  word. The index after it, or -1. */
  const span = (i: number, mark: string, word: boolean): { inside: string; end: number } | null => {
    if (!line.startsWith(mark, i) || line[i + mark.length] === ' ' || line[i + mark.length] === undefined) return null;
    if (word && i > 0 && WORD.test(line[i - 1]!)) return null;
    const from = i + mark.length + 1;
    const close = remembered(`${word ? 'w' : 'm'}${mark}`, from, () => {
      for (let at = from; ;) {
        const c = line.indexOf(mark, at);
        if (c < 0) return -1;
        const after = line[c + mark.length];
        if (line[c - 1] !== ' ' && line[c - 1] !== '\\' && !(word && after !== undefined && WORD.test(after))) return c;
        at = c + 1;
      }
    });
    return close < 0 ? null : { inside: line.slice(i + mark.length, close), end: close + mark.length };
  };
  let brackets: Map<number, number> | undefined;
  let parens: Map<number, number> | undefined;
  let i = 0;
  while (i < line.length) {
    const c = line[i]!;
    if (c === '\\' && i + 1 < line.length && '\\`*_{}[]()#+-.!|~<>'.includes(line[i + 1]!)) {
      text += line[i + 1];
      i += 2;
      continue;
    }
    if (c === '`') {
      let n = 1;
      while (line[i + n] === '`') n++;
      const ticks = '`'.repeat(n);
      const end = next(ticks, i + n);
      if (end >= 0) {
        keep();
        const inside = line.slice(i + ticks.length, end);
        out.push({ kind: 'code', text: /^ .* $/.test(inside) ? inside.slice(1, -1) : inside });
        i = end + ticks.length;
        continue;
      }
      text += ticks;
      i += ticks.length;
      continue;
    }
    const both = span(i, '***', false) ?? span(i, '___', true);
    if (both) {
      keep();
      out.push({ kind: 'bold', runs: [{ kind: 'italic', runs: readRuns(both.inside, depth + 1) }] });
      i = both.end;
      continue;
    }
    const bold = span(i, '**', false) ?? span(i, '__', true);
    if (bold) {
      keep();
      out.push({ kind: 'bold', runs: readRuns(bold.inside, depth + 1) });
      i = bold.end;
      continue;
    }
    const struck = span(i, '~~', false);
    if (struck) {
      keep();
      out.push({ kind: 'strike', runs: readRuns(struck.inside, depth + 1) });
      i = struck.end;
      continue;
    }
    const italic = c === '*' || c === '_' ? span(i, c, c === '_') : null;
    if (italic) {
      keep();
      out.push({ kind: 'italic', runs: readRuns(italic.inside, depth + 1) });
      i = italic.end;
      continue;
    }
    const image = c === '!' && line[i + 1] === '[';
    if (c === '[' || image) {
      const open = image ? i + 1 : i;
      // its own `]`, past brackets in its words -- `[1] or [docs](...)` is two, the first no link (the 2026-10-25 audit)
      // -- then its `(`, and the `)` that closes it, past parentheses in its address
      brackets ??= pairs(line, '[', ']');
      const close = brackets.get(open) ?? -1;
      parens ??= pairs(line, '(', ')');
      const end = close < 0 || line[close + 1] !== '(' ? -1 : parens.get(close + 1) ?? -1;
      if (close > open + 1 && end > close + 2) {
        // the address, without a title after it (`[x](https://... "title")`)
        const href = line.slice(close + 2, end).trim().split(/\s+/)[0] ?? '';
        const words = unlinked(readRuns(line.slice(open + 1, close), depth + 1));
        keep();
        if (followable(href)) out.push({ kind: 'link', href, runs: words });
        else out.push(...words);
        i = end + 1;
        continue;
      }
    }
    if (c === '<') {
      const end = next('>', i + 1);
      const href = end < 0 ? '' : line.slice(i + 1, end);
      if (followable(href)) {
        keep();
        out.push({ kind: 'link', href, runs: [{ kind: 'text', text: href.replace(/^mailto:/i, '') }] });
        i = end + 1;
        continue;
      }
    }
    if ((c === 'h' || c === 'H') && (i === 0 || !WORD.test(line[i - 1]!))) {
      BARE_LINK.lastIndex = i;
      const bare = BARE_LINK.exec(line);
      if (bare) {
        // a sentence's last mark is the sentence's, not the address's
        const href = bare[0].replace(/[.,;:!?'")\]]+$/, '');
        if (followable(href)) {
          keep();
          out.push({ kind: 'link', href, runs: [{ kind: 'text', text: href }] });
          i += href.length;
          continue;
        }
      }
    }
    text += c;
    i += 1;
  }
  keep();
  return out;
}

/**
 * The text as elements of `doc`: a heading `h3`-`h6` (a tile's own title is its heading), a paragraph `p`, a list
 * `ul`/`ol` (a task's box drawn, not pressed), a quote `blockquote`, code `pre`, a rule `hr`, a table `table`; every word
 * a text node, a link opening in a new tab. `preview`: drawn for its editor's preview -- the words' type, not the tile's
 * drawn words (what a page saves and exports: `.dc-text-tile`).
 */
export function textElement(doc: Document, blocks: readonly TextBlock[], preview = false): HTMLElement {
  const body = doc.createElement('div');
  body.className = preview ? 'dc-text-words' : 'dc-text-tile dc-text-words';
  drawBlocks(doc, body, blocks);
  return body;
}

/** The rows of a CSV file a text box draws; the rest are counted. */
export const FILE_ROWS = 200;
/** The columns of a CSV file a text box draws; the rest are counted (the 2026-10-25 audit: a file of commas was a header
 *  of two million cells). */
export const FILE_COLUMNS = 100;

/**
 * A FILE IN A TEXT BOX (text-files.ts; docs/DATACUBE_UI_REVIEW_2026_10_24.md §8): Markdown drawn as the box draws its own
 * words, plain text as it is, JSON indented (as it is, and said, when it does not read), CSV as a table -- its first line
 * the header, its first FILE_ROWS rows and FILE_COLUMNS columns, the rest counted. Every word a text node: a file's own
 * markup is never run. The words' type, not the tile's drawn words (`.dc-text-tile`: what a page exports is the box's
 * own words). `cut`: the text is the file's start, said so (`its first 2 MB`) -- JSON then shown as it is, unread.
 */
export function fileElement(doc: Document, format: TextFileFormat, text: string, separator: ',' | '\t', cut?: string): HTMLElement {
  const box = doc.createElement('div');
  box.className = 'dc-text-file dc-text-words';
  const pre = (words: string): void => {
    const el = doc.createElement('pre');
    el.className = 'dc-text-file-plain';
    el.textContent = words;
    box.append(el);
  };
  const note = (words: string): void => {
    const el = doc.createElement('p');
    el.className = 'dc-text-file-note';
    el.textContent = words;
    box.append(el);
  };
  switch (format) {
    case 'markdown':
      drawBlocks(doc, box, readText(text));
      break;
    case 'text':
      pre(text);
      break;
    case 'json': {
      // its start is no JSON a browser reads: shown as it is, the cut said by its caller, not as a fault of the file
      if (cut !== undefined) {
        pre(text);
        break;
      }
      let indented: string | undefined;
      try {
        indented = JSON.stringify(JSON.parse(text), null, 2);
      } catch (e) {
        note(`This is not JSON a browser reads (${e instanceof Error ? e.message : String(e)}); shown as it is.`);
      }
      pre(indented ?? text);
      break;
    }
    case 'csv': {
      const { rows, more } = csvRows(text, separator, FILE_ROWS + 1);
      const [head, ...body] = rows;
      if (head === undefined) {
        note('The file is empty.');
        break;
      }
      const table = doc.createElement('table');
      table.className = 'dc-text-file-table';
      const thead = doc.createElement('thead');
      const tr = doc.createElement('tr');
      const wide = Math.max(head.length, ...body.map((r) => r.length));
      for (const cell of head.slice(0, FILE_COLUMNS)) {
        const th = doc.createElement('th');
        th.textContent = cell;
        tr.append(th);
      }
      thead.append(tr);
      const tbody = doc.createElement('tbody');
      for (const row of body) {
        const line = doc.createElement('tr');
        for (const cell of row.slice(0, FILE_COLUMNS)) {
          const td = doc.createElement('td');
          td.textContent = cell;
          line.append(td);
        }
        tbody.append(line);
      }
      table.append(thead, tbody);
      box.append(table);
      const within = cut === undefined ? '' : ` in ${cut}`;
      if (more > 0) note(`and ${more.toLocaleString(UI_LOCALE)} more row${more === 1 ? '' : 's'}${within}`);
      if (wide > FILE_COLUMNS) {
        const over = wide - FILE_COLUMNS;
        note(`and ${over.toLocaleString(UI_LOCALE)} more column${over === 1 ? '' : 's'}`);
      }
      break;
    }
  }
  return box;
}

/**
 * `text` as CSV rows, `most` of them kept and the rest counted (`more`): RFC 4180's quoting -- a quoted field may hold
 * the separator, a line break, and "" for a quote; a line break is CRLF or LF; a last empty line is no row.
 */
export function csvRows(text: string, separator: string, most: number): { rows: string[][]; more: number } {
  const rows: string[][] = [];
  let more = 0;
  let row: string[] = [];
  let field = '';
  let quoted = false;
  let started = false;
  const endRow = (): void => {
    row.push(field);
    if (rows.length < most) rows.push(row);
    else more += 1;
    row = [];
    field = '';
    started = false;
  };
  for (let i = 0; i < text.length; i++) {
    const c = text[i]!;
    if (quoted) {
      if (c === '"' && text[i + 1] === '"') {
        field += '"';
        i++;
      } else if (c === '"') quoted = false;
      else field += c;
      continue;
    }
    if (c === '"' && field === '') {
      quoted = true;
      started = true;
    } else if (c === separator) {
      row.push(field);
      field = '';
      started = true;
    } else if (c === '\n' || c === '\r') {
      if (c === '\r' && text[i + 1] === '\n') i++;
      endRow();
    } else {
      field += c;
      started = true;
    }
  }
  if (started || field !== '' || row.length > 0) endRow();
  return { rows, more };
}

function drawRuns(doc: Document, into: HTMLElement, list: readonly TextRun[]): void {
  for (const r of list) {
    switch (r.kind) {
      case 'text':
        into.append(doc.createTextNode(r.text));
        break;
      case 'code': {
        const el = doc.createElement('code');
        el.textContent = r.text;
        into.append(el);
        break;
      }
      case 'bold':
      case 'italic':
      case 'strike': {
        const el = doc.createElement(r.kind === 'bold' ? 'strong' : r.kind === 'italic' ? 'em' : 'del');
        drawRuns(doc, el, r.runs);
        into.append(el);
        break;
      }
      case 'link': {
        const el = doc.createElement('a');
        el.href = r.href;
        el.target = '_blank';
        el.rel = 'noopener noreferrer';
        drawRuns(doc, el, r.runs);
        into.append(el);
        break;
      }
      case 'break':
        into.append(doc.createElement('br'));
        break;
    }
  }
}

function drawBlocks(doc: Document, into: HTMLElement, blocks: readonly TextBlock[]): void {
  for (const b of blocks) {
    switch (b.kind) {
      case 'heading':
      case 'paragraph': {
        const el = doc.createElement(b.kind === 'heading' ? `h${Math.min(b.level + 2, 6)}` : 'p');
        drawRuns(doc, el, b.runs);
        into.append(el);
        break;
      }
      case 'list': {
        const el = doc.createElement(b.ordered ? 'ol' : 'ul');
        if (b.start !== undefined) el.setAttribute('start', String(b.start));
        for (const item of b.items) {
          const li = doc.createElement('li');
          if (item.checked !== undefined) {
            li.className = 'dc-text-task';
            const box = doc.createElement('input');
            box.type = 'checkbox';
            // its attribute, not only its property: an export writes the element's HTML (the 2026-10-25 audit)
            box.toggleAttribute('checked', item.checked);
            box.disabled = true;
            li.append(box);
          }
          drawRuns(doc, li, item.runs);
          if (item.blocks) drawBlocks(doc, li, item.blocks);
          el.append(li);
        }
        into.append(el);
        break;
      }
      case 'quote': {
        const el = doc.createElement('blockquote');
        drawBlocks(doc, el, b.blocks);
        into.append(el);
        break;
      }
      case 'code': {
        const pre = doc.createElement('pre');
        const code = doc.createElement('code');
        if (b.language !== undefined) code.dataset['language'] = b.language;
        code.textContent = b.text;
        pre.append(code);
        into.append(pre);
        break;
      }
      case 'rule':
        into.append(doc.createElement('hr'));
        break;
      case 'table': {
        // in a box of its own, which scrolls sideways where the tile is narrower than the table
        const box = doc.createElement('div');
        box.className = 'dc-text-table';
        const table = doc.createElement('table');
        const row = (cells: readonly (readonly TextRun[])[], tag: 'th' | 'td'): HTMLTableRowElement => {
          const tr = doc.createElement('tr');
          cells.forEach((cell, c) => {
            const el = doc.createElement(tag);
            const align = b.align[c];
            if (align) el.style.textAlign = align;
            drawRuns(doc, el, cell);
            tr.append(el);
          });
          return tr;
        };
        const head = doc.createElement('thead');
        head.append(row(b.head, 'th'));
        table.append(head);
        if (b.rows.length > 0) {
          const rows = doc.createElement('tbody');
          for (const r of b.rows) rows.append(row(r, 'td'));
          table.append(rows);
        }
        box.append(table);
        into.append(box);
        break;
      }
    }
  }
}

/** The toolbar's drawings, as the tiles' (ui/tile-icons.ts): one stroke on a 16-unit square. Here, not there: they are
 *  fetched with the editor, not with every page. */
const ICONS = {
  // code: angle brackets
  code: ['M6 4.5 2.5 8 6 11.5', 'M10 4.5 13.5 8 10 11.5'],
  // a list: dots and lines
  bullets: ['M3 4.5h.01', 'M3 8h.01', 'M3 11.5h.01', 'M6 4.5h7.5', 'M6 8h7.5', 'M6 11.5h7.5'],
  // a numbered list: a one, a two, and lines
  numbers: ['M2.6 3.3l.9-.6v3', 'M2.6 9.4a.9.9 0 0 1 1.8.2c0 .6-1.8 1.7-1.8 1.7h1.9', 'M7 4.5h6.5', 'M7 11h6.5'],
  // tasks: a box ticked, a box, lines
  tasks: ['M2.5 3h4v4h-4z', 'M3.5 5l1 1 1.5-2', 'M9 5h4.5', 'M2.5 9.5h4v4h-4z', 'M9 11.5h4.5'],
  // a quote: a bar and its words
  quote: ['M3 4v8', 'M6 5.5h7', 'M6 8h7', 'M6 10.5h5'],
  // a link: two links of a chain
  link: ['M7 9.5 9 7.5', 'M6 7.5 4.5 9a1.8 1.8 0 0 0 2.5 2.5L8.5 10', 'M10 8.5 11.5 7A1.8 1.8 0 0 0 9 4.5L7.5 6'],
  // a block of code: braces
  codeBlock: ['M6 3c-1.5 0-1.5 1-1.5 2.2S4.2 8 3 8c1.2 0 1.5.6 1.5 2.8S4.5 13 6 13', 'M10 3c1.5 0 1.5 1 1.5 2.2S11.8 8 13 8c-1.2 0-1.5.6-1.5 2.8S11.5 13 10 13'],
  // a table: a grid
  table: ['M2.5 3.5h11v9h-11z', 'M2.5 6.5h11', 'M2.5 9.5h11', 'M8 3.5v9'],
  // a rule: a line
  rule: ['M2.5 8h11'],
} as const;

/** One of the toolbar's marks: its name, its face (a letter, or drawn), its key with Ctrl (or ⌘), and what it does to
 *  the words. */
interface Mark {
  readonly name: string;
  readonly face: string | { readonly icon: keyof typeof ICONS };
  readonly key?: string;
  readonly act: (area: HTMLTextAreaElement) => void;
}

/** The selection wrapped in `before` and `after` -- or, none, `placeholder` between them, chosen to type over. */
function wrap(area: HTMLTextAreaElement, before: string, after: string, placeholder: string): void {
  const { selectionStart: from, selectionEnd: to, value } = area;
  const chosen = value.slice(from, to);
  const inside = chosen === '' ? placeholder : chosen;
  replace(area, from, to, `${before}${inside}${after}`, from + before.length, from + before.length + inside.length);
}

/**
 * Each line the selection touches started with `prefix` (its number, for a numbered list) in place of any mark of
 * `kind` it had -- a bulleted list made a task list, not taken apart (the 2026-10-25 audit) -- or, all of them already
 * marked as `own` says, no longer. A blank line is left blank, and counts no number.
 */
function prefixLines(area: HTMLTextAreaElement, prefix: (n: number) => string, own: RegExp, kind: RegExp = own): void {
  const { selectionStart, selectionEnd, value } = area;
  const from = value.lastIndexOf('\n', selectionStart - 1) + 1;
  const endAt = value.indexOf('\n', selectionEnd);
  const to = endAt < 0 ? value.length : endAt;
  const lines = value.slice(from, to).split('\n');
  const words = lines.filter((l) => l.trim() !== '');
  const off = words.length > 0 && words.every((l) => own.test(l));
  let n = 0;
  const next = lines.map((l) => {
    if (l.trim() === '') return l;
    const bare = l.replace(kind, '');
    n += 1;
    return off ? bare : `${prefix(n)}${bare}`;
  }).join('\n');
  replace(area, from, to, next, from, from + next.length);
}

/**
 * `value[from, to)` replaced by `text`, `select` chosen after, told as an edit is (an `input` event: the preview). Typed
 * where the browser types (`insertText`), so its undo takes it back as it would a person's typing (the 2026-10-25
 * audit: a toolbar's edit was no step of the textarea's undo); else written in place.
 */
function replace(area: HTMLTextAreaElement, from: number, to: number, text: string, start: number, end: number): void {
  area.focus();
  area.setSelectionRange(from, to);
  const doc = area.ownerDocument as Document & { execCommand?: (command: string, ui: boolean, value: string) => boolean };
  let typed = false;
  try {
    typed = typeof doc.execCommand === 'function' && doc.execCommand('insertText', false, text) && area.value.slice(from, from + text.length) === text;
  } catch {
    typed = false;
  }
  if (!typed) {
    area.setRangeText(text, from, to, 'preserve');
    area.dispatchEvent(new (doc.defaultView ?? globalThis).Event('input', { bubbles: true }));
  }
  area.setSelectionRange(start, end);
}

/** At the line's start, a block of its own (code, a table, a rule): on a line of its own, blank lines about it. */
function insertBlock(area: HTMLTextAreaElement, block: string, chooseFrom: number, chooseTo: number): void {
  const { selectionStart: from, selectionEnd: to, value } = area;
  const before = value.slice(0, from);
  const lead = before === '' || before.endsWith('\n\n') ? '' : before.endsWith('\n') ? '\n' : '\n\n';
  const after = value.slice(to);
  const trail = after === '' || after.startsWith('\n\n') ? '' : after.startsWith('\n') ? '\n' : '\n\n';
  const at = from + lead.length;
  replace(area, from, to, `${lead}${block}${trail}`, at + chooseFrom, at + chooseTo);
}

const LIST_MARK = /^\s*(?:[-*+]|\d{1,9}[.)])\s+(?:\[[ xX]\]\s+)?/;
/** Each list's own mark: what its button takes off again (a bullet that is not a task's, a number, a task's box). */
const BULLET_MARK = /^\s*[-*+]\s+(?!\[[ xX]\])/;
const NUMBER_MARK = /^\s*\d{1,9}[.)]\s+/;
const TASK_MARK = /^\s*[-*+]\s+\[[ xX]\]\s+/;

const MARKS: readonly (Mark | null)[] = [
  { name: 'Bold', face: 'B', key: 'b', act: (a) => wrap(a, '**', '**', 'bold') },
  { name: 'Italic', face: 'I', key: 'i', act: (a) => wrap(a, '*', '*', 'italic') },
  { name: 'Strikethrough', face: 'S', act: (a) => wrap(a, '~~', '~~', 'struck') },
  { name: 'Inline code', face: { icon: 'code' }, act: (a) => wrap(a, '`', '`', 'code') },
  null,
  {
    name: 'Heading', face: 'H',
    // a line's heading, one level deeper each time: #, ##, ###, then none
    act: (a) => {
      const { selectionStart, value } = a;
      const from = value.lastIndexOf('\n', selectionStart - 1) + 1;
      const endAt = value.indexOf('\n', selectionStart);
      const to = endAt < 0 ? value.length : endAt;
      const line = value.slice(from, to);
      const level = /^(#{1,6})\s/.exec(line)?.[1]?.length ?? 0;
      const words = line.replace(/^#{1,6}\s+/, '');
      const next = level >= 3 ? words : `${'#'.repeat(level + 1)} ${words}`;
      replace(a, from, to, next, from + next.length - words.length, from + next.length);
    },
  },
  { name: 'Bulleted list', face: { icon: 'bullets' }, act: (a) => prefixLines(a, () => '- ', BULLET_MARK, LIST_MARK) },
  { name: 'Numbered list', face: { icon: 'numbers' }, act: (a) => prefixLines(a, (n) => `${n}. `, NUMBER_MARK, LIST_MARK) },
  { name: 'Task list', face: { icon: 'tasks' }, act: (a) => prefixLines(a, () => '- [ ] ', TASK_MARK, LIST_MARK) },
  { name: 'Quote', face: { icon: 'quote' }, act: (a) => prefixLines(a, () => '> ', /^\s{0,3}>\s?/) },
  null,
  {
    name: 'Link', face: { icon: 'link' }, key: 'k',
    act: (a) => {
      const { selectionStart: from, selectionEnd: to, value } = a;
      const words = value.slice(from, to) || 'words';
      const text = `[${words}](https://)`;
      // the address chosen, to type
      replace(a, from, to, text, from + words.length + 3, from + text.length - 1);
    },
  },
  {
    name: 'Code block', face: { icon: 'codeBlock' },
    act: (a) => {
      const chosen = a.value.slice(a.selectionStart, a.selectionEnd);
      const inside = chosen === '' ? 'code' : chosen;
      insertBlock(a, `\`\`\`\n${inside}\n\`\`\``, 4, 4 + inside.length);
    },
  },
  {
    name: 'Table', face: { icon: 'table' },
    act: (a) => {
      const table = '| Column | Column |\n| --- | --- |\n| Cell | Cell |';
      insertBlock(a, table, 2, 8);
    },
  },
  { name: 'Horizontal rule', face: { icon: 'rule' }, act: (a) => insertBlock(a, '---', 3, 3) },
];

/**
 * A TEXT TILE'S EDITOR (Power BI's text box, edited in place; the user, 2026-10-25: Markdown with a toolbar): a toolbar
 * that writes the marks around what is chosen (Ctrl+B, Ctrl+I, Ctrl+K as well), its words in a box, and what they will
 * look like beside them, drawn as they are typed. OK judged by the tile's rule (textProblem: refused, the words are
 * said and the editor stays) before `onOk` tells them, Cancel -- or Escape -- leaving them as they were; Ctrl+Enter is
 * OK. Its elements, and its box to focus once they are placed.
 */
export function textEditor(doc: Document, o: {
  readonly words: string;
  readonly onOk: (words: string) => void;
  readonly onCancel: () => void;
}): { readonly elements: readonly HTMLElement[]; readonly area: HTMLTextAreaElement } {
  const area = doc.createElement('textarea');
  area.className = 'dc-text-area';
  area.value = o.words;
  area.spellcheck = true;
  area.setAttribute('aria-label', 'Text, in Markdown');
  const preview = doc.createElement('div');
  preview.className = 'dc-text-preview';
  preview.setAttribute('aria-label', 'How it will look');
  const paint = (): void => {
    preview.replaceChildren(textElement(doc, readText(area.value), true));
  };
  paint();
  area.addEventListener('input', paint);

  const tools = doc.createElement('div');
  tools.className = 'dc-text-tools';
  tools.setAttribute('role', 'toolbar');
  tools.setAttribute('aria-label', 'Formatting');
  for (const mark of MARKS) {
    if (mark === null) {
      const sep = doc.createElement('span');
      sep.className = 'dc-text-tools-sep';
      sep.setAttribute('aria-hidden', 'true');
      tools.append(sep);
      continue;
    }
    const b = doc.createElement('button');
    b.type = 'button';
    b.className = 'dc-text-tool';
    b.dataset['mark'] = mark.name;
    if (typeof mark.face === 'string') b.textContent = mark.face;
    else b.append(drawIcon(doc, 'dc-tool-icon', ICONS[mark.face.icon], false));
    b.title = mark.key ? `${mark.name} (Ctrl+${mark.key.toUpperCase()})` : mark.name;
    b.setAttribute('aria-label', mark.name);
    // the words keep their selection: a press does not take the focus from them
    b.addEventListener('mousedown', (e) => e.preventDefault());
    b.addEventListener('click', () => mark.act(area));
    tools.append(b);
  }

  const said = doc.createElement('div');
  said.className = 'dc-text-problem';
  said.setAttribute('role', 'alert');
  const button = (label: string, primary: boolean, act: () => void): HTMLButtonElement => {
    const b = doc.createElement('button');
    b.type = 'button';
    b.className = primary ? 'dc-button dc-primary' : 'dc-button';
    b.textContent = label;
    b.addEventListener('click', act);
    return b;
  };
  const ok = (): void => {
    const problem = area.value === o.words ? null : textProblem(area.value);
    if (problem !== null) said.textContent = problem;
    else o.onOk(area.value);
  };
  area.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      e.preventDefault();
      o.onCancel();
    } else if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
      e.preventDefault();
      ok();
    } else if ((e.ctrlKey || e.metaKey) && !e.altKey && !e.shiftKey) {
      const mark = MARKS.find((m) => m?.key === e.key.toLowerCase());
      if (mark) {
        e.preventDefault();
        mark.act(area);
      }
    }
  });
  const panes = doc.createElement('div');
  panes.className = 'dc-text-panes';
  panes.append(area, preview);
  const bar = doc.createElement('div');
  bar.className = 'dc-text-bar';
  const help = doc.createElement('a');
  help.className = 'dc-text-help';
  help.href = 'https://docs.github.com/en/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax';
  help.target = '_blank';
  help.rel = 'noopener noreferrer';
  help.textContent = 'Markdown';
  help.title = 'GitHub’s Markdown: what each mark does';
  bar.append(help, said, button('Cancel', false, o.onCancel), button('OK', true, ok));
  return { elements: [tools, panes, bar], area };
}
