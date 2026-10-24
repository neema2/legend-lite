// A TEXT TILE'S WORDS (Power BI's text box, Tableau's text object; docs/DATACUBE_BI_EXCEL_PARITY_2026_10_24.md P4): a
// page's title, a note beside a chart, a sheet's explanation -- written in a small Markdown, read here into blocks that
// say what they are (a heading, a paragraph, a list) and runs that say how they read (bold, italic, code, a link), and
// drawn as elements whose every word is text (never HTML), a link only http(s) or mailto -- anything else is its words.
// Its editor too. A chunk of its own, fetched with the first text tile drawn; its rule (page-text-rule.ts) is the page's.

import { textProblem } from './page-text-rule.ts';

export type TextRun =
  | { readonly kind: 'text'; readonly text: string }
  | { readonly kind: 'bold' | 'italic'; readonly runs: readonly TextRun[] }
  | { readonly kind: 'code'; readonly text: string }
  | { readonly kind: 'link'; readonly href: string; readonly runs: readonly TextRun[] }
  | { readonly kind: 'break' };

export type TextBlock =
  | { readonly kind: 'heading'; readonly level: 1 | 2 | 3; readonly runs: readonly TextRun[] }
  | { readonly kind: 'paragraph'; readonly runs: readonly TextRun[] }
  | { readonly kind: 'list'; readonly ordered: boolean; readonly items: readonly (readonly TextRun[])[] };

const HEADING = /^(#{1,3})\s+(.*)$/;
const BULLET = /^[-*]\s+(.*)$/;
const NUMBERED = /^\d+[.)]\s+(.*)$/;

/** The text's blocks: blank lines part paragraphs; `#`, `##`, `###` start a heading; `- ` or `1. ` a list's item. */
export function readText(text: string): TextBlock[] {
  const out: TextBlock[] = [];
  let paragraph: string[] = [];
  let list: { ordered: boolean; items: string[] } | null = null;
  const flush = (): void => {
    if (paragraph.length > 0) {
      out.push({ kind: 'paragraph', runs: paragraph.flatMap((line, i) => [...(i > 0 ? [{ kind: 'break' } as const] : []), ...readRuns(line)]) });
      paragraph = [];
    }
    if (list !== null) {
      out.push({ kind: 'list', ordered: list.ordered, items: list.items.map(readRuns) });
      list = null;
    }
  };
  for (const raw of text.replace(/\r\n?/g, '\n').split('\n')) {
    const line = raw.trimEnd();
    if (line.trim() === '') {
      flush();
      continue;
    }
    const heading = HEADING.exec(line.trimStart());
    if (heading) {
      flush();
      out.push({ kind: 'heading', level: heading[1]!.length as 1 | 2 | 3, runs: readRuns(heading[2]!) });
      continue;
    }
    const bullet = BULLET.exec(line.trimStart());
    const numbered = bullet ? null : NUMBERED.exec(line.trimStart());
    const item = bullet ?? numbered;
    if (item) {
      const ordered = numbered !== null;
      if (paragraph.length > 0 || (list !== null && list.ordered !== ordered)) flush();
      list ??= { ordered, items: [] };
      list.items.push(item[1]!);
      continue;
    }
    if (list !== null) flush();
    paragraph.push(line.trim());
  }
  flush();
  return out;
}

/** A link's address a page follows: http, https or mailto; anything else is no link (its words are kept as text). */
export function followable(href: string): boolean {
  return /^(https?:\/\/|mailto:)\S+$/i.test(href);
}

/** A line's runs: `**bold**`, `*italic*` or `_italic_`, `` `code` ``, `[words](address)`; a `\` keeps the next mark as
 *  text. A mark with no end is text. */
export function readRuns(line: string): TextRun[] {
  const out: TextRun[] = [];
  let text = '';
  const keep = (): void => {
    if (text !== '') out.push({ kind: 'text', text });
    text = '';
  };
  let i = 0;
  while (i < line.length) {
    const c = line[i]!;
    if (c === '\\' && i + 1 < line.length && '\\*_`[]()'.includes(line[i + 1]!)) {
      text += line[i + 1];
      i += 2;
      continue;
    }
    if (c === '`') {
      const end = line.indexOf('`', i + 1);
      if (end > i + 1) {
        keep();
        out.push({ kind: 'code', text: line.slice(i + 1, end) });
        i = end + 1;
        continue;
      }
    }
    if (line.startsWith('**', i)) {
      const end = line.indexOf('**', i + 2);
      if (end > i + 2) {
        keep();
        out.push({ kind: 'bold', runs: readRuns(line.slice(i + 2, end)) });
        i = end + 2;
        continue;
      }
    }
    if (c === '*' || c === '_') {
      const end = line.indexOf(c, i + 1);
      if (end > i + 1 && line[i + 1] !== ' ') {
        keep();
        out.push({ kind: 'italic', runs: readRuns(line.slice(i + 1, end)) });
        i = end + 1;
        continue;
      }
    }
    if (c === '[') {
      const close = line.indexOf('](', i + 1);
      const end = close < 0 ? -1 : line.indexOf(')', close + 2);
      if (close > i + 1 && end > close + 2) {
        const href = line.slice(close + 2, end).trim();
        const words = readRuns(line.slice(i + 1, close));
        keep();
        if (followable(href)) out.push({ kind: 'link', href, runs: words });
        else out.push(...words);
        i = end + 1;
        continue;
      }
    }
    text += c;
    i += 1;
  }
  keep();
  return out;
}

/** The text as elements of `doc`: a heading `h3`-`h5` (a tile's own title is its heading), a paragraph `p`, a list
 *  `ul`/`ol`; every word a text node, a link opening in a new tab. */
export function textElement(doc: Document, blocks: readonly TextBlock[]): HTMLElement {
  const body = doc.createElement('div');
  body.className = 'dc-text-tile';
  const runs = (into: HTMLElement, list: readonly TextRun[]): void => {
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
        case 'italic': {
          const el = doc.createElement(r.kind === 'bold' ? 'strong' : 'em');
          runs(el, r.runs);
          into.append(el);
          break;
        }
        case 'link': {
          const el = doc.createElement('a');
          el.href = r.href;
          el.target = '_blank';
          el.rel = 'noopener noreferrer';
          runs(el, r.runs);
          into.append(el);
          break;
        }
        case 'break':
          into.append(doc.createElement('br'));
          break;
      }
    }
  };
  for (const b of blocks) {
    if (b.kind === 'list') {
      const el = doc.createElement(b.ordered ? 'ol' : 'ul');
      for (const item of b.items) {
        const li = doc.createElement('li');
        runs(li, item);
        el.append(li);
      }
      body.append(el);
      continue;
    }
    const el = doc.createElement(b.kind === 'heading' ? `h${b.level + 2}` : 'p');
    runs(el, b.runs);
    body.append(el);
  }
  return body;
}

/**
 * A TEXT TILE'S EDITOR (Power BI's text box, edited in place): its words in a box, OK judged by the tile's rule
 * (textProblem: refused, the words are said and the editor stays) before `onOk` tells them, Cancel -- or Escape --
 * leaving them as they were; Ctrl+Enter is OK. Its elements, and its box to focus once they are placed.
 */
export function textEditor(doc: Document, o: {
  readonly words: string;
  readonly onOk: (words: string) => void;
  readonly onCancel: () => void;
}): { readonly elements: readonly HTMLElement[]; readonly area: HTMLTextAreaElement } {
  const area = doc.createElement('textarea');
  area.className = 'dc-text-area';
  area.value = o.words;
  area.setAttribute('aria-label', 'Text (Markdown: # a heading, **bold**, *italic*, - a list, [a link](https://...))');
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
    }
  });
  const bar = doc.createElement('div');
  bar.className = 'dc-text-bar';
  bar.append(said, button('Cancel', false, o.onCancel), button('OK', true, ok));
  return { elements: [area, bar], area };
}
