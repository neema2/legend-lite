// A change shown as upstream's diffs are (plan A7): an element's text before beside its text after, in Monaco's diff
// editor -- empty on the left for a new element, on the right for a deleted one. Local Changes and the review's changes
// both open it.

import type * as Monaco from 'monaco-editor/editor/editor.api';

import { icon } from '../../../legend-art/src/icon.ts';
import { h } from './dom.ts';
import { editorTheme, PURE } from './pure-language.ts';
import { theme } from './theme.ts';

/** A change of one element: its path, how it changed, and its text before and after (undefined: none). */
export interface ElementChange {
  readonly path: string;
  readonly type: 'CREATE' | 'MODIFY' | 'DELETE';
  readonly before: string | undefined;
  readonly after: string | undefined;
}

const WORD = { CREATE: 'new', MODIFY: 'modified', DELETE: 'deleted' } as const;

export function showDiff(monaco: typeof Monaco, change: ElementChange): void {
  const original = monaco.editor.createModel(change.before ?? '', PURE);
  const modified = monaco.editor.createModel(change.after ?? '', PURE);
  const host = h('div', { class: 'diff-view__editor' });
  const close = (): void => {
    diff.dispose();
    original.dispose();
    modified.dispose();
    overlay.remove();
  };
  const overlay = h('div', { class: 'overlay', 'data-testid': 'diff-view', onkeydown: ((e: KeyboardEvent) => { if (e.key === 'Escape') close(); }) as EventListener },
    h('div', { class: 'diff-view', role: 'dialog' },
      h('div', { class: 'diff-view__header' },
        h('span', { class: 'diff-view__title' }, `${change.path.split('::').pop() ?? change.path} (${WORD[change.type]})`),
        h('span', { class: 'diff-view__path' }, change.path),
        h('button', { class: 'panel-group__action', title: 'Close (Escape)', 'data-testid': 'diff-close', onclick: close }, icon('x', '18px'))),
      host));
  document.body.append(overlay);
  const diff = monaco.editor.createDiffEditor(host, {
    theme: editorTheme(theme() === 'light'), automaticLayout: true, readOnly: true, originalEditable: false,
    fontFamily: "'Roboto Mono'", fontSize: 14, renderSideBySide: true,
  });
  diff.setModel({ original, modified });
  diff.getModifiedEditor().focus();
}

/**
 * A conflict resolved (upstream's conflict resolution's merge editor, plan A7): the project line's text on the left, read
 * only, and on the right this workspace's -- or the resolution so far -- to edit into the element both should become.
 * "Use this resolution" hands the right side's text to `use`.
 */
export function showMerge(monaco: typeof Monaco, path: string, line: string | undefined, mine: string, use: (text: string) => void): void {
  const original = monaco.editor.createModel(line ?? '', PURE);
  const modified = monaco.editor.createModel(mine, PURE);
  const host = h('div', { class: 'diff-view__editor' });
  const close = (): void => {
    diff.dispose();
    original.dispose();
    modified.dispose();
    overlay.remove();
  };
  const overlay = h('div', { class: 'overlay', 'data-testid': 'merge-view', onkeydown: ((e: KeyboardEvent) => { if (e.key === 'Escape') close(); }) as EventListener },
    h('div', { class: 'diff-view', role: 'dialog' },
      h('div', { class: 'diff-view__header' },
        h('span', { class: 'diff-view__title' }, `Resolve ${path.split('::').pop() ?? path}`),
        h('span', { class: 'diff-view__path' }, "the project line's text (left) beside yours (right): edit the right into the element both should become"),
        h('button', { class: 'btn btn-small btn-primary', 'data-testid': 'merge-use', onclick: () => { use(modified.getValue()); close(); } }, 'Use this resolution'),
        h('button', { class: 'panel-group__action', title: 'Close (Escape)', 'data-testid': 'merge-close', onclick: close }, icon('x', '18px'))),
      host));
  document.body.append(overlay);
  const diff = monaco.editor.createDiffEditor(host, {
    theme: editorTheme(theme() === 'light'), automaticLayout: true, originalEditable: false,
    fontFamily: "'Roboto Mono'", fontSize: 14, renderSideBySide: true,
  });
  diff.setModel({ original, modified });
  diff.getModifiedEditor().focus();
}

/** The paths both sides changed from `base`, differently (a three-way merge's conflicts), in path order. */
export function conflictsAmong(base: ReadonlyMap<string, string>, line: ReadonlyMap<string, string>, mine: ReadonlyMap<string, string>): string[] {
  const paths = [...new Set([...base.keys(), ...line.keys(), ...mine.keys()])].sort();
  return paths.filter((p) => base.get(p) !== line.get(p) && base.get(p) !== mine.get(p) && line.get(p) !== mine.get(p));
}

/** What changed between two sets of files (path → text), by path: deleted, then created and modified, in path order. */
export function changesBetween(before: ReadonlyMap<string, string>, after: ReadonlyMap<string, string>): ElementChange[] {
  const paths = [...new Set([...before.keys(), ...after.keys()])].sort();
  const out: ElementChange[] = [];
  for (const path of paths) {
    const b = before.get(path);
    const a = after.get(path);
    if (b === a) continue;
    out.push({ path, type: b === undefined ? 'CREATE' : a === undefined ? 'DELETE' : 'MODIFY', before: b, after: a });
  }
  return out;
}
