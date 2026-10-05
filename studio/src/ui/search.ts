// Upstream's element search (Ctrl + P, plan A7): every element of the workspace by its path, narrowed as you type --
// names that start with what is typed first -- the arrows to choose, Enter to open, Escape to leave.

import { typeIcon } from '../../../legend-art/src/type-icon.ts';
import { clear, h } from './dom.ts';

export interface SearchItem {
  readonly key: string;
  /** The element's full path (`demo::party::Party`). */
  readonly path: string;
  /** The keyword its text declares it with (`Class`, `function`, ...), for its icon. */
  readonly kind: string | undefined;
}

const SHOWN = 50;

/** The items `query` finds: every word of it in the path, a name that starts with it first, then by path. */
export function findElements(items: readonly SearchItem[], query: string): SearchItem[] {
  const words = query.trim().toLowerCase().split(/\s+/).filter((w) => w !== '');
  const name = (i: SearchItem): string => (i.path.split('::').pop() ?? i.path).toLowerCase();
  return items
    .filter((i) => words.every((w) => i.path.toLowerCase().includes(w)))
    .sort((a, b) => {
      const first = words[0] ?? '';
      const rank = (i: SearchItem): number => (name(i).startsWith(first) ? 0 : name(i).includes(first) ? 1 : 2);
      return rank(a) - rank(b) || a.path.localeCompare(b.path);
    });
}

export function openSearch(items: readonly SearchItem[], open: (key: string) => void): void {
  let found = findElements(items, '');
  let chosen = 0;
  const list = h('div', { class: 'search-modal__list', 'data-testid': 'element-search-list' });
  const close = (): void => overlay.remove();
  const draw = (): void => {
    clear(list);
    if (found.length === 0) list.append(h('div', { class: 'search-modal__empty' }, 'No element matches.'));
    found.slice(0, SHOWN).forEach((i, n) => {
      const name = i.path.split('::').pop() ?? i.path;
      list.append(h('div', {
        class: `search-modal__item${n === chosen ? ' search-modal__item--chosen' : ''}`, 'data-path': i.path, title: i.path,
        onmousedown: (e: Event) => { e.preventDefault(); close(); open(i.key); },
      }, typeIcon(i.kind), h('span', { class: 'search-modal__name' }, name), h('span', { class: 'search-modal__path' }, i.path)));
    });
    list.querySelector('.search-modal__item--chosen')?.scrollIntoView({ block: 'nearest' });
  };
  const input = h('input', {
    class: 'input search-modal__input', placeholder: 'Search for an element by its path', 'data-testid': 'element-search', spellcheck: 'false',
    oninput: () => { found = findElements(items, input.value); chosen = 0; draw(); },
    onkeydown: ((e: KeyboardEvent) => {
      const last = Math.min(found.length, SHOWN) - 1;
      if (e.key === 'ArrowDown') { e.preventDefault(); chosen = Math.min(chosen + 1, last); draw(); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); chosen = Math.max(chosen - 1, 0); draw(); }
      else if (e.key === 'Enter') { e.preventDefault(); const i = found[chosen]; if (i) { close(); open(i.key); } }
      else if (e.key === 'Escape') { e.preventDefault(); close(); }
    }) as EventListener,
  });
  const overlay = h('div', { class: 'overlay', onmousedown: (e: Event) => { if (e.target === overlay) close(); } },
    h('div', { class: 'search-modal', role: 'dialog', 'aria-label': 'Search for an element' }, input, list));
  document.body.append(overlay);
  draw();
  input.focus();
}
