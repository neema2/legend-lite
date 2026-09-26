// The Member Selection window: a dimension's members as a tree, the
// picks as a list, and the shortcuts that pick many at once (Children,
// Descendants, Bottom Level, Same Level).
//
// The tree is the SOURCE's members, looked up a generation at a time as
// a node opens -- never the whole dimension up front, which for a
// dimension with a million leaves would be the whole table. Search is
// the exception: it asks for every generation once, then filters.

import type { MemberPath } from './state.ts';
import { memberLabel } from './query.ts';

export interface MemberSelectionOptions {
  readonly dimension: string;
  /** Many members for an axis; one for the POV. */
  readonly mode: 'many' | 'one';
  readonly selected: readonly MemberPath[];
  /** How many generations the dimension has. */
  readonly deepest: number;
  /** The members beneath `under` at generation `depth`. */
  readonly members: (under: MemberPath, depth: number) => Promise<MemberPath[]>;
  readonly onFailure: (error: unknown) => void;
  readonly onOk: (picked: MemberPath[]) => void;
  readonly onClose: () => void;
}

const key = (m: MemberPath): string => JSON.stringify(m);

export function buildMemberSelection(host: HTMLElement, options: MemberSelectionOptions): void {
  const doc = host.ownerDocument;
  const { dimension, deepest } = options;
  const many = options.mode === 'many';
  let picked: MemberPath[] = [...options.selected];
  /** The node the shortcuts act on: the last one clicked. */
  let focus: MemberPath = [];
  const children = new Map<string, MemberPath[]>();
  const open = new Set<string>([key([])]);
  let everything: MemberPath[] | null = null;
  let query = '';

  host.classList.add('dc-adhoc-members');
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent?: HTMLElement):
  HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    e.className = cls;
    parent?.append(e);
    return e;
  };
  const button = (label: string, cls: string, parent: HTMLElement, onClick: () => void):
  HTMLButtonElement => {
    const b = el('button', `dc-button ${cls}`, parent);
    b.type = 'button';
    b.textContent = label;
    b.addEventListener('click', onClick);
    return b;
  };

  const body = el('div', 'dc-adhoc-members-body', host);
  const left = el('div', 'dc-adhoc-members-left', body);
  const search = el('input', 'dc-adhoc-members-search', left);
  search.type = 'search';
  search.placeholder = `Search ${dimension}...`;
  search.setAttribute('aria-label', `Search ${dimension}`);
  const tree = el('div', 'dc-adhoc-members-tree', left);
  tree.setAttribute('role', 'tree');
  const shortcuts = el('div', 'dc-adhoc-members-shortcuts', left);
  const right = el('div', 'dc-adhoc-members-right', body);
  const heading = el('div', 'dc-adhoc-members-heading', right);
  heading.textContent = many ? 'Selection' : 'Member';
  const list = el('ol', 'dc-adhoc-members-picked', right);
  const footer = el('div', 'dc-adhoc-members-footer', host);
  const clear = button('Clear', 'dc-adhoc-members-clear', footer, () => {
    picked = [];
    paint();
  });
  clear.hidden = !many;
  const buttons = el('div', 'dc-adhoc-members-buttons', footer);
  button('Cancel', 'dc-adhoc-members-cancel', buttons, () => options.onClose());
  const ok = button('OK', 'dc-adhoc-members-ok', buttons, () => options.onOk([...picked]));

  const has = (m: MemberPath): boolean => picked.some((p) => key(p) === key(m));
  const toggle = (m: MemberPath): void => {
    if (!many) picked = [m];
    else picked = has(m) ? picked.filter((p) => key(p) !== key(m)) : [...picked, m];
    paint();
  };
  const add = (ms: readonly MemberPath[]): void => {
    for (const m of ms) if (!has(m)) picked = [...picked, m];
    paint();
  };

  async function childrenOf(m: MemberPath): Promise<MemberPath[]> {
    const k = key(m);
    const known = children.get(k);
    if (known) return known;
    const found = m.length >= deepest ? [] : await options.members(m, m.length + 1);
    children.set(k, found);
    return found;
  }

  /** Every member at every generation beneath `m`, parents first. */
  async function descendantsOf(m: MemberPath): Promise<MemberPath[]> {
    const out: MemberPath[] = [];
    for (const c of await childrenOf(m)) out.push(c, ...await descendantsOf(c));
    return out;
  }

  const guarded = (work: () => Promise<void>): void => {
    work().catch((e: unknown) => options.onFailure(e));
  };

  // The shortcuts, on the focused member.
  if (many) {
    button('Children', 'dc-adhoc-members-children', shortcuts,
      () => guarded(async () => add(await childrenOf(focus))));
    button('Descendants', 'dc-adhoc-members-descendants', shortcuts,
      () => guarded(async () => add(await descendantsOf(focus))));
    button('Bottom Level', 'dc-adhoc-members-bottom', shortcuts,
      () => guarded(async () => add(await options.members(focus, deepest))));
    button('Same Level', 'dc-adhoc-members-same', shortcuts,
      () => guarded(async () => add(focus.length === 0 ? [[]] : await options.members([], focus.length))));
  }

  function node(m: MemberPath, depth: number): HTMLElement {
    const row = el('div', 'dc-adhoc-member');
    row.setAttribute('role', 'treeitem');
    row.dataset['member'] = key(m);
    row.style.paddingLeft = `${depth * 16}px`;
    const expandable = m.length < deepest;
    const k = key(m);
    const twist = el('button', 'dc-adhoc-member-twist', row);
    twist.type = 'button';
    twist.textContent = expandable ? (open.has(k) ? '▾' : '▸') : '';
    twist.disabled = !expandable;
    twist.setAttribute('aria-label', open.has(k) ? 'Collapse' : 'Expand');
    if (expandable) row.setAttribute('aria-expanded', String(open.has(k)));
    twist.addEventListener('click', () => {
      if (open.has(k)) open.delete(k);
      else open.add(k);
      guarded(async () => {
        await childrenOf(m);
        paint();
      });
    });
    const box = el('input', 'dc-adhoc-member-pick', row);
    box.type = many ? 'checkbox' : 'radio';
    box.name = 'dc-adhoc-member';
    box.checked = has(m);
    box.setAttribute('aria-label', memberLabel(dimension, m));
    box.addEventListener('change', () => toggle(m));
    const label = el('span', 'dc-adhoc-member-label', row);
    label.textContent = memberLabel(dimension, m);
    if (key(focus) === k) row.classList.add('dc-adhoc-member-focus');
    label.addEventListener('click', () => {
      focus = m;
      paint();
    });
    label.addEventListener('dblclick', () => toggle(m));
    return row;
  }

  function paintTree(): void {
    tree.replaceChildren();
    if (query !== '' && everything) {
      const q = query.toLowerCase();
      for (const m of everything) {
        if (memberLabel(dimension, m).toLowerCase().includes(q)) tree.append(node(m, 0));
      }
      if (tree.childElementCount === 0) {
        const none = el('div', 'dc-adhoc-members-none', tree);
        none.textContent = 'No member matches';
      }
      return;
    }
    const walk = (m: MemberPath, depth: number): void => {
      tree.append(node(m, depth));
      if (!open.has(key(m))) return;
      for (const c of children.get(key(m)) ?? []) walk(c, depth + 1);
    };
    walk([], 0);
  }

  function paintPicked(): void {
    list.replaceChildren();
    for (const m of picked) {
      const item = el('li', 'dc-adhoc-picked', list);
      item.dataset['member'] = key(m);
      const label = el('span', 'dc-adhoc-picked-label', item);
      label.textContent = m.length === 0 ? dimension : m.map((v) => memberLabel(dimension, [v])).join(' / ');
      if (many) {
        const at = picked.indexOf(m);
        const up = button('↑', 'dc-adhoc-picked-up', item, () => {
          if (at <= 0) return;
          const next = [...picked];
          [next[at - 1], next[at]] = [next[at] as MemberPath, next[at - 1] as MemberPath];
          picked = next;
          paint();
        });
        up.setAttribute('aria-label', 'Move up');
        up.disabled = at === 0;
        const remove = button('×', 'dc-adhoc-picked-remove', item, () => toggle(m));
        remove.setAttribute('aria-label', `Remove ${label.textContent}`);
      }
    }
  }

  function paint(): void {
    paintTree();
    paintPicked();
    ok.disabled = picked.length === 0;
  }

  search.addEventListener('input', () => {
    query = search.value.trim();
    if (query === '' || everything) {
      paint();
      return;
    }
    guarded(async () => {
      const all: MemberPath[] = [[]];
      for (let depth = 1; depth <= deepest; depth++) all.push(...await options.members([], depth));
      everything = all;
      paint();
    });
  });

  paint();
  guarded(async () => {
    await childrenOf([]);
    paint();
  });
}
