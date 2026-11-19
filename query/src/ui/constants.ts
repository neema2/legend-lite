// Constants (upstream's constants panel): named values the query uses as `$name`, written as
// `let name = value;` ahead of it. A simple constant is a typed value edited here; a calculated
// one is any Pure expression (it may use the parameters), checked by the compiler when applied.
// A constant still in use cannot be deleted or renamed.

import type { AppContext } from '../app/context.ts';
import type { Session } from '../app/session.ts';
import { resultColumns } from '../app/types.ts';
import { referencedVariables } from '../builder/build.ts';
import type { Constant, QueryState, Value } from '../builder/state.ts';
import { queryVariables } from '../builder/state.ts';
import { simpleName } from '../model/graph.ts';
import { lambda } from '../../../pure-protocol/src/index.ts';
import { dialog, h, mount, panelAction, panelHeader, type Child } from './dom.ts';
import { PRIMITIVES } from './params.ts';
import { valueEditor, valueLabel } from './values.ts';

export function renderConstants(container: HTMLElement, app: AppContext, session: Session): void {
  const q = session.query;
  const used = referencedVariables(q);
  const constants = q.constants ?? [];
  mount(container,
    panelHeader('constants', [], [panelAction('plus', 'Add a constant', () => void constantDialog(app, session))]),
    h('div', { class: 'q-panel__content', style: 'padding:6px 10px; display:flex; flex-direction:column; gap:6px' },
      constants.length === 0 ? h('div', { class: 'q-faint', style: 'font-size:12px' }, 'None. Add one to name a value the query uses.') : null,
      constants.map((c): Child => {
        const shown = h('span', { class: 'q-faint mono', style: 'overflow:hidden; text-overflow:ellipsis; white-space:nowrap' },
          'type' in c ? `${simpleName(c.type)} = ${valueLabel(c.value)}` : 'calculated');
        if ('calculated' in c) void app.engine.queryPrint(lambda([], c.calculated), 'standard').then((t) => { shown.textContent = `= ${t.replace(/^\|/, '')}`; }, () => undefined);
        return h('div', { class: 'q-constant', style: 'display:flex; align-items:center; gap:6px; border:1px solid var(--border); border-radius:6px; padding:5px 7px; min-width:0' },
          h('b', { class: 'mono' }, `$${c.name}`), shown,
          h('span', { class: 'q-spacer' }),
          h('button', { class: 'q-icon-btn', title: 'Edit', onclick: () => void constantDialog(app, session, c) }, '✎'),
          h('button', {
            class: 'q-icon-btn', disabled: used.has(c.name), title: used.has(c.name) ? 'Used in the query: remove its uses first' : 'Delete',
            onclick: () => session.update((s) => withConstants(s, (s.constants ?? []).filter((x) => x.name !== c.name))),
          }, '✕'));
      })));
}

function withConstants(q: QueryState, constants: readonly Constant[]): QueryState {
  const { constants: _drop, ...rest } = q;
  void _drop;
  return constants.length > 0 ? { ...rest, constants } : rest;
}

/** Create or edit a constant: a name, then a typed value or a Pure expression. */
export async function constantDialog(app: AppContext, session: Session, existing?: Constant): Promise<void> {
  const graph = session.project.graph;
  const inUse = existing !== undefined && referencedVariables(session.query).has(existing.name);
  const name = h('input', {
    class: 'q-input', value: existing?.name ?? '', placeholder: 'e.g. threshold', 'aria-label': 'Constant name',
    disabled: inUse, title: inUse ? 'Used in the query: remove its uses to rename it' : '',
  });
  const kind = h('select', { class: 'q-select', 'aria-label': 'Constant kind' },
    h('option', { value: 'value', selected: !existing || 'type' in existing }, 'A value'),
    h('option', { value: 'calculated', selected: existing !== undefined && 'calculated' in existing }, 'Calculated (a Pure expression)'));
  const type = h('select', { class: 'q-select', 'aria-label': 'Constant type' },
    h('optgroup', { label: 'Primitive' }, PRIMITIVES.map((t) => h('option', { value: t, selected: existing && 'type' in existing ? t === existing.type : t === 'String' }, t))),
    graph.enumerations.size > 0
      ? h('optgroup', { label: 'Enumeration' }, [...graph.enumerations.keys()].map((t) => h('option', { value: t, selected: existing && 'type' in existing && t === existing.type }, simpleName(t))))
      : null);
  const list = h('input', { type: 'checkbox', id: 'q-const-list', style: 'justify-self:start', checked: existing !== undefined && 'type' in existing && existing.value.kind === 'list' });
  let value: Value | undefined = existing && 'type' in existing ? existing.value : undefined;
  const valueSlot = h('span');
  const drawValue = (): void => {
    mount(valueSlot, valueEditor({
      graph, type: type.value, many: list.checked, value, variables: [],
      onChange: (v) => { value = v; drawValue(); },
    }));
  };
  const expression = h('textarea', { class: 'q-textarea', rows: 3, spellcheck: false, placeholder: 'today()->adjust(-7, DurationUnit.DAYS)', 'aria-label': 'Constant expression' });
  expression.value = existing && 'calculated' in existing ? (await app.engine.queryPrint(lambda([], existing.calculated), 'standard')).replace(/^\|/, '') : '';
  const valueRows = h('div', null,
    h('div', { class: 'q-field' }, h('label', null, 'Type'), type),
    h('div', { class: 'q-field' }, h('label', { for: 'q-const-list' }, 'A list'), list),
    h('div', { class: 'q-field' }, h('label', null, 'Value'), valueSlot));
  const calculatedRows = h('div', null,
    h('div', { class: 'q-field' }, h('label', null, 'Expression'), expression),
    h('div', { class: 'q-faint' }, 'Any Pure expression; it may use the parameters, as ', h('code', null, '$name'), '.'));
  const drawKind = (): void => {
    valueRows.hidden = kind.value !== 'value';
    calculatedRows.hidden = kind.value === 'value';
  };
  type.addEventListener('change', () => { value = undefined; drawValue(); });
  list.addEventListener('change', () => { value = undefined; drawValue(); });
  kind.addEventListener('change', drawKind);
  drawValue();
  drawKind();
  const error = h('div');

  dialog(existing ? `Edit $${existing.name}` : 'New constant', (d) => ({
    body: [h('div', { class: 'q-field' }, h('label', null, 'Name'), name), h('div', { class: 'q-field' }, h('label', null, 'Kind'), kind), valueRows, calculatedRows, error],
    foot: [
      h('button', { class: 'q-btn', onclick: () => d.close() }, 'Cancel'),
      h('button', {
        class: 'q-btn primary',
        onclick: async () => {
          const say = (m: string): void => { error.replaceChildren(h('div', { class: 'q-error-box' }, m)); };
          const n = name.value.trim();
          if (!/^[a-z_][A-Za-z0-9_]*$/.test(n)) return say('A name starts with a lower-case letter or _, then letters, digits or _.');
          if (queryVariables(session.query).some((v) => v.name === n && v.name !== existing?.name)) return say(`There is already a parameter or constant $${n}.`);
          try {
            let c: Constant;
            if (kind.value === 'value') {
              if (value === undefined || (value.kind === 'list' && value.values.length === 0)) return say('Give the constant a value.');
              c = { name: n, type: type.value, value };
            } else {
              const parsed = await app.engine.queryParse(`|${expression.value.trim()}`);
              if (parsed.body.length !== 1 || !expression.value.trim()) return say('The expression is one Pure expression.');
              c = { name: n, calculated: parsed.body[0]! };
            }
            const before = session.query.constants ?? [];
            const next = (q: QueryState): QueryState =>
              withConstants(q, existing ? before.map((x) => (x.name === existing.name ? c : x)) : [...before, c]);
            // compile it with the query (the tab's planner) before it is applied: a mistake is said here
            await resultColumns(app, session, next(session.query));
            session.update(next);
            d.close();
          } catch (e) {
            say((e as Error).message);
          }
        },
      }, existing ? 'Save' : 'Create'),
    ],
  }), { wide: true });
}
