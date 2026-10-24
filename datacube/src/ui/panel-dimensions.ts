// Dimensions: named hierarchies, drilled as a unit.
//
// A DataCube dimension is a name plus an ORDERED list of columns --
// Geography being region, country, city. The value is that the order
// is defined once by whoever knows the model rather than
// rediscovered by each analyst, so the editor's job is to make the
// order editable and obvious, which is why the columns sit in the
// same two-pane selector as the pivots rather than in a list with
// up/down buttons.
//
// A dimension's columns are drawn from the GROUPABLE ones only. A
// hierarchy whose next level is a notional is not a hierarchy, and
// discovering that by drilling into one group per distinct amount is
// an expensive way to find out.
//
// WHAT IS DONE WITH THEM starts here too, not from a menu (the user,
// 2026-10-23): the grid's rows grouped by a dimension, and Ad Hoc
// Analysis -- the cube worked member by member down these hierarchies.
// Each applies the draft first, so it acts on what the tab shows.

import type { Dimension } from '../dimensions.ts';
import { groupableColumns } from '../grid-edits.ts';
import { button, textInput } from './form.ts';
import {
  panelShell,
  selectable,
  selectorInto,
  type PanelBuilder,
} from './panel-kit.ts';

/** Where the panel's cursor is, per editor. */
interface DimensionsUi {
  index: number;
}

function ui(state: Record<string, unknown>): DimensionsUi {
  const existing = state['dimensions'] as DimensionsUi | undefined;
  if (existing) return existing;
  const fresh: DimensionsUi = { index: 0 };
  state['dimensions'] = fresh;
  return fresh;
}

/** A name no existing dimension has. */
export function freshName(existing: readonly Dimension[]): string {
  const taken = new Set(existing.map((d) => d.name));
  for (let i = 1; ; i++) {
    const name = `Dimension ${i}`;
    if (!taken.has(name)) return name;
  }
}

export const dimensionsPanel: PanelBuilder = (ctx) => {
  const doc = ctx.doc;
  const draft = ctx.draft();
  const dimensions = draft.dimensions;
  const cursor = ui(ctx.state);
  if (cursor.index >= dimensions.length) cursor.index = dimensions.length - 1;
  if (cursor.index < 0) cursor.index = 0;

  const setDimensions = (next: readonly Dimension[]): void => {
    ctx.setDimensions(next);
    ctx.refresh();
  };
  // EVERY HANDLER READS THE DRAFT AS IT IS NOW. `dimensions` above is the
  // draft when this panel was built; the hierarchy selector writes the draft
  // without rebuilding the panel, so a rename or Add built from the old array
  // threw the hierarchy edits away (P2-187).
  const live = (): readonly Dimension[] => ctx.draft().dimensions;
  const counts: HTMLElement[] = [];

  const list = doc.createElement('div');
  list.className = 'dc-dimension-list';
  list.setAttribute('role', 'listbox');
  list.setAttribute('aria-label', 'Dimensions');

  dimensions.forEach((d, i) => {
    const row = doc.createElement('div');
    row.className = 'dc-dimension-row';
    row.setAttribute('role', 'option');
    const current = i === cursor.index;
    row.setAttribute('aria-selected', String(current));
    row.classList.toggle('dc-on', current);
    row.tabIndex = current ? 0 : -1;

    const label = doc.createElement('span');
    label.className = 'dc-dimension-name';
    label.textContent = d.name;
    const count = doc.createElement('span');
    count.className = 'dc-dimension-count';
    count.textContent = `${d.columns.length}`;
    counts[i] = count;
    row.append(label, count);
    row.addEventListener('click', () => {
      cursor.index = i;
      ctx.refresh();
    });
    list.append(row);
  });

  if (dimensions.length === 0) {
    const empty = doc.createElement('div');
    empty.className = 'dc-note';
    empty.textContent = 'No dimensions defined.';
    list.append(empty);
  }

  const controls = doc.createElement('div');
  controls.className = 'dc-dimension-controls';
  controls.append(
    button(doc, 'Add', () => {
      const now = live();
      cursor.index = now.length;
      setDimensions([...now, { name: freshName(now), columns: [] }]);
    }),
    button(
      doc,
      'Delete',
      () => {
        setDimensions(live().filter((_, i) => i !== cursor.index));
      },
      { disabled: dimensions.length === 0 },
    ),
  );

  const left = doc.createElement('div');
  left.className = 'dc-dimension-side';
  left.append(list, controls);

  const right = doc.createElement('div');
  right.className = 'dc-dimension-detail';

  const active = dimensions[cursor.index];
  if (active) {
    const rename = doc.createElement('div');
    rename.className = 'dc-field';
    const l = doc.createElement('div');
    l.className = 'dc-field-label';
    l.textContent = 'Name:';
    const body = doc.createElement('div');
    body.className = 'dc-field-body';
    body.append(
      textInput(
        doc,
        active.name,
        (name) => {
          // An unnamed dimension cannot be offered in a menu, so a
          // cleared name keeps the old one rather than producing a
          // row nobody can pick.
          if (name === undefined) {
            ctx.refresh();
            return;
          }
          setDimensions(live().map((d, i) => (i === cursor.index ? { ...d, name } : d)));
        },
        { width: 240 },
      ),
    );
    rename.append(l, body);

    const hint = doc.createElement('div');
    hint.className = 'dc-note';
    hint.textContent =
      'Columns run coarsest to finest. Drilling follows this order.';

    const selector = selectorInto(
      ctx,
      { all: selectable(groupableColumns(draft)), selected: active.columns },
      (columns) => {
        ctx.setDimensions(live().map((d, i) => (i === cursor.index ? { ...d, columns: [...columns] } : d)));
        // the list's count follows the hierarchy (the panel is not rebuilt)
        const count = counts[cursor.index];
        if (count) count.textContent = `${columns.length}`;
      },
      { selectedLabel: 'Hierarchy (coarsest first):' },
    );

    right.append(rename, hint, selector);
    // the grid's rows grouped by its first level -- the draft applied first, so a dimension just made can be used
    if (ctx.useDimension) {
      const use = ctx.useDimension;
      const row = doc.createElement('div');
      row.className = 'dc-dimension-use';
      row.append(button(doc, `Group Rows by ${active.name}`, () => use(live()[cursor.index]?.name ?? active.name), {
        disabled: active.columns.length === 0,
        title: 'Apply, close, and group the grid\'s rows by this dimension\'s first level',
      }));
      right.append(row);
    }
  }

  const body = doc.createElement('div');
  body.className = 'dc-dimension-body';
  body.append(left, right);

  // AD HOC ANALYSIS: the other way to work the cube, down these hierarchies -- left by its own bar's Exit
  const parts: HTMLElement[] = [body];
  if (ctx.adHoc) {
    const adHoc = ctx.adHoc;
    const strip = doc.createElement('div');
    strip.className = 'dc-dimension-adhoc';
    const note = doc.createElement('div');
    note.className = 'dc-note';
    note.textContent = 'Ad Hoc Analysis works the cube member by member: double-click a member to zoom into its next '
      + 'level, keep or remove members, and move dimensions between rows, columns and the point of view.';
    strip.append(note, button(doc, 'Start Ad Hoc Analysis', () => adHoc(), {
      title: 'Apply, close, and switch the grid to Ad Hoc Analysis (its Exit button comes back)',
    }));
    parts.push(strip);
  }

  return panelShell(doc, 'Dimensions', ...parts);
};
