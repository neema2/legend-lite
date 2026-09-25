// Column Properties: everything about ONE column.
//
// Labels and grouping are DataCube's, from
// DataCubeEditorColumnPropertiesPanel: Choose Column, then Column
// Kind, Display Name, Aggregation, Number Format, Scale, Unit,
// Missing Value Format, Visibility, Pin, Width, Font and Colors,
// with the advanced settings behind a checkbox as it has them.
//
// The panel is rebuilt whenever the chosen column changes, because
// every control below the chooser is bound to that column; an
// in-place update would have to re-point twenty controls and would
// get one of them wrong. Everything else changes in place, so typing
// a display name does not cost the caret.

import { SCALES, type FontCase, type NumberScale } from '../format.ts';
import type { ColumnFormat, FormatKind } from '../format.ts';

type FormatPatch = {
  [K in keyof ColumnFormat]?: ColumnFormat[K] | undefined;
};
import {
  SCALE_LABELS,
  columnConfig,
  prune,
  type ColumnConfiguration,
  type ColumnPatch,
  type Patch,
  type WidthMode,
} from '../config.ts';
import { numberDefaults, withColumn } from '../config.ts';
import {
  kindOf,
  rowColumns,
  type AggregateFn,
  type ColumnKind,
} from '../snapshot.ts';
import type { CellAppearance, ColourSet } from '../style.ts';
import type { PinPlacement } from '../grid/columns.ts';
import {
  badge,
  button,
  checkbox,
  dropdown,
  field,
  numberInput,
  section,
  textInput,
} from './form.ts';
import { colourGrid, fontControls } from './panel-general.ts';
import { dataTypeOf, type DataType } from './filter-editor.ts';
import { docHint } from './docs.ts';
import {
  SORT_DIRECTIONS,
  allColumns,
  panelShell,
  type PanelBuilder,
} from './panel-kit.ts';

export const KINDS: readonly { value: ColumnKind; label: string }[] = [
  { value: 'dimension', label: 'Dimension' },
  { value: 'measure', label: 'Measure' },
];

/**
 * Every aggregate the snapshot can express, with DataCube's labels.
 *
 * Exhaustive by construction: the type below fails to compile if an
 * aggregate is added to the snapshot and not offered here, which is
 * the failure mode worth catching -- a measure the query supports
 * and the UI cannot reach.
 */
export const AGGREGATES: readonly { value: AggregateFn; label: string }[] = [
  { value: 'sum', label: 'sum' },
  { value: 'average', label: 'average' },
  { value: 'count', label: 'count' },
  { value: 'min', label: 'min' },
  { value: 'max', label: 'max' },
  { value: 'median', label: 'median' },
  { value: 'stdDevPopulation', label: 'std deviation (population)' },
  { value: 'stdDevSample', label: 'std deviation (sample)' },
  { value: 'variancePopulation', label: 'variance (population)' },
  { value: 'varianceSample', label: 'variance (sample)' },
  { value: 'joinStrings', label: 'join strings' },
  { value: 'wavg', label: 'weighted average' },
  // DataCube's default for every non-numeric column: the value when
  // the group has exactly one, otherwise blank.
  { value: 'unique', label: 'unique value' },
];

/**
 * The aggregates a column of this type can take, as upstream's
 * `isCompatibleWithColumn` decides them, plus min and max on dates and
 * text (the database keeps their type, and "the latest trade date" is
 * a question people ask). An aggregate must keep the column's type,
 * which is why count and joinStrings are not offered everywhere they
 * would run. Unknown type (a calculated column not yet run): all.
 */
export function aggregatesFor(
  type: string | undefined,
): readonly { value: AggregateFn; label: string }[] {
  if (type === undefined) return AGGREGATES;
  const allowed: ReadonlySet<AggregateFn> = new Set<AggregateFn>(
    ({
      number: ['sum', 'average', 'count', 'min', 'max', 'median',
        'stdDevPopulation', 'stdDevSample', 'variancePopulation',
        'varianceSample', 'wavg', 'unique'],
      text: ['joinStrings', 'min', 'max', 'unique'],
      date: ['min', 'max', 'unique'],
      time: ['min', 'max', 'unique'],
      boolean: ['unique'],
    } satisfies Record<DataType, AggregateFn[]>)[dataTypeOf(type)],
  );
  return AGGREGATES.filter((a) => allowed.has(a.value));
}

/** Fails to compile when an aggregate is added and not offered above. */
const _EVERY_AGGREGATE: Record<AggregateFn, true> = {
  sum: true,
  average: true,
  count: true,
  min: true,
  max: true,
  median: true,
  stdDevPopulation: true,
  stdDevSample: true,
  variancePopulation: true,
  varianceSample: true,
  joinStrings: true,
  unique: true,
  wavg: true,
};
void _EVERY_AGGREGATE;

const FORMAT_KINDS: readonly { value: FormatKind; label: string }[] = [
  { value: 'auto', label: 'Auto' },
  { value: 'number', label: 'Number' },
  { value: 'percent', label: 'Percent' },
  { value: 'currency', label: 'Currency' },
  { value: 'date', label: 'Date' },
  { value: 'datetime', label: 'Date and time' },
  { value: 'text', label: 'Text' },
];

const SCALE_CHOICES: readonly { value: NumberScale; label: string }[] = (
  ['auto', ...(Object.keys(SCALES) as (keyof typeof SCALES)[])] as NumberScale[]
).map((value) => ({ value, label: SCALE_LABELS[value] }));

const FONT_CASES: readonly { value: FontCase; label: string }[] = [
  { value: 'lowercase', label: 'lowercase' },
  { value: 'uppercase', label: 'UPPERCASE' },
  { value: 'capitalize', label: 'Capitalize' },
];

const PINS: readonly { value: PinPlacement; label: string }[] = [
  { value: 'left', label: 'Left' },
  { value: 'right', label: 'Right' },
];

const WIDTH_MODES: readonly { value: WidthMode; label: string }[] = [
  { value: 'any', label: '(Any)' },
  { value: 'fixed', label: 'Fixed' },
  { value: 'range', label: 'In range' },
];

/** Which column the panel shows, and whether advanced is open. */
export interface ColumnPanelUi {
  chosen: string | null;
  advanced: boolean;
}

/**
 * The panel's own view state, kept per editor rather than per module
 * so two open editors do not fight over which column is showing.
 */
export function columnPanelUi(state: Record<string, unknown>): ColumnPanelUi {
  const existing = state['columnProperties'] as ColumnPanelUi | undefined;
  if (existing) return existing;
  const fresh: ColumnPanelUi = { chosen: null, advanced: false };
  state['columnProperties'] = fresh;
  return fresh;
}

export const columnPropertiesPanel: PanelBuilder = (ctx) => {
  const doc = ctx.doc;
  const draft = ctx.draft();
  const columns = allColumns(draft);
  const uiState = columnPanelUi(ctx.state);
  if (columns.length === 0) {
    return panelShell(
      doc,
      'Column Properties',
      note(doc, 'This cube has no columns.'),
    );
  }
  // A column that has gone -- the source changed, or the cube was
  // reopened -- must not leave the panel bound to nothing.
  if (
    uiState.chosen === null ||
    !columns.some((c) => c.name === uiState.chosen)
  ) {
    uiState.chosen = (columns[0] as { name: string }).name;
  }
  const name = uiState.chosen;
  const spec = draft.snapshot.columns.find((c) => c.name === name);
  const cfg = (): ColumnConfiguration => columnConfig(ctx.draft().config, name);

  const patch = (change: ColumnPatch): void => {
    ctx.setConfig(withColumn(ctx.draft().config, name, change));
  };
  // A format patch, like a column patch, must be able to CLEAR a
  // field, which `exactOptionalPropertyTypes` makes a distinct type.
  const patchFormat = (change: FormatPatch): void => {
    patch({
      format: prune<ColumnFormat>({
        ...(cfg().format ?? { kind: 'auto' }),
        ...change,
      }),
    });
  };
  const patchAppearance = (change: Patch<CellAppearance>): void => {
    // Pruned, so clearing a colour leaves the column inheriting the
    // grid's rather than carrying an explicit undefined.
    const next = prune<CellAppearance>({ ...(cfg().appearance ?? {}), ...change });
    patch({
      appearance: Object.keys(next).length === 0 ? undefined : next,
    });
  };

  const head = doc.createElement('div');
  head.className = 'dc-panel-head-row';
  head.append(
    checkbox(doc, 'Show advanced settings?', uiState.advanced, (v) => {
      uiState.advanced = v;
      ctx.refresh();
    }),
  );

  const chooser = field(
    doc,
    'Choose Column:',
    dropdown(
      doc,
      name,
      columns.map((c) => ({ value: c.name, label: c.name })),
      (next) => {
        // Every control below is bound to the chosen column, so this
        // is the one change that rebuilds the panel.
        uiState.chosen = next ?? null;
        ctx.refresh();
      },
      { width: 260 },
    ),
    spec ? badge(doc, spec.type) : badge(doc, 'Derived'),
  );

  const c = cfg();
  const kind = c.kind ?? (spec ? kindOf(spec) : 'measure');
  const isMeasure = kind === 'measure';
  // The type decides which sections apply; a calculated column's is
  // learned from its first result, and until then every section shows.
  const type = spec?.type
    ?? [...rowColumns(draft.snapshot), ...(draft.snapshot.groupDerived ?? [])]
      .find((x) => x.name === name)?.type;
  const dataType = type === undefined ? undefined : dataTypeOf(type);
  // Upstream: a pivot's kind is what makes it a pivot, so it cannot
  // change while the column is one.
  const pivoted = draft.snapshot.rows.includes(name)
    || draft.snapshot.pivotOn.includes(name);

  const kindField = field(
    doc,
    'Column Kind:',
    lockedKind(
      dropdown(
        doc,
        kind,
        KINDS,
        (v) => {
          // Kind decides whether the column can be grouped and
          // whether it is aggregated, so the form below changes
          // with it. As upstream, a new kind resets the exclusion
          // from the horizontal pivot: a dimension is out of it.
          if (v === undefined || v === kind) return;
          patch({
            kind: v,
            excludedFromPivot: v === 'dimension' ? true : undefined,
          });
          ctx.refresh();
        },
        { width: 160, disabled: pivoted },
      ),
      pivoted,
    ),
    docHint(doc, 'data-cube.column-configuration.kind'),
  );
  const identity = section(
    doc,
    '',
    chooser,
    // Upstream's ADVANCED setting: the kind is set by the column's
    // type, and changing it is a deliberate act.
    ...(uiState.advanced ? [kindField] : []),
    field(
      doc,
      'Display Name:',
      textInput(doc, c.displayName, (displayName) => patch({ displayName }), {
        placeholder: name,
        width: 260,
      }),
    ),
  );

  const aggregation = section(
    doc,
    'Aggregation',
    field(
      doc,
      'Aggregation:',
      dropdown(
        doc,
        c.aggregateFn,
        withCurrent(aggregatesFor(type), c.aggregateFn),
        (aggregateFn) => {
          patch({ aggregateFn });
          ctx.refresh();
        },
        { allowNone: true, width: 200, disabled: !isMeasure },
      ),
      isMeasure ? doc.createElement('span') : badge(doc, 'Dimensions only'),
    ),
    field(
      doc,
      'Weight column:',
      dropdown(
        doc,
        c.aggregationParameters?.[0],
        columns.map((x) => ({ value: x.name, label: x.name })),
        (weight) =>
          patch({
            aggregationParameters: weight === undefined ? undefined : [weight],
          }),
        {
          allowNone: true,
          width: 200,
          // Only the weighted average takes one, and offering it
          // elsewhere invites a parameter the engine will ignore.
          disabled: c.aggregateFn !== 'wavg',
        },
      ),
    ),
    field(
      doc,
      '',
      checkbox(
        doc,
        'Exclude from horizontal pivot',
        c.excludedFromPivot,
        (v) => patch({ excludedFromPivot: v ? true : undefined }),
        { disabled: !isMeasure },
      ),
    ),
    field(
      doc,
      // Upstream's `pivotStatisticColumnFunction`: the aggregate this
      // measure's pivot TOTAL takes. Unset, the total uses the
      // measure's own aggregate -- a true total for an average too.
      'Pivot total function:',
      dropdown(
        doc,
        c.pivotStatisticColumnFunction,
        withCurrent(aggregatesFor(type).filter((a) => a.value !== 'wavg'),
          c.pivotStatisticColumnFunction),
        (pivotStatisticColumnFunction) =>
          patch({ pivotStatisticColumnFunction }),
        { allowNone: true, width: 200, disabled: !isMeasure },
      ),
    ),
    field(
      doc,
      'Pivot sort direction:',
      dropdown(
        doc,
        c.pivotSortDirection,
        SORT_DIRECTIONS,
        (pivotSortDirection) => patch({ pivotSortDirection }),
        { allowNone: true, width: 140 },
      ),
    ),
  );

  const format = c.format ?? { kind: 'auto' };
  // What an unset field SHOWS is what the grid renders: the type's
  // defaults (upstream's), not a blank that reads as "none".
  const defaults = numberDefaults(type);
  // Upstream shows these for EVERY column: a text column has a
  // missing value and a case too. Only the number section is by type.
  const formatting = section(
    doc,
    'Format',
    field(
      doc,
      'Format:',
      dropdown(
        doc,
        format.kind,
        FORMAT_KINDS,
        (k) => {
          patchFormat({ kind: k ?? 'auto' });
          ctx.refresh();
        },
        { width: 170 },
      ),
      format.kind === 'currency'
        ? textInput(
            doc,
            format.currency,
            (currency) => patchFormat({ currency }),
            { placeholder: 'USD', width: 80 },
          )
        : doc.createElement('span'),
    ),
    field(
      doc,
      'Missing Value Format:',
      textInput(doc, format.nullText, (nullText) => patchFormat({ nullText }), {
        placeholder: '(blank)',
        width: 150,
      }),
      docHint(doc, 'data-cube.column-configuration.missing-value-format'),
    ),
    field(
      doc,
      'Case:',
      dropdown(
        doc,
        format.fontCase,
        FONT_CASES,
        (fontCase) => patchFormat({ fontCase }),
        { allowNone: true, width: 150 },
      ),
    ),
  );

  const numbers = section(
    doc,
    'Number Format',
    field(
      doc,
      'Decimals:',
      numberInput(doc, format.decimals ?? defaults?.decimals, (decimals) => patchFormat({ decimals }), {
        min: 0,
        max: 10,
      }),
      // Ticked when UNSET: the formatter groups digits unless told
      // `displayCommas: false`, so an unticked box beside "85,034.3"
      // was the setting contradicting the screen (2026-09-25 sweep).
      checkbox(doc, 'Display commas', format.displayCommas !== false, (v) =>
        patchFormat({ displayCommas: v }),
      ),
      checkbox(
        doc,
        'Negative number in parens',
        format.negativeParens ?? defaults?.negativeParens,
        (v) => patchFormat({ negativeParens: v }),
      ),
    ),
    field(
      doc,
      'Scale:',
      dropdown(
        doc,
        format.numberScale,
        SCALE_CHOICES,
        (numberScale) => patchFormat({ numberScale }),
        { allowNone: true, width: 190 },
      ),
    ),
    field(
      doc,
      'Unit:',
      textInput(doc, format.unit, (unit) => patchFormat({ unit }), {
        width: 110,
      }),
      docHint(doc, 'data-cube.column-configuration.unit'),
    ),
  );

  const widthMode = c.widthMode ?? (c.width !== undefined ? 'fixed' : 'any');
  const display = section(
    doc,
    'Display',
    field(
      doc,
      'Visibility:',
      checkbox(doc, 'Blur content', c.blurred, (v) =>
        patch({ blurred: v ? true : undefined }),
      ),
      checkbox(doc, 'Hide from view', c.hidden, (v) =>
        patch({ hidden: v ? true : undefined }),
      ),
    ),
    field(
      doc,
      'Pin:',
      dropdown(doc, c.pinned, PINS, (pinned) => patch({ pinned }), {
        allowNone: true,
        width: 130,
      }),
    ),
    field(
      doc,
      'Width:',
      dropdown(
        doc,
        widthMode,
        WIDTH_MODES,
        (m) => {
          // The numbers are KEPT when the mode changes, so switching
          // to (Any) and back does not lose what was typed.
          patch({ widthMode: m ?? 'any' });
          ctx.refresh();
        },
        { width: 130 },
      ),
      widthMode === 'fixed'
        ? numberInput(doc, c.width, (width) => patch({ width }), {
            min: 20,
            max: 2000,
            width: 90,
          })
        : doc.createElement('span'),
      widthMode === 'range'
        ? numberInput(doc, c.minWidth, (minWidth) => patch({ minWidth }), {
            min: 20,
            max: 2000,
            width: 90,
          })
        : doc.createElement('span'),
      widthMode === 'range'
        ? numberInput(doc, c.maxWidth, (maxWidth) => patch({ maxWidth }), {
            min: 20,
            max: 2000,
            width: 90,
          })
        : doc.createElement('span'),
    ),
  );

  const links = section(
    doc,
    'Link',
    field(
      doc,
      '',
      checkbox(doc, 'Display as link', c.displayAsLink, (v) => {
        patch({ displayAsLink: v ? true : undefined });
        ctx.refresh();
      }),
      docHint(doc, 'data-cube.column-configuration.display-value-as-link'),
    ),
    field(
      doc,
      'Use parameter in link as label:',
      textInput(
        doc,
        c.linkLabelParameter,
        (linkLabelParameter) => patch({ linkLabelParameter }),
        { placeholder: 'dataCube.linkLabel', width: 220 },
      ),
    ),
  );

  const heat = c.heatmap;
  const colouring = section(
    doc,
    'Colors',
    ...fontControls(doc, c.appearance ?? {}, patchAppearance, () => ctx.refresh()),
    colourGrid(doc, (c.appearance ?? {}) as ColourSet, patchAppearance),
    field(
      doc,
      'Heatmap:',
      checkbox(doc, 'On', heat !== undefined, (on) => {
        patch({
          heatmap: on ? { from: '#ffffff', to: '#ff8a65' } : undefined,
        });
        ctx.refresh();
      }),
      ...(heat
        ? [
            fieldColour(doc, 'from', heat.from, (from) =>
              patch({ heatmap: { ...heat, from: from ?? '#ffffff' } }),
            ),
            fieldColour(doc, 'to', heat.to, (to) =>
              patch({ heatmap: { ...heat, to: to ?? '#ff8a65' } }),
            ),
          ]
        : []),
    ),
    button(doc, 'Use Default Styling', () => {
      patch({ appearance: undefined, heatmap: undefined });
      ctx.refresh();
    }),
  );

  // Upstream's sections by type: number formatting for a number,
  // links for text; an untyped calculated column shows both.
  const parts = [identity, aggregation, formatting];
  if (dataType === undefined || dataType === 'number') parts.push(numbers);
  parts.push(display, colouring);
  if (dataType === undefined || dataType === 'text') parts.push(links);

  return panelShell(doc, 'Column Properties', head, ...parts);
};

/** The kind control, with upstream's reason when a pivot locks it. */
function lockedKind(control: HTMLSelectElement, locked: boolean): HTMLSelectElement {
  if (locked) control.title = 'Column kind cannot be changed while the column is used in pivot';
  return control;
}

/**
 * Choices, keeping a current value the type would not offer -- a cube
 * saved elsewhere can carry one, and a dropdown showing blank would
 * say the column has no aggregate when it has one.
 */
function withCurrent(
  choices: readonly { value: AggregateFn; label: string }[],
  current: AggregateFn | undefined,
): readonly { value: AggregateFn; label: string }[] {
  if (current === undefined || choices.some((c) => c.value === current)) return choices;
  const known = AGGREGATES.find((a) => a.value === current);
  return [...choices, { value: current, label: known?.label ?? current }];
}

function fieldColour(
  doc: Document,
  label: string,
  value: string,
  onChange: (value: string | undefined) => void,
): HTMLElement {
  const wrap = doc.createElement('span');
  wrap.className = 'dc-inline-colour';
  const l = doc.createElement('span');
  l.textContent = label;
  const input = doc.createElement('input');
  input.type = 'color';
  input.className = 'dc-color-input';
  input.value = value;
  input.addEventListener('change', () => onChange(input.value));
  wrap.append(l, input);
  return wrap;
}

function note(doc: Document, text: string): HTMLElement {
  const el = doc.createElement('div');
  el.className = 'dc-note';
  el.textContent = text;
  return el;
}
