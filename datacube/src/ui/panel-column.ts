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
import { withColumn } from '../config.ts';
import { kindOf, type AggregateFn, type ColumnKind } from '../snapshot.ts';
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

  const identity = section(
    doc,
    '',
    chooser,
    field(
      doc,
      'Column Kind:',
      dropdown(
        doc,
        kind,
        KINDS,
        (v) => {
          // Kind decides whether the column can be grouped and
          // whether it is aggregated, so the form below changes with
          // it.
          patch({ kind: v });
          ctx.refresh();
        },
        { width: 160 },
      ),
    ),
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
        AGGREGATES,
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
  const numbers = section(
    doc,
    'Number Format',
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
      'Decimals:',
      numberInput(doc, format.decimals, (decimals) => patchFormat({ decimals }), {
        min: 0,
        max: 10,
      }),
      checkbox(doc, 'Display commas', format.displayCommas, (v) =>
        patchFormat({ displayCommas: v }),
      ),
      checkbox(
        doc,
        'Negative number in parens',
        format.negativeParens,
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
    ),
    field(
      doc,
      'Missing Value Format:',
      textInput(doc, format.nullText, (nullText) => patchFormat({ nullText }), {
        placeholder: '(blank)',
        width: 150,
      }),
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
    ...fontControls(doc, c.appearance ?? {}, patchAppearance),
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

  const parts = [identity, aggregation, numbers, display, colouring];
  if (uiState.advanced) parts.push(links);

  return panelShell(doc, 'Column Properties', head, ...parts);
};

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
