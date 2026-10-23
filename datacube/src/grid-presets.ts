// THE GRID'S PRESETS (calc-presets.ts, docs/DATACUBE_ONE_API_PLAN_2026_10_21.md K4): a date taken to its period, a
// measure shown as something computed from it, a number taken to its bin -- each the calculated columns the chart's
// presets are, added to the grid under a name no column of the grid has. One rule for the header menu's By Day/Week/…
// and Show As, and for Python's grid.grain, grid.show_as and grid.bins (D6, D21); the screen's Bins opens the column
// editor on the same expression for a person to name and change.

import { binColumns, grainColumn, showAsColumns } from './calc-presets.ts';
import { SHOW_AS_KINDS, TIME_GRAINS, type Bins, type ShowAsKind, type TimeGrain } from './chart-spec.ts';
import type { GridCommand } from './grid-commands.ts';
import { CubeRefusal, rowColumns, type CubeSnapshot, type DerivedColumn } from './snapshot.ts';

/** A preset as the menu offers it and Python names it. */
export type GridPreset =
  | { readonly kind: 'grain'; readonly column: string; readonly grain: TimeGrain }
  | { readonly kind: 'showAs'; readonly column: string; readonly showAs: ShowAsKind }
  | { readonly kind: 'bins'; readonly column: string; readonly bins: Bins };

/**
 * The commands a preset adds to the grid as it is (`s`): its columns, each a `calculate` at the preset's level --
 * a period and a bin before the grouping, a Show As after it, along the grid's row groups in order -- named after the
 * column and the preset (`notional_runningTotal`), else that name with `_2`, `_3`… where the grid has a column of it.
 * A grain, a Show As or bins DataCube has not are refused, in its words.
 */
export function presetCommands(s: CubeSnapshot, preset: GridPreset): GridCommand[] {
  const { level, base, make } = shaped(s, preset);
  const taken = new Set([...rowColumns(s).map((c) => c.name), ...(s.groupDerived ?? []).map((d) => d.name)]);
  const fits = (name: string): boolean => make(name).every((d) => !taken.has(d.name));
  let free = base;
  for (let i = 2; !fits(free); i += 1) free = `${base}_${i}`;
  return make(free).map((column): GridCommand => ({ kind: 'calculate', level, column }));
}

function shaped(s: CubeSnapshot, preset: GridPreset):
  { readonly level: 'row' | 'group'; readonly base: string; readonly make: (name: string) => DerivedColumn[] } {
  switch (preset.kind) {
    case 'grain':
      if (!TIME_GRAINS.includes(preset.grain)) {
        throw new CubeRefusal(`a period is ${TIME_GRAINS.join(', ')}, not ${JSON.stringify(preset.grain)}`);
      }
      return { level: 'row', base: `${preset.column}_${preset.grain}`, make: (n) => [grainColumn(preset.column, preset.grain, n)] };
    case 'showAs': {
      if (!SHOW_AS_KINDS.includes(preset.showAs)) {
        throw new CubeRefusal(`a Show As is ${SHOW_AS_KINDS.join(', ')}, not ${JSON.stringify(preset.showAs)}`);
      }
      // along the grid's own groupings, in order: said, not left to the level's sort -- which may be this very column
      const along = [...s.rows];
      const show = { kind: preset.showAs };
      return { level: 'group', base: `${preset.column}_${preset.showAs}`, make: (n) => showAsColumns(show, preset.column, along, [], n) };
    }
    case 'bins': {
      const { width, count } = preset.bins;
      if ((width === undefined) === (count === undefined)) throw new CubeRefusal('bins are a width or a count, one of them');
      if (width !== undefined && !(typeof width === 'number' && width > 0)) {
        throw new CubeRefusal(`a bin's width is a positive number, not ${JSON.stringify(width)}`);
      }
      if (count !== undefined && !(Number.isInteger(count) && count >= 1)) {
        throw new CubeRefusal(`a count of bins is a whole number from 1, not ${JSON.stringify(count)}`);
      }
      return { level: 'row', base: `${preset.column}_bins`, make: (n) => binColumns(preset.column, preset.bins, n) };
    }
  }
}

/** A preset read from JSON (Python's): its kind one of DataCube's, its fields those the kind takes -- or refused in
 *  words. What it names (a column the grid has) is the apply's to judge. */
export function readGridPreset(raw: unknown): GridPreset {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined || typeof o['column'] !== 'string') throw new CubeRefusal(`a preset is {kind, column, ...}, not ${JSON.stringify(raw)}`);
  const column = o['column'];
  const only = (keys: readonly string[]): void => {
    const odd = Object.keys(o).filter((k) => !['kind', 'column', ...keys].includes(k));
    if (odd.length > 0) throw new CubeRefusal(`a '${String(o['kind'])}' preset takes ${keys.join(', ')}, not ${odd.join(', ')}`);
  };
  switch (o['kind']) {
    case 'grain':
      only(['grain']);
      return { kind: 'grain', column, grain: o['grain'] as TimeGrain };
    case 'showAs':
      only(['showAs']);
      return { kind: 'showAs', column, showAs: o['showAs'] as ShowAsKind };
    case 'bins': {
      only(['bins']);
      const bins = o['bins'];
      if (bins === null || typeof bins !== 'object' || Array.isArray(bins)) throw new CubeRefusal('bins are {width} or {count}');
      const b = bins as Record<string, unknown>;
      return { kind: 'bins', column, bins: {
        ...(b['width'] !== undefined ? { width: b['width'] as number } : {}),
        ...(b['count'] !== undefined ? { count: b['count'] as number } : {}),
      } };
    }
    default:
      throw new CubeRefusal(`a preset is 'grain', 'showAs' or 'bins', not ${JSON.stringify(o['kind'])}`);
  }
}
