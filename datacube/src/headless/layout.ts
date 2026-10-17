// A SHEET'S LAYOUT EDITED, HEADLESS (docs/PYTHON_ASKS_DATACUBE_2026_10_15.md, item 3): what Python's pages ask of
// DataCube's own layout rules (layout/bands.ts) -- a new tile's place (sheet.add, page.grid, grid.chart), a tile taken
// off (page.remove), the places arranged (sheet.arrange, sheet.layout) and tiles stacked (page.stack) -- so Python
// writes none of them again. Each takes a sheet's layout as a page writes it (`{kind: 'bands', fit, bands}`, JSON) and
// gives it back as a page saves it (bands.ts asSaved: no stack's tile in front).

import {
  NotALayout, add, arrange, asSaved, problems, remove, stackTiles, type Bands, type Preset,
} from '../layout/bands.ts';

/** A sheet's layout, held to bands.ts's rules as its page holds it: one that breaks them is refused, never mended. */
function read(layout: string): Bands {
  const { kind, fit, bands } = JSON.parse(layout) as { kind?: unknown; fit: boolean; bands: Bands['bands'] };
  if (kind !== 'bands') throw new Error(`not a layout of bands: ${layout.slice(0, 200)}`);
  const laid: Bands = { fit, bands };
  const wrong = problems(laid);
  if (wrong.length > 0) throw new Error(`the layout cannot be laid out: ${wrong.join('; ')}`);
  return laid;
}

const written = (layout: Bands): string => JSON.stringify({ kind: 'bands', ...asSaved(layout) });

/** A new tile's place, as DataCube places one (bands.ts add): beside `near` while its band has room, else a band of
 *  its own below it; with no `near` (null), a band at the bottom. */
export function layoutAdd(layout: string, tile: string, near: string | null): string {
  return written(add(read(layout), tile, near ?? undefined));
}

/** The layout without `tile` (bands.ts remove): its place closed by its neighbours, an emptied band gone. */
export function layoutRemove(layout: string, tile: string): string {
  return written(remove(read(layout), tile));
}

/** `tiles` (a JSON list) stacked where the first one is (bands.ts stackTiles). */
export function layoutStack(layout: string, tiles: string): string {
  return written(stackTiles(read(layout), JSON.parse(tiles) as string[]));
}

/**
 * The places arranged as one of the layout picker's presets (bands.ts arrange: 'side-by-side', 'stacked', 'rows:2-1',
 * 'columns:1-2', 'focus-left', ...), in the order of `order`'s tiles (a JSON list; null: reading order). JSON: the
 * layout, or why the preset is not one DataCube draws (`problems`), for the asker to say in its own way.
 */
export function layoutArrange(layout: string, preset: string, order: string | null): string {
  const bands = read(layout);
  let arranged: Bands;
  try {
    arranged = order === null ? arrange(bands, preset as Preset)
      : arrange(bands, preset as Preset, JSON.parse(order) as string[]);
  } catch (e) {
    if (!(e instanceof NotALayout)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
  return JSON.stringify({ layout: JSON.parse(written(arranged)) as unknown });
}
