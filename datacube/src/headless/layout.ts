// A SHEET'S LAYOUT READ, HEADLESS (docs/PYTHON_ASKS_DATACUBE_2026_10_15.md, item 3): what Python's pages ask of
// DataCube's own layout rules (layout/bands.ts) beyond the page commands that edit a layout (page-commands.ts, headless
// pageApply: a tile placed, moved, stacked or taken off, a sheet arranged) -- a sheet's tiles in reading order, and
// bands of tiles as the layout picker's shape -- so Python walks no layout by rules of its own. Each takes a sheet's
// layout as a page writes it (`{kind: 'bands', fit, bands}`, JSON).

import { leafOf, problems, stackOf, tiles, type Bands } from '../layout/bands.ts';

/** A sheet's layout, held to bands.ts's rules as its page holds it: one that breaks them is refused, never mended. */
function read(layout: string): Bands {
  const { kind, fit, bands } = JSON.parse(layout) as { kind?: unknown; fit: boolean; bands: Bands['bands'] };
  if (kind !== 'bands') throw new Error(`not a layout of bands: ${layout.slice(0, 200)}`);
  const laid: Bands = { fit, bands };
  const wrong = problems(laid);
  if (wrong.length > 0) throw new Error(`the layout cannot be laid out: ${wrong.join('; ')}`);
  return laid;
}

/** The tiles of a sheet's layout, in reading order, a stack's in its tabs' order (bands.ts tiles): JSON, a list. */
export function layoutTiles(layout: string): string {
  return JSON.stringify(tiles(read(layout)));
}

/**
 * BANDS OF TILES AS THE LAYOUT PICKER LAYS THEM OUT (Python's sheet.layout([grid, chart], [other])): `bands`, a JSON
 * list of lists of tile ids, top to bottom, each band's places side by side and alike; a stack named by any of its
 * tiles; every tile of the sheet in one band, once. JSON: `{preset, order}` -- the picker's rows shape (`rows:2-1`) and
 * the tiles in its order, for an arrange command -- or `{problems}`, in DataCube's words. A sheet with no tile, laid out
 * in no bands, is as it is: `{preset: null}`.
 */
export function layoutBands(layout: string, bands: string): string {
  const laid = read(layout);
  const asked = JSON.parse(bands) as string[][];
  const used = new Set<string>();
  for (const band of asked) {
    for (const tile of band) {
      if (leafOf(laid, tile) === undefined) return JSON.stringify({ problems: [`'${tile}' is not a tile of the sheet`] });
      const place = stackOf(laid, tile);
      if (place.some((t) => used.has(t))) return JSON.stringify({ problems: [`'${tile}' (or a tile stacked with it) is in the layout twice`] });
      place.forEach((t) => used.add(t));
    }
  }
  const left = tiles(laid).filter((t) => !used.has(t));
  if (left.length > 0) return JSON.stringify({ problems: [`every tile on the sheet is in the layout: ${left.join(', ')} not`] });
  if (asked.length === 0) return JSON.stringify({ preset: null });
  return JSON.stringify({ preset: `rows:${asked.map((b) => b.length).join('-')}`, order: asked.flat() });
}
