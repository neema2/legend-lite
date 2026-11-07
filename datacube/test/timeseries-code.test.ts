// A TIMESERIES TILE AS THE CODE THAT BUILDS IT (src/timeseries-code.ts; the user, 2026-10-26: "an option that shows how
// exactly to build that chart programmatically from TS and python"): every tile of the timeseries gallery written as
// TypeScript, and the program imported and run -- over the cubes it names (yours to set: the tile's here) the spec it
// writes is the tile's, its command adds the tile to the sheet it is given as the screen's Create ▸ Timeseries does,
// and its questions are the Query window's; its Python a page of its own, never this page's ids (the user, 2026-10-28:
// "a generic snippet ... to just create exactly that tile in their own datacube"). (Python's, run, is test_page.py's;
// not a headless test: it writes the program to a file and imports it.)

import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { describe, it } from 'node:test';

import { timeseriesGalleryPage } from '../src/examples/timeseries-gallery.ts';
import { layoutTiles, pageToJson, readPage, type PageDocument } from '../src/page-document.ts';
import { codeOfPage, tileCode } from '../src/timeseries-code.ts';
import { tileAsks } from '../src/timeseries-query.ts';
import type { TimeseriesSpec } from '../src/timeseries-spec.ts';
import { readPageTexts } from './catalog-builder.ts';

describe('a timeseries tile as the code that builds it', () => {
  it('the timeseries gallery as code: each tile\'s TypeScript, run, builds the tile it was written of', async () => {
    const page = await readPageTexts(readPage(pageToJson(await timeseriesGalleryPage())));
    const src = new URL('../src/', import.meta.url).href;
    const dir = mkdtempSync(join(process.env['TEST_TMPDIR'] ?? tmpdir(), 'ts-code-'));
    const all = page.views.flatMap((v) => (v.kind === 'timeseries' ? [v] : []));
    assert.ok(all.length >= 30);
    for (const v of all) {
      const { spec, of } = codeOfPage(page, v.id);
      const code = tileCode(spec, of, 'typescript');
      assert.ok(code.includes("kind: 'addTimeseries'"), v.id);
      const file = join(dir, `${v.id}.ts`);
      writeFileSync(file, code.replaceAll("from './datacube/src/", `from '${src}`));
      const made = await import(pathToFileURL(file).href) as {
        cubes: Record<string, string>; spec: TimeseriesSpec; withTile: (p: PageDocument, sheet: string) => PageDocument; questions: string[];
      };
      assert.deepEqual(made.cubes, Object.fromEntries(spec.sources.map((s) => [s.alias, s.cube])), `${v.id}: the cubes it reads, by alias`);
      assert.deepEqual(made.spec, spec, `${v.id}: the spec its program writes is the tile's`);
      const on = page.sheets.find((x) => layoutTiles(x.layout).includes(v.id))!.id;
      const added = made.withTile(page, on);
      const fresh = added.views.find((x) => !page.views.some((y) => y.id === x.id));
      assert.ok(fresh?.kind === 'timeseries', `${v.id}: a tile added`);
      assert.deepEqual(fresh.spec, spec, `${v.id}: the tile it adds`);
      assert.equal(fresh.title, v.title, `${v.id}: titled as it is`);
      const sheet = added.sheets.find((x) => layoutTiles(x.layout).includes(fresh.id));
      assert.equal(sheet?.id, on, `${v.id}: on the sheet it is given`);
      assert.deepEqual(made.questions, tileAsks(spec).map((a) => a.what), `${v.id}: the Query window's questions`);
      // and its Python: a page of its own, a grid opened as each source's was, each series' expression as the catalog
      // writes it, each one's Pure printed -- no id of this page's (its sheets', its tiles')
      const python = tileCode(spec, of, 'python');
      assert.ok(python.includes('page = ll.Page()') && python.includes('ll.show(page)'), v.id);
      assert.ok(python.includes("print(s, tile.query(s)['pure'])"), v.id);
      for (const lookup of ['Page.load', 'page.sheets', 'page.grids', '.id ==']) assert.ok(!python.includes(lookup), `${v.id}: finds nothing of this page (${lookup})`);
      for (const s of spec.series) assert.ok(python.includes(`'${s.text.replace(/'/g, "\\'")}'`), `${v.id} ${s.id}: ${s.text}`);
    }
  });

  it('a title or a grid\'s title with a line end in it ends no comment: the program is the tile\'s still', async () => {
    // the review of the generated code, 2026-10-28: a title's line end wrote the rest of it as a line of code
    const page = await readPageTexts(readPage(pageToJson(await timeseriesGalleryPage())));
    const v = page.views.find((x) => x.kind === 'timeseries')!;
    const { spec, of } = codeOfPage(page, v.id);
    const title = 'Two\nlines\r\nexport const x = 1; export const y = 2;';
    const grids = Object.fromEntries(Object.entries(of.grids).map(([k, g]) => [k, g && { ...g, title: `A\rgrid\nimport os` }]));
    const code = tileCode(spec, { title, grids }, 'typescript');
    const dir = mkdtempSync(join(process.env['TEST_TMPDIR'] ?? tmpdir(), 'ts-code-'));
    const file = join(dir, 'named.ts');
    writeFileSync(file, code.replaceAll("from './datacube/src/", `from '${new URL('../src/', import.meta.url).href}`));
    const made = await import(pathToFileURL(file).href) as Record<string, unknown> & { withTile: (p: PageDocument, sheet: string) => PageDocument };
    assert.deepEqual(Object.keys(made).sort(), ['cubes', 'questions', 'spec', 'withTile']);
    const on = page.sheets.find((x) => layoutTiles(x.layout).includes(v.id))!.id;
    const added = made.withTile(page, on).views.find((x) => !page.views.some((y) => y.id === x.id));
    assert.equal(added?.title, title);
    const python = tileCode(spec, { title, grids }, 'python');
    assert.deepEqual(python.split('\n').filter((l) => /^(import|export|print)/.test(l)), ['import legend_lite as ll']);
    // Python reads a `coding=` on its first two lines as the file's encoding (PEP 263): none of a person's words there
    const coded = tileCode(spec, { title: 'coding=utf-7 +ACc-', grids }, 'python');
    assert.ok(!coded.split('\n').slice(0, 2).join('\n').includes('coding'), coded.split('\n').slice(0, 3).join('\n'));
    // and a direction mark is never written as itself (it shows the code otherwise than it runs)
    const marked = tileCode(spec, { title: 'a\u202eb', grids }, 'python') + tileCode(spec, { title: 'a\u202eb', grids }, 'typescript');
    assert.ok(!marked.includes('\u202e'));
  });

});
