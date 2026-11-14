// THE BRAND'S CASTS, READ BY THE CHECKER (the re-audit's S5, 2026-11-12; brand-casts.ts): each planted way of making a
// plan's statement or a planner's answer out of other text is read as what it is, and the product's casts that carry
// either brand -- each a narrowing of a value already typed so, or a generic cast -- are pinned by file, exactly: a new
// one is red here, naming its file and line. Over the three TypeScript programs: DataCube's (engine-client, datacube,
// pure-protocol, query-store, sdlc-client, depot-client, model-changes), Studio's and Query's.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { counted, PRODUCT, readCasts, type Cast } from './brand-casts.ts';

const PLANTED = 'datacube/test/fixtures/brand-casts/';

/**
 * The product's casts that carry a brand, by file and kind. A count moves only with a dated note here (AGENTS.md: guard
 * files are load-bearing).
 * - brand: the mints (planned-sql.ts: fromPlan, planFromAnswer); the rest each narrow a value its type already holds the
 *   brand in -- a constructor's union of a planner or its options (cube.ts), an optional planner (app.ts), a host's
 *   planner as the LitePlanner it is (headless join, grid, model), a cube's options (query's cube.ts) -- never text.
 * - predicate: model-writer.ts writesModels, `x is ModelWriter` (a planner's own model writer, read by its methods).
 * - generic: a cast to a bare type parameter, which a caller could instantiate as a brand: each counted.
 * 2026-11-12 (S5, measured): python-host.ts's two casts of Python's JSON answers (as HostCopy, as UploadResult, each
 * holding copySelectList: a PlannedSql) were typed without the brand -- the regex census had not seen them.
 */
const PINNED: Readonly<Record<string, number>> = {
  'datacube/src/app.ts: brand': 1,
  'datacube/src/app/join-sources.ts: generic': 1,
  'datacube/src/chart-spec.ts: generic': 1,
  'datacube/src/config.ts: generic': 2,
  'datacube/src/cube-document.ts: generic': 1,
  'datacube/src/cube.ts: brand': 4,
  'datacube/src/file-handles.ts: generic': 1,
  'datacube/src/headless/grid.ts: brand': 1,
  'datacube/src/headless/join.ts: brand': 1,
  'datacube/src/headless/model.ts: brand': 1,
  'datacube/src/join-how.ts: generic': 1,
  'datacube/src/model-writer.ts: predicate': 1,
  'datacube/src/page-parameters.ts: generic': 1,
  'datacube/src/runner.ts: generic': 1,
  'datacube/src/state-owner.ts: generic': 2,
  'datacube/src/ui/form.ts: generic': 1,
  'engine-client/src/planned-sql.ts: brand': 2,
  'engine-client/src/warehouse.ts: generic': 1,
  'query/src/app/cube.ts: brand': 1,
  'query/src/ui/dom.ts: generic': 1,
  'sdlc-client/src/records.ts: generic': 2,
};

const where = (c: Cast): string => `${c.file}:${c.line}: ${c.kind} (${c.type})`;

/** The product's casts that carry a brand: its three programs read by the checker, once. */
let product: readonly Cast[] | undefined;
function productCasts(): readonly Cast[] {
  if (product !== undefined) return product;
  const seen = new Map<string, Cast>();
  let files = 0;
  for (const config of ['datacube/tsconfig.json', 'studio/tsconfig.json', 'query/tsconfig.json']) {
    const read = readCasts(config, process.cwd(), PRODUCT);
    assert.deepEqual(read.unreadable, [], `${config}: a type the checker could not read is never taken for one that carries nothing`);
    files += read.files;
    for (const c of read.casts) seen.set(`${c.file}:${c.at}:${c.kind}`, c);
  }
  assert.ok(files > 500, `the three programs read ${files} product files`);
  product = [...seen.values()];
  return product;
}

/** The census's pinned comparison: `casts` counted by file and kind are PINNED, exactly. */
function pinned(casts: readonly Cast[]): void {
  assert.deepEqual(counted(casts), PINNED, `a cast that makes a plan's statement or a planner's answer of other text:\n${casts.map(where).join('\n')}`);
}

describe("the brand's casts, read by TypeScript's own checker (the re-audit's S5)", () => {
  it('reads every planted way of making a brand as what it is, and no cast that makes none', () => {
    const read = readCasts(`${PLANTED}tsconfig.json`, process.cwd(), (f) => f.startsWith(PLANTED));
    assert.deepEqual(read.unreadable, []);
    assert.equal(read.files, 1);
    const lines = readFileSync(`${PLANTED}planted.ts`, 'utf8').split('\n');
    const planted = lines.flatMap((l, i) => {
      const m = /\/\/ planted: (brand|generic|predicate)$/.exec(l);
      return m ? [`${i + 1}: ${m[1]}`] : [];
    });
    assert.equal(planted.length, 11);
    assert.deepEqual(read.casts.map((c) => `${c.line}: ${c.kind}`), planted, read.casts.map(where).join('\n'));
  });

  it("pins the product's casts that carry a brand, by file, exactly", () => {
    pinned(productCasts());
  });

  it("fails its pinned comparison on a planted violator, naming it (check 6, the final audit's item 5)", () => {
    // the comparison above, run over the product's casts and the planted file's as the checker reads them: it must FAIL,
    // naming the planted file -- never merely that the checker sees each form (the test before this one)
    const read = readCasts(`${PLANTED}tsconfig.json`, process.cwd(), (f) => f.startsWith(PLANTED));
    assert.ok(read.casts.length > 0);
    assert.throws(() => pinned([...productCasts(), ...read.casts]),
      (e: unknown) => e instanceof assert.AssertionError && e.message.includes(`${PLANTED}planted.ts:`));
  });
});
