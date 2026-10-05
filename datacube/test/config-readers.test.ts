// Every setting the editor can write must be READ by something.
//
// Eight shipped that were not (census §2): Column Properties >
// Aggregation, its weight, the pivot total's function and name,
// "Initially expand to level", grid mode and the two link settings.
// Each was a control the panels wrote into the configuration and
// nothing between the configuration and the query or the grid ever
// looked at -- so the dropdown moved, the query stayed byte-identical,
// and no test noticed, because tests of the panels prove they WRITE
// and tests of the query prove what it does with the fields it knows.
//
// So this reads source, like `menu-ids.test.ts`: a field of
// `ColumnConfiguration` or `CubeConfiguration` passes when some file
// outside the editor panels accesses it as a property (`.field`). The
// editor (everything under src/ui/) is excluded because it is the
// writer and its display of a value is not the value taking effect;
// the declarations and defaults are excluded because they are not
// property accesses.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { Sources } from '../../tools/js/runfiles.mts';

// the sources this reads, as the BUILD target declares them (Bazel workplan P3-29)
const SOURCES = new Sources('SOURCES', 'datacube');

const CONFIG = SOURCES.read('src/config.ts');

/** The `readonly` fields of one exported interface in config.ts. */
function fieldsOf(name: string): string[] {
  const start = CONFIG.indexOf(`export interface ${name} {`);
  assert.ok(start >= 0, `no interface ${name} in config.ts`);
  const body = CONFIG.slice(start, CONFIG.indexOf('\n}', start));
  return [...body.matchAll(/^\s+readonly (\w+)\??:/gm)].map((m) => m[1] as string);
}

// The whole EDITOR is excluded, not only its panels: `editor.ts` read
// `pivotSortDirection` to show it in the Horizontal Pivots tab, and that
// one read was enough for this check to pass a setting nothing else
// used (2026-09-25 sweep). Showing a setting is not honouring it.
const readers = SOURCES.under('src', '.ts')
  .filter((f) => !f.startsWith('src/ui/'))
  .map((f) => SOURCES.read(f))
  .join('\n');

describe('every configuration field has a reader', () => {
  for (const iface of ['ColumnConfiguration', 'CubeConfiguration']) {
    it(iface, () => {
      const fields = fieldsOf(iface);
      assert.ok(fields.length > 10, `only ${fields.length} fields found`);
      const unread = fields.filter(
        (f) => !new RegExp(`\\.${f}\\b`).test(readers));
      assert.deepEqual(unread, [],
        `written by the editor and read by nothing: ${unread.join(', ')}`);
    });
  }
});
