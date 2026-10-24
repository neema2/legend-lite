// Every menu entry must LAND somewhere, and do something when it does.
//
// The dispatch in app.ts ends in `default: return`, so a menu id
// nobody handles is a button that does nothing, silently, for ever.
// Two shipped that way -- "Auto-size to Fit Content" and "Auto-size
// All Columns" -- and no test noticed: a test that builds the menu
// proves the entry exists, a test that calls a handler proves the
// handler works, and nothing checked that the two sets MATCH.
//
// The first version of this file only looked for a `case` LABEL, and
// a mutant that reduced a case to a bare `return;` sailed through it
// -- a check that proves well-formedness will pass a placeholder. So
// it reads each case's BODY, and a case whose body does nothing but
// return is as dead as no case at all.
//
// It reads source rather than behaviour because that is where the two
// halves live: the ids the builder can emit, and the cases anything
// switches on. A behavioural test cannot see a `default` branch being
// taken.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { Sources } from '../../tools/js/runfiles.mts';

// the sources this reads, as the BUILD target declares them (Bazel workplan P3-29)
const SOURCES = new Sources('SOURCES', 'datacube');
const read = (path: string): string => SOURCES.read(path);

// Ad Hoc Analysis mode builds and handles its own entries.
const MODE = read('src/adhoc/mode.ts');
const MENU = read('src/ui/menu.ts') + MODE;
const APP = read('src/app.ts') + MODE;

/** Ids the menu builder can put on an item -- and the title bar's menu, built in app.ts (New ▸ Source…). */
const emitted = new Set([
  ...[...MENU.matchAll(/id: '([a-zA-Z][\w.]*)'/g)].map((m) => m[1] as string),
  // app.ts also names views (`id: 'grid'`): only a menu id is dotted
  ...[...APP.matchAll(/id: '([a-zA-Z]\w*\.[\w.]+)'/g)].map((m) => m[1] as string),
]);

/**
 * Ids that are switched on AND act.
 *
 * Every `case` is read together with the statements that follow it,
 * up to the next `case` or `default`. An empty body is not
 * automatically dead -- `case 'a': case 'b': doThing();` is the
 * ordinary way to share one action between two ids -- so the body of
 * a fallthrough group is attributed to every label in it. What is
 * dead is a group whose whole body is `return;`.
 */
function acting(source: string): Set<string> {
  const out = new Set<string>();
  // Split on case/default labels, keeping the id each one carries.
  const parts = source.split(/\n\s*(?:case ('[a-zA-Z][\w.]*')|default)\s*:/);
  // parts[0] is everything before the first label; then pairs of
  // (id-or-undefined, body).
  let pending: string[] = [];
  for (let i = 1; i < parts.length; i += 2) {
    const raw = parts[i];
    const body = parts[i + 1] ?? '';
    const id = raw === undefined ? undefined : raw.slice(1, -1);
    if (id !== undefined) pending.push(id);
    const meat = body.replace(/\/\/[^\n]*/g, '').trim();
    // An EMPTY body falls through to the next label, so the ids seen
    // so far wait for whatever acts below them. A body of `return;`
    // does NOT fall through -- return terminates -- so those ids are
    // dead, and clearing `pending` without recording them is the
    // difference between this check and the one a bare-`return;`
    // mutant walked straight through.
    if (meat === '') continue;
    if (/^return;$/.test(meat)) {
      pending = [];
      continue;
    }
    for (const name of pending) out.add(name);
    pending = [];
  }
  return out;
}

/** Ids that are a grid's gestures (grid-gestures.ts: the menu's own ids where it has the gesture), which app.ts makes
 *  whole by one dispatch (ui/menu.ts menuGesture) -- each one the menu emits acts, by the gesture's own function; the
 *  rest are other controls' (a drag in the zones, a dragged column edge), no menu's (2026-10-22, S8). */
const GESTURES = read('src/grid-gestures.ts');
// each with what it needs: a menu entry carries a column, an operator and a value, so a gesture needing more (a width,
// a filter) is no menu entry's (the S8 audit's 9) -- and, since 2026-10-24 (the BI and Excel parity program's P2), a
// date range (Filter > Date Range) and a top N's N and side (Filter > Top 10, Bottom 10): ui/menu.ts menuGesture
// ...and Conditional Formatting's choices: data bars on or off, a colour scale, an icon set, a rule (conditional.ts)
const CARRIED = new Set(['column', 'operator', 'value', 'range', 'n', 'bottom', 'on', 'scale', 'set', 'rule']);
const gestures = [...GESTURES.slice(GESTURES.indexOf('const GESTURES = {'), GESTURES.indexOf('} satisfies {'))
  .matchAll(/^  '([a-zA-Z][\w.]*)': \{\s*args: \[([^\]]*)\]/gm)]
  .filter((m) => [...(m[2] as string).matchAll(/'(\w+)'/g)].every((a) => CARRIED.has(a[1] as string)))
  .map((m) => m[1] as string);
const handled = new Set([...acting(MENU), ...acting(APP), ...(/\bmenuGesture\(/.test(APP) ? gestures.filter((id) => emitted.has(id)) : [])]);

describe('the menu has no dead entries', () => {
  it('found the ids at all', () => {
    // A regex that matched nothing would make every assertion below
    // pass. The counts are a floor, not an exact number, so adding
    // entries does not mean editing this test.
    assert.ok(emitted.size > 30,
      `only ${emitted.size} ids emitted — the scan is broken`);
    assert.ok(handled.size > 30,
      `only ${handled.size} ids act — the scan is broken`);
    assert.ok(gestures.length > 30, `only ${gestures.length} grid gestures a menu entry can make found — the scan is broken`);
    assert.ok(!gestures.includes('column.show') && gestures.includes('column.pinLeft'), 'a gesture needing more than an entry carries is none of its');
  });

  it('every id the menu emits is handled by something that acts', () => {
    const dead = [...emitted].filter((id) => !handled.has(id)).sort();
    assert.deepEqual(dead, [],
      `these menu entries do nothing when clicked: ${dead.join(', ')}`);
  });

  it('nothing is handled that the menu cannot emit', () => {
    // The other direction catches a rename that left the handler
    // behind: the feature is then unreachable, which is the failure
    // that "unit-tested but unreachable" is about. `view.*` are the
    // title bar's own entries, built in app.ts.
    const orphans = [...handled]
      .filter((id) => id.includes('.') && !emitted.has(id))
      .filter((id) => !id.startsWith('view.'))
      .sort();
    assert.deepEqual(orphans, [],
      `these handlers can never be reached from the menu:`
      + ` ${orphans.join(', ')}`);
  });
});
