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
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';

const read = (path: string): string =>
  readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');

// Ad Hoc Analysis mode builds and handles its own entries.
const MODE = read('src/adhoc/mode.ts');
const MENU = read('src/ui/menu.ts') + MODE;
const APP = read('src/app.ts') + MODE;

/** Ids the menu builder can put on an item. */
const emitted = new Set(
  [...MENU.matchAll(/id: '([a-zA-Z][\w.]*)'/g)].map((m) => m[1] as string),
);

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

const handled = new Set([...acting(MENU), ...acting(APP)]);

describe('the menu has no dead entries', () => {
  it('found the ids at all', () => {
    // A regex that matched nothing would make every assertion below
    // pass. The counts are a floor, not an exact number, so adding
    // entries does not mean editing this test.
    assert.ok(emitted.size > 30,
      `only ${emitted.size} ids emitted — the scan is broken`);
    assert.ok(handled.size > 30,
      `only ${handled.size} ids act — the scan is broken`);
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
