// A CROSS TAB'S CHECK (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL3, FL8): a Cross Tab whose columns are fixed by its
// pinned values (FL2: query.ts crossTabRows) gives no column to a value its rows gain since -- its rows are counted in
// its total, in no column of their own -- so the values its rows hold that its pin lacks are asked of the database
// (query.ts unpinnedValuesLambda) and said, never dropped in silence: on its box, in a run, and by Python. A box whose
// every value has a column passes. One set of words for the canvas and Python (headless gridFailing).
//
// PURE: no DOM, no planner.

import type { FilterValue } from './snapshot.ts';

/** The most values a Cross Tab's question counts (query.ts MAX_PIVOT_VALUES, its one past them asked: pinned equal by
 *  test/flow-sheet.test.ts) -- named here, so the canvas's words fetch no query code. */
export const VALUES_COUNTED = 500;

/** A pivot's value in words: an empty one "(empty)". */
export const valueWords = (v: FilterValue | null): string => (v === null ? '(empty)' : typeof v === 'object' ? JSON.stringify(v) : String(v));

/** What a pinned Cross Tab checks, in words: "every region has a column". */
export const lackingCheck = (on: string): string => `every ${on} has a column`;

/** Its values with no column, in words: "region APAC, LATAM have no column of their own: Load values again to add
 *  them" (the first `max` named; past what its question counts, VALUES_COUNTED, at least as many). */
export function lackingWords(on: string, values: readonly (FilterValue | null)[], max = 8): string {
  const shown = values.slice(0, max).map(valueWords).join(', ');
  const rest = values.length > VALUES_COUNTED ? ` and at least ${values.length - max} more`
    : values.length > max ? ` and ${values.length - max} more` : '';
  const one = values.length === 1;
  return `${on} ${shown}${rest} ${one ? 'has no column of its own' : 'have no column of their own'}: Load values again to add ${one ? 'it' : 'them'}`;
}
