// A JOIN IN WORDS (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17): what a join does, said -- the Join window's, and a
// flow's Join box's (flow-boxes.ts boxSays: docs/DATACUBE_FLOW_MODE_2026_10_24.md M2). A module of its own, light, so a box
// says its join without the join's chunk, and the page's start loads none of it (test/bundle-budget.test.ts).

import { isEveryPair, pickWords, SIMILAR_WITHIN, type JoinCondition, type JoinHow, type JoinKindWord, type JoinRow } from './join-how.ts';

const KIND_WORDS: Readonly<Record<JoinKindWord, string>> = {
  inner: 'matching rows only', left: 'all of mine', right: 'all of theirs', full: 'everything',
};

/** A row in words: `cust = id`. */
export function rowWords(row: JoinRow): string {
  if (row.op === '~') return `${row.left} ≈ ${row.right} (within ${row.within ?? SIMILAR_WITHIN})`;
  return `${row.left} ${row.op === '==' ? '=' : row.op} ${row.right}`;
}

/** What a join does, in words: `all of mine, on cust = id`; `every pair`; `nearest at = at, on sym = sym`; a join of
 *  every row with every row (isEveryPair), `each row with every row of theirs`. */
export function joinWords(how: JoinHow): string {
  if (isEveryPair(how)) return 'each row with every row of theirs';
  const on = (c: JoinCondition): string => ('lambda' in c ? 'a condition in Pure'
    : c.rows.length === 0 ? 'every pair' : c.rows.map(rowWords).join(' and '));
  switch (how.form) {
    case 'join': return `${KIND_WORDS[how.kind]}, on ${on(how.on)}${how.score !== undefined ? `, scored as ${how.score}` : ''}`;
    case 'asOf': return `nearest ${rowWords(how.match)}${'rows' in how.on && how.on.rows.length === 0 ? '' : `, on ${on(how.on)}`}`;
    case 'semi': return `my rows with a match, on ${on(how.on)}`;
    case 'anti': return `my rows with no match, on ${on(how.on)}`;
    case 'append': return how.pick !== undefined ? `the rows of the one ${pickWords(how.pick)} names` : 'the rows of both, columns by name';
  }
}
