// Getting a user's text into a Pure string and back out intact.
//
// A filter value and an odd column name both end up inside single
// quotes in generated Pure, and both come from a person. Escaping the
// quote alone is not enough, because a BACKSLASH escapes whatever
// follows it -- including the closing quote:
//
//   value  C:\      ->  'C:\'     an unterminated literal
//   value  back\'   ->  'back\''  the text becomes grammar
//
// The first is a crash from a Windows path somebody pasted. The second
// is injection: the value stops being a value, and a filter travels
// inside a saved view that one person can hand to another.
//
// Both were live until the torture work went looking. The fix is
// ordering -- escape the backslash FIRST, so that by the time quotes
// are escaped every backslash in the text is already doubled -- and
// these tests exist because the ordering is the kind of thing a later
// edit silently reverses.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { ident, literal, serialize } from '../src/serialize.ts';

/** What a Pure single-quoted literal decodes back to. */
function decodePure(quoted: string): string {
  assert.ok(
    quoted.startsWith("'") && quoted.endsWith("'"),
    `not a quoted literal: ${quoted}`,
  );
  const body = quoted.slice(1, -1);
  let out = '';
  for (let i = 0; i < body.length; i++) {
    if (body[i] === '\\') {
      i += 1;
      out += body[i] ?? '';
    } else {
      out += body[i];
    }
  }
  return out;
}

/** Whether the quotes actually balance -- the unterminated-literal bug. */
function isWellFormed(quoted: string): boolean {
  if (!quoted.startsWith("'") || !quoted.endsWith("'") || quoted.length < 2) {
    return false;
  }
  const body = quoted.slice(1, -1);
  for (let i = 0; i < body.length; i++) {
    if (body[i] === '\\') {
      i += 1; // whatever follows is escaped
      continue;
    }
    if (body[i] === "'") return false; // a bare quote closed it early
  }
  return true;
}

const HOSTILE = [
  ['a plain value', 'AMER'],
  ['an apostrophe', "it's"],
  ['a trailing backslash', 'C:\\'],
  ['two trailing backslashes', 'C:\\\\'],
  ['a backslash before a quote', "back\\'"],
  ['a backslash in the middle', 'back\\slash'],
  ['only a backslash', '\\'],
  ['quote then backslash', "'\\"],
  ['many alternating', "\\'\\'\\'"],
  ['a newline', 'a\nb'],
  ['a tab', 'a\tb'],
  ['unicode', 'Ünïcødé'],
  ['an emoji', '🙂'],
  ['sql-ish', "'; DROP TABLE t; --"],
  ['pure-ish', "')->select(~[x])->from(evil"],
  ['the pivot separator', '__|__'],
] as const;

describe('a string value reaching Pure', () => {
  for (const [label, value] of HOSTILE) {
    it(`stays one well-formed literal when it is ${label}`, () => {
      const out = literal(value);
      assert.ok(isWellFormed(out), `escaped out of its quotes: ${out}`);
    });

    it(`round-trips unchanged when it is ${label}`, () => {
      assert.equal(decodePure(literal(value)), value);
    });
  }
});

describe('an odd column name reaching Pure', () => {
  for (const [label, value] of HOSTILE) {
    it(`quotes safely when the name is ${label}`, () => {
      const out = ident(value);
      // A plain identifier is emitted bare; anything else is quoted,
      // and the quoted form must survive the same way a value does.
      if (out.startsWith("'")) {
        assert.ok(isWellFormed(out), `escaped out of its quotes: ${out}`);
        assert.equal(decodePure(out), value);
      } else {
        assert.equal(out, value);
        assert.match(out, /^[A-Za-z_][A-Za-z0-9_]*$/);
      }
    });
  }
});

/**
 * Blank out every single-quoted literal, honouring backslash escapes.
 *
 * Counting `->select(` in the raw text is not an injection test: a
 * payload that merely CONTAINS that text will match while sitting
 * safely inside quotes, which is exactly what a correct escaper
 * produces. The question is whether the payload became a STAGE, so
 * the literals have to come out first.
 */
function stripLiterals(pure: string): string {
  let out = '';
  let inStr = false;
  for (let i = 0; i < pure.length; i++) {
    const ch = pure[i];
    if (inStr) {
      if (ch === '\\') {
        i += 1; // skip the escaped character
        continue;
      }
      if (ch === "'") inStr = false;
      continue;
    }
    if (ch === "'") {
      inStr = true;
      continue;
    }
    out += ch;
  }
  assert.equal(inStr, false, `unterminated literal in: ${pure}`);
  return out;
}

describe('the injection attempt in a whole query', () => {
  const snap = {
    source: { expression: '#>{db.T}#' },
    columns: [
      { name: 'region', type: 'String' },
      { name: 'notional', type: 'Float' },
    ],
    derived: [],
    rows: ['region'],
    pivotOn: [],
    measures: [{ name: 'm', column: 'notional', fn: 'sum' as const }],
    sorts: [],
    epoch: 1,
  };

  it('cannot close the literal and append its own pipeline', () => {
    const evil = "x')->select(~[region])->limit(1)->filter(y|'";
    const pure = serialize(
      { ...snap, filter: { kind: 'condition', column: 'region', operator: 'equal', value: evil } },
      { level: 1, parent: [], limit: 10 },
    );
    // The payload's pipeline must appear only INSIDE the quotes, never
    // as a stage of the query. With the literals removed, the query
    // must have exactly the stages a clean cube of this shape emits.
    const bare = stripLiterals(pure);
    assert.equal((bare.match(/->select\(/g) ?? []).length, 1, bare);
    assert.equal((bare.match(/->limit\(/g) ?? []).length, 1, bare);
    assert.equal((bare.match(/->filter\(/g) ?? []).length, 1, bare);
  });

  it('cannot break out through a column name either', () => {
    const evil = "region')->select(~[";
    const pure = serialize(
      { ...snap, rows: [evil], columns: [...snap.columns, { name: evil, type: 'String' }] },
      { level: 1, parent: [], limit: 10 },
    );
    const bare = stripLiterals(pure);
    assert.equal((bare.match(/->select\(/g) ?? []).length, 1, bare);
    assert.equal((bare.match(/->groupBy\(/g) ?? []).length, 1, bare);
  });
});
