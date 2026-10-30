// A FLOW'S STANDARD MACROS (macros.ts; docs/DATACUBE_FLOW_MODE_2026_10_24.md M8): a box's own steps read as a function
// of its input's rows, a sub-flow's composed, a page's macros judged and read -- each refused in words.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { fn, gt, lambda, lit, property, toJson, variable, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { boxFunction, macroOf, macroProblem, macroWords, MACRO_BOXES, readMacros, type PageMacro } from '../src/macros.ts';
import { CubeRefusal } from '../src/snapshot.ts';

/** The rows a box hands on: `base->filter(x|$x.notional > 90)->limit(5)`. */
const handedOn = (base: ValueSpecification): Lambda =>
  lambda([], fn('limit', fn('filter', base, lambda(['x'], gt(property(variable('x'), 'notional'), variable('min')))), lit.integer(5)));
const TRADES = fn('meta::pure::functions::relation::from', variable('trades'));
const big: Lambda = lambda(['input'], fn('filter', variable('input'), lambda(['x'], gt(property(variable('x'), 'notional'), lit.integer(90)))));

describe("a box's own steps, a sub-flow's composed, as one Pure function of its input's rows", () => {
  it("reads a box's base as its own steps over $input -- its document's Pure, a parameter it reads kept", () => {
    const f = boxFunction(handedOn(TRADES), TRADES, { name: 'Filter' });
    assert.equal(toJson(f), toJson(lambda(['input'], fn('limit', fn('filter', variable('input'),
      lambda(['x'], gt(property(variable('x'), 'notional'), variable('min')))), lit.integer(5)))));
    // a Pure box: its base its Pure of its input, read as its document's function over $input
    const bound = lambda(['input'], fn('filter', variable('input'), lambda(['x'], gt(property(variable('x'), 'notional'), lit.float(90)))));
    const base = fn('filter', TRADES, lambda(['x'], gt(property(variable('x'), 'notional'), lit.float(90))));
    const pured = boxFunction(handedOn(base), base, { name: 'Pure', pure: lambda(['input'], fn('filter', variable('input'),
      lambda(['x'], gt(property(variable('x'), 'notional'), variable('min'))))) });
    assert.ok(toJson(pured).includes('"name":"min"'), "its Pure's parameter kept: $min, not its value now");
    assert.notEqual(toJson(pured), toJson(boxFunction(handedOn(base), base, { name: 'Pure', pure: bound })));
    assert.throws(() => boxFunction(handedOn(TRADES), variable('other'), { name: 'Join' }),
      (e: unknown) => e instanceof CubeRefusal && /Join's rows are not read from its input's/.test(e.message));
  });

  it('composes the steps in order, each over the rows before it', () => {
    const limit5 = lambda(['input'], fn('limit', variable('input'), lit.integer(5)));
    assert.equal(toJson(macroOf([big, limit5])), toJson(lambda(['input'], fn('limit', big.body[0]!, lit.integer(5)))));
    assert.throws(() => macroOf([]), /one box at least/);
    assert.throws(() => macroOf(Array.from({ length: MACRO_BOXES + 1 }, () => limit5)), /20 boxes at most/);
  });

  it("judges a macro among the page's others, reads the page's, and says one in words", () => {
    const m: PageMacro = { name: 'Big trades', pure: big };
    assert.equal(macroProblem(m, []), undefined);
    assert.match(macroProblem(m, [m])!, /a macro 'Big trades' already/);
    assert.match(macroProblem({ ...m, name: ' ' }, [])!, /a macro's name is words/);
    assert.match(macroProblem({ name: 'x', pure: lambda(['input'], lit.integer(1)) }, [])!, /x: its Pure never reads \$input/);
    assert.deepEqual(readMacros([m], (v) => v as Lambda), [m]);
    assert.throws(() => readMacros([m, m], (v) => v as Lambda), /already/);
    assert.throws(() => readMacros([{ ...m, extra: 1 }], (v) => v as Lambda), /takes no extra/);
    assert.equal(macroWords(m, []), 'filter');
    const reading: PageMacro = { name: 'Over min', pure: lambda(['input'], fn('filter', variable('input'), lambda(['x'], gt(property(variable('x'), 'notional'), variable('min'))))) };
    assert.equal(macroWords(reading, [{ name: 'min', type: 'Float', value: 1 }]), 'filter; reads $min');
  });
});
