# Step 6B: what is waiting for the user's word (gathered 2026-11-28, about 04:50 UTC)

*One page, so nothing found on the way is lost. Nothing here blocks the slices in flight.*

## A. A compatibility door answers differently than before (slice 5), each toward its own upstream

| # | Where | Before | Now | Why |
|---|---|---|---|---|
| A1 | legend-pure's door: a value upcast to `Number`, `Any` or `Date` and then filtered, sorted, deduplicated, reversed or cut (`take`, `drop`, `slice`, `tail`, `init`), or one value read from such a list (`toOne`, `first`, `last`, `at`, `head`), compared with a value of another type. `[2, 1]->cast(@Number)->sort()->contains(1.0)` | the database's comparison: true | false | legend-pure compares by the type a value holds, and an Integer is no Float there. The type-checker now reads the held type through these functions (`EqualityClasses.valuesOf`), for lite's rule and for this door alike. legend-engine's door does not move. |
| A2 | legend-pure's door: two optional enumeration values that both hold nothing, `==` | no value (the database's `=`) | true | Pure: two empty sides are equal. The enumeration exception in the empty-side rule copied legend-engine's SQL. legend-engine's door inside a store query does not move. |

PCT runs at legend-pure's door: if a PCT test's answer moves for A1 or A2, it is listed with the lanes' reports.

## B. To rule (the measure marks these `?`; `measure-gaps-2026-11-28.md`)

1. **A number literal against a value typed only when the query runs** (`1->cast(@Number) == 1.0`): the rule does
   not say whether the literal adapts; the code adapts a whole number.
2. **A literal read through a `let`**: it is a variable there, which the literal rule does not adapt (a compile
   error where the same written in place is accepted).
3. **A literal the query itself upcast** (`1.0->cast(@Number)`): the upcast of a literal is the literal again
   before the lowering. Written in place it is now answered by the type it holds (false against an Integer);
   through a `let` it is still read by the literal rule (true).
4. **Inside a function over one type variable**, two literals under an upcast to `Any` are refused at the call
   (an Integer and a String), two computed values under the same upcast are compared when the query runs.

## C. Found on the way, not equality's (each a task; none fixed without the user's word where SQL size may move)

| Task | What | Size risk |
|---|---|---|
| 26 | A `map` over an EMPTY property value answers a value (`$r.i->map(x \| 7)` is 7 where `i` holds nothing): the store resolver composes the function over the read with no guard | SQL size pins may move |
| 27 | About sixty time-series words whose refusal check sits on rows a later filter drops | none known |
| 29 | A `map` whose function may answer nothing is typed with its source's count (`[2]`), so the Java checker fails an `assertEquals` over such a list | new compile errors possible |
| - | ONE year written under the abstract `Date` against ONE held date-time fails when the query runs (the answer is false) | none |

## C2. One fix that needs your word, because a SQL size pin would rise

**A `map` over a list that another `map` built with gaps.** `[1, 2]->map(x | if($x == 1, |1, |[]))->map(y | 7)`
answers `[7, 7]`; Pure answers `[7]` (the first `map` answers nothing for 2, so there is no second element). It is
wrong on every database and at every door, and older than step 6B. The fix is small and was built and measured in
slice 5: where the query's text shows a list is built with such gaps, the next `map` reads it without them. It was
taken out again because it adds a list filter, twice, to ONE of core's pinned statements (+66 characters on
DuckDB): the statement that reads `...->map(i | $i->get('sku'))->map(s | $s->to(@String)->toOne())` over a JSON
payload -- where an item with no `sku` is today a gap the second `map` is applied to. That is the fix doing its
job, but "SQL size pins go up only with the user's word". **Say yes and it goes back in with the pin moved by that
amount and a dated note; say no and the measure keeps the row as a known wrong answer.** (Compacting every mapped
list where it is built, the first thing tried, cost more: one filter in each of several PCT and core statements.)

## D. Standing

- `vol_swap_volatility` default `@BTCUSD` is refused again where gs-quant answers (register S149), since slice 2.
- Slice 7 (a door's comparison as its own function chosen by the type-checker) needs the edition package
  (`compiler.spec.edition`), which does not exist yet: the edition program's step.
- A `let` read twice is computed twice: the lean-SQL follow-up's first slice, as planned.

## E. Passing to refusing, counted (the user's question of 2026-11-28, about 04:57 UTC)

- Slice 1: none (CI green, lanes with no difference).
- Slice 2: one, deliberate: the gs-quant case `vol_swap_volatility` default `@BTCUSD` (section D).
- Slice 3: none in the local round or in the lanes back so far; comparisons that FAILED when the query ran inside
  a DuckDB list's function (two tables, two maps, pairs) are refused by name now.
- Slice 3 to slice 5, the measure: of 5,701 results asked in both runs, none went from answered to refused; 8 went
  from refused to answered. Slice 6's measure is slice 5's.
- Known, slice 5, by reading and with no test on it: two date-times with digits past the microsecond, the same to
  the letter, compared equal as texts and now raise the named refusal that reading them as instants raises.
