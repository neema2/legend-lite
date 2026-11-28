# Independent review of slice 6's first step (2026-11-28, about 05:00 UTC)

*An Opus reviewer, read-only, given `git diff 6f33e9206 385f8c5b3 -- core/src/main` of the scratch branch
`claude/step-6b-slice-6` (an `assertEquals`, `assertNotEquals` or `assertSameElements` of plain values and dates
planned as the product's own comparison over sides bound once), tenet 5 and the equality rule. It read the code and
ran nothing. Shortened; the author's disposition under each.*

## Findings

**1. (most severe) A Decimal a store read carries as its TEXT met a number under `=`.** The member this path
replaces read the other side as the same text (`besideDecimals`) and refused a text against a number by name
(`oneCarrier`); the new path bound the rows as carried, and the product's comparison of two expressions reads
their Pure types. `assertEquals($t.qty, $t.price)`: a conversion error on DuckDB, false on a database that carries
a list as JSON.
- *Fixed by narrowing the path:* a pair moves only where each side is held as its own kind's SQL type
  (`SideEqualities.heldAsItsKind`; a date-time as TIMESTAMP or as its written text) and the two are of ONE kind.
  Anything else stays with the member that reads it.

**2. A verdict with no value**, which reads as a failure under either polarity: two sides declared exactly one,
one of which reads a store and holds nothing (a NULL under a `[1]` property). `assertNotEquals($t.name, 'x')`
failed where the Java checker passes.
- *Fixed by narrowing:* a side declared exactly one that reads a store does not move
  (`DatabaseJudge.exactlyOneFromAStore`). A side that reads no store cannot hold nothing under `[1]`; an optional
  side is answered for its emptiness by the product's own rule.

**3. Two declared types that differ** (an Integer against a Float, a day against a date-time, reached inside a
function whose types the caller gives) were refused at planning or compared by the database, where the member
answers "the same only where both are empty" as the Java checker does.
- *Fixed by narrowing:* the two declared types must be the same plain type.

**4. SQL size: a single-valued verdict written three times** (the verdict and the two "where it fails" columns),
each copy with the instant reading for date-times.
- *Fixed:* the verdict of a pair on this path is computed once beneath the row (`VerdictSql.typed(.., once)`), as
  the instruction's always was.

**5. (a suspicion) A list of days carried as texts against held days**: read as "no instant". *Excluded by 1.*

**6. The empty-list cast was written on a database that compares rows**, for two written lists that are no exact
numbers when one side is one value (`assertEquals(['a'], ['a', 'b'])`): their rows did not ride.
- *Fixed:* over sides bound once the rows always ride (`Equalities.asSameValues(.., boundSides)`), as the member's
  always did; a rows database never writes the two bound values.

**7. (a suspicion) An Integer list against a Decimal list of a scale above 19 on DuckDB** loses exactness in the
database's own cast. *Excluded by 1 (one kind).* It is the product's own `==` too: recorded for the measure.

## Found nothing in

The lowering never lowers a bound side again (every way a side's node is lowered passes the two entries that read
the bound map); the temporary planner and the bound map are restored and do not escape; the moved single-value
helper; the common tables' names; `MIN` as "the one value" on every dialect read; the record's lists; a constant
answer (the sides stay in the statement and are evaluated); layering (no SQL text, no fallback, nothing of Java in
the plan).

## What the narrowing means

The path is right for what it takes, and takes less than it was built for: two sides of one plain type, each held
as that type, no store value declared exactly one. The member it replaces (`SideEqualities.values`) keeps every
other pair, so it is not deleted by this step. What would let it go is in `slice-6-half-page.md` ("Next").
