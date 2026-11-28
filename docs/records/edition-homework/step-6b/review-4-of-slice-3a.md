# Independent review of slice 3a's product change (2026-11-28, about 02:10 UTC)

*An Opus reviewer, read-only, given the diff `claude/step-6b-slice-2..HEAD -- core/src/main` of the scratch branch
(at `e0b6…`, before the value form was asked for lists typed `Number`), AGENTS.md's equality rule and the half
page; not the author's conclusions. It read the code and probed DuckDB 1.5.6 (the product's jar) directly. Its
findings, as it gave them, shortened; the author's disposition under each.*

## BLOCKER 1: date-time lists lose their instant reading where the planner now stands aside

`RootComparison.plan` returns null for lists of days and date-times by their Pure types alone. Before, they went
through `SideEqualities.pair`, which reads each side's rows through `readOnce`/`instants` (the instant cast, for a
text and for a held date-time). What replaces it: `asInstants` turns the two VALUES into instants, and
`asSameValues` attaches `sideRows(side)`, the raw side's rows with no `instants` applied. A dialect that compares
rows writes only those rows, so a text row meets a TIMESTAMP row; and two lists of texts match no branch of
`asInstants`, so they are compared as texts on every database.
- `[date(2015,4,16,14)]->filter(x|true) == [%2015-04-16T14:00:00]->filter(x|true)`: true by the rule and before;
  now true on DuckDB, an error or false on a rows database.
- `[date(2015,4,16,14)]->filter(x|true) == [date(2015,4,16,14,0)]->filter(x|true)`: now false everywhere; true by
  the rule.
- SQLite: two held date-times of one instant in two texts lose `instantOfHeld`'s one form.
Fix: the rows through `SideEqualities.instants` for days and date-times; a branch for two lists of texts; or the
planner keeps date kinds until both are done.

## BLOCKER 2: the Nil rule drops a side that raises or has an effect

`PlatformTypes.isNil(type)` is not only the written empty list's type: anything generic over `[]` binds its type
variable to Nil (`[]->toOne()`, `[]->at(0)`), and the natives that answer nothing (`print`, `println`, a temporary
table's making and dropping) are Nil too. `[]->toOne() == 1` raises in Pure and raised before; now the side is
never written and the answer is false: "a NULL where Pure raises", never accepted. It runs at the doors too.
Fix: the shortcut for a written `[]` alone; any other Nil-typed side stays evaluated (`Scalars.emptiness` of both).

## SHOULD FIX 3: the family is read off the SQL carrier even where the Pure type is known

On ClickHouse, BigQuery and Snowflake a Float literal is typed a Decimal in the SQL model, so it reads as "exact":
`let a = 1->cast(@Number); let b = 1.0->cast(@Number); $a == $b` is false on DuckDB, H2 and Postgres and `1 = 1.0`,
true, on those three. Enumerations are not read at all (one held as an INTEGER against an `Any` column of INTEGER is
"exact" against "exact"). And it is a second "never equal" classifier beside `HeldKinds`
(`MixedEncoding.kindOfValue`, `NumberFamilies.TWO_FAMILIES.canEqual`) and `SideEqualities` (`typedClass`, `kind`).
Fix: a side's kind from its Pure type where that is known; the carrier only for a side typed Number, Any or the
abstract Date; decided by `NumberFamilies.TWO_FAMILIES.canEqual`. That also replaces the private strings and the
`default -> null`.

## SHOULD FIX 4: "a literal against a column" is too broad, and tied to the SQL's shape

Any literal against any `SqlExpr.Column` is exempted; the rule's adaptation is a NUMBER literal, exact in the
column's family (`HeldKinds.readsAs`). `$x.a == '1'` with `a: Any[1]` on an INTEGER column is sent as `col = '1'`,
which DuckDB and Postgres coerce: true where the rule says false. And a `Number` property mapped to an expression is
no `Column`, so `$x.n == 0` over a DOUBLE expression would be answered false on every row (read, not run).
Fix: only a number literal that reads as the other side's family; keyed on the other side being a property read.

## SHOULD FIX 5: the compatibility doors change, unrecorded

The written-year list branch, the list arms of `asInstants` and the Nil rule run for the doors' rule too.
`[date(2015,4,16,14)]->filter(x|true) == [%2015-04-16T14:00:00]` at a door: the doors' rule (the written precision
is part of the value) says false, it was a database refusal before, and is now true.
Fix: the new arms only for lite's rule (`TWO_FAMILIES`), or the doors' date reading extended to lists; any door
change goes to the user with a register row.

## Notes

6. `heldApart`'s list branch picks emptiness by multiplicity (`Scalars.emptiness`): a to-many navigation's column
   is one value a row, and gets a list's length. Choose by the lowered value being a list.
7. `heldFamily`'s `default -> null` leaves out TIME: `%10:00:00->cast(@Any) == '10:00:00'->cast(@Any)` goes to the
   database's `=`. Make the switch exhaustive, the text carriers explicit nulls (invariant 4).
8. `heldApart` does not skip comparisons lite writes itself (no source position: a mapping's translation, a
   milestoning filter), as `HeldKinds` does. Speculative.
9. `bothWritten` patches a symptom: the cause is the let erasing the upcast. `let a = 1->cast(@Number); let b =
   $x.f->cast(@Number); $a == $b` with `f` a Float column is still `1 = f`.
10. The DuckDB refusal of a sub-query inside a list's function is sound: 1.5.6 refuses every form; the rows of the
    "same values" instruction are the only arguments DuckDB does not write; `SqlRewriter` visits a Lambda's body
    and nested queries. The comment says 1.4.4; the jar is 1.5.6.

## Found nothing in

Floats (NaN, -0.0: the list comparison agrees with the single value's); a day list against a date-time list
(protected: DuckDB itself answers such two lists true); the planner's order rule and fresh-draw refusal still run
before the early return; argument order in every date branch; each side written once in the two "never equal"
forms; Integer against Decimal by value; `heldApart` cannot fire at the doors; no SQL text in the lowering.

## Not checked by the reviewer

No Pure run end to end; Postgres, H2 and SQLite not probed (their answers in blocker 1 are read from the code);
`[]->toOne()` typed Nil read in the typer, not run; ClickHouse's portable form over a DECIMAL and a DOUBLE.
