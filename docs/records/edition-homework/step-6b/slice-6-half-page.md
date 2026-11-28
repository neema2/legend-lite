# Slice 6: an assert is the comparison it means (2026-11-28)

*The half page the plan of record asks for. Read: `testplan/DatabaseJudge.databaseVerdict`, `databaseSize`,
`databaseContains`; `lowering/SideEqualities.values`, `Pair`, `Compared`; `lowering/VerdictSql.typed`,
`typedOverValues`; `planner/SidePlanner.over`; `lowering/Lowerer.over`; `testexec/AssertRecords.ofTypedRow`.*

## What is there today

- `assertSize` is already the comparison it means: each side's ONE value bound once to a column, the verdict lowered
  BY THE PRODUCT from the Pure condition over those columns (`VerdictSql.typedOverValues`, `SidePlanner.over`).
- `assertEquals` and `assertSameElements` of plain kinds bind each side once too (`SideEqualities.values`: the
  side's rows as a common table, one list folded from them) and write the product's instruction over the two lists
  -- but DECIDE for themselves how the two are read: a date by its instant, an exact number beside a Decimal, two
  kinds never equal as "the same only where both are empty". That is the second set of decisions, beside the ones
  `Equalities` makes for `==`.
- Before that, `DatabaseJudge.databaseVerdict` gates the pair (a kind gate that answers "unjudged", a literal
  channel, "a pair with no typed form").

## The step

An assert's verdict is the product's own lowering of the comparison it means, **with each side bound once by its
typed node**: the lowering, meeting that node, writes the column that already holds its value instead of computing
the side again (`Lowerer.overSides`; the rows a database that compares rows needs are the bound rows). The sides
keep their typed trees, so what the equality reads off the query's text -- a kind under an upcast, a written
literal, a side that may hold no value -- is read as it is for `==` written in a query. Bound to named values
instead (`Lowerer.over`, enough for `assertSize`'s two Integers), those readings would be lost:
`assertEquals(1->cast(@Number), 1.0->cast(@Number))` would be the database's `=`.

`SideEqualities.boundOnce` frames it: the two common tables, each side's list (the failure's record reads it, as
before), for a side of at most one value its one value read beside the list in the same read, and the verdict the
caller's product lowering gives.

## Built and measured (the scratch branch `claude/step-6b-slice-6`, the no-Bazel loop)

- **Step A:** `assertEquals` and `assertNotEquals` of two sides of Integers, Decimals, Floats, Strings or Booleans,
  in order. Every assert place of the measure answers as on slice 5, on DuckDB, H2 and SQLite. One thing found on
  the way: a side's list folded from no rows is no list, where the product's own empty list is the empty list; one
  value against such a list was not "both empty". Bound as the empty list in that pairing.
- **Tried and taken back: days and date-times.** Two single date-times carried as their written texts
  (`date(2020, 1, 1, 10)` against `date(2020, 1, 1, 10, 0)`) are compared right by the product where they are
  written, and wrong once each is a bound column: the reading there hangs on the lowered expression's shape, not on
  the two types. To move dates, that reading must first be by type (`Equalities.asInstants` has it for lists and
  for a text beside a held date-time, not for two single texts).

## Next, in order

1. Two single date-time texts by type, then dates through this path.
2. `assertSameElements` as `hasSameElements` (both sides lists; the instruction in any order has no form for one
   value).
3. A side typed Nil (the written empty list).
4. `assertContains`: the product's `contains` has only the list form; a database that compares rows (H2 has no
   list function) needs membership over a side's rows as an instruction of its own first.
5. Then `SideEqualities.values` and the checker's kind gates are deleted where nothing reaches them.

## Not in this slice

Grids (their own family), instances, mixed lists (part 2), `assertEqWithinTolerance`, `assertTdsEquivalent` and
`assertJsonStringsEqual` (meanings of their own: the plan's 2.4).

## Later the same day (the no-Bazel loop)

- **Days and date-times moved** (after slice 5's arm for two single date-time texts): 372 assert outcomes on the
  date rows, none other than slice 5's. The full measure on the slice is line for line slice 5's.
- **`assertSameElements` of two lists moved** (the product's `hasSameElements`); its record unchanged
  ("expected the elements [2, 1, 1] in any order, got [1, 2, 2]").
- **Tried and taken back: a side typed Nil (the written empty list).** The product answers it by the other side's
  emptiness where it is written, which for a list is its length: on H2, which has no list function, the bound list
  has none and the statement was a syntax error (it passed before, by the rows). What it shows: the "same values"
  instruction has a rows form, but the other value-form readings (a list's emptiness, membership) have none. They
  need one -- an instruction each, written by the dialect over the side's rows -- before a pair that uses them can
  move. That is the same gap as `assertContains` (item 4 above), and it is one piece of work: **emptiness and
  membership as instructions with a rows form.**
