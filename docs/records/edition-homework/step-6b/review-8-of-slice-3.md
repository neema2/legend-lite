# Fifth independent review of slice 3: the last fix pass and the helper swap (2026-11-28, about 04:10 UTC)

*An Opus reviewer, read-only, given `git diff 675d1b8dc 4b77dadaf -- core/src/main` (the fourth review's fixes,
`f5b542d6c`, and the three list emissions moved onto shared helpers, `4b77dadaf`), the rule, and four things to check
by name. It read the code and ran nothing. Shortened; the author's disposition under each.*

## Findings

**1. (medium, "partly traced") A root comparison of two date-time lists changes at the doors:** the planner stands
aside for them (`RootComparison.heldAsTheValueFormReads`) with no look at the door, and the lowering's instant
reading is the product's alone, so a door would now compare the written texts where the planner's statement compared
instants. The reviewer could not check whether the planner is reached for a door's code.
- *Disposition: does not hold, read in the code.* The lowering asks the planner only for a comparison no door named
  a rule for: `Lowerer.wholeSides` and `Lowerer.wholeAnswer` both return before asking where
  `Scalars.doorsRule(...) != null`, and both doors name a rule for every `==`. A door's comparison never reached
  this class before slice 3 and does not now; its answer is its own rule's, whose lines slice 3 left as they were
  (finding "nothing found", below).

**2. The new arm (one date-time text against a computed list of such texts):** no defect. Argument order right both
ways; both declared types must be DateTime, so no day, year or abstract Date; never at a door; an optional side that
holds nothing is compacted to the empty list.

**3. (low, a suspicion, older than this pass) `instantOfText` and a zone offset:** a date-time written to the hour
WITH an offset would get `:00` appended after the offset and the cast would raise, if such a text is ever carried.
- *Disposition: recorded, not run.* A row for the measure (a date-time written to the hour with an offset), with
  slice 5's rows.

**4. (low) The fixed lambda names `_dtx`, `_dth`:** as before the swap; no capture by nesting (the source list is
outside the function's body, which reads only its own parameter); two sides using the same name are sibling scopes.
- *Disposition: left.* The rule has no fresh-name supplier (it is a static rule of two arguments, as every scalar
  rule is); the scoping the reviewer traced is why none is needed.

**5. (low) The helper swap's typing:** the texts' element is now stated VARCHAR (it had no stated type), the held
element nullable. Traced for DuckDB, H2, SQLite and MariaDB: nothing reads either. Not traced: ClickHouse,
Databricks, BigQuery (their text helpers branch on a stated type; any difference would be a cast of a text to a
text, never another value).
- *Disposition: the lanes on the final commit answer it.*

## Found nothing in

The written-`[]` ladder at the doors (the line is the one slice 2 had); the Nil-list arm (the product's rule alone);
`holdsNoEmptySlot` for sort, deduplication, take, drop, slice and concatenate (none adds a slot; a clean list joined
to an optional value goes to the default, which compacts; the default compacts whatever it does not know);
`Scalars.noElements` (the same tree as the code it replaced); layering (typed nodes only, no type inferred, no
fallback).

## Not checked by the reviewer

How H2, Postgres and SQLite write the instruction for a comparison inside a list's function that reads the row;
what the wire cast does to the new one-element list; ClickHouse, Databricks, BigQuery, MongoDB.
