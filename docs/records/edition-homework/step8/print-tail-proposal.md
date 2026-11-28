# Step 8: the print tail, a proposal (2026-11-26; ruled by the user the same day, section 9)

*Written by the step 8 session for the orchestrator. The user's words (2026-11-26): "start 8 - I think we should
also look at toString/toCSV in the middle - for example we saw toCSV->replace() and we should do that as one call
at the end to replace newline in csv, so maybe small set up string/count functions chained should all be in java".
Nothing here is built. The reach numbers are a research subagent's, from the pinned sources (legend-pure 5.99.0,
legend-engine 4.145.0) with an approximate parser; its full report was kept in the session's scratchpad and its
tables are copied below.*

## 1. What exists today

- **One rule reads a printed expression:** `edge/PrintedRelations.side`. It matches a relation's `toString`,
  legend-engine's `toCSV` helper or a TDS's `csv`, followed by any number of `replace` calls whose arguments are
  literals. The replaces are already Java's (the user's ruling of 2026-11-24, "also do the replace in java too").
- **Two kinds of door use it:**
  - a query door, PCT channel A (`PctPlanRoute`): the print is taken off the query's last statement, the run
    computes the relation, a Java receiver prints it and applies the replaces (`PrintedRelations.lift`);
  - a body door, PCT channel B and the corpus (`UpstreamPrints.read`): each printed side outside a lambda is read
    as its own query before the body is planned, printed in Java, and put back as a String literal. What follows
    the print then runs on that literal.
- So `toCSV(...)->replace('\n', ',')` is one Java step at the end today, on all three doors.

## 2. The mechanism proposed

Widen the one rule from "a print and its replaces" to "a print and its tail":

- `Side(printed, print, tail)`: `tail` is a list of steps from one closed, sealed type in the edge. Each step is
  one function of the set below with literal arguments. `Replace` becomes its first member.
- `side(e)` walks down from the root while the call is in the set and every argument but the text is a literal,
  then asks for a print beneath. Anything else answers null and nothing is lifted, as today.
- A step's result is a String, an Integer or a Boolean. A step that does not answer a String ends the tail.
- At a query door the receiver prints, then applies the tail in order; the answer is one value of the tail's type.
- At a body door the side is replaced by a literal of the tail's type.
- The lift is taken only at a compatibility door. The product's road does not change.

It needs the value lift to be worth much (section 4): a `toString` of a scalar as a print, which is the research's
slices 8.2 and 8.3.

## 3. The closed set

Each member must be a function of text alone whose Java meaning is exactly legend-pure's, with no error case and
nothing that depends on where it runs. From legend-pure's own Java (`CompiledSupport`, `CoreHelper`, and the
interpreter's natives; file and line in the research report):

| Member | legend-pure's Java | Why it is safe |
|---|---|---|
| `replace(lit, lit)` | `String.replace` | in the rule today |
| `length()` | `String.length()` | UTF-16 units; a database counts code points, so Java is the more faithful |
| `trim()` | `String.trim()` | strips every character up to U+0020, which no database's TRIM does |
| `startsWith(lit)`, `endsWith(lit)`, `contains(lit)` | the `String` methods | exact |
| `indexOf(lit)`, `indexOf(lit, n)` | `String.indexOf` | -1 when absent; a start out of range is clamped, never an error |
| `+` with literal Strings | `joinStrings('', '', '')` | concatenation |

**A count beyond `length`** (the user's words were "string/count functions"): none. legend-pure's string functions
(`platform/pure/essential/string` and `grammar/functions/string`, 5.99.0) hold no count of matches or of
occurrences; legend-engine's additions (`chunk`, `splitIntoLines`, `makeCamelCase`, `pad` and the like) hold none
either. `length` and the two `indexOf` forms are the set's only members that answer a number. `lastIndexOf`
(`String.lastIndexOf`, by its file name; its Java was not read) would fit the same rule and can join when read.
`size` counts a list, which only `split` makes from a text, and `split` is left out below.

**Left out, each for a reason, to add only when a test needs it:**
- `toUpper`, `toLower`: legend-pure uses the JVM's default locale, so the answer depends on the machine.
- `ltrim`, `rtrim`: commons-lang's `stripStart`/`stripEnd`, a different set of characters from `trim`'s.
- `substring`: an index out of range raises, and a lift taken before the run cannot hand the raise back to the
  database.
- `split`: a `StringTokenizer` (the token is a set of characters, empty fields are dropped), and it answers a
  list, which needs `size`, `at` and `toOne` after it.
- `reverseString` (by code point), `size`, `toOne` (raises).
- `lpad`, `rpad`, `repeatString`: Pure bodies in legend-engine, not natives; `pad` truncates.
- `makeString`, `joinStrings`, `format`, `toRepresentation`: these are prints, not tails (section 5).

## 4. What it reaches (measured)

| Where | Expressions or sides holding a print | The print is last | A print, then a tail from the candidates | The print is in the middle |
|---|---|---|---|---|
| PCT channel A (81 tests) | 147 | 98 (35 tests) | 1 | 48 |
| PCT channel B (498 tests) | 574 | 564 | 1 | 9 |
| The relational corpus (764 of 2,412 tests) | 935 | 582 (421 tests) | 41 | 312 |

- **The tail reaches no test the rule of today misses.** The corpus's 41 are all
  `toCSV($result.values)->replace('\n', ',')` (the calendar aggregations), already lifted. PCT's one is a
  `joinStrings` over texts printed per row. No `length`, `trim`, `contains` or any other candidate follows a print
  anywhere in either.
- **Where the tests are:**
  - PCT channel A's 98 last-step prints are all of scalars: `toString` 48 (Float 14, DateTime 13, StrictDate 6,
    others 15), `format` 42, `joinStrings` 8. None is lifted today; the edge's SQL writes each.
  - The corpus's 312 mid-chain sides: 204 are a row's cells joined inside a `map` (`TDSNull`'s shape, ruled a row
    of legend-engine's table on 2026-11-26), 54 a `toString` joined to a computed value, 46 a `toString` inside
    a list.
- The method misses asserts inside helper functions (151 corpus tests have none in their own body) and prints
  reached through user functions.

## 5. What stays in the database

- A function outside the set, or one of the set with an argument that is not a literal.
- A print inside a lambda (per row), or one that feeds a filter, a join, a group, a sort or an `extend`.
- A print of a relation whose columns the compiler does not know.
- At legend-engine's door, a scalar's `toString` inside a store query: the database's cast is legend-engine's
  answer (the ruling of 2026-11-26). Only a relation's print is lifted there, as today.

## 6. The guards

- The set is one sealed type; a test lists its members exactly, so a member is added with a dated note or not at
  all.
- `WriterCarriageGuardTest` and `JavaEvalLedgerTest` register the tail's Java, dated, citing the user's words.
- A parity test: each member's Java answer against lite's own SQL for the same function, on plain texts, on DuckDB,
  H2 and SQLite, so the lift moves no answer on ordinary input; where they differ (a non-ASCII length) the case is
  listed.
- `PrintedRelationsTest` and `UpstreamPrintsTest`: a call outside the set, a computed argument and a print in a
  lambda are each left as written.

## 7. Recommendation, and the two questions it raises

1. **Build the tail with the value lift, not before it.** Alone it is about 100 lines and moves no test. With the
   value lift it answers the objection that removed S6a ("two printers at one door"): `x->toString()->length()` is
   then Java's all the way, for every function of the set.
2. **Is a last-step `format` or `toRepresentation` a print the door may lift?** 42 of channel A's 98 are `format`.
   Lifting one means Java writes `format`'s text from typed operands (the research's question 2). It is also what
   decides slice 1's leftover: if a `%r` of a Float at a last step is Java's, the one used further can be lite's
   own text, and the SQL composition of legend-pure's Float print leaves the edge too, with `FLOOR_RAW` and its 12
   arms. That moves answers (`format('%r', [0.1 + 0.2])` used further: `0.3`, not `0.30000000000000004`), so it is
   the user's to rule.
3. **The corpus's per-row joins** are not a tail and are not touched by this proposal.

## 8. The orchestrator's answers (2026-11-26)

- The tail is built with the scalar value lift (slices 8.2 and 8.3), not before it.
- The set as proposed goes to the user.
- A last-step `format` or `toRepresentation` may be lifted with no further ruling, provided its answer at a last step
  is the same text as today's; every test whose answer moves is listed for the user. Start with what moves nothing.
- A print used further is lite's own text (tenet 1 as amended): the answers that would move are measured first.

## 9. The user's ruling (2026-11-26, about 13:57, relayed by the orchestrator)

On the set's seven members (`replace`, `length`, `trim`, `startsWith`, `endsWith`, `contains`, `indexOf`), put to
them as needing a guard exception because Java computes a length, an index or a true/false over text the edge
printed: **"yes should add guard"**. `+` with literal Strings stays out until its typed shape is read. Built as
`slice2-value-lift-and-tail.md` records.
