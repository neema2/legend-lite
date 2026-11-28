# The compatibility program's rules of record (2026-11-19)

**One page. Every design, plan, brief and review in this program is checked against it first.** A proposal that breaks
a rule here stops and goes to the user; it is never argued again inside a slice. Written at the user's request
("Maybe we need to write these down in a consolidated way/doc?", ~15:20 UTC), because the rules were on record in
pieces and this program relitigated them.

## The four rules (the user's words, 2026-11-19 ~15:20 UTC, verbatim)

1. **"Database calculates everything (there is some compiler fold rules for constant string and boolean) including
   format and tostring"**
2. **"Presentation is thin layer - in product it does not get to do anything but just serialize values correctly"**
3. **"Presentation is compatibility does all the pure or engine fixes in java it's allowed to do those in java to match
   pure/engine upstream but only from output of typed values from lite"**
4. **"Host judge is only judge in compatibility, database and host both in product with carve out that host is allowed
   to compare"**

And, the same afternoon: **"legend lite tenet is java orchestrates and database does the work, upstream is not our
implementation guide"** (~15:10) and **"everything must be pushed to database"** (~15:05).

## The six tenets (adopted by the user, 2026-11-19 ~16:40 UTC: "Adopt tenets yes")

Each has a test, so it cannot quietly erode.

1. **The database computes.** In the PRODUCT, every value a program produces is computed by the database: numbers,
   `toString()` and `format()` text, an error's values, product verdicts. *Test:* could this value differ from one row
   to the next? Then the database computes it. In COMPATIBILITY, how a value is written for legend-pure or
   legend-engine is presentation, done in Java at the edge from the typed value the database computed, wherever it can
   be; the database writes lite's own text only where the program goes on computing with it (a `toString()` in the
   middle of a chain). **AMENDED by the user, 2026-11-25:** "Tenet 1 should say for the PRODUCT, database computes every value in database including toString(), for compatibility everything presentation that can be should be done at the edge as much as possible (excluding toString in middle of chain for example)". With it, rule zero of the edition design (v3): a
   mode changes the SQL only when the difference changes what the database returns (rows, whether an error happens, a
   value the query goes on computing with) and Java cannot fix it afterwards.
2. **Java orchestrates and decides nothing about a value.** It plans, sends the statements, carries typed results and
   serializes them; product presentation never chooses, rounds, reorders or rewords. *Test:* does this Java change what
   a value is, or only how it is written on the wire?
3. **Compatibility re-spells, never re-computes.** At the edge, Java may change how lite's typed output is written to
   match legend-pure or legend-engine. If upstream's value differs, that is a meaning difference: answered Pure's way
   and recorded. *Test:* would this fix change the answer for some input, not just its spelling?
4. **Upstream is the spec of the answer, never the design.** *Test:* is the reason for a design "upstream does it that
   way"? Then stop. Where a tenet and upstream fidelity conflict, the tenet wins and the difference is recorded.
5. **Two judges in the product, one in compatibility.** In the product the database judge and the host judge implement
   the same rules independently and must agree; the host judge is the one Java comparison allowed, because its
   independence catches bugs. Compatibility uses the host judge alone.
6. **No modes in the core.** The core never branches on who is asking; differences between audiences live only at the
   door (input) and in the presenters (output).

**Two readings ruled the same minute:**
- **Folds** ("Folds okay"): the compiler may fold **constants and facts fixed by the types, never row data**. That
  covers constant text and Boolean, Integer constants (including `substring`'s position arithmetic), a list's size
  fixed by its type, and a query's column layout worked out from constants. Anything that could differ between rows is
  the database's.
- **Error records** ("Error records yes"): a raised error's record (its kind, its place and the program's own constants,
  and later the database's typed values) is lite's typed output, so compatibility may word upstream's sentence in Java
  from it (tenet 3).

## The switch rule (the user, 2026-11-19 ~20:35: "Agree with the decision 2 and 3"; tenet 6 made precise)

The user's words that started it (~19:20): "I still like the FLAG concept for very narrow use cases - it like feature
flags that we are going to want anyway for turning on/off features / We would have to very tightly guard how many
flags so doesn't become a mess". Ruled in its strict, door-only form:

- **Only door switches.** A switch is read in exactly one place, the door a request comes in through, and becomes a
  rewrite of the request's program or a refusal before the planner sees anything. **Nothing in the core reads a
  switch:** not the compiler, the planner, the lowering, the SQL writers, exec or the judges.
- **One registry, a closed list:** each switch's name, values, default, owner, the one place that reads it, why it
  exists, and when it goes away.
- **A ceiling** on how many switches exist, starting at 5, moved only on the user's word with a dated note.
- **The guards:** an architecture test fails the build if anything but a switch's registered reader reads it; every
  switch is tested in every position; a switch past its removal date fails the build until it is removed or the user
  re-dates it.
- **No speed-only switches inside the core** until a real need is brought to the user.
- **The first entry, deleted:** `LEGEND_LITE_PURE_MEANING` (`refuse` the default, `warn`, `engine`), read at the
  legend-engine door, was deleted on 2026-11-20 by the user's ruling ~07:55, "Delete the settings, make engine only mode
  for engine for everything" (claude/compat-program 25ede64f §1). legend-engine's door now always answers legend-engine's
  meaning, on the databases where it is checked, and refuses by name elsewhere. The registry stays, empty, with its
  ceiling and guards.
- **Today's exception, to be removed:** legend-engine's own feature flags that lite's core still reads (the members of
  `platform.Feature` consumed in the lowering) move to the door or are deleted in B5; the registry and its guards are
  B5's lasting guard.
- **AMENDED 2026-11-25, the edition** (the user: "Edition seems best of all options", "Yes to all the amendments";
  `docs/records/edition-design-2026-11-25.md` §11). "Nothing in the core reads a switch" now reads: switches are read at
  the door; the one exception is the edition, a property of code (`lite`, `pure`, `engine`; per project, set by the
  admitting door until projects declare one), read only by the typer's edition package (`compiler.spec.edition`), which
  turns it into ordinary typed calls, so nothing after the typer can see it. It is the registry's first and only
  permanent entry: three values, default `lite`, owner the compatibility program, reader the typer's one package, no
  removal date (the removal-date guard exempts it alone). Tenet 6 gains the same one exception: in the first version
  the door sets the edition, so for now it is effectively who is asking, which is why the tenet is amended rather than
  reread. The 2026-11-20 ruling stands with a wider reach: legend-engine's door gives the `engine` edition, which now
  covers function bodies, mappings and library code, not only the query. B5's goal becomes "the switch counts read one,
  the edition". `platform.Feature` leaves the lowering: a flag that changes meaning becomes an input to an `engine`
  edition row read by the typer, a flag written in a `lite` program is refused by name, and the rest is deleted or moves
  to a presenter (the design's §10c).

## What they mean, concretely

| Rule | In the product (lite's own route) | In compatibility (the legend-pure and legend-engine routes) |
|---|---|---|
| 1. The database computes | every value, including a program's `toString()` and `format()` and an error's message with its values. The only computation before the database is the compiler's folding of constant String and Boolean expressions | compatibility asks lite for typed values and never computes a value itself; how a value is WRITTEN for upstream (a Float's or a DateTime's text, a list's, `TDSNull`) is presentation in Java at the edge wherever it can be, the database writing lite's text only where the program computes with it further (AMENDED 2026-11-25, the user) |
| 2 and 3. Presentation | thin: it serializes typed values correctly (JSON, CSV, Arrow) and decides nothing | in Java, at the edge: every fix that makes the output match legend-pure or legend-engine (printed tables and values, error sentences, envelopes), **working only from lite's typed output** |
| 4. Judges | both: the database judge (the database computes the verdict) and the host judge (Java, allowed to compare typed rows) | the host judge only |
| Upstream | defines what the answer is, never how lite computes it | the same |

## Where these were already on record (so they are not new)

- **The host judge in Java:** `docs/GOALS_OF_RECORD_2026_11_07.md` G2 ("The host judge compares typed rows in Java
  (ruled: correct in Java)") and its §3 ruling of 2026-11-07 ("The host judge is correct to be in java").
- **Typed values on the product path, text at the edge:** G3 ("The product path answers TYPED values ... a value's text
  is an edge concern").
- **Java at the edges for compatibility:** G6 and the 2026-11-07 ruling ("If at the edges of compatibility we need java
  code to do small formatting things so that our product SQL doesn't get super bad then that's okay but it should be
  ... confined to the edges and be in one place").
- **"Fix up later in Java":** the presentation program's goal, the user's words of 2026-11-18 ("mostly everything that
  could be would really be at the edge edge mostly flexing the product and then trying to fix up later in java instead
  of parallel tracks for product and compatibility"), `docs/records/presentation-program-design-2026-11-18.md` §1 on
  `claude/presentation-design` ("Presentation at a root or a side is always Java").
- **Java orchestrates, the database executes:** AGENTS.md's execution tenet.

**Written down for the first time today:** the host judge is the ONLY judge in compatibility; the compiler's folds are
for constant String and Boolean; `format()` and `toString()` are the database's in the product; compatibility's Java
works only from lite's typed output.

## The equality rule: strict typing (the user, 2026-11-24, "Love it")

Two values are equal only when they have the same type and the same value; a value is what it denotes, not how it was
written. Different types never compare: a compile error where the types are known, the type carried beside the value
where it is not; ordering holds to the same rule; within one type a Decimal compares by its number (S139) and a
date-time by its instant (S399); arithmetic keeps its typed promotion; upstream's looseness lives at the doors. Decided as
a tenet ("We just should decide our tenets"), not by matching anyone's answers. The full statement: AGENTS.md ("The
equality rule"); the design and its cost: `docs/records/compat-stage-b-plan-2026-11-24.md`. Built by B3 step 3.

## How the program's plan stands against them (checked 2026-11-19 ~15:25)

- **Stage A:** A1 (the nets), A2 (legend-engine's SQL spelling deleted), A3 (`substring`/`indexOf` computed Pure's way
  in SQL; the detector reads the query's types at planning) and A4 (the database judge leaves compatibility: under
  rule 4 compatibility runs the host judge alone) all hold.
- **Stage B:** B1 (error records: the database raises; compatibility words upstream's sentence in Java from the typed
  record), B2 (compatibility's printed tables and values in Java from lite's typed rows), B3 (the host judge alone in
  compatibility; the corpus door's rewrites at planning), B4 and B5 all hold. **Restored:** this session withdrew the
  Java formatters at ~15:10 by misreading the correction; rule 3 allows them.
- **The one call that broke a rule, reversed:** the AdjustBy date literal answered as a plan constant instead of being
  sent to the database (a fold beyond rule 1's String and Boolean). Option 2 keeps it in the database.
- **A product gap the rules name, now ruled (the user, 2026-11-19 ~18:05, "Agree with all four table printing
  recommendations"):** lite's own route will print a relation's `toString()` in the database (rule 1), replacing
  today's refusal; a whole Float prints `2.0`; a date with a time prints lite's standard `...000000Z`; it lands with
  B2, so compatibility prints tables in Java from typed rows (rule 3). Measured first (H14): 390 of PCT's 423 table
  tests pass on DuckDB this way, every database printing the same text.
- **Compatibility's table text (the user, ~18:12, "Yes record it"):** where a relation's `toString` is the program's
  last step, the legend-pure route takes it over (the database returns typed rows, Java prints legend-pure's text);
  where the text is used further in the query, the database computes lite's own text and the difference is recorded.
  No SQL is kept for legend-pure's spelling.

## The core follows Pure; compatibility lives at the edge (the user, 2026-11-22; amends the upstream-fidelity tenet)

The user's words, verbatim: "compatibility should be able to emulate upstream pure and engine at the edges and that the
lite core should try to stick as close to pure as possible so that compatibility with pure is as small as possible";
"lite really should be as clean as possible bc everything works in typed values - so for semantics it should be as
close to pure faithful when there is a decision to be made but compatibility with pure and engine should be at edges
when doesnt make sense to copy pure (since we like to push everything to database for product)". On the wording below,
with its yardstick and its clause on Pure's artifacts: "Yes good".

> **lite's core follows Pure; compatibility lives at the edge.** The core takes Pure's meaning whenever there is a
> choice, so the compatibility layer for legend-pure stays as small as possible. Where Pure's answer is an artifact of
> its implementation rather than its design (two separately built Variants are never equal: Pure compares them by
> identity), the core may depart for a stated reason, as a register row the user rules. The core computes in the
> database on typed values: where matching Pure exactly would need disproportionate SQL for a rare or cosmetic
> difference, the core takes the database's native typed answer and records it -- a register row, and a known-quirks
> table the both-judges runs read, which grows only by the user's word. The yardstick: a cast or one native function
> per value is proportionate (stay faithful to Pure); a CASE tree per dialect, a subquery per value or a table function
> is disproportionate (take the database's answer, record it). A difference that flips a verdict on ordinary inputs,
> loses data, or returns NULL where Pure raises is never accepted for cost. Presentation in the core is one product
> spelling per type: for text the program computes, written by the database's cheapest native expression; for typed
> values it returns, by the result writer, which spells each type once and never changes a value. legend-pure's and
> legend-engine's spellings, and legend-engine's departures from Pure, belong to the edge: a door rewrite before the
> planner or a presenter after it, in Java as needed.

Why the core follows Pure rather than the database (the reasoning the user accepted): the edge rewrites a program
before the planner and re-spells an answer after the run, but it never re-computes a value inside the database
(tenet 3 of the six). A meaning the core took from a database instead of from Pure would therefore reach compatibility
users with no way to fix it at the edge; a core that follows Pure keeps the compatibility gap small by construction.

First applications (2026-11-22): S400's casts retired (a list of Decimals takes its database's own scale -- Postgres,
H2 and MariaDB keep each element's, as Pure does; DuckDB, SQLite's JSON list, ClickHouse, Snowflake and BigQuery give an
array one type natively -- recorded); a Variant compared by the database's native typed equality (DuckDB's VARIANT, Postgres's
jsonb), DuckDB's `true = 1` recorded -- the Variant application since replaced by S401 (ruled by the user 2026-11-23:
Pure compares two Variants by identity, which no database computes, so lite refuses that comparison by name and answers
a Variant against another type false). To build: the known-quirks table.
