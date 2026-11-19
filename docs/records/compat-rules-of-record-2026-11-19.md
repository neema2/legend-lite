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

## What they mean, concretely

| Rule | In the product (lite's own route) | In compatibility (the legend-pure and legend-engine routes) |
|---|---|---|
| 1. The database computes | every value, including a program's `toString()` and `format()` and an error's message with its values. The only computation before the database is the compiler's folding of constant String and Boolean expressions | the same: compatibility asks lite for typed values and never computes a value itself |
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
- **A product gap the rules name, not this program's:** lite's own route refuses a relation's `toString()` by name
  today; under rule 1 the database computes it.
