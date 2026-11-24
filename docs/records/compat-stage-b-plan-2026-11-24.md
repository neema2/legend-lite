# Stage B after item 3: the order, and the one equality (2026-11-24)

The plan of record for the compatibility program's stage B once item 3 (assert verdicts as typed records, PR #132)
lands. Written at the user's request ("write this plan down so we don't forget", 2026-11-24). Ruled: the order (below)
and the equality's semantics -- strict typing everywhere (the user, 2026-11-24: "Either all numbers by value or all
numbers by type and value ... if we say yes then we should actually be strict (same with timestamp == date)", then, on
strict typing including ordering, "I like it" and "Love it").

## Why the order is what it is

- **The cause of item 3's long tail.** Pure's equality is implemented three times: the product's lowering
  (`lowering/InstanceEquality`, `NullSemantics`, `Scalars`' rules, `compiler/spec/typed/VariantEquality`), the database
  judge (`testplan/DatabaseJudge`, `JudgmentFacts`, `lowering/VerdictSql`) and the host judge (`testexec/Equality`,
  `HostJudgments`). Every audit finding in Variants, enumerations against Strings and days against date-times was two of
  them disagreeing (the design review, 2026-11-23). One equality removes the cause; until then `VerdictSql` is frozen
  (shrink-only pin) and item 3 takes no new behaviour.
- **The measurement** (2026-11-23, the progress record's last section): 312 of 694 assert bodies answer differently as
  their Pure bodies over today's `==` (about 170 meaning, 104 product-road gaps, 38 assert gaps); under the rewritten form
  the two judges disagree on 1 to 3 bodies a database instead of about 50. Size: about +1.4k to 2.3k lines in lowering,
  -2.5k to 3k (VerdictSql's comparison layer, `JudgmentFacts.typedPair`, DatabaseJudge's equality planning), 3 to 5 slices.
- **Equality waits for B3's first two steps, not in parallel** (the user, 2026-11-24: "we do b3 steps 1 and 2 and then
  equality as step 3 - and b2 in parallel"): the equality and B3's host rulebook and referee work edit the same judge
  files; the equality's change to the core's `==` needs legend-engine's door to rewrite `==` back to the database's
  comparison first, or the corpus moves; and with one host rulebook first the equality changes one set of host rules.

## The order

0. **Item 3 lands (PR #132), frozen.** Only CI red, or a regression the last commit introduced against main or
   f9e74256, blocks; any other finding becomes a register row or goes to step 3. Then CI green on the head, the warm
   lanes on the head, one narrow review of the last commit under the same rule, the PR marked ready; merged on the
   user's word.
1. **B3 step 1: one host rulebook.** Compatibility runs the host judge alone, with one set of rules (the mixed-run
   hazard, ruled option 4 on the condition "as long as we actually get to b3"). No answer moves.
2. **B3 step 2: the doors.**
   - legend-engine's door rewrites at planning: the corpus's held appeal (`testToSqlGenerationFirstDayOfWeek`), and `==`,
     `!=`, `in` and `contains` mapped to a named operation, the database's comparison (today's lowering, kept), list `==`
     refused as legend-engine refuses it (`pureToSQLQuery.pure` `processEquals`, :8363-8445). Neutral when it lands: the
     core's `==` is still the database's.
   - The referee and appeal runs out of the test plans into the edge: the SQL-text and plan-text goldens' Java join
     (Q-S7a's carve-out), `Appeal.contract`, `EngineTextJudgment.decline`, `Failure.unjudgedReason`.
3. **B3 step 3: one equality** (the design below), in 3 to 5 slices: the core's `==` by the rules below; `!=`, `in`,
   `contains`, `indexOf`, `removeDuplicates`, `distinct` derived from it; the asserts planned as their Pure bodies
   (`assertEquals(e, a)` is `assert(e == a)`, the record still carrying both sides); `VerdictSql`'s comparison layer
   deleted; the host judge kept as tenet 5's independent second implementation of the same rules; the register rows
   amended as the behaviour lands (S127, S169's column rule, S399's product line).
- **B2, in parallel on its own branch:** legend-pure's and legend-engine's printed forms at the edge from typed rows;
  S398 retired (legend-pure's assert words written at the edge from item 3's records); the product's whole Float printed
  `2.0` (Q10 amended 2026-11-23, "I think we do it as 2.0"). It shares only the assert words with the judges: B2 adds the
  edge's spelling and never changes the records.

## The one equality: the design

Equality is by value within a comparison class; across classes it is never equal. The types are known when the query
is compiled, so a cross-class `==` is a constant `false` (or "both empty" where a side may be empty) with no SQL -- the
fold the rules of record allow ("facts fixed by the types", 2026-11-19).

1. **Classes from the types** (decisions 1 and 2 set the boundaries): String; Boolean; each enumeration (decision 3);
   numbers (decision 1); a day; a date-time; each partial-date kind; Variant (S401: refused where only identity
   decides); a class's instances.
2. **Same class:** the database compares the values, null-safe -- a date-time by its instant and a Decimal by its value
   (decision 4).
3. **Classes known only when it runs** (`Any`, `Number`, the abstract `Date`): the class rides as a tag beside the value
   (`SqlExpr.Tagged`); where the plan cannot carry it, refused by name. No JSON boxing.
4. **Lists:** the same length, element by element in order, one portable row-based form (H2 has no list functions).
5. **Objects:** a keyed class by its keys, each key by these rules; a keyless one by identity where the plan can decide
   it, else refused by name.
6. **Everything else derives from this `==`**, as in Pure, which removes today's contradictions
   (`[1, 2]->contains(1.0)` true while `1.0->in([1, 2])` is false; `indexOf` against `contains` for an enumeration).

## The decisions

| # | The comparison | legend-pure | legend-engine and the databases | Lite today | Ruling |
|---|---|---|---|---|---|
| 1 | numbers of different kinds, `1 == 1.0`, `1D == 1` | interpreted: false, false; compiled: false, true | true, true | product true; asserts false (S127) | **ruled 2026-11-24: by type and value -- a compile error where the types are known, unequal at run time** |
| 2 | a day against a date-time, `%2020-01-02 == %2020-01-02T00:00` | false | DuckDB, H2, Postgres true; SQLite false | product splits by database; asserts false (S399) | **ruled 2026-11-24: a compile error where the types are known, unequal at run time** |
| 3 | an enumeration against a String, a literal or a column | false | engine compares a column's stored value: true when it matches | literal false (S169), column true | **ruled 2026-11-24: never equal in the core (a compile error where the types are known); legend-engine's door keeps engine's true** |
| 4 | S399 (date-times by instant, not written precision) and S139 (Decimals by value, not scale) | precision and scale are part of the value | by instant, by value | asserts follow both; the product's `==` follows Pure for date-time literals and Decimal keys | **ruled 2026-11-24: keep both -- what "the same value" means within one type** |

**The ruling (2026-11-24), decided as a tenet, not by matching anyone's answers** (the user: "Checking ours or pure's
answers is definitely not the way we should settle it ... We just should decide our tenets"): **two values are equal
only when they have the same type and the same value; a value is what it denotes, not how it was written.** G3 says it
already: "The product path answers TYPED values, matched by type and value." It extends S127 (the asserts' number kinds)
to the product, so no earlier ruling reverses.

What strict means, all of it:

1. **Types known when the query is compiled** (literals, columns, parameters, computed expressions -- almost all code):
   comparing two different types is a **compile error**, never a silent `false` -- `==`, `!=`, `in`, `contains`,
   `indexOf`, the deduplications, joins and filters; Integer, Float and Decimal each a type; a day and a date-time; an
   enumeration and a String; two enumerations; two unrelated classes. The message names the fix (`1000.0` for `1000`,
   `toFloat()`). Invariant 4: the compiler catches the mistake. A column's type is its property's declared type and its
   store column's (a `Number` property mapped to a BIGINT column holds Integers), so it is known at planning.
2. **Types known only at run time** (`Any`, `Number`, the abstract `Date`, a generic position, a union of mappings whose
   columns differ): the type rides beside the value -- the existing tagged value and tagged list (`SqlExpr.Tagged`,
   `SqlExpr.TaggedList`: a tag, one typed channel per type, a comparable channel) -- and equality is the same tag and the
   same value in its own channel. `1` and `1.0` in a `Number[*]` are two values (`distinct` keeps both, `contains(1.0)`
   is false). `TaggedLists.refuseCrossingNumbers`' refusals become answers: comparing the tag removes their reason.
3. **Ordering is held to the same rule:** `<`, `max`, `min`, `sort` across statically different types are a compile
   error; a run-time-mixed list orders by its comparable (numbers a DOUBLE, dates a TIMESTAMP; a day at its midnight),
   then by type, so exactly one of `<`, `==`, `>` holds. A caveat to measure: the DOUBLE comparable ties two different
   numbers beyond 2^53 (consistent, not exact at the extremes; a DECIMAL comparable where the database has one).
4. **Within one type, value not representation:** a Decimal by its number, not its scale (S139); a date-time by its
   instant, not its written digits (S399).
5. **Arithmetic keeps its typed promotion:** `1 + 1.5` is a Float; computing a value of a defined type is not comparing.
6. **Classes and polymorphism, the same rule:** the class is the type. Two classes with no subtype relation either way
   are a compile error. Related classes (a `Person` position that may hold an `Employee`) compare the run-time class
   first (from the literal, or the mapping's set implementation for a store row, carried as a tag), then the keys for a
   keyed class or the identity for a keyless one (refused by name where the plan cannot decide it): an `Employee` and a
   `Person` with the same keys are not equal; to compare entities across a hierarchy, compare their keys. Generics
   follow their arguments (`Pair<Integer, Float>` against `Pair<Integer, Integer>` is a compile error; `List<Number>`
   against `List<Integer>` compares at run time).
7. **The doors carry upstream's looseness:** legend-pure's door rewrites a cross-type `==` to `false` (Pure's answer),
   legend-engine's to the database's comparison; upstream programs still compile, lite's own do not.
8. **Implementable on every database:** the static case sends no SQL (a compile error) or one SQL type's `=` (single-
   typed lists keep the native list functions, DuckDB's `list_contains` and the rest); the run-time case reuses the tagged
   list's row forms, which already run on every lane. H2's missing list functions are a gap with or without this (step
   3's portable list form).
9. **Its cost:** lite's own code and tests that mix number literals in comparisons stop compiling; step 3 measures how
   many before it lands. A guard: a property test that exactly one of `<`, `==`, `>` holds for every comparable pair, on
   every database.

## What each audience's door does

- **legend-engine:** one door rewrite (step 2) turns the core's `==`, `!=`, `in` and `contains` back into the database's
  comparison and refuses list `==`; enumeration columns keep `EnumSourceValues`' engine-matching comparison. Open: ask the
  engine runner whether legend-engine's plan executor answers `1.5 == 1.5D` false where every database says true (read
  from S127, never asked).
- **legend-pure:** each move of the core toward Pure retires a recorded difference. Lite's deliberate departures stay
  register rows answered lite's way (tenet 3: no recomputing in Java at the edge), unless a door rewrite the database
  computes gives Pure's answer (a static kind fold is one). Before step 3: list the PCT tests whose expected answers lean
  on compiled Pure's `1D == 1` being true.

## Also open

- The gaps item 3 recorded, which step 3 closes or re-records: S401's relation-level Variant comparisons and an `Any`
  from a variable or a stored property; S399's let-bound list of days and date-times; S169's enumeration carried through
  an `Any` side holding other kinds; the database judge's typed forms for enumeration and Variant membership; H2's
  JSON-null record (awaiting its ruling).
