# Order tracking: lite's tests are deterministic (design, 2026-11-05)

**Status:** design, not ratified. Nothing here is built yet; the first build step is a report-only measurement (§8).

## 0. The ruling this serves

The user, 2026-11-05: "I think no sort order is compatibility only bc I think our tests in lite should always be
deterministic and fail if not, that's a stricter testing policy so that we don't rely on undefined database behavior
which to me is super lazy test code - hold lite to better standard of testing." Then: "I love it, I would love to
implement this", "I do want it."

So:

- **Lite's own tests** (the `LITE` judging rules) must be deterministic. An operation whose result depends on an order
  the program never defined fails at planning. It names the operation and the fix.
- **Compatibility mode** (`COMPILED_PURE_COMPAT`: PCT channel B, the corpus) keeps register row S131: an unsorted
  query's rows compared as a multiset. S131 becomes compatibility-only.

## 1. The rule, stated once

> A value whose content or order depends on the order of a collection whose order is undefined is non-deterministic.
> In a test body under the LITE rules, the operation that reads that order is refused at planning.

The refusal belongs at the operation that reads the order, not at the assert. These are all the same mistake:

- `assertEquals('Ann', $people->at(0).name)` over an unsorted query;
- `$people->first()`;
- `$people.name->joinStrings(',')`;
- `$rel->toString()`;
- `assertEquals([...], $people.name)`.

A rule keyed on the assert would catch only the last one.

## 2. Two facts per value, with the node that caused them

Every typed expression in a test body gets an **order fact**:

| fact | values | meaning |
|---|---|---|
| `order` | `DEFINED` / `UNDEFINED` | is the sequence's order fixed by the program? |
| `contents` | `DEFINED` / `UNDEFINED` | is *which* elements it holds fixed? (`take(2)` over an undefined order: no) |
| `origin` | a typed node + its source position | where the undefinedness entered, for the error message |

`contents = UNDEFINED` is strictly worse than `order = UNDEFINED`. A multiset comparison would not rescue it either,
so it is refused even in places that ignore order (an `assertSize` is fine, but `assertSameElements` over
`$people->take(2)` is not).

A to-one value has no order. Its fact is about its contents: `$people->first()` has contents UNDEFINED.

## 3. Where undefinedness enters (sources)

Order is UNDEFINED at:

- **Store reads:** `getAll`, a table reference, `#>{db.t}#`, raw SQL, a catalog grid, an execute frame's rows when the
  frame's chain is unsorted (the frame's chain is visible through the splice, as `OrderView` already does).
- **Operations whose SQL has no order**, whatever their input:
  - `groupBy`, `aggregate`, `pivot`;
  - `join` / `asOfJoin`;
  - a relation's `concatenate` (UNION ALL).
- **To-many navigation from a store-backed instance:** `$person.addresses` where `$person` came from a store. The order
  of the mapping's join is undefined. A literal `^Person(addresses=[a, b])` is DEFINED. This needs a provenance bit:
  `STORE` or `LITERAL` per value, carried with the fact.

Order is DEFINED at:

- literals, `range`, the language's own collection constructors;
- a TDS or relation literal (`#TDS ... #`);
- a sort (but see ties, §5).

## 4. Transfer: a closed table, no default

Today `OrderView` answers DEFINED for any node it does not know. That optimistic default is the first thing to go.

- **Typed nodes.** Every `TypedSpec` variant gets an explicit case in a switch over the sealed hierarchy, so javac
  enforces exhaustiveness. There is no `default`.
- **Natives.** Every native with a collection or relation input carries an explicit **order class** in one table keyed
  by the native's catalog identity (`com.legend.builtin.Pure`). Today that is about 359 overloads of about 184 names.
  A native with **no** collection or relation parameter cannot read an order. That is decided from its signature's
  multiplicities, so it is not a default.
- **A native with a collection input and no entry** fails planning of the test body: "order class unknown for <fqn>".
  A guard test enumerates the catalog and pins that every such native has a class. Adding a native forces a decision.

The order classes:

| class | output fact | examples |
|---|---|---|
| `PRESERVES` | the input's fact, element-wise | `map`, `filter`, `project`, `extend` (no window), `rename`, `select`, `cast`, property access on to-one |
| `DESTROYS` | order UNDEFINED, contents kept | `groupBy`, `join`, a relation's `concatenate`, `pivot` |
| `DEFINES` | order DEFINED, subject to the tie rule (§5) | `sort`, `sortBy`, a relation's `sort` |
| `COMBINES` | DEFINED only if every input is DEFINED | a collection's `concatenate`, `union` |
| `INDEPENDENT` | the result does not read order | `size`, `count`, `isEmpty`, `exists`, `forAll`, `contains`, `sum`, `max`, `min`, `average`, `assertSize`, `assertSameElements`, `assertContains` |
| `READS` | refused on an UNDEFINED input; otherwise the input's fact | `at`, `first`, `last`, `head`, `tail`, `init`, `take`, `limit`, `drop`, `slice`, `indexOf`, `zip`, `reverse`, `fold`, `joinStrings`, `makeString`, `toString` of a collection or relation, `toCSV`, `removeDuplicates` (keeps first occurrences), a window function with no `ORDER BY` (`rowNumber`, `lag`, `lead`, `first`/`last`/`nth` value, running frames), an order-sensitive aggregate inside `groupBy` (`joinStrings`, `first`, `last`), JSON serialization of a to-many, `assertEquals`/`assertNotEquals`/`equal` over to-many sides |

`fold` is READS because its function is arbitrary. One that is commutative and associative (integer `plus`) could be
INDEPENDENT, but proving that is a later refinement.

`distinct` is PRESERVES in Pure (first occurrences). **The lowering must back every DEFINED claim.** Wherever the
tracker says DEFINED (a `distinct`, `filter` or `map` over a literal list or TDS), lite's SQL has to keep that order
(ordinality), not rely on DuckDB happening to keep it. §8's measurement checks this with an order-shuffling run (§8.3).

## 5. Ties

A sort whose keys tie on rows that differ leaves the order among those rows undefined. This is the same hazard as no
sort at all. Two ways to decide it:

1. **Static:** the sort's keys must cover a declared primary key of the source. This is precise only where a key is
   declared. Many test stores declare none, so it refuses sorts that are in fact total.
2. **At run time (recommended):** the plan carries a tie guard beside the sorted side. It is one predicate the database
   computes over the same rows: some two rows with equal sort keys differ in a column the assert reads. A true guard
   fails the test with the tied keys named. Test data is fixed, so the guard's answer is deterministic. It refuses
   nothing that is actually total.

The guard is a verdict-shaped plan step, so both judges read it the same way. It applies only under the LITE rules.

## 6. Where it runs

- **After G½** (`UserCallInliner.inlineBody`), over the whole test body. A helper function that does `->first()` over
  a store read is caught in the body it was inlined into.
- **Lite's test bodies only, under the LITE rules.** The test door (`TestDoor.planned`) asks for each body's facts
  before it plans. Compatibility runs skip it and keep S131.
- **Product queries are not checked.** A product query is free to return rows in arrival order; the consumer decides.
  Whether `->take(5)` over an unsorted product query should warn is an **open question** for the user (§9).
- **Lambdas:** a `map` or `filter` lambda is analysed with its parameter's fact. This catches
  `$people->map(p | $p.addresses->first())` when `$people` is store-backed.

The error, at planning:

```
the order of getAll(my::Person) (line 3) is undefined, and ->first() (line 5) reads it:
sort it by a key that is unique (->sort(...)), or compare without order (assertSameElements)
```

## 7. What changes for the judges

- **S131** moves to compatibility mode. Under the LITE rules, every comparison is positional. An unsorted side that
  reaches a positional comparison was refused at planning, so the judges never see one.
- `OrderView` is replaced by the tracker. Its four callers in the judges ask the tracker's fact instead.
- Rendered-text asserts in clean mode compare one string (the agreed 2026-11-05 design). An unsorted render is refused
  here, before any comparison exists.

## 8. Rollout

1. **Report-only measurement.** `-Dlegend.diagnostics=order-reads` prints every READS refusal the tracker would make
   in core's tests, with the test, the operation and the origin. Nothing is refused yet.
2. **Fix core's offenders.** Each one gets a sort by a unique key, or an order-free assert. This is a real fix to
   lite's tests, not a ratchet. No count of offenders gets pinned.
3. **Prove the DEFINED claims.** Run core once more with lowering told to shuffle the arrival order where lite claims
   none: an `ORDER BY random()` under every unsorted store read, behind a test switch. Any test that changes outcome
   either depends on an order the tracker missed (a tracker bug) or relies on order SQL never promised (a lowering
   bug).
4. **Enforce under the LITE rules.** Move S131 to compatibility-only (a register row edit, ruled).
5. **The tie guard** (§5) lands after step 4, measured the same way.

## 9. Open questions for the user

1. **Product queries:** should an order-reading operation over an undefined order (`->take(5)` on an unsorted query)
   be a warning, a refusal, or nothing outside tests? (Recommendation: nothing for now; tests only.)
2. **Float aggregates:** a Float `sum` over many rows is not bit-deterministic on DuckDB even *with* a defined order
   (parallel aggregation). Treat Float `sum`/`average` as INDEPENDENT with the Float comparison rule, or require
   Decimal in lite's tests for exact sums?
3. **Ties:** run-time guard (recommended) or static key coverage?

## 10. Size

- The tracker: one pass, about 400 to 600 lines, replacing `OrderView`'s 235.
- The native table: about 184 names.
- The guard test.
- The diagnostic.

It is new compiler work, not a new feature for users: the hard part is making the native table exhaustive and
honest.
