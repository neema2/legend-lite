# MappingResolver refactor — handoff for next session

## Current commits (in order)

| SHA | What |
|-----|------|
| `b80d8b8` | **Phase 1a** — Extracted `HirRewriter` kernel from `UserCallInliner`. Pure refactor, behavior-equivalent. Identity-preserving tree walker with `Scope` (scalar+lambda bindings) and `Hooks` interface. |
| `41c2912` | **Phase 1b.i** — Added `HirRewriter.alphaRename()` static utility + `renameBinder` hook + 8 unit tests. Capture-avoiding bound-variable renaming. |
| `<recent>` | **Phase 0b** — `MappingResolverParityTest` with 9 byte-identical SQL snapshots covering simple_relational, filter_local, project_and_sort, association_one_hop, association_two_hop, m2m_chain, extend_scalar, limit_only, multi_op_chain. Run with `-DupdateParitySnapshots=true` to (re)write. |

All three commits keep the baseline broken-test count at exactly **158** (69 failures + 89 errors), matching `progress/baseline-failures.txt`.

## What was attempted and reverted (Option C1)

**Goal**: splice `TypedGetAll` into the user HIR at MR time so no `TypedGetAll` survives MR. Keep `StoreResolution` sidecar authoritative for now.

**Approach**: `MappingResolver.resolve()` runs an `HirRewriter` pass first whose `rewriteGetAll` hook returns the compiled mapping body, recursively splicing inner `TypedGetAll`s for M2M chains.

**Why it failed**: the kernel rebuilds compound parent nodes whose children change (the standard tree-rewrite contract). For M2M synth bodies like `Raw.all()->extend(~fullName : ...)`, splicing `Raw.all()` triggers a rebuild of the parent `TypedExtend`. The new `TypedExtend` is a different Java object — and the sidecar (`IdentityHashMap<TypedSpec, StoreResolution>`) loses its stamp. `ExtendLowering` then can't find the M2M extension's class store, and the M2M extend gets dropped from output.

Concrete failure: `RelationalMappingCompositionTest$M2MWithAssociation.m2mLocalOnly` produced
```
SELECT "t1"."fullName" AS "name" FROM (SELECT "t0".*, "t0"."FIRST" AS "first", ...) AS "t1"
```
where `t1.fullName` doesn't exist because the `~fullName` extend was lost.

121 tests regressed.

## The lesson

Splicing-while-keeping-the-sidecar is fundamentally dual-world and breaks at every rebuild boundary. To keep the sidecar working, every rebuild needs stamp-copying — which is bookkeeping pollution and undermines the whole reason to splice.

**Do not retry C1 in this shape.** Either:

1. **Pick C2-style migration** (additive): start by moving one piece of info from the sidecar onto the AST (e.g., `physicalColumn` on `TypedPropertyAccess`). MR populates the new field by reading the sidecar one last time. Lowering reads from the field. Sidecar still exists for everything else, shrinks over time. No splicing, no rebuild gotcha. Each piece of info migrated = one shippable commit.

2. **Pick true big-bang C** (target): redesign so the HIR carries everything lowering needs (physicalColumn on TypedPropertyAccess, explicit TypedJoin nodes for associations, etc.), THEN splice TypedGetAll, THEN delete the sidecar — all in one go, validated against parity fixture. Bigger risk but cleaner end-state.

## What's solid going forward

- **`HirRewriter`** is the right kernel — α-rename works, hook system is clean, identity-preserving.
- **`UserCallInliner`** is now a thin Hooks impl on the kernel (~165 lines, was 607). Future passes plug in the same way.
- **Parity fixture** (`MappingResolverParityTest`) catches byte-level SQL drift on 9 representative shapes. Run before/after any MR change.
- **9 cases** is a starting fixture. Embedded mapping, Otherwise mapping, self-join, struct-array, graph fetch are not yet covered — add as needed.

## Recommended next-session shape

1. **Choose 1 vs 2 above.** If 1: pick the smallest sidecar field (probably `propertyToColumn`) and migrate it first.
2. **Start by adding the field to the AST node**, populating it during a tree walk in MR (not at TypeChecker time — keeps blast radius small).
3. **Update consumers** (e.g., `PropertyAccessLowering`) to read from the field.
4. **Verify against parity fixture.** Should be byte-identical.
5. **Repeat for next field.**

No more dual-world splicing attempts.

## Files of note

- `/Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/HirRewriter.java` — kernel
- `/Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/UserCallInliner.java` — model Hooks impl
- `/Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/MappingResolver.java` — the refactor target (1216 lines)
- `/Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/StoreResolution.java` — the sidecar (target for retirement)
- `/Users/neema/legend/legend-lite/engine/src/test/java/com/gs/legend/test/MappingResolverParityTest.java` — regression net
- `/Users/neema/legend/legend-lite/engine/src/test/resources/parity/*.sql` — 9 SQL snapshots
- `/Users/neema/legend/legend-lite/.windsurf/plans/mapping-resolver-as-rewrite.md` — original plan (now known to under-specify the rebuild-loses-stamps problem)

## Sidecar surface (to retire)

`StoreResolution` / `storeFor()` / `propertyToColumn` / `joins` / `columnFor` are referenced from **257 sites across 30 files** (per `grep_search`). Top consumers: `MappingResolver` (108), `ExtendLowering` (16), `GroupByAggregateLowering` (16), `JsonEnvelope` (14), `LoweringContext` (12), `CollectionLowering` (10), `PropertyAccessLowering` (9). Plan migration order around these clusters.
