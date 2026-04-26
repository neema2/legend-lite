# Fallback Cleanup TODO (Phase C.2 and beyond)

> Per AGENTS.md invariant 4: NO FALLBACKS. NO DEFAULTING.
>
> This file tracks every fallback / null-coalescing / silent-default site
> that masks an upstream bug. Each must be removed by fixing the upstream
> source, not by tightening at the consumer.

## 1. `PropertyAccessLowering.physicalColumnFor` (HIGH severity)

**File:** `engine/src/main/java/com/gs/legend/plan/lowering/scalar/PropertyAccessLowering.java`

```java
private static String physicalColumnFor(String property, StoreResolution store) {
    if (store == null) return property;                    // FALLBACK A
    String col = store.columnFor(property);
    return col != null ? col : property;                   // FALLBACK B
}
```

**Why exists:** The codebase systemically leaks physical column names into
`TypedPropertyAccess` nodes (e.g., `TypedPropertyAccess(source, "STATUS")`
where `STATUS` is the column, not the property `status`). Sources of leaks
identified so far:

- Enum mapping synthesis (`MappingNormalizer` produces nodes referencing
  enum-keyed physical columns by name).
- Association join condition synthesis (foreign key columns embedded in
  conditions as physical names).
- Mapping `sourceSpec` body construction.

**Tightening attempt result:** Throwing surfaces ~600 errors. Each is a
real upstream bug that must be fixed first.

**Fix path:**

1. Catalog every site that constructs a `TypedPropertyAccess` whose
   `property` is a physical column name. Look in:
   - `MappingNormalizer` enum-mapping handlers
   - `MappingNormalizer.buildTraverseChain` association handling
   - Any `sourceSpec` synthesis in `PureModelBuilder`
   - `MappingResolver.lowerExtensionCol` (forwardPassthrough path)
2. For each site, replace the physical-column-named TypedPropertyAccess
   with either:
   - The property-name TypedPropertyAccess (preferred), letting
     `physicalColumnFor` map it via the store, OR
   - A direct `SqlExpr.Column(alias, physicalName)` if the site is
     producing SQL-shaped IR (not user-facing typed HIR).
3. Once leaks are gone, replace fallbacks A and B with throws.

## 2. `MappingResolver.inherit` (MEDIUM severity)

**File:** `engine/src/main/java/com/gs/legend/compiler/MappingResolver.java`

```java
private void inherit(TypedSpec node, TypedSpec source, StoreResolution active) {
    StoreResolution fromSource = resolutions.get(source);
    StoreResolution chosen = fromSource != null ? fromSource : active;  // FALLBACK
    if (chosen != null) {                                                // FALLBACK
        resolutions.put(node, chosen);
    }
}
```

**Why exists:** Some `TypedSpec` node kinds don't get stamped with a
resolution before `inherit` is called on them. Specifically inside
`TypedUserCall` body recursion, the body's resolution may not be cached
in `resolutions` yet when a containing operator runs `inherit`.

**Fix path:**

1. Audit every node kind reachable as a `source` operand of a relation
   operator. Each must produce a stamped resolution before `inherit` is
   called.
2. `TypedUserCall` walking — confirm body's resolution propagates to
   the call site.
3. Any literal / variable / control-flow node that can show up as a
   relation source — stamp it.
4. Once every relation source stamps reliably, drop both fallbacks
   and throw on null.

## 3. `MappingResolver.requireResolution` (LOW severity)

**File:** `engine/src/main/java/com/gs/legend/compiler/MappingResolver.java`

```java
private StoreResolution requireResolution(TypedSpec source, StoreResolution active) {
    StoreResolution res = resolutions.get(source);
    return res != null ? res : active;                                   // FALLBACK
}
```

**Why exists:** Same root cause as `inherit` — some sources don't get
stamped. Used by schema-changing operators (Project, GroupBy, etc.).

**Fix path:** Same as `inherit`. Once all relation sources stamp
reliably, drop the active fallback.

## 4. `WindowBindings.reducer` (LOW severity)

**File:** `engine/src/main/java/com/gs/legend/plan/lowering/natives/WindowBindings.java`

```java
private static Binding reducer(Function<SqlExpr, SqlAggregate.Reducer> ctor) {
    return args -> ctor.apply(args.isEmpty() ? null : args.get(0));      // FALLBACK
}
```

**Why exists:** The 0-arg case (count() with no operand) has its own
dedicated arm above — `Pure.COUNT__RELATION_1__WINDOW_1__T_1` arm
explicitly returns `CountStar` for empty args. The fallback in
`reducer` is defensive and unreachable in correct code paths.

**Fix path:** One-line change — replace with `throw new IllegalStateException(...)`.
Verify no test exercises this path (run full suite). Should be the
easiest of the four to remove.

## 5. Soft-handling in `MappingResolver.walk` for `TypedUserCall` (LOW severity)

**File:** `engine/src/main/java/com/gs/legend/compiler/MappingResolver.java`

```java
case TypedUserCall uc -> {
    TypedSpec body = uc.callee().body().hir();
    walk(body, active);
    StoreResolution bodyRes = resolutions.get(body);
    if (bodyRes != null) resolutions.put(node, bodyRes);                 // FALLBACK
    for (var arg : uc.args()) walk(arg, null);
}
```

**Why exists:** A user call to a function that returns a non-relation
body (scalar, void, etc.) won't have a stamped resolution. The
`if (bodyRes != null)` skips stamping for those.

**Fix path:** Distinguish at call-shape time: if the call returns a
relation, body MUST stamp; if it returns a scalar, body need not stamp.
The current code conflates both cases with a guard. Refactor to
distinguish, then throw if a relation-call's body has no stamp.

## 6. Other latent fallbacks (audit pending)

Run a sweep:

```sh
# Search for null-coalescing patterns in plan/lowering and compiler
grep -rnE '!= null \? .* : (null|active|property)' \
    engine/src/main/java/com/gs/legend/plan/lowering/ \
    engine/src/main/java/com/gs/legend/compiler/

# Search for getOrDefault, orElse with side-effect-free defaults
grep -rnE '(getOrDefault|orElse)\(' \
    engine/src/main/java/com/gs/legend/plan/lowering/ \
    engine/src/main/java/com/gs/legend/compiler/

# Search for "fallback" / "tolerate" / "default" in comments
grep -rnE 'fallback|tolerate|defaulting|legacy behavior' \
    engine/src/main/java/com/gs/legend/plan/lowering/ \
    engine/src/main/java/com/gs/legend/compiler/
```

Add findings here.

## 7. Spurious association joins in mapping sourceSpec (HIGH severity, non-fallback)

**Symptom:** `Person.all()->project(~lastName)->groupBy(...)` emits SQL
with two `LEFT JOIN T_ADDRESS` even though the query never navigates an
association. The extra rows duplicated by the joins inflate aggregates
(SUM/AVG/COUNT). Surfaced by Phase C.1 unblocking 17 tests that
previously errored on alias mismatch but now run with wrong values.

**Root cause:** Two parallel pruning mechanisms exist; only one is
hooked up.

1. `lowerExtensionCol` for `TypedAssociationExtendCol` filters by
   `neededAssocs` (the set of associations actually navigated by the
   query, populated by `TypeChecker`). This affects what's in
   `StoreResolution.joins`.
2. The actual SQL JOIN clauses come from `SourceLowering.lower(TypedGetAll)`,
   which lowers the entire synthesized `mappingFn.body()`. That body
   STILL contains all association extends and emits JOINs for all of
   them — `neededAssocs` is not consulted at this layer.
3. `StoreResolution.ExtendOverride` (with `isActive`/`isFullyCancelled`)
   exists for cancellation tracking, but `stampExtendOverrideIfNeeded`
   only considers scalar/window/traverse extends — not association or
   embedded extends. AND nothing in `plan/lowering/` reads
   `extendOverride()` to skip cancelled cols.

**Fix paths:**

- **Path A (extension):** Extend `stampExtendOverrideIfNeeded` to
  include association/embedded aliases. Then in `ExtendLowering` (or
  `SourceLowering` walking the sourceSpec body), check
  `store.extendOverride().isActive(col.alias())` and skip lowering
  cancelled cols. SQL would only emit JOINs for navigated
  associations.

- **Path B (prune body):** Pre-pass on `mappingFn.body()` before
  lowering — strip TypedAssociationExtendCol entries whose alias isn't
  in `neededAssocs`. Less plumbing but more invasive.

Either way the goal: only emit `LEFT JOIN T_ADDRESS` if the query
actually navigates `addresses` or `primaryAddress`.

## Acceptance criteria for "fallbacks gone"

- All five fallbacks above replaced with throws.
- Full test suite runs with no IllegalStateException-from-fallback failures
  (i.e., the upstream issues that motivated the fallbacks are all fixed).
- A grep for the patterns above (over plan/lowering and compiler) returns
  zero results.
