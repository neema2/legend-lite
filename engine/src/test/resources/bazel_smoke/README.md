# Bazel Smoke Test Fixtures

Tiny 2-project corpus that acts as the **regression canary** for every phase of the Bazel cross-project dependency work. Each phase (A: `typeFqn`, B: element serialization, C: lazy loading + validation) must keep this corpus green.

See `docs/BAZEL_IMPLEMENTATION_PLAN.md` §1.2 for how this feeds into the plan, and `docs/BAZEL_DEPENDENCY_PROPOSAL.md` §6 for the architecture.

## Layout

```
bazel_smoke/
  refdata/
    model.pure      — model-only project (2 classes, 1 enum, 1 association, 1 function)
  trading/
    model.pure      — depends on refdata (class refs, enum refs, superclass, association, function call)
    impl.pure       — Database + Mapping (depends on trading::Trade which reaches into refdata)
```

## Cross-project dependency types exercised

The trading project references refdata in **every** way a Legend project can reference another:

| Dependency kind | Where | Code |
|---|---|---|
| Property type (class) | `trading::Trade.sector` | `sector: refdata::Sector[1]` |
| Property type (enum) | `trading::Trade.rating` | `rating: refdata::Rating[0..1]` |
| Superclass | `trading::InternalTrade` | `extends refdata::Categorized` |
| Association end | `trading::TradeRegion.region` | `region: refdata::Region[0..1]` |
| Function-to-function call | `trading::tradeSummary` body | `refdata::formatSector($t.sector)` |
| Property chain through association | `trading::sectorRegionCode` | `$t.sector.region.code` — reaches across refdata's own `SectorRegion` association |
| Impl-level class ref | `trading::TradingMapping` | `trading::Trade: Relational { ... }` forces transitive resolution of `refdata::Sector` + `refdata::Rating` |

## How to use this corpus

### Phase A — `typeFqn` refactor
Build a model from both projects combined (single `PureModelBuilder`) using `addSource` on each `.pure` file, then run queries that exercise the cross-project chains. Every existing test pattern still works because Phase A doesn't change loading semantics.

### Phase B — Element Serialization
Run `ElementExtractor` against each project separately, emitting per-element JSON files into `bazel_smoke/refdata/elements/` and `bazel_smoke/trading/elements/`. Round-trip every element and verify byte equality on second pass (idempotent serialization).

### Phase C — Lazy Loading + `validateElement`
Build a `PureModelBuilder` configured with:
- `addSource` for trading's `.pure` files (current project)
- `addDepElementDir(Path.of("bazel_smoke/refdata/elements"))` for refdata (dependency)

Then:
1. Call `findClass("refdata::Sector")` → must lazy-load from disk.
2. Call `validateElement("trading::tradeSummary")` → must succeed.
3. Call `validateElement("trading::sectorRegionCode")` → must succeed (navigates `sector.region.code`).
4. Break one source file (e.g., rename `refdata::Sector.code` → `refdata::Sector.symbol`) and verify `validateElement` on `trading::sectorRegionCode` returns a precise error with source location.

## Not in scope for this corpus

- Multi-hop transitive deps (A → B → C)
- M2M mappings
- XStore cross-expression mappings

These can be added later as the corpus grows. Keep the initial set minimal so it's easy to reason about when debugging.

## Profile cross-project references (added for NameResolver FQN canonicalization)

- `refdata::RefDataProfile` is the cross-project profile
- `trading::Trade` declares `<<RefDataProfile.rootEntity>>` and `{RefDataProfile.description = '...'}` using short-name references via `import refdata::*`
- After `NameResolver` runs, both profile references must resolve to the FQN `refdata::RefDataProfile` in the stored `StereotypeApplication.profileName` / `TaggedValue.profileName` fields

## Infrastructure cross-project references (Category A Phase B prep)

`refdata/impl.pure` declares reusable infrastructure:
- `refdata::RefDB` — Database (reusable cross-project DB include)
- `refdata::RefMapping` — Mapping over RefDB (reusable cross-project mapping include)
- `refdata::RefConn` — RelationalDatabaseConnection bound to `refdata::RefDB`

`trading/impl.pure` consumes them via short-name references (relying on `import refdata::*`):
- `Database trading::TradingDB ( include RefDB ... )` — short-name DB include
- `Mapping trading::TradingMapping ( include RefMapping ... )` — short-name mapping include
- `Runtime trading::TradingRuntime { mappings: [TradingMapping]; connections: [RefDB: [c1: RefConn]] }` — short-name mapping, store key, and connection value

After `NameResolver` runs, every short-name reference above must canonicalize to its `refdata::*` or `trading::*` FQN in the stored record fields.
