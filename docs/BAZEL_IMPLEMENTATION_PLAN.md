# Legend-Lite Bazel Integration — Implementation Plan

> **Companion doc to `BAZEL_DEPENDENCY_PROPOSAL.md`.**
> The proposal explains *why* and *what*. This doc explains *how* and *in what order* — with per-file checklists, contracts, test gates, and done criteria you can actually check off.

**Scope of this plan:** the three risky engine refactors that must land before any Bazel work:

1. **Phase A** — `typeFqn` storage refactor (proposal §11 Step 2.5)
2. **Phase B** — Element serialization + `NameResolver` expansion (proposal §11 Step 2)
3. **Phase C** — Lazy element loading + `validateElement` primitive (proposal §11 Step 3)

**NOT in scope:** Bazel rules, module extensions, external repo integration, scale test, frontend multi-file editing. Those steps (proposal §11 Steps 0, 1, 4, 5, 6, 7, 8) are self-contained and keep their existing bullets in the proposal.

---

## Table of Contents

- [0. Purpose & Relationship to the Proposal](#0-purpose--relationship-to-the-proposal)
- [1. Pre-Work](#1-pre-work)
- [2. Phase A — `typeFqn` Refactor](#2-phase-a--typefqn-refactor)
- [3. Phase B — Element Serialization](#3-phase-b--element-serialization)
- [4. Phase C — Lazy Loading + `validateElement`](#4-phase-c--lazy-loading--validateelement)
- [5. Risk Register](#5-risk-register)
- [6. Execution Sequence Summary](#6-execution-sequence-summary)
- [7. Out of Scope](#7-out-of-scope)

---

## 0. Purpose & Relationship to the Proposal

The proposal doc is the **decision artifact** — it sold the plan, captured the design, and enumerated the architectural invariants. It's intentionally written for someone evaluating the approach, not someone executing it.

This doc is the **execution artifact**. It:

- Lists every call site that needs to change
- Publishes the JSON schema that Phase B produces
- Specifies the exact contracts that Phase C implements
- Gives per-phase test gates and done criteria
- Records the decisions (branch strategy, CI gating, repo layout) that every implementer needs

**Read linearly.** The phases are sequentially dependent (A unblocks B unblocks C). Don't start Phase B before Phase A is done; don't start Phase C before Phase B is done. Within a phase, the per-file checklists can be parallelized.

---

## 1. Pre-Work

Five things must exist before Phase A starts. Items 1.1 and 1.2 are done (results captured below). Items 1.3, 1.4, 1.5 are reviewed-and-frozen before any coding.

### 1.1 `typeFqn` Call-Site Inventory (DONE)

Ran targeted grep across `engine/src/main/java`:

- `\.genericType\(\)` — accesses `Property.genericType()`, which returns a resolved `Type`
- `\.superClasses\(\)` — accesses `PureClass.superClasses()`, which returns `List<PureClass>`

**Total: 14 call sites for `genericType()` + 16 for `superClasses()` = 30 sites across ~10 files.**

#### 1.1a `Property.genericType()` call sites

| File | Sites | Pattern | Migration |
|---|---|---|---|
| `compiler/TypeChecker.java` | 4 (lines 724, 856, 971, 1030) | `GenericType.fromType(prop.genericType())` | **Trivial** — swap for `GenericType.fromTypeName(prop.typeFqn())` |
| `compiler/checkers/AbstractChecker.java` | 1 (line 1124) | Same | **Trivial** |
| `compiler/MappingResolver.java` | 2 (lines 255, 565) | `.genericType() instanceof PureClass` | **Medium** — replace with `modelContext.findClass(prop.typeFqn()).isPresent()` |
| `model/PureModelBuilder.java` | 2 (lines 774, 792) | `.genericType().typeName()` | **Trivial** — `typeFqn` is already a string |
| `model/mapping/RelationalMapping.java` | 3 (lines 156, 181, 221) | Mix of `fromType` + `instanceof PrimitiveType` | **Medium** — primitive detection via `GenericType.Primitive.fromTypeName` |
| `model/mapping/PureClassMapping.java` | 1 (line 105) | `GenericType.fromType` | **Trivial** |
| `server/DiagramService.java` | 1 (line 88) | `.genericType().typeName()` | **Trivial** |

**Summary: 9 trivial sites, 5 medium sites.**

The proposal's initial estimate of "~36 files, ~200-500 lines" was a worst-case across all checker files. Reality: **checkers consume `GenericType` (already string-based), not `Property.genericType()` directly.** Actual scope is ~10 files, ~100-200 lines.

#### 1.1b `PureClass.superClasses()` call sites

| File | Sites | Pattern | Migration |
|---|---|---|---|
| `model/m3/PureClass.java` | record field + walkers + `toString` | `List<PureClass> superClasses` | **Core refactor** — field becomes `List<String> superClassFqns`; walkers take a `ClassLookup` functional interface |
| `compiler/TypeChecker.java` | 1 (line 523) | `cls.get().superClasses()` iterates resolved objects | **Medium** — iterate FQNs, `findClass` each |
| `parser/NameResolver.java` | 3 (lines 78, 93) | `classDef.superClasses()` (def layer, already `List<String>`) | **No change** — def layer is already FQN-string-based |
| `model/PureModelBuilder.java` | 3 (lines 434, 440) | `resolveSuperclasses()` loops FQNs → `PureClass` | **Medium** — becomes a no-op (lazy) or stays for eager mode |
| `model/ModelContext.java` | 2 (lines 171, 185) | Queue walks `superClasses()` | **Medium** — FQN queue with `findClass` lookup |
| `antlr/PackageableElementBuilder.java` | 1 | Builder field setter | **Trivial** |
| `parser/PureModelParser.java` | 1 | Parser field setter | **Trivial** |
| `server/DiagramService.java` | 1 (line 116) | `.superClasses()` for generalisation list | **Medium** — iterate FQNs + `findClass` |

**Summary: The `m3/PureClass` record change is the hinge — everything else follows from it.**

#### 1.1c Crucial discovery

`GenericType.fromType(m3.Type)` at `GenericType.java:481-486` only reads `typeName()` / `qualifiedName()` from the m3 `Type` — it never touches the full `PureClass` object's internal structure. That's why this refactor is viable: **the round-trip through a resolved `PureClass` object is already unnecessary.** We're deleting a redundant indirection, not redesigning the type system.

### 1.2 Smoke Test Project (DONE)

Regression canary at `engine/src/test/resources/bazel_smoke/`:

- `refdata/model.pure` — model-only project: 3 classes, 1 enum, 1 association, 1 function
- `trading/model.pure` — depends on refdata via every cross-project ref type
- `trading/impl.pure` — Database + Mapping for trading
- `README.md` — full breakdown of what each cross-project dep exercises

**Every phase keeps this corpus green.** Phase A tests it with the current whole-project loader; Phase B round-trips its elements to JSON; Phase C loads refdata's JSON files lazily from trading's compile path.

Cross-project dependency types exercised (one-to-one with the Complete Dependency Reference Matrix in proposal §6):

| Ref kind | Where | Example |
|---|---|---|
| Class-typed property | `trading::Trade.sector` | `sector: refdata::Sector[1]` |
| Enum-typed property | `trading::Trade.rating` | `rating: refdata::Rating[0..1]` |
| Superclass | `trading::InternalTrade` | `extends refdata::Categorized` |
| Association end | `trading::TradeRegion.region` | `region: refdata::Region[0..1]` |
| Function-to-function call in body | `trading::tradeSummary` | `refdata::formatSector($t.sector)` |
| Property chain through association | `trading::sectorRegionCode` | `$t.sector.region.code` |
| Impl-level transitive ref | `trading::TradingMapping` | `trading::Trade: Relational { ... }` forces loading `refdata::Sector` + `refdata::Rating` |

### 1.3 JSON Element File Schema

Published in §3.2 of this doc. **Review and freeze before writing any serializer code.** Every element file carries:

- `schemaVersion: int` — required first field
- `kind: string` — one of `class | enum | association | function | database | mapping | runtime | connection | service | profile`
- `fqn: string` — the element's fully qualified name
- `sourceLocation: object` — original file + line + column (for error reporting)
- Kind-specific payload fields

### 1.4 Worked Examples

Each element kind has one complete example in §3.2, derived from the smoke test corpus. These are the ground-truth fixtures for the round-trip test harness (§3.7).

### 1.5 Execution Decisions

| Decision | Choice | Rationale |
|---|---|---|
| **Branch strategy for Phase A** | Single feature branch, incremental commits | Phase A touches ~10 files, fits in a few PRs. Long-lived branch is simpler than cherry-picking. |
| **CI gating** | All 2100+ tests must stay green at every commit | Phase A is a behavior-preserving refactor. A broken-tests budget defeats the purpose. |
| **Adapter pattern for Phase A** | Yes — add `typeFqn` alongside `genericType`, flag-day removal at the end | Allows incremental migration with test gates between commits. |
| **Repo layout for element files (test fixtures)** | `engine/target/elements/{project}/` during tests | Keep internal during engine refactor; proposal §4 picks the production location. |
| **Thread safety for lazy loading (Phase C)** | `PureModelBuilder` stays single-threaded | LSP serializes access; Bazel actions are single-threaded per compile. |
| **Validation failure mode** | Collect all errors (don't stop at first) | Build output should show every issue in one pass. |
| **Pre-parsed M2M field fallback** | If absent, re-parse the text field | Graceful handling of element files serialized before pre-parse landed. |

---

## 2. Phase A — `typeFqn` Refactor

**Goal:** Make `Property` store type references as FQN strings (`String typeFqn`) and `PureClass` store superclasses as FQN strings (`List<String> superClassFqns`), so that no `addClass()` call requires other classes to be loaded first.

**Non-goal:** Do not wire up lazy loading yet. Phase A is a pure storage-format refactor, validated by existing tests.

### 2.1 Success Criteria

- All ~2100 engine tests pass unchanged
- `Property.genericType()` and `PureClass.superClasses()` are gone from the `m3` layer
- `Property.typeFqn()` and `PureClass.superClassFqns()` are the canonical accessors
- Model construction order no longer matters for property types — a class whose type is referenced does NOT need to exist yet when the referring class is added
- Smoke test corpus (§1.2) builds and executes a cross-project query end-to-end

### 2.2 Migration Strategy

**Adapter pattern, not flag day.**

The replacement for the resolved `Type genericType` reference is a lightweight `TypeRef` — a sealed interface with three variants (`PrimitiveRef` / `ClassRef` / `EnumRef`) that carries only the FQN string and the kind. It mirrors the existing `Type` sealed interface shape but strips the resolved object payload, so a property can describe its type without forcing the target class or enum to be loaded eagerly.

Step 1 — Define `TypeRef` and add it as a record component alongside the existing `genericType` field:

```java
public sealed interface TypeRef permits TypeRef.PrimitiveRef, TypeRef.ClassRef, TypeRef.EnumRef {
    String fqn();
    record PrimitiveRef(String fqn) implements TypeRef {}
    record ClassRef(String fqn) implements TypeRef {}
    record EnumRef(String fqn) implements TypeRef {}

    static TypeRef of(Type type) {
        return switch (type) {
            case PrimitiveType pt -> new PrimitiveRef(pt.typeName());
            case PureClass pc -> new ClassRef(pc.qualifiedName());
            case PureEnumType et -> new EnumRef(et.qualifiedName());
        };
    }
}

public record Property(
    String name,
    Type genericType,         // existing (temporary, removed at flag day)
    Multiplicity multiplicity,
    List<TaggedValue> taggedValues,
    TypeRef typeRef           // NEW — derived from genericType in compact constructor
) {
    public Property {
        // ... existing validation ...
        if (typeRef == null) {
            typeRef = TypeRef.of(genericType);
        }
    }

    /** Convenience accessor — returns typeRef().fqn(). */
    public String typeFqn() { return typeRef.fqn(); }
}
```

Both fields are populated simultaneously during construction. Legacy constructors pass `null` for `typeRef`; the compact constructor derives it.

Step 2 — Migrate each call site that reads `prop.genericType()`:
- Sites that only need the FQN string (display, logging) call `prop.typeFqn()`.
- Sites that need the kind (class vs enum vs primitive) for type checking or plan generation switch on `prop.typeRef()` or pass it to `GenericType.fromTypeRef(ref)` which preserves the enum-vs-class distinction without any `ModelContext` lookup.

Step 3 — Once every call site reads `typeRef`, delete `genericType` from the record and all legacy constructors. The redundant `Type` field disappears.

Same pattern for `PureClass.superClasses` → `PureClass.superClassFqns`, except the internal `findProperty` / `allProperties` methods need a `ClassLookup` parameter to walk the FQN chain:

```java
@FunctionalInterface
public interface ClassLookup {
    Optional<PureClass> find(String fqn);
}

public Optional<Property> findProperty(String propertyName, ClassLookup lookup) { ... }
```

Callers pass `modelContext::findClass`.

### 2.3 Per-File Checklist

Follow the order below. Each row is one commit.

| Order | File | Action | Validation |
|---|---|---|---|
| 1 | `model/m3/TypeRef.java` (new) + `model/m3/Property.java` | Define `TypeRef` sealed interface (`PrimitiveRef` / `ClassRef` / `EnumRef`); add `TypeRef typeRef` record component to `Property`, auto-populated from `genericType` in the compact constructor; add convenience `typeFqn()` method returning `typeRef().fqn()` | Existing tests pass — `typeRef` is silent until consumers migrate |
| 2 | `model/m3/PureClass.java` | Add `List<String> superClassFqns` record component; auto-populate from `superClasses`; add `ClassLookup` overloads for `findProperty` / `allProperties` / `findPropertyInSuperclasses` / `collectInheritedProperties` | Same |
| 3 | `server/DiagramService.java` | Migrate 1 `prop.genericType().typeName()` → `prop.typeFqn()` and 1 `superClasses()` walk | Diagram tests pass |
| 4 | `compiler/checkers/AbstractChecker.java` | Migrate 1 site | Checker tests pass |
| 5 | `compiler/TypeChecker.java` | Migrate 4 `genericType()` sites + 1 `superClasses()` site (use `ClassLookup`) | All 2100 tests pass |
| 6 | `model/mapping/PureClassMapping.java` | Migrate 1 `genericType` site | Mapping tests pass |
| 7 | `model/mapping/RelationalMapping.java` | Migrate 3 sites (primitive detection via `GenericType.Primitive.fromTypeName`) | Mapping + end-to-end tests pass |
| 8 | `compiler/MappingResolver.java` | Migrate 2 `instanceof PureClass` sites → `findClass(typeFqn).isPresent()` | Mapping resolution tests pass |
| 9 | `model/PureModelBuilder.java` | Migrate 2 `genericType` sites + 3 `superClasses` sites | All tests pass |
| 10 | `model/ModelContext.java` | Migrate 2 superclass queue walks to FQN-based | LCA / subtype tests pass |
| 11 | `antlr/PackageableElementBuilder.java`, `parser/PureModelParser.java` | Update builder sites | Parser tests pass |
| 11b | `test/BazelSmokeTest.java` and any other test files using `genericType()` / `superClasses()` | Migrate to `typeFqn()` / `superClassFqns()` | `mvn test -Dtest=BazelSmokeTest` passes |
| 12 | All files | **Flag day** — delete `genericType` + `superClasses` from record definitions; remove legacy constructors and overloads | All 2100 tests pass |

### 2.4 Test Gates

After each commit:

```bash
cd engine && mvn test -q
```

Must report **zero failures, zero errors, zero skipped increase**. If a test breaks, **do not proceed** — debug and fix before the next file. This is behavior-preserving work.

Incremental gate at commit 8 (MappingResolver): run mapping-heavy tests specifically:

```bash
mvn test -q -Dtest='*Mapping*,*M2M*,*Relational*'
```

Final gate at commit 12 (flag day): full suite including stress tests:

```bash
mvn test -q
```

### 2.5 Rollback Plan

Because the adapter pattern keeps both fields alive until commit 12, **any commit through 11 is independently revertable.** If commit N breaks tests that commit N-1 didn't, revert commit N and debug.

Commit 12 (flag day) is the only atomic step. If it breaks something, revert it and iterate on commit 11's state until all consumers are on `typeFqn`.

### 2.6 Done Criteria

- [ ] `grep -r "genericType()" engine/src/main/java` returns zero matches
- [ ] `grep -r "superClasses()" engine/src/main/java` returns only `NameResolver.java` and `ClassDefinition.java` (def-layer, unrelated to the `m3` refactor)
- [ ] `Property.typeRef()` and `PureClass.superClassFqns()` are the canonical accessors; `typeFqn()` remains as a convenience for FQN-only sites
- [ ] `GenericType.fromTypeRef(ref)` is the standard plan-layer conversion; `fromType(m3.Type)` and `fromTypeName(String)` are gone
- [ ] `mvn test -q` reports the exact same pass/fail counts as the pre-refactor baseline
- [ ] Smoke test corpus builds and runs a cross-project query (e.g., `Trade.all()->project(~[sectorCode: t | $t.sector.code])`)

---

## 3. Phase B — Element Serialization

**Goal:** Serialize every `PackageableElement` to a standalone JSON file with resolved-AST bodies and all cross-project FQNs canonicalized. Deserialize round-trips back to the same record form.

**Non-goal:** Do not wire serialized files into the build yet. Phase B produces files; Phase C consumes them.

### 3.1 Success Criteria

- Every element kind in the smoke test corpus serializes to `bazel_smoke/refdata/elements/*.json` and `bazel_smoke/trading/elements/*.json`
- Round-trip (serialize → deserialize → re-serialize) is byte-identical on the second pass
- All Category A FQN refs in the Complete Dependency Reference Matrix (proposal §6) are resolved before serialization
- Resolved AST covers function bodies, derived properties, constraints, service queries, M2M property expressions, XStore cross expressions
- Association navigation sidecars (`*.nav.json`) are emitted for classes with association-backed properties
- `schemaVersion` field on every file; deserializer fails fast on unrecognized versions

### 3.2 JSON Element File Schema (Schema version 1)

Every element file has this top-level shape:

```json
{
  "schemaVersion": 1,
  "kind": "class | enum | association | function | database | mapping | runtime | connection | service | profile",
  "fqn": "package::SubPackage::ElementName",
  "sourceLocation": { "file": "relative/path.pure", "line": 42, "column": 1 }
}
```

#### 3.2a `class` payload

```json
{
  "schemaVersion": 1,
  "kind": "class",
  "fqn": "refdata::Sector",
  "sourceLocation": { "file": "refdata/model.pure", "line": 18, "column": 1 },
  "superClassFqns": ["refdata::Categorized"],
  "properties": [
    { "name": "code", "typeRef": { "kind": "primitive", "fqn": "String" }, "multiplicity": { "lower": 1, "upper": 1 }, "stereotypes": [], "taggedValues": [] },
    { "name": "name", "typeRef": { "kind": "primitive", "fqn": "String" }, "multiplicity": { "lower": 1, "upper": 1 }, "stereotypes": [], "taggedValues": [] }
  ],
  "derivedProperties": [],
  "constraints": [],
  "stereotypes": [],
  "taggedValues": []
}
```

Derived property with resolved AST body:

```json
{
  "name": "displayName",
  "returnTypeRef": { "kind": "primitive", "fqn": "String" },
  "multiplicity": { "lower": 1, "upper": 1 },
  "parameters": [],
  "body": { "kind": "AppliedFunction", "function": "meta::pure::functions::string::plus", "parameters": [ ... ] }
}
```

#### 3.2b `enum` payload

```json
{
  "schemaVersion": 1,
  "kind": "enum",
  "fqn": "refdata::Rating",
  "values": ["AAA", "AA", "A", "BBB", "BB", "B"],
  "stereotypes": [],
  "taggedValues": []
}
```

#### 3.2c `association` payload

```json
{
  "schemaVersion": 1,
  "kind": "association",
  "fqn": "refdata::SectorRegion",
  "ends": [
    { "propertyName": "sector", "targetClassFqn": "refdata::Sector", "multiplicity": { "lower": 0, "upper": 1 } },
    { "propertyName": "region", "targetClassFqn": "refdata::Region", "multiplicity": { "lower": 1, "upper": 1 } }
  ]
}
```

#### 3.2d `function` payload

```json
{
  "schemaVersion": 1,
  "kind": "function",
  "fqn": "refdata::formatSector",
  "parameters": [
    { "name": "s", "typeRef": { "kind": "class", "fqn": "refdata::Sector" }, "multiplicity": { "lower": 1, "upper": 1 } }
  ],
  "returnTypeRef": { "kind": "primitive", "fqn": "String" },
  "returnMultiplicity": { "lower": 1, "upper": 1 },
  "stereotypes": [],
  "taggedValues": [],
  "body": {
    "kind": "AppliedFunction",
    "function": "meta::pure::functions::string::plus",
    "parameters": [
      { "kind": "PropertyAccess", "receiver": { "kind": "Variable", "name": "s" }, "property": "code" },
      { "kind": "CString", "value": "/" },
      {
        "kind": "PropertyAccess",
        "receiver": { "kind": "PropertyAccess", "receiver": { "kind": "Variable", "name": "s" }, "property": "region" },
        "property": "code"
      }
    ]
  }
}
```

**Resolved AST invariants:**

- Every function name is a FQN (`meta::pure::functions::string::plus`, not `+`)
- Every class reference in a class literal or `.all()` call is a FQN
- Every property access carries the explicit receiver (never implicit)
- No text body — if re-parsing is needed, it's a future backwards-compat concern, not a v1 field

#### 3.2e `database` payload

```json
{
  "schemaVersion": 1,
  "kind": "database",
  "fqn": "trading::TradingDB",
  "includesFqns": [],
  "schemas": [],
  "tables": [
    {
      "name": "TRADE",
      "columns": [
        { "name": "ID", "type": "VARCHAR", "size": 50, "nullable": false, "primaryKey": true },
        { "name": "SECTOR_CODE", "type": "VARCHAR", "size": 50, "nullable": true, "primaryKey": false },
        { "name": "NOTIONAL", "type": "DOUBLE", "nullable": true, "primaryKey": false }
      ]
    }
  ],
  "views": [],
  "joins": [],
  "filters": []
}
```

#### 3.2f `mapping` payload

```json
{
  "schemaVersion": 1,
  "kind": "mapping",
  "fqn": "trading::TradingMapping",
  "includes": [],
  "classMappings": [
    {
      "classFqn": "trading::Trade",
      "setId": "trade_main",
      "kind": "relational",
      "mainTable": { "databaseFqn": "trading::TradingDB", "tableName": "TRADE" },
      "propertyMappings": [
        { "propertyName": "id", "kind": "column", "column": "ID" },
        { "propertyName": "notional", "kind": "column", "column": "NOTIONAL" }
      ],
      "filter": null
    }
  ],
  "associationMappings": [],
  "enumerationMappings": []
}
```

M2M variant — replaces `kind: "relational"` with `kind: "m2m"`:

```json
{
  "classFqn": "trading::Trade",
  "kind": "m2m",
  "sourceClassFqn": "trading::RawTrade",
  "parsedFilter": { "kind": "Lambda", "parameters": [...], "body": { ... } },
  "parsedM2MExpressions": {
    "sector": { "kind": "Lambda", "parameters": [...], "body": { ... } },
    "rating": { "kind": "Lambda", "parameters": [...], "body": { ... } }
  }
}
```

Mapping include with store substitutions:

```json
{
  "mappingFqn": "core::BaseMapping",
  "storeSubstitutions": [
    { "originalFqn": "core::DB", "substituteFqn": "trading::TradingDB" }
  ]
}
```

XStore association mapping:

```json
{
  "kind": "xstore",
  "associationFqn": "cross::TradeCounterparty",
  "parsedCrossExpression": { "kind": "Lambda", "parameters": [...], "body": { ... } }
}
```

#### 3.2g `runtime` / `connection` / `service` / `profile` payloads

```json
// runtime
{ "schemaVersion": 1, "kind": "runtime", "fqn": "trading::TradingRuntime",
  "mappingFqns": ["trading::TradingMapping"],
  "connectionBindings": { "trading::TradingDB": "trading::TradingConnection" } }

// connection
{ "schemaVersion": 1, "kind": "connection", "fqn": "trading::TradingConnection",
  "storeFqn": "trading::TradingDB", "databaseType": "DuckDB",
  "specification": { "type": "InMemory" }, "authentication": { "type": "NoAuth" } }

// service
{ "schemaVersion": 1, "kind": "service", "fqn": "trading::GetTrades",
  "pattern": "/api/trades/{sectorCode}",
  "pathParams": ["sectorCode"],
  "mappingFqn": "trading::TradingMapping",
  "runtimeFqn": "trading::TradingRuntime",
  "body": { ... resolved AST ... },
  "documentation": "Returns trades by sector" }

// profile
{ "schemaVersion": 1, "kind": "profile", "fqn": "nlq::Meta",
  "stereotypes": ["rootEntity", "dimension"],
  "tags": ["description", "example"] }
```

#### 3.2h Association Navigation Sidecar

Filename: `{FQN-with-double-colons-replaced-by-double-underscores}__nav.json`

Example `refdata__Sector__nav.json`:

```json
{
  "schemaVersion": 1,
  "classFqn": "refdata::Sector",
  "navigations": {
    "region": {
      "associationFqn": "refdata::SectorRegion",
      "targetClassFqn": "refdata::Region",
      "multiplicity": { "lower": 1, "upper": 1 }
    }
  }
}
```

Classes with no association-contributed properties get no sidecar.

### 3.3 Serializer Tasks

New files:

- `engine/src/main/java/com/gs/legend/serializer/ElementSerializer.java`
- `engine/src/main/java/com/gs/legend/serializer/ValueSpecificationSerializer.java`
- `engine/src/main/java/com/gs/legend/serializer/ElementDeserializer.java`
- `engine/src/main/java/com/gs/legend/serializer/NavigationSidecarWriter.java`

Test fixtures:

- `engine/src/test/resources/bazel_smoke/refdata/elements/*.json` — golden output
- `engine/src/test/resources/bazel_smoke/trading/elements/*.json` — golden output
- `engine/src/test/java/com/gs/legend/serializer/ElementSerializerTest.java` — round-trip tests

Task checklist:

| # | Task | File | Depends on |
|---|---|---|---|
| 1 | Write JSON schema fixtures by hand (golden files) | `bazel_smoke/*/elements/*.json` | — |
| 2 | Implement `ValueSpecificationSerializer.serialize` / `deserialize` covering every `ValueSpecification` sealed subtype | `ValueSpecificationSerializer.java` | — |
| 3 | Implement `ElementSerializer.serialize(PackageableElement)` dispatching per kind | `ElementSerializer.java` | (2) |
| 4 | Implement `ElementDeserializer.deserialize(String)` | `ElementDeserializer.java` | (2) |
| 5 | Implement `NavigationSidecarWriter.write(String classFqn, List<NavEnd>)` | `NavigationSidecarWriter.java` | — |
| 6 | Write round-trip test per element kind | `ElementSerializerTest.java` | (3), (4) |
| 7 | Write golden-file byte-equality test (serialize → compare) | `ElementSerializerTest.java` | (1), (3) |
| 8 | Write schema-version rejection test | `ElementSerializerTest.java` | (4) |
| 9 | Write backwards-compat harness (load v1 fixture with current deserializer) | `ElementSerializerTest.java` | (1), (4) |

### 3.4 `NameResolver` Expansion Checklist

Required before serialization. Checklist (from proposal §6, Category A matrix):

- [ ] `DatabaseDefinition.includes` — every included DB FQN
- [ ] `MappingDefinition.includes` (`MappingInclude.includedMappingPath`) — every included mapping FQN
- [ ] `MappingInclude.storeSubstitutions` — both `originalStore` and `substituteStore` FQNs per pair
- [ ] `ServiceDefinition.mappingRef` and `ServiceDefinition.runtimeRef`
- [ ] `RuntimeDefinition.mappings` list + `connectionBindings` values
- [ ] `ConnectionDefinition.storeName`
- [ ] `EnumerationMappingDefinition.enumType`
- [ ] `StereotypeApplication.profileName` and `TaggedValue.profileName` on every carrier (class, property, derived property, function)

Each bullet: add a case arm in `NameResolver.resolveDefinition()` + a field walk in a new `resolveX()` method.

**Scope:** ~100-150 lines total, plus ~1-3 lines per affected record's constructor (when a ref resolves, allocate a new record).

### 3.5 Pre-Parsed M2M / XStore Expressions

New record fields:

- `MappingDefinition.ClassMappingDefinition.parsedM2MExpressions: Map<String, ValueSpecification>` alongside existing `m2mPropertyExpressions: Map<String, String>`
- `AssociationMappingDefinition.AssociationPropertyMapping.parsedCrossExpression: ValueSpecification` alongside existing `crossExpression: String`

Parse once during `addMapping()` when the source text is available (reuses the existing `PureParser.parseQuery` path at `PureModelBuilder.java:907-936`).

Serialize the parsed field as `ValueSpecification` AST in the element file. Deserialize reads AST directly; **fallback to re-parsing text if the parsed field is absent** (graceful handling of element files from earlier deserializer versions).

**Scope:** ~6 hours total.

### 3.6 Association Navigation Sidecars

After serializing all association elements, walk them once and group by class endpoint. For each class that's the receiver of an association-injected property, write `{class-fqn-with-underscores}__nav.json` alongside the class file.

Implementation sketch:

```java
// In ElementSerializer.flush()
Map<String, List<NavEnd>> byClass = new LinkedHashMap<>();
for (AssociationDefinition a : associations) {
    byClass.computeIfAbsent(a.property1().targetClass(), k -> new ArrayList<>())
        .add(navEndFromOtherEnd(a.property2(), a.qualifiedName()));
    byClass.computeIfAbsent(a.property2().targetClass(), k -> new ArrayList<>())
        .add(navEndFromOtherEnd(a.property1(), a.qualifiedName()));
}
for (var e : byClass.entrySet()) {
    Path navFile = outDir.resolve(e.getKey().replace("::", "__") + "__nav.json");
    Files.writeString(navFile, NavigationSidecarWriter.write(e.getKey(), e.getValue()));
}
```

### 3.7 Round-Trip Test Matrix

Minimum coverage before Phase B is done:

| # | Element kind | Fixture | Assertion |
|---|---|---|---|
| 1 | `class` (primitives only) | `refdata::Region` | Round-trip equal |
| 2 | `class` with superclass | `refdata::Sector` | `superClassFqns` preserved |
| 3 | `class` with derived property + resolved AST | hand-extend `trading::Trade` | AST re-materializes identically |
| 4 | `class` with constraint | hand-extend `trading::Trade` | Constraint body AST preserved |
| 5 | `enum` | `refdata::Rating` | All values preserved |
| 6 | `association` | `refdata::SectorRegion` | Both ends with target class FQNs |
| 7 | `function` with cross-project call | `trading::tradeSummary` | AST has FQN function name |
| 8 | `function` with cross-project property chain | `trading::sectorRegionCode` | AST preserved |
| 9 | `database` with table + join | `trading::TradingDB` | All fields preserved |
| 10 | `mapping` relational, simple | `trading::TradingMapping` | Preserved |
| 11 | `mapping` M2M with parsed expressions | add M2M fixture | `parsedM2MExpressions` AST preserved |
| 12 | `mapping` XStore | add XStore fixture | `parsedCrossExpression` preserved |
| 13 | `mapping` with includes + storeSubstitutions | hand-extend | All FQNs preserved |
| 14 | Nav sidecar | `refdata__Sector__nav.json` | One entry for `region` nav |
| 15 | Schema version rejection | synthetic `"schemaVersion": 999` | Throws |
| 16 | Golden-file byte equality | all smoke test elements | Second serialize matches first byte-for-byte |

### 3.8 Done Criteria

- [ ] All round-trip tests in §3.7 pass
- [ ] Smoke test corpus serializes to `bazel_smoke/*/elements/*.json` with golden byte equality
- [ ] `grep -r "parseQuery" engine/src/main/java | grep -v /test/` shows only one call site (one-time parse during `addMapping()`, not per-load)
- [ ] `NameResolver` has case arms for every Category A ref type (§3.4 checklist all checked)
- [ ] Schema v1 spec committed and frozen (no changes without schema version bump)

---

## 4. Phase C — Lazy Loading + `validateElement`

**Goal:** Let `PureModelBuilder` load dependency elements on demand from filesystem and expose a `validateElement(fqn)` primitive that runs the appropriate checkers.

### 4.1 Success Criteria

- `addDepElementDir(Path)` records a dep dir with zero I/O
- `findClass` / `findEnum` / `findAssociation` / `findFunction` lazy-load from dep dirs on cache miss
- `findProperty` on a class checks local → superclass chain → nav sidecar
- `validateElement(fqn)` runs `MappingNormalizer` + `TypeChecker` as appropriate; returns all errors with source locations
- LSP calls `validateElement` on edited element + reverse-deps
- Smoke test corpus: trading project loads refdata elements lazily; every cross-project ref works end-to-end

### 4.2 Lazy Loading Contract

```java
public class PureModelBuilder {
    /**
     * Records a directory containing dependency element files.
     * Zero I/O at this call — the directory is only consulted when
     * a findX() method misses its local cache.
     *
     * Multiple calls are allowed; directories are searched in registration order.
     */
    public PureModelBuilder addDepElementDir(Path elementsDir);

    /**
     * Returns the class with the given FQN, lazy-loading from dep dirs if not cached.
     *
     * Resolution order:
     *   1. Check local cache (classes added via addSource or addElementFiles)
     *   2. For each registered dep dir, check if {fqn-with-colons-as-underscores}.json exists
     *   3. If found, deserialize and cache; return it
     *   4. If not found anywhere, return Optional.empty()
     *
     * Never throws for missing element — only throws for corrupt/malformed JSON.
     * Thread safety: single-threaded. Caller must serialize access.
     */
    @Override
    public Optional<PureClass> findClass(String fqn);

    // Symmetric: findEnum, findAssociation, findFunction
}
```

**Corrupt file behavior:** throw `IllegalStateException` with the file path and parser error. Do not silently skip — a broken file is a programming error, not a missing element.

**Precedence:** local cache wins over dep dirs. If `trading::Trade` is registered locally AND a file `trading__Trade.json` exists in a dep dir, the local one is authoritative. (Dep dirs should never contain the current project's own elements in practice.)

### 4.3 `validateElement` Contract

```java
public class PureModelBuilder {
    /**
     * Validates one element by running the appropriate type checkers over it.
     *
     * - ClassDefinition: TypeChecker on each derived-property expression and constraint body
     * - FunctionDefinition: TypeChecker on resolvedBody
     * - MappingDefinition: MappingNormalizer synthesizes sourceSpec, TypeChecker stamps it
     * - AssociationMappingDefinition: integrated with owning mapping's sourceSpec
     * - ServiceDefinition: TypeChecker on functionBody
     * - Database / Runtime / Connection / Enum / Profile: structural checks only
     *
     * Does not short-circuit on first error — collects all errors and returns them.
     * Missing element: returns a single-entry list with a "not found" error.
     * Thread safety: single-threaded.
     */
    public List<ValidationError> validateElement(String fqn);

    public record ValidationError(
        String elementFqn,
        SourceLocation location,   // file + line + column
        String message,
        String severity             // "error" or "warning"
    ) {}
}
```

Bazel's `ArtifactCompiler` invokes `validateElement` for every own element after `addSource`. LSP invokes it on the edited element plus reverse-deps.

**Failure mode on broken cross-project ref:** if `trading::sectorRegionCode` references `refdata::Sector.region.code` and `refdata::Sector` doesn't have a `region` property, `validateElement("trading::sectorRegionCode")` must return an error pointing at the `$t.sector.region` access with line/column from the original Pure source.

### 4.4 Association Navigation Sidecar Loading

`findProperty(classFqn, propName)` resolution order:

1. Load `classFqn`'s class file (lazy if needed)
2. Check local properties → hit returns
3. Walk `superClassFqns`, calling `findProperty` recursively on each → hit returns
4. Check `{classFqn}__nav.json` sidecar (lazy-load once) → if `propName` is an association-backed nav, return synthesized `Property` record with `typeFqn = nav.targetClassFqn`, `multiplicity = nav.multiplicity`
5. Miss returns `Optional.empty()`

Sidecar cache: once a class's nav sidecar is loaded, keep it in memory for the builder's lifetime. No file-watching in Phase C.

### 4.5 LSP Wiring

In `PureLspServer`:

```java
// In rebuildAndPublishAll() — configure builder with dep dirs from workspace config
builder.addDepElementDir(workspaceConfig.depElementsDir());

// In didChangeTextDocument() — after re-parsing
List<ValidationError> errors = new ArrayList<>();
for (String fqn : changedElementFqns) {
    errors.addAll(builder.validateElement(fqn));
}
for (String fqn : reverseDepsOf(changedElementFqns)) {
    errors.addAll(builder.validateElement(fqn));
}
publishDiagnostics(errors);
```

**Reverse-dep computation:** `builder.reverseDepsOf(fqn)` → returns FQNs that reference `fqn`. In Phase C, this is a simple in-memory graph built during `addSource` from the FQN-canonicalized refs. Not lazy; built eagerly for the current project's own elements.

### 4.6 Thread Safety & Caching

- **Single-threaded per builder.** LSP calls `validateElement` on the server thread; Bazel compile actions are single-threaded per action. If future work needs concurrency, add it then with explicit locking.
- **Cache lifetime = builder lifetime.** Lazy-loaded elements stay in the `PureModelBuilder`'s maps for the builder's life. No eviction in Phase C.
- **Cache invalidation:** when `addSource` is called (LSP on file change), re-parsing invalidates the local cache for classes/mappings defined in that file. Lazy-loaded dep elements are NOT invalidated — they come from disk and haven't changed unless the dep dir changes (which only happens on a Bazel rebuild anyway).

### 4.7 Smoke Test Scenarios

Using the corpus from §1.2. Each row is a JUnit test in `engine/src/test/java/com/gs/legend/test/BazelSmokeTest.java`:

| # | Test | Setup | Assertion |
|---|---|---|---|
| 1 | `lazyLoadsClass` | `addDepElementDir(refdata/elements)` + `addSource(trading/model.pure)` + `findClass("refdata::Sector")` | Returns Some; file loaded exactly once |
| 2 | `lazyLoadsEnum` | Same; `findEnum("refdata::Rating")` | Returns Some |
| 3 | `lazyLoadsAssociation` | Same; `findAssociation("refdata::SectorRegion")` | Returns Some |
| 4 | `lazyLoadsFunction` | Same; `findFunction("refdata::formatSector")` | Returns Some |
| 5 | `propertyAccessNavigatesCrossProject` | Build full trading model; evaluate `$t.sector.region.code` via TypeChecker | Resolves correctly; TypeInfo has `ClassType("refdata::Region")` at the right node |
| 6 | `functionCallCrossProject` | `validateElement("trading::tradeSummary")` | Returns no errors |
| 7 | `propertyChainCrossProject` | `validateElement("trading::sectorRegionCode")` | Returns no errors |
| 8 | `superclassCrossProject` | `findProperty(trading::InternalTrade, "category")` | Returns Some, navigated via `refdata::Categorized` superclass |
| 9 | `associationNavCrossProject` | `findProperty(trading::Trade, "region")` via `trading::TradeRegion` | Returns Some (nav sidecar lookup) |
| 10 | `mappingReferencesCrossProject` | `validateElement("trading::TradingMapping")` | Returns no errors (transitively loads `refdata::Sector`) |
| 11 | `brokenBodyReportsLocation` | Modify `refdata::Sector` to remove `code`; `validateElement("trading::tradeSummary")` | Returns error pointing at `.code` access in original Pure source |
| 12 | `missingElement` | `findClass("nonexistent::Thing")` | Returns `Optional.empty`, no exception |
| 13 | `corruptElementFile` | Write invalid JSON at `refdata/elements/refdata__Sector.json`; `findClass("refdata::Sector")` | Throws `IllegalStateException` with file path |

### 4.8 Done Criteria

- [ ] All 13 scenarios in §4.7 pass
- [ ] `validateElement` is the ONLY validation entry point (no new inline calls inside `addSource`)
- [ ] LSP publishes diagnostics for edited element + reverse-deps on `didChangeTextDocument`
- [ ] Smoke test: end-to-end query `Trade.all()->project(~[sectorCode: t | $t.sector.code])` succeeds against cross-project lazy-loaded model

---

## 5. Risk Register

Concrete risks for the three phases, in rough likelihood × severity order:

| Risk | Phase | Mitigation |
|---|---|---|
| `typeFqn` refactor breaks a subtle `Type`-identity assumption somewhere | A | Adapter pattern + per-file test gate. Revert the offending commit. |
| Primitive detection change in `RelationalMapping` (`instanceof PrimitiveType`) misbehaves | A | Dedicated mapping-heavy test pass at checkpoint 8. |
| Serialized AST schema locks in before we know we need a node type | B | `schemaVersion` field + forward-compat "ignore unknown fields" policy. Bump schema version if we have to add a node type. |
| Resolved AST serializer forgets to resolve a call site (e.g., misses a `ValueSpecification` sealed subtype) | B | Round-trip test must iterate the sealed interface permit list. |
| Lazy loader misses a precedence edge case (e.g., partial local + partial dep for same FQN) | C | Contract says local wins; test `corruptElementFile` + a precedence scenario explicitly. |
| LSP reverse-dep graph is stale after a rename | C | Rebuild the graph on `addSource` — cheap for in-project elements. Don't try to update incrementally. |
| `validateElement` on M2M mappings misses an association extend | C | Reuse existing `MappingNormalizer.addAssociationExtends` (already handles this). Scenario 10 covers it. |
| Smoke test corpus drifts from real-world usage | All | Keep `README.md` current; prefer extending the smoke test over inventing ad-hoc fixtures. |
| Pre-parsed M2M field absence in old element files | B/C | Fallback to re-parsing text (§1.5 decision). Backwards-compat harness covers this. |

---

## 6. Execution Sequence Summary

| Checkpoint | Phase | Scope | Gate |
|---|---|---|---|
| Pre-work | — | Schema spec reviewed + frozen; smoke test committed (DONE); decisions log agreed | No coding until Phase A starts |
| A.1 | Phase A | Add `typeFqn` / `superClassFqns` fields (checklist rows 1-2) | All tests pass |
| A.2 | Phase A | Migrate call sites (checklist rows 3-11) | All tests pass after each commit |
| A.3 | Phase A | Flag day — remove `genericType` / `superClasses` (checklist row 12) | All tests pass |
| B.1 | Phase B | Write golden JSON fixtures by hand | — |
| B.2 | Phase B | Implement `NameResolver` expansion (§3.4) | Resolution tests for each ref type pass |
| B.3 | Phase B | Implement `ValueSpecificationSerializer` | Round-trip tests for each AST node type pass |
| B.4 | Phase B | Implement `ElementSerializer` / `ElementDeserializer` | Round-trip tests per element kind pass |
| B.5 | Phase B | Pre-parsed M2M/XStore fields + serializer integration (§3.5) | M2M round-trip tests pass |
| B.6 | Phase B | Nav sidecar writer (§3.6) | Sidecar test passes |
| B.7 | Phase B | Golden byte-equality + backwards-compat tests | All §3.7 tests pass |
| C.1 | Phase C | `addDepElementDir` + lazy `findClass/findEnum/findAssociation/findFunction` (§4.2) | `lazyLoads*` smoke tests pass |
| C.2 | Phase C | Nav sidecar lazy-load in `findProperty` (§4.4) | `associationNavCrossProject` passes |
| C.3 | Phase C | `validateElement` primitive (§4.3) | `validateElement` smoke tests pass |
| C.4 | Phase C | LSP wiring (§4.5) | Manual LSP test: edit trading, see diagnostics on a broken refdata ref |
| C.5 | Phase C | Full smoke test pass (§4.7) | All 13 scenarios green |

Total estimate: **~4-6 sessions of focused work** for Phases A + B + C combined, assuming no major surprises. Pre-work is a few hours.

---

## 7. Out of Scope

Explicitly deferred — handled by the main proposal's §11 or by future work:

- **Bazel-native work** — `legend_library` rule, module extension, `ArtifactCompiler` CLI (proposal §11 Steps 1, 4)
- **External repo integration** — `git_repository` wiring, `external_repos.bzl` generation (proposal §11 Step 5)
- **`legend_test` rule** — eager-loading test runner (proposal §11 Step 6)
- **Scale validation** — synthetic monorepo stress test (proposal §11 Step 0)
- **Compat checker + CI** — upcast compatibility on element changes (proposal §11 Step 7)
- **Frontend multi-file editing** — studio-lite sidebar + tab-per-file (proposal §11 Step 8)
- **DataSpace as a packageable element** — future work
- **Extension (trait-style) packageable element** — future work, see `CROSS_PROJECT_JOINS.md` §8
- **Per-property shredding** — Phase C produces per-class files; per-property is a future optimization if profiling demands it

These can all be implemented after Phases A + B + C land, using the stable engine core that Phase C produces.

---

**Cross-references:**

- Proposal: `docs/BAZEL_DEPENDENCY_PROPOSAL.md` (*why* and *what*)
- Cross-project joins: `docs/CROSS_PROJECT_JOINS.md` (Extension design, migration auto-convert, compile-as-validation)
- Smoke test corpus: `engine/src/test/resources/bazel_smoke/`
- Architectural invariants: `AGENTS.md`
