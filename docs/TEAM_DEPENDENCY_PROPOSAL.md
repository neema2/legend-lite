# Project Artifacts, Dependency Loading, and Developer Experience

Multi-team model sharing via immutable compiled artifacts in a shared monorepo with wavefront validation, git sparse checkout for minimal downloads, upcast-compatible diamond resolution with per-edge change tokens, content-addressed parse cache, and dependency loading via `addDependency()` — all designed around immutability to avoid cache invalidation complexity.

---

## Design Philosophy: Immutability Over Invalidation

**Never mutate compiled state.** Instead:
1. Hash source content → look up compiled artifact
2. If hit → use it (zero work)
3. If miss → compile from scratch, store result
4. Compose results by layering immutable snapshots

Same strategy as Nix, Docker image layers, and Git content-addressed storage. Simple, correct, scales without cache invalidation nightmares.

---

## Zero Overhead for Standalone Projects

**If you don't use dependencies, none of this code runs.**

The only change to the existing hot path is ParseCache wrapping `PureParser.parseModelWithImports()` — a pure win (cache hit = skip parse entirely). Dependency machinery lives in `ProjectLoader`, `DiamondResolver`, and a `ModelLayer` extraction inside `PureModelBuilder` — all **inert** for standalone projects.

```
Standalone project code path (unchanged):
  PureModelBuilder.addSource(src)         ← ParseCache wraps this (only change)
    → PureParser.parseModelWithImports()  ← cache hit = free, cache miss = same as today
    → phases 0-5: register, resolve, build
  PureModelBuilder AS ModelContext         ← used directly, no wrapper
    → TypeChecker → MappingResolver → PlanGenerator → SQL

Project-with-dependencies code path (new, opt-in):
  ProjectLoader.load(pure-project.yaml)    ← only if manifest exists
  model.addDependency(depArtifact)          ← loads shapes + definitions eagerly
    → same PureModelBuilder, separate internal ModelLayer for deps
    → all lookups chain: local layer → dep layer
    → ALL existing pipeline code works unchanged (same class, same methods)
```

No wrapper objects, no interface dispatch indirection, no composite adapters. Downstream code (`MappingNormalizer`, `PlanGenerator`, `TypeChecker`) calls the same `PureModelBuilder` methods — the chaining is internal. A user who never creates `pure-project.yaml` gets exactly today's code path + ParseCache speedup.

---

## End-to-End Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    ARTIFACT MONOREPO (GitHub)                        │
│                                                                     │
│  wavefront.yaml          ← CI-validated version set                 │
│  com.gs.products/                                                   │
│    3.1.0.legend           ← compiled shapes + used symbols          │
│  com.gs.refdata/                                                    │
│    1.5.0.legend                                                     │
│  com.gs.risk/                                                       │
│    2.0.0.legend                                                     │
│                                                                     │
│  CI: on every PR, validate ALL artifacts work together              │
│      → update wavefront.yaml → merge                                │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
              git sparse checkout (only packages you need)
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│                    CONSUMER PROJECT                                  │
│                                                                     │
│  pure-project.yaml        ← manifest: sources + artifact repo       │
│  src/                                                               │
│    model/Trade.pure       ← local Pure source files                 │
│    mapping/TradingMapping.pure                                      │
│  .legend/                                                           │
│    artifacts/              ← sparse git checkout (gitignored)       │
│      wavefront.yaml        ← downloaded, ~500 bytes                 │
│      com.gs.products/      ← only the deps you import              │
│        3.1.0.legend                                                 │
│      com.gs.refdata/                                                │
│        1.5.0.legend                                                 │
│                                                                     │
│  Build: ParseCache → PureModelBuilder.addSource(local files)        │
│         + model.addDependency(artifact) for each dep                │
│         → TypeChecker → MappingResolver → PlanGenerator → SQL       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## What a Data Engineer's Day Looks Like

### First Time Setup
```bash
cd my-trading-project
legend init                    # creates pure-project.yaml
legend deps sync               # sparse-clones artifact monorepo, downloads only needed packages
```

### Daily Workflow
```bash
# Edit Pure files in Studio Lite (or any editor with LSP)
# → LSP rebuilds model from all files (parse cache: only changed file re-parsed)
# → Cross-file diagnostics via dependency shapes
# → Query execution uses local model + dependency artifacts

# When ready to share:
legend compile                 # produces com.gs.trading-1.0.0.legend
legend publish                 # PR to artifact monorepo
                               # CI validates wavefront compatibility
                               # merge → other teams can use your model
```

### Updating Dependencies
```bash
legend deps sync               # git pull on sparse checkout
                               # downloads only changed artifacts
                               # typically 0-2 files × 200KB = <1 second
```

---

## Artifact Monorepo + Wavefront

### Structure

```
legend-artifacts/                         # single shared GitHub repo
├── com.gs.products/
│   ├── 3.2.0.legend                     # latest
│   ├── 3.1.0.legend                     # previous (kept for backward compat)
│   └── 3.0.0.legend                     # oldest in tree
├── com.gs.refdata/
│   ├── 1.5.0.legend
│   └── 1.2.0.legend
├── com.gs.risk/
│   └── 2.0.0.legend
├── wavefront.yaml                        # CI-validated version set
└── .github/workflows/validate.yml        # CI pipeline
```

### `wavefront.yaml` — The Validated Version Set

```yaml
# Latest set of artifact versions proven to work together.
# Updated automatically by CI when a new artifact passes validation.
# deps: derived from each artifact's usedSymbols keys at publish time.
wavefront: "2026-04-13"
artifacts:
  com.gs.products:
    version: "3.2.0"
    deps: ["com.gs.refdata"]            # transitive dependency edges
  com.gs.refdata:
    version: "1.5.0"
    deps: []
  com.gs.risk:
    version: "2.0.0"
    deps: ["com.gs.refdata"]
  com.gs.trading:
    version: "1.0.0"
    deps: ["com.gs.products", "com.gs.risk"]
```

The `deps` edges are derived from `usedSymbols` keys in each artifact — no manual maintenance. This enables **automatic transitive resolution**: the loader reads wavefront, walks the graph from the user's direct deps, and computes the full closure before downloading anything.

Each wavefront is a git commit. You can:
- **Use latest**: `git pull` → newest validated set
- **Pin**: `git checkout <commit>` → freeze to a specific version set
- **Bisect**: `git bisect` → find which artifact broke things

Even at 500 artifacts, wavefront.yaml is ~30KB (3 lines × 500 entries) — trivially small. Future extension: named wavefront files for release channels (`wavefront-stable.yaml`, `wavefront-preview.yaml`).

### CI Validation (on every PR to monorepo)

```
1. Load ALL latest artifacts
2. For each pair sharing a transitive dependency:
   → Run diamond resolution (upcast + change token check)
3. For each artifact:
   → Verify its usedSymbols are satisfied by resolved versions
4. ALL pass → update wavefront.yaml → merge ✅
5. ANY fail → block merge → print conflicting symbols ❌
```

### Consumer Downloads Only What They Need

```bash
# legend deps sync does this automatically:

# 1. Sparse clone (wavefront only — ~500 bytes)
git clone --depth 1 --filter=blob:none --sparse \
    https://github.com/gs/legend-artifacts.git .legend/artifacts
git sparse-checkout set wavefront.yaml

# 2. Read wavefront → resolve transitive closure from user's direct deps
#    User listed: [com.gs.products, com.gs.risk]
#    Wavefront deps graph: products→refdata, risk→refdata
#    Full closure: [com.gs.products, com.gs.risk, com.gs.refdata]

# 3. Fetch all needed artifacts in one call
git sparse-checkout add com.gs.products com.gs.risk com.gs.refdata

# Update: only fetches changes to your packages
git pull    # ~0 bytes if nothing changed, ~200KB if one artifact updated
```

**Download sizes:**

| Scenario | Full clone | Sparse clone (5 of 50 deps) |
|---|---|---|
| First download | ~10MB | **~1MB** |
| Daily update | ~500KB | **~0-200KB** |
| Offline | ✅ Works | ✅ Works (cached after first download) |

---

## Compiled Artifact Format (`.legend`)

An immutable, serialized snapshot of a project's **parser output** (definition records). Contains everything needed to use this project's models, mappings, and databases WITHOUT source code.

**Key insight**: We serialize the parser output (flat records — strings, ints, nested records), NOT the compiler output (ValueSpecification AST). The consumer's compiler processes definitions exactly as if parsed from source. This makes serialization straightforward — every type is a Java record with primitive fields.

```
LegendArtifact {
    // ── IDENTITY ──
    name: "com.gs.products"
    version: "3.1.0"
    sourceHash: "sha256:..."

    // ── SHAPES (always loaded, ~1KB per class) ──
    shapes: {
        classes: [
            { fqn: "products::Instrument",
              properties: [{name: "ticker", type: "String", mult: [1,1]}, ...],
              supers: [] },
            ...
        ],
        enums: [ { fqn: "products::InstrumentType", values: ["EQUITY","BOND"] } ],
        associations: [ { fqn: "products::Instrument_Sector", ends: [...] } ],
        functions: [ { fqn: "products::activeInstruments", params: [...], returnType: "String[*]" } ]
    }

    // ── USED SYMBOLS (change tokens, per-dependency edge) ──
    usedSymbols: {
        "com.gs.refdata": {
            classProperties: { "refdata::Sector": ["name", "code"] },
            enumValues: { "refdata::Region": ["US", "EU"] },
            associations: ["refdata::Sector_Region"],
            functions: ["refdata::activeSectors"]
        }
    }

    // ── DEFINITIONS (full parser output, loaded into PureModelBuilder) ──
    definitions: {
        mappings: [ <serialized MappingDefinition records> ],
        databases: [ <serialized DatabaseDefinition records> ],
        functions: [ <serialized FunctionDefinition records> ],
        runtimes: [ <serialized RuntimeDefinition records> ],
        connections: [ <serialized ConnectionDefinition records> ],
        services: [ <serialized ServiceDefinition records> ]
    }
}
```

- **Shapes**: small, always loaded. Everything TypeChecker needs to compile queries that REFERENCE these types.
- **Used symbols**: records exactly which cross-project symbols this artifact references from each dependency. Foundation for change token resolution.
- **Definitions**: full parser output. ALL flat Java records (strings, ints, nested records). The most complex type is `RelationalOperation` — a sealed interface with 13 record subtypes, all containing strings/ints. Serialization is a ~100-line switch statement. Consumer loads these into `PureModelBuilder`, then the normal compilation pipeline runs (MappingNormalizer → TypeChecker → MappingResolver → PlanGenerator).
- **Format**: JSON (inspectable, git-diff-friendly, delta-compresses well).

### Why Definitions Are Easy to Serialize

Every definition type is a flat record:

| Type | Fields | Complex? |
|---|---|---|
| `ClassDefinition` | strings, `List<PropertyDefinition>` (name, type, bounds) | No |
| `EnumDefinition` | string name, `List<String>` values | No |
| `AssociationDefinition` | two `AssociationEndDefinition` records (strings + ints) | No |
| `MappingDefinition` | `List<ClassMappingDefinition>` (strings + `PropertyMappingDefinition`) | No |
| `DatabaseDefinition` | `List<TableDefinition>`, `List<JoinDefinition>` (strings + `RelationalOperation`) | No |
| `RelationalOperation` | 13-subtype sealed interface — `ColumnRef(db,table,col)`, `Comparison(left,op,right)`, `FunctionCall(name,args)`, etc. All strings/ints/recursive RelOp. | ~100 lines |
| `FunctionDefinition` | strings (name, body as raw expression string) | No |

Total serialization code: **~800-850 lines** (straightforward switch-cases, no tricky parts). Includes `ConnectionDefinition`, `RuntimeDefinition`, `ServiceDefinition` which are omitted from the table above.

---

## Parse Cache (Layer 1)

Content-addressed, per-`addSource` call. Plugs into `PureModelBuilder.addSource()`.

```java
public final class ParseCache {
    private static final ParseCache GLOBAL = new ParseCache();
    // Bounded LRU — evicts oldest entries beyond MAX_ENTRIES
    private final LinkedHashMap<String, ParseResult> cache =
        new LinkedHashMap<>(32, 0.75f, true) {
            @Override protected boolean removeEldestEntry(Map.Entry<String, ParseResult> eldest) {
                return size() > 200;
            }
        };

    public static ParseCache global() { return GLOBAL; }

    public ParseResult getOrParse(String source) {
        return cache.computeIfAbsent(source, PureParser::parseModelWithImports);
    }
}
```

**No invalidation** — content-addressed. Changed file = different key = cache miss.

---

## Dependency Loading: ModelLayer + `addDependency()` (Layer 3)

Dependencies are loaded **inside** `PureModelBuilder` via a separate internal `ModelLayer`. No wrapper class, no composite adapter — the same `PureModelBuilder` serves everything. All existing downstream code (`MappingNormalizer`, `PlanGenerator`, `TypeChecker`) works unchanged.

### Architecture: Separate Collections, Shared SymbolTable

```java
public final class PureModelBuilder implements ModelContext {
    private final SymbolTable symbols = new SymbolTable();  // ONE, shared

    // ── LOCAL layer (built from addSource) ──
    private final ModelLayer local = new ModelLayer();

    // ── DEP layer (built from addDependency, null until first dep) ──
    private ModelLayer dep;  // separate storage, same SymbolTable

    // Track which layer build phases write to
    private ModelLayer activeLayer = local;

    // Existing path — unchanged, writes to local layer
    public PureModelBuilder addSource(String pureSource) {
        activeLayer = local;
        // ... same build phases (parse, register, resolve) ...
    }

    // New path — reuses SAME build phases, writes to dep layer
    // Eager: loads shapes + definitions in ONE pass. If you never call this,
    // the dep layer is null and all lookups short-circuit — zero overhead.
    public PureModelBuilder addDependency(LegendArtifact artifact) {
        if (dep == null) dep = new ModelLayer();
        activeLayer = dep;

        // ── Shapes (classes, enums, associations) ──
        // Phase 0: Intern all FQNs into shared SymbolTable
        for (var classDef : artifact.shapes().classes())
            symbols.intern(classDef.qualifiedName());
        for (var enumDef : artifact.shapes().enums())
            symbols.intern(enumDef.qualifiedName());
        // Phase 2: Register enums (needed for property type resolution)
        for (var enumDef : artifact.shapes().enums())
            registerEnum(enumDef);
        // Phase 3a: Register class stubs (forward references)
        for (var classDef : artifact.shapes().classes())
            registerClassStub(classDef);
        // Phase 3b: Resolve class properties
        for (var classDef : artifact.shapes().classes())
            addClass(classDef);
        // Phase 4: Resolve superclasses
        resolveSuperclasses();
        // Associations
        for (var assocDef : artifact.shapes().associations())
            addAssociation(assocDef);

        // ── Definitions (databases, mappings, runtimes, etc.) ──
        // Artifact defs are pre-resolved: FQN names, includes already flattened.
        // Phase 1 (name resolution) and Phase 6 (function body parsing) are SKIPPED.
        for (var dbDef : artifact.definitions().databases())
            addDatabase(dbDef);    // → populates tables, joins, views, filters
        for (var mappingDef : artifact.definitions().mappings())
            addMapping(mappingDef); // → uses findTable/findJoin (chained lookups)
        for (var funcDef : artifact.definitions().functions())
            addFunction(funcDef);   // resolvedBody already set
        for (var connDef : artifact.definitions().connections())
            addConnection(connDef);
        for (var runtimeDef : artifact.definitions().runtimes())
            addRuntime(runtimeDef);
        for (var serviceDef : artifact.definitions().services())
            addService(serviceDef);

        // Track symbol origin for usedSymbols extraction
        for (var classDef : artifact.shapes().classes())
            symbolOrigin.put(classDef.qualifiedName(), artifact.name());

        activeLayer = local;
        return this;
    }

    // All lookups chain: local → dep (local always wins)
    @Override
    public Optional<PureClass> findClass(String name) {
        var result = local.findClass(symbols, name);
        if (result.isPresent()) return result;
        return dep != null ? dep.findClass(symbols, name) : Optional.empty();
    }

    public SQLDialect resolveDialect(String runtimeName) {
        var result = local.resolveDialect(symbols, runtimeName);
        if (result != null) return result;
        if (dep != null) {
            result = dep.resolveDialect(symbols, runtimeName);
            if (result != null) return result;
        }
        throw new IllegalArgumentException("Runtime not found: " + runtimeName);
    }

    // Same chaining for: resolveMappingNames, resolveConnection,
    // getMappingRegistry, findEnum, findTable, findJoin, findView, findFilter,
    // findFunction, findAssociation, findAllAssociationNavigations,
    // findMappingExpression, getAllClasses, getAllAssociations, getAllEnums
}
```

### Why Shared SymbolTable Is Critical

`MappingRegistry` and `MappingNormalizer` use **integer IDs** from `SymbolTable` (not string names) for O(1) lookups. If local and dep had separate SymbolTables, `products::Instrument` might be ID 5 in dep and ID 12 in local — IDs wouldn't match across layers. One shared SymbolTable guarantees consistent IDs everywhere.

### What `ModelLayer` Contains

```java
// Extracted from existing PureModelBuilder fields — no new logic
class ModelLayer {
    ArrayList<PureClass> classes;
    ArrayList<Association> associations;
    Map<String, Table> tables;
    Map<String, Join> joins;
    Map<String, View> views;
    Map<String, Filter> filters;
    ArrayList<DatabaseDefinition> databases;
    ArrayList<ProfileDefinition> profiles;
    ArrayList<ConnectionDefinition> connections;
    ArrayList<RuntimeDefinition> runtimes;
    ArrayList<ServiceDefinition> services;
    ArrayList<EnumDefinition> enums;
    ArrayList<MappingDefinition> mappingDefinitions;
    ArrayList<List<FunctionDefinition>> functions;
    MappingRegistry mappingRegistry;
    Map<String, Join> explicitAssociationJoins;  // association/class property → Join
    // lookup helpers: findClass(symbols, name), findTable(name), findJoin(name), etc.
}
```

This is a pure extraction of existing fields — `PureModelBuilder` currently has all of these as direct fields. Moving them into `ModelLayer` is a mechanical refactor. Build methods (`addMapping`, `addDatabase`, etc.) that read from these collections change from `tables.get(x)` to `findTable(x).orElse(null)` (chained lookup). A few new chained methods are needed (`findView`, `findFilter` — ~3 lines each).

**Transient fields** (NOT in ModelLayer — cleared after each build cycle):
- `pendingClassDefinitions` — used between `registerClassStub()` and `resolveSuperclasses()`, then cleared

**Lazy indexes** (`classToAssociations`, `propertyToJoin`) stay at PureModelBuilder level, rebuilt from both layers on invalidation.

### Build Phase Matrix

| Phase | `addSource()` | `addDependency()` | Why |
|---|---|---|---|
| 0: Intern FQNs | ✅ | ✅ | SymbolTable needs all names |
| 1: Name resolution | ✅ | ❌ SKIP | Artifact defs are already FQN |
| 2: Register enums | ✅ | ✅ | Needed for property type resolution |
| 3a: Class stubs | ✅ | ✅ | Forward references |
| 3b: Class properties | ✅ | ✅ | Type resolution |
| 4: Superclasses | ✅ | ✅ | Inheritance chain |
| 5: Remaining defs | ✅ | ✅ | Databases, mappings, etc. |
| 5b: DB includes | ✅ | ❌ SKIP | Artifact stores pre-flattened includes |
| 5c: Mapping includes | ✅ | ❌ SKIP | Artifact stores pre-flattened includes |
| 6: Function bodies | ✅ | ❌ SKIP | Artifact has pre-resolved bodies |

### Immutability Guarantees

- **Local layer**: only `addSource()` writes here. Dependencies never touch it.
- **Dep layer**: only `addDependency()` writes here. Local edits never touch it.
- **SymbolTable**: append-only (names are interned, never removed). Both layers share it safely.
- **On edit cycle**: rebuild local layer from scratch (parse cache makes this fast). Dep layer is untouched — it's from immutable artifacts.
- **On dependency update**: discard dep layer, reload from new `.legend` files. Local layer untouched.

### Symbol Origin Tracking (for Change Tokens)

```java
// Inside PureModelBuilder — tracks which dep provided each symbol
private final Map<String, String> symbolOrigin = new HashMap<>(); // FQN → dep name

// In addDependency():
symbolOrigin.put(classDef.fqn(), artifact.name());

// At legend compile time:
public Map<String, UsedSymbols> computeUsedSymbolsByDep() {
    // Walk local definitions, collect cross-dep symbol refs, group by symbolOrigin
}
```

---

## Diamond Resolution: Upcast + Change Tokens

### The Problem

```
        com.gs.trading (your project)
           /              \
  com.gs.products@3.1    com.gs.risk@2.0
          \                /
      com.gs.refdata@1.2   com.gs.refdata@1.5
               ↑ CONFLICT ↑
```

### Backward Compatibility Rules

| Change | Compatible? | Why |
|---|---|---|
| Add optional property (`[0..1]`/`[*]`) | ✅ | Existing code doesn't reference it |
| Add new class/enum/association/function | ✅ | No existing code references it |
| Add new enum value | ✅ | Existing switches on subset still work |
| Remove/rename property or class | ❌ | Existing references break |
| Change property type | ❌ | Type mismatch |
| Narrow multiplicity (`[0..1]` → `[1]`) | ❌ | Constraint violation |

### Three-Level Resolution

```
Level 1: IDENTICAL SHAPES → pick newer (shape signatures match)
Level 2: UPCAST COMPATIBLE → use newer (newer ⊇ older)
Level 3: CHANGE TOKEN CHECK → per-consumer symbol verification
```

**Level 3 example:**
```
refdata@1.2 has: Sector{name}
refdata@1.5 has: Sector{name, code}

products.legend usedSymbols from refdata = {Sector.name}
risk.legend     usedSymbols from refdata = {Sector.name, Sector.code}

→ @1.5 satisfies both ✅  (products only uses Sector.name, present in @1.5)
→ @1.2 fails risk ❌     (risk uses Sector.code, NOT in @1.2)
→ Resolve to @1.5
```

If NEITHER version works → hard error naming conflicting symbols.

### Publish-Time Compatibility Check

```
legend publish --check-compat
  → Load previous artifact (com.gs.refdata@1.4.legend)
  → Compare shapes: is new upcast-compatible with old?
  → If yes → allow minor/patch publish
  → If breaking → require --breaking flag
  → Print: "Removed refdata::Sector.code — breaking change"
```

### Three Layers of Protection

| Layer | Protects | Against | When |
|---|---|---|---|
| **Shapes** (runtime) | Ad-hoc queries + published code | Missing class/property/enum | Every compilation — immediate TypeChecker error |
| **Upcast compatibility** (publish-time) | ALL consumers | Breaking changes (removed/changed symbols) | `legend publish --check-compat` |
| **Change tokens** (diamond resolution) | Published code in specific artifact | Unnecessary diamond failures | CI wavefront validation |

Change tokens are a **diamond resolution optimization**, not a safety net. Safety comes from shapes (runtime) and upcast checks (publish-time). Ad-hoc queries are protected by shapes at runtime — no change tokens needed.

### Used Symbol Collection: Static Extraction (Not TypeChecker)

TypeChecker's `classPropertyAccesses` is **query-scoped** — it only tracks what ONE query accesses. But artifact `usedSymbols` needs to be **project-scoped** (union of ALL code).

**Solution**: Static extraction at `legend compile` time — walk all definitions and extract referenced symbols:

```java
// ArtifactCompiler — static definition walk (no TypeChecker)
for (ClassDefinition cd : classDefs)
    // Record superclass refs + property type refs from dependency
for (MappingDefinition md : mappingDefs)
    // Record mapped property names + M2M expression refs
for (FunctionDefinition fd : funcDefs)
    // Record class/property refs in function body
for (AssociationDefinition ad : assocDefs)
    // Record referenced class names
```

This captures what the PROJECT'S CODE references — not everything from every dependency. If a mapping maps 5 of 20 properties, usedSymbols contains those 5.

TypeChecker's `classPropertyAccesses` stays for **runtime pruning** (ExtendOverride in MappingResolver) — that IS query-dependent and should remain so.

### Implementation Cost: ~100 Lines

- Static symbol extractor walking definitions (~85 lines — class/enum/assoc defs ~20, mapping defs ~15, function body AST walk ~40, symbol origin grouping ~10)
- `symbolOrigin` tracking in `PureModelBuilder.addDependency()` (~10 lines)
- Diamond resolver per-consumer check (~30 lines)
- Note: function body extraction walks the already-resolved AST (`FunctionDefinition.resolvedBody()`) — no re-parsing needed

**Runtime performance impact: zero.** Static extraction runs at `legend compile` time only. Diamond resolution runs in CI only. No query-path overhead.

### Wavefront Eliminates Diamond Resolution for Consumers

Because CI pre-validates all artifact versions together, consumers using the wavefront **never encounter diamonds at build time**. The wavefront IS the pre-resolved version set.

---

## Project Manifest

```yaml
# pure-project.yaml
name: com.gs.trading
version: 1.0.0

sources:
  - src/**/*.pure

artifacts:
  repo: "https://github.com/gs/legend-artifacts.git"
  # wavefront: "2026-04-13"  # optional pin — defaults to HEAD

dependencies:               # direct deps only (versions + transitives from wavefront)
  - com.gs.products
  - com.gs.risk

exports:
  - trading::*
  - trading::TradingMapping
```

**Users list only DIRECT dependencies** — no versions, no transitives. Versions come from the wavefront. Transitives are resolved automatically: the loader reads the wavefront's `deps` graph, walks from the user's direct deps, and computes the full transitive closure before downloading.

**Collision rules**: if two dependencies export the same FQN → hard error at load time. Local definitions always shadow dependency definitions (local-first chaining in `PureModelBuilder`).

**Load order**: leaves first (dependencies before dependents). If `com.gs.products → com.gs.refdata`, load refdata first so products' definitions can reference refdata's shapes.

---

## Multi-File Editor Support (Studio Lite)

### Backend

```java
// PureLspServer — already has Map<String, String> documents
// On didChange for any file:
PureModelBuilder model = new PureModelBuilder();
for (var dep : loadedDependencies)
    model.addDependency(dep);           // dep layer: shapes + definitions
for (var entry : documents.entrySet())
    model.addSource(entry.getValue());  // local layer: parse cache → unchanged files free
// Publish diagnostics for ALL files — model sees both local + dep types
```

### Frontend

- File tree sidebar showing project structure
- Tab-per-file editing
- Each file tracked via existing LSP protocol
- Model rebuilt from all files on any change (parse cache makes this fast)

---

## Implementation Plan

### Step 1: Parse Cache ✅ DONE
Already committed (bef15be). `ParseCache.java` + `PureModelBuilder.addSource()` integration + 976 lines of tests.

### Step 2: Multi-File LSP Rebuild (1 session)
**Files**: `PureLspServer.java`

- On `didChange`, rebuild model from ALL documents (not just changed one)
- Parse cache → only changed file re-parsed
- Publish diagnostics for ALL open files (cross-file errors)

### Step 3: ModelLayer Extraction + Artifact Format + Serialization (2-3 sessions)
**Files**: `ModelLayer.java` (new, extracted from PureModelBuilder), `LegendArtifact.java`, `ArtifactCompiler.java`, `ArtifactSerializer.java`, `PureExpressionPrinter.java`, `ShapeCompat.java` (all new), `PureModelBuilder.java` (refactored)

**3a: ModelLayer extraction** (~1 session)
- Extract existing `PureModelBuilder` fields into `ModelLayer` internal class (including `explicitAssociationJoins`, `profiles`)
- All existing build phases (`registerClass`, `resolveProperties`, etc.) write to `activeLayer`
- All lookup methods (`findClass`, `resolveDialect`, etc.) chain: `local → dep`
- Build methods that read collections change `tables.get(x)` → `findTable(x).orElse(null)` (chained). New helpers needed: `findView()`, `findFilter()` (~3 lines each)
- Lazy indexes (`classToAssociations`, `propertyToJoin`) stay at PureModelBuilder level, rebuilt from both layers on invalidation
- `addDependency(LegendArtifact)` method: creates dep layer, loads shapes + definitions eagerly using same build phases (see Build Phase Matrix above)
- **Zero downstream changes** — `MappingNormalizer`, `PlanGenerator`, `TypeChecker` all call same methods
- Test: two-project test — producer defines classes+mappings, consumer loads artifact via `addDependency()`, runs full query pipeline end-to-end

**3b: Artifact format + serialization** (~1-2 sessions)
- `LegendArtifact` record: shapes + usedSymbols + definitions (full parser output)
- `ArtifactCompiler.compile(PureModelBuilder) → LegendArtifact` — extracts shapes AND definitions. Pre-flattens DB/mapping includes so `addDependency()` can skip include resolution.
- `PureExpressionPrinter` — renders resolved `ValueSpecification` AST (19 subtypes) back to Pure source with FQN names (~100-150 lines). Applied to:
  - `FunctionDefinition.body()` → FQN body from `resolvedBody` AST
  - `ClassMappingDefinition.m2mPropertyExpressions()` → FQN expressions (NameResolver does NOT resolve these)
  - `ClassMappingDefinition.filterExpression()` → FQN filter expression
- `ArtifactSerializer` — JSON read/write for ALL definition types (~800-850 lines total):
  - Shapes: ~150 lines (classes, enums, associations — all strings/ints)
  - `RelationalOperation`: ~100 lines (13-subtype switch, ser + deser)
  - `MappingDefinition` + inner records: ~120 lines
  - `DatabaseDefinition` + inner records: ~80 lines
  - `ConnectionDefinition`, `RuntimeDefinition`, `ServiceDefinition`: ~140 lines
  - `FunctionDefinition`, `ClassDefinition`, `AssociationDefinition`, `EnumDefinition`, `ProfileDefinition`: ~80 lines
  - Deserializers: mirror of serializers (~300 lines included above)
- `ShapeCompat.isUpcastCompatible(old, new)` — structural backward compat check
- Test: compile sales-trading model → serialize → deserialize → verify round-trip + FQN function bodies + FQN M2M expressions

**3c: UsedSymbols extraction** (~0.5 session)
- `UsedSymbolExtractor` — walks resolved definitions to collect cross-dep symbol refs (~100 lines total):
  - Class/enum/association defs: trivial record walk (~20 lines)
  - Mapping defs (relational): trivial record walk (~15 lines)
  - Function bodies (resolved AST): AST walker collecting refs (~40 lines)
  - M2M expressions: reuses same AST walker (~10 lines)
  - Symbol origin grouping: HashMap bookkeeping (~15 lines)
- Integrated into `ArtifactCompiler.compile()`
- Test: verify correct symbol grouping by dependency origin

### Step 4: Project Loading + Diamond Resolution (1-2 sessions)
**Files**: `PureProject.java`, `ProjectLoader.java`, `WavefrontResolver.java`, `DiamondResolver.java` (all new)

- `PureProject` — loads `pure-project.yaml` (parses direct `dependencies` list, artifact repo URL, optional wavefront pin)
- `WavefrontResolver` — parses wavefront.yaml, walks `deps` graph from user's direct deps, computes full transitive closure, returns load-ordered list (leaves first)
- `ProjectLoader` — orchestrates the full flow:
  1. Read manifest → get direct deps + repo URL
  2. Sparse checkout wavefront.yaml
  3. `WavefrontResolver.resolve(directDeps)` → full closure with versions + load order
  4. Sparse checkout all needed artifact directories (single `git sparse-checkout add` call)
  5. Deserialize `.legend` files → `LegendArtifact` records
  6. Call `model.addDependency(artifact)` in load order (leaves first)
- `DiamondResolver` — upcast + change token verification (used by CI validation only; consumers using wavefront never hit diamonds)
- Tests: transitive resolution, load ordering, compatible diamond, incompatible diamond, collision detection, cycle detection in deps graph

### Step 5: CLI Tools + Wavefront CI (1-2 sessions)
**Files**: `LegendCli.java` (new), `.github/workflows/validate.yml` (for artifact monorepo)

- `legend init` — create `pure-project.yaml`
- `legend compile` — produce `.legend` artifact with shapes + usedSymbols
- `legend publish` — PR to artifact monorepo
- `legend publish --check-compat` — compare shapes vs previous, flag breaking changes
- `legend deps sync` — sparse-clone/update artifact monorepo
- CI workflow: validate wavefront on every PR to artifact monorepo

### Step 6: Frontend Multi-File Support (2 sessions)
**Files**: Studio Lite frontend changes

- File tree sidebar, tab-per-file editing
- Each file tracked via LSP protocol
- Model rebuilt from all files on change (Step 2 backend)

---

## What This Does NOT Include (Out of Scope)

- **Incremental compilation** — rebuild-from-scratch + parse cache handles 90% of redundant work
- **Registry server** — monorepo + sparse checkout is simpler and faster
- **Hot reloading** — update dependency = discard dep layer + reload via `addDependency()` (no live-swap)
- **Access control** — all exports are public; `internal` visibility is future work
- **Version ranges** — wavefront provides the version set; no per-dependency ranges
- **ValueSpecification serialization** — NOT needed in the artifact. We serialize parser output (definition records), not compiler output (typed AST). However, `PureExpressionPrinter` renders the resolved AST back to FQN source strings at `legend compile` time — so function bodies in artifacts use fully qualified names regardless of the original file's import scope.

---

## Performance Impact

| Scenario | Today | After |
|---|---|---|
| Edit 1 file in 50-file project (LSP) | Parse all 50 (~500ms) | Parse 1 + 49 cache hits (~15ms) |
| First dependency download (5 of 50 packages) | N/A | Sparse clone ~1MB, <2s |
| Daily dependency update | N/A | `git pull` ~0-200KB, <1s |
| Self-contained project (no deps) | Same as today | Same + ParseCache speedup (zero overhead) |
| Open project with 3 deps (1000 classes each) | N/A | Load 3 artifacts ~5ms |
| Query referencing dependency class | N/A | `findClass` → shape lookup ~1μs |
| Diamond resolution (CI, 50 artifacts) | N/A | ~1ms (set intersections) |
| StressTestChaotic second run (same JVM) | ~2.3s parse+build | ~7ms (100% cache hit) |
| Change token overhead at TypeCheck time | N/A | ~0 (HashMap.put on existing lookups) |
