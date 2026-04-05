# Phase 5 & 6: Full Pipeline Implementation Plan

Implement all remaining mapping/store features end-to-end (grammar → builder → model → normalizer → resolver → planGen → SQL), covering DynaFunction expressions, scope blocks, embedded/inline/otherwise mappings, association mappings, set IDs, includes, views, self-joins, local properties, grammar additions (XStore, AggAware, Relation), binding transformer, milestoning, and union/merge.

---

## Current State (What's Already Done)

| Feature | Extraction | Model | Normalizer | Resolver | PlanGen |
|---------|-----------|-------|------------|----------|---------|
| 5.1 Join chains + join types | ✅ | ✅ | ✅ | ✅ | ✅ |
| 5.7 Association mappings | ✅ | ✅ (partial) | ✅ | ✅ | ✅ |
| 5.8 Mapping filters | ✅ | ✅ | ✅ | ✅ | ✅ |
| 5.9 Set IDs / root | ✅ extracted | ✅ stored | — | ⚠️ no lookup | — |
| 5.10 Includes | ✅ extracted | ✅ `resolveMappingIncludes` | — | — | — |
| 6.3 Complex join conditions | ✅ | ✅ | ✅ | ✅ | ✅ |

---

## Implementation Order

Features grouped by dependency. Each group is independently testable. Tests written BEFORE implementation per project convention.

### Group A: DynaFunction Expressions (5.2) — HIGH PRIORITY

**Why first**: Most impactful. Infrastructure already exists — just needs wiring.

**Current state**: Grammar parses `mappingFunctionOperation`. Builder has `buildMappingFunctionOperation` → `RelationalOperation.FunctionCall`. But `visitMappingOperation` falls back to raw string. `RelationalMappingConverter` already converts `FunctionCall` → `AppliedFunction` ValueSpec.

**Pipeline**:

1. **Builder** (`PackageableElementBuilder.visitMappingOperation`): Add branch for function operations before the fallback. Use existing `buildMappingOperation(ctx)` to get `RelationalOperation` tree → store as `PropertyMappingDefinition.mappingExpression()`.
   - Also handle column operations without database pointer that currently miss (bare function calls like `concat(...)`)

2. **PureModelBuilder** (`addMapping`): When `pm.hasMappingExpression()`, convert `RelationalOperation` → `ValueSpecification` via `RelationalMappingConverter.convert()`, then create a new `PropertyMapping` variant that carries the expression.
   - New: `PropertyMapping.dynaExpression(name, columnName, ValueSpecification expr)`

3. **MappingResolver** (`resolveRelational`): New `PropertyResolution.DynaExpression(ValueSpecification expr)` — stamps TypeInfo on the expression nodes (like M2M expressions).

4. **PlanGenerator** (`resolveColumnExpr`): New case for `DynaExpression` → calls `generateScalar(expr, "$row", store, alias)`.

5. **Tests**: Add E2E test in `RelationalMappingIntegrationTest`: `concat(T.FIRST, ' ', T.LAST)`, `substring(T.NAME, 1, 3)`, nested functions like `upper(concat(...))`.

**Key insight**: This is NOT Pure DynaFunc syntax. It's the relational mapping operation syntax: `functionName(arg1, arg2, ...)` where args are columns, literals, or nested function calls. The function names (`concat`, `substring`, `trim`, `upper`, `lower`, `parseInteger`, `toString`, `case`, `if`) map through `RelationalMappingConverter` → `AppliedFunction` → TypeChecker → PlanGenerator → Dialect.

---

### Group B: Scope Blocks (5.3) — Parse-time desugaring

**Current state**: Grammar gap — `relationalPropertyMapping` in composed `PureParser.g4` only has `localMappingProperty | standardPropertyMapping`. The scope rule exists in the raw relational grammar but wasn't composed.

1. **Grammar** (`PureParser.g4`): Add `scopePropertyMapping` alternative to `relationalPropertyMapping`:
   ```
   relationalPropertyMapping: localMappingProperty | standardPropertyMapping | scopePropertyMapping
   scopePropertyMapping: SCOPE PAREN_OPEN databasePointer mappingTableRef? PAREN_CLOSE
       PAREN_OPEN (relationalPropertyMapping (COMMA relationalPropertyMapping)*)? PAREN_CLOSE
   ```

2. **Builder**: `visitRelationalClassMappingBody` — when `scopePropertyMapping` found, desugar: prepend `[DB] TABLE.` to each inner property mapping. Pure parse-time transformation — no runtime changes needed.

3. **Tests**: Enable the 2 @Disabled scope tests in `MappingDefinitionExtractionTest`.

---

### Group C: Embedded Mappings (5.4)

**Current state**: Grammar rule `embeddedPropertyMapping` exists and parses. Builder falls back to raw string at `visitRelationalPropertyValue` line 1583.

1. **Model**: Add `PropertyMapping.embedded(name, List<PropertyMapping> subMappings)` factory. Add `EmbeddedMapping` record or field to PropertyMapping.

2. **Builder**: `visitRelationalPropertyValue` — when `embeddedPropertyMapping` is present, recursively extract sub-property mappings (each is a `relationalPropertyMapping`) and create structured `PropertyMappingDefinition` with embedded sub-properties.

3. **PureModelBuilder**: When property has embedded sub-mappings, create `PropertyMapping.embedded(...)` → register sub-property column mappings flattened under the parent property namespace (e.g., `firm.legalName` → `FIRM_NAME`).

4. **MappingResolver**: For embedded properties, create a nested `StoreResolution` or flatten the sub-properties into the parent's `propertyToColumn` with dotted keys.

5. **PlanGenerator**: In `resolveColumnExpr`, handle embedded by looking up `parentProp.childProp` in the store.

6. **Tests**: E2E test with `firm(legalName: T.FIRM_NAME, employeeCount: T.EMP_COUNT)` and query `$p.firm.legalName`.

---

### Group D: Inline Mappings (5.5)

**Current state**: Grammar rule `inlineEmbeddedPropertyMapping` parses `() Inline[setId]`. Builder falls back.

1. **Model**: `PropertyMappingDefinition` with inline target set ID. New factory: `PropertyMappingDefinition.inline(name, targetSetId)`.

2. **Builder**: Extract `targetSetId` from `inlineEmbeddedPropertyMapping` rule.

3. **MappingResolver**: On inline property, look up the referenced set ID's class mapping. Substitute its property mappings in place (like copy-paste from the referenced mapping).

4. **Tests**: Two mappings for `Firm` (one with setId `firm_set1`), Person property `firm() Inline[firm_set1]`. Query `$p.firm.legalName`.

---

### Group E: Otherwise Mappings (5.6)

**Current state**: Grammar rule `otherwiseEmbeddedPropertyMapping` parses. Builder falls back.

1. **Model**: `PropertyMappingDefinition` with embedded part + fallback (setId + join chain).

2. **Builder**: Extract embedded sub-properties + `Otherwise([fallbackSetId]: [DB]@Join)`.

3. **MappingResolver**: Resolve embedded part. For the otherwise fallback, resolve the join to the fallback mapping. Produce a combined resolution.

4. **PlanGenerator**: Emit COALESCE or LEFT JOIN pattern: try embedded first, fall back to joined mapping.

5. **Tests**: E2E test with otherwise pattern.

---

### Group F: Set IDs + Root + Extends (5.9)

**Current state**: Extraction works. `RelationalMapping` stores `setId` and `isRoot`. Extends stored but not wired.

1. **MappingRegistry**: Add `findBySetId(mappingPath, setId)` lookup. Add `findRootMapping(mappingPath, className)` (returns the `*`-marked mapping, or first if none marked).

2. **MappingNormalizer**: When resolving, use root marker to pick default mapping. Wire `extends` — child mapping inherits parent's property mappings (override by name).

3. **MappingResolver**: Use set ID from `sourceAndTargetMappingId` on property mappings to pick correct source/target mapping when multiple exist.

4. **Tests**: Multiple set IDs for same class. Root marker. Extends with property override.

---

### Group G: Mapping Includes + Store Substitution (5.10)

**Current state**: Extraction works. `resolveMappingIncludes` copies class mappings. Store substitution not applied.

1. **PureModelBuilder**: In `resolveMappingIncludes`, when store substitutions exist, replace database references in included mapping's property mappings (column refs, join refs) with the substitute store.

2. **Tests**: Include a base mapping, override store. Verify SQL uses substituted table names.

---

### Group H: Views as Data Sources (6.1)

**Current state**: View extraction and model registration done.

1. **PureModelBuilder**: Allow `~mainTable` to resolve to a View (not just Table). View becomes a subquery source.

2. **MappingNormalizer**: When source is a View, synthesize a subquery `ValueSpecification` from the view's column mappings, filter, groupBy.

3. **PlanGenerator**: Render view as inline subquery in FROM clause.

4. **Tests**: Database with View, mapping referencing view as mainTable.

---

### Group I: Self-Joins (6.2)

**Current state**: `TargetColumnRef` in `RelationalOperation`. `RelationalMappingConverter.convert()` maps `{target}` → `Variable("__target__")`. Join condition architecture already handles complex conditions.

1. **MappingNormalizer**: Self-join detection — when join references the same table, generate a traverse with self-alias.

2. **MappingResolver**: Self-join produces `JoinResolution` where source and target are same table, different aliases.

3. **PlanGenerator**: Already handles via `generateScalar` with variable aliasing.

4. **Tests**: Hierarchical data (employee → manager via self-join).

---

### Group J: Local Mapping Properties (6.4)

**Current state**: Builder extracts property name but loses `+` prefix, type, and multiplicity.

1. **Model**: `PropertyMappingDefinition` gets `isLocal`, `localType`, `localMultiplicity` fields.

2. **Builder**: Extract `+` prefix, type from `qualifiedName`, multiplicity from `localMappingProperty` grammar rule.

3. **MappingResolver**: Local properties are available in the mapping's store resolution but not visible externally. Used for XStore expressions.

4. **Tests**: Local property extraction and query.

---

### Group K: Grammar Additions (6.5, 6.6, 6.7)

#### 6.5 XStore

1. **Grammar**: Add `XSTORE` to `classMappingType`. XStore body has Pure expressions with `$this`/`$that`.
2. **Builder/Model/Resolver/PlanGen**: Cross-store correlation via subquery or temp table pattern.

#### 6.6 AggregationAware

1. **Grammar**: Add `AGGREGATION_AWARE` mapping type with `~views`, `~mainMapping` sub-directives.
2. **Builder/Model**: Extract aggregate view definitions and main mapping fallback.
3. **Resolver/PlanGen**: Query routing — match query shape against aggregate views.

#### 6.7 Relation Mapping

1. **Grammar**: Add `RELATION` mapping type with `~func` directive.
2. **Builder/Model**: Extract function reference.
3. **Resolver**: Resolve function → Relation type → column types.
4. **PlanGen**: Call function, use result as source relation.

---

### Group L: Binding Transformer (6.8)

1. **Builder**: Extract `Binding qualifiedName` from `bindingTransformer` rule → store as structured field on `PropertyMappingDefinition`.
2. **Model/Resolver**: Binding-based deserialization (JSON/XML column → object).
3. **PlanGen**: Emit JSON extraction pattern.

---

### Group M: Milestoning (6.9)

1. **Model**: Capture milestoning spec from table definition (business/processing dates).
2. **Resolver**: Auto-inject WHERE clause for temporal date ranges.
3. **PlanGen**: Add milestoning predicates to generated SQL.

---

### Group N: Union/Merge (6.10)

1. **Grammar/Builder**: Multiple mappings for same class treated as union.
2. **Resolver**: Combine StoreResolutions via UNION ALL pattern.
3. **PlanGen**: Emit `SELECT ... UNION ALL SELECT ...`.

---

## Execution Order

Priority order (Groups A-G are Phase 5, H-N are Phase 6):

1. **Group A** (DynaFunction) — highest impact, most infrastructure already exists
2. **Group B** (Scope blocks) — pure parse-time, quick win
3. **Group C** (Embedded) — important pattern, needed for D-E
4. **Group D** (Inline) — depends on F (set IDs)
5. **Group F** (Set IDs/root/extends) — needed by D, G
6. **Group E** (Otherwise) — depends on C
7. **Group G** (Includes + store sub) — low risk
8. **Groups H-N** (Phase 6) — advanced, can be ordered flexibly

## Testing Strategy

- Each group gets integration tests in `RelationalMappingIntegrationTest` BEFORE implementation
- Tests start @Disabled, get enabled as features land
- Each test validates full pipeline: Pure source → query → SQL → DuckDB execution → result
