# Rosetta Implementation Checklist

Filtered view of all ❌/⚠️ features that need implementation, ordered by priority tier. Each links to its Rosetta Stone doc with the full proposed Relation API design.

Status key: ❌ = not implemented, ⚠️ = partial, 🔧 = needs grammar addition

---

## Tier 2: Core Missing Features

| # | Feature | Status | Rosetta Doc | What to Build |
|---|---------|--------|-------------|---------------|
| B8 | Set ID lookup | ⚠️ | [identity-extends](rosetta/identity-extends.md) | Set ID queryable at runtime for union/inline/otherwise refs |
| B9 | Extends | ❌ | [identity-extends](rosetta/identity-extends.md) | Builder processes `extends` clause; `redefine()` function for overrides |
| C2 | Store substitution | ❌ | [includes-substitution](rosetta/includes-substitution.md) | Parameterized store accessor `#>{$store.T}#` |
| B10 | Scope blocks | ❌🔧 | [class-directives](rosetta/class-directives.md) | Grammar rule for `scope` keyword (Relation API: not needed — syntax sugar) |
| C5 | Local properties | ❌ | [local-properties](rosetta/local-properties.md) | Builder recognizes `+prop` prefix; MappingNormalizer emits `extend()` |

## Tier 3: Database Features

| # | Feature | Status | Rosetta Doc | What to Build |
|---|---------|--------|-------------|---------------|
| B6 | ~primaryKey | ❌ | [class-directives](rosetta/class-directives.md) | Parser: `#>{db.T \| [pk]}#` syntax for PK annotation |
| B3 | ~filter via join | ❌ | [class-directives](rosetta/class-directives.md) | Filter-on-traverse → EXISTS subquery in PlanGenerator |

## Tier 4: Advanced / Grammar Additions

| # | Feature | Status | Rosetta Doc | What to Build | Effort |
|---|---------|--------|-------------|---------------|--------|
| E6 | Union | ❌ | [union-merge](rosetta/union-merge.md) | Parse `Operation { union(...) }`; emit `->concatenate()` | Medium |
| E7 | Merge | ❌ | [union-merge](rosetta/union-merge.md) | Parse `Operation { merge(...) }`; emit `->join()` + coalesce | Medium |
| A9 | Binding | ❌ | [binding-transformer](rosetta/binding-transformer.md) | `toVariant()` + `get()` for JSON field extraction | Medium |
| A10 | Source/Target IDs | ❌ | [xstore-crossdb](rosetta/xstore-crossdb.md) | Named functions solve this; builder needs set ID resolution | Low |
| E3 | XStore | ❌🔧 | [xstore-crossdb](rosetta/xstore-crossdb.md) | Grammar + executor federation layer | Hard🔧 |
| E4 | AggregationAware | ❌🔧 | [aggregation-aware](rosetta/aggregation-aware.md) | `->aggregate()` function; optimizer intercepting `groupBy()` | Hard🔧 |
| E5 | Relation mapping | ❌🔧 | [relation-class-mapping](rosetta/relation-class-mapping.md) | Grammar: `Relation` keyword + `~func`; emit rename chain | Medium🔧 |
| G | Milestoning | ❌ | [milestoning](rosetta/milestoning.md) | `->asOf()` function; traverse propagation; table metadata | Hard |
| A11 | Cross-DB refs | ❌ | [xstore-crossdb](rosetta/xstore-crossdb.md) | Same infrastructure as XStore federation | Medium |
| D10 | TabularFunction | ❌ | [database-objects](rosetta/database-objects.md) | Parameterized store accessor `#>{db.func(args)}#` | Low |
| D11 | MultiGrainFilter | ❌ | [database-objects](rosetta/database-objects.md) | Just `->filter()` with discriminator — trivial | Low |

---

## Summary

| Category | ✅ Done | ❌/⚠️ Remaining |
|----------|---------|-----------------|
| A. Property Mappings | 8 | 3 (A9, A10, A11) |
| B. Class Directives | 5 | 4 (B3, B6, B9, B10) + 1 partial (B8) |
| C. Mapping-Level | 3 | 2 (C2, C5) |
| D. Database Objects | 9 | 2 (D10, D11) |
| E. Class Mapping Types | 2 | 5 (E3, E4, E5, E6, E7) |
| G. Milestoning | 0 | 1 |
| **Total** | **27** | **17** (+1 partial) |

**Key dependency chains**:
- B9 (extends) depends on B8 (set ID lookup)
- C2 (store substitution) depends on parameterized function support
- E3/A11 (xStore/cross-DB) both need executor federation infrastructure
- E4 (AggregationAware) is a query optimizer — highest complexity
