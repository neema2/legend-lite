# The eager probe's residue, taken apart (2026-10-07)

`//spec:eager_corpus_compile` types every body of the corpus's world: 9,942 bodies, 1,620 fail (build 1.8 s, typing 2.0 s;
`EAGER_COMPILE_PROFILE_2026_10_07.md`). The probe sorts the failures into families; `residue_shapes.py` groups the
bodies by the shape of their message (names and numbers replaced), so one row stands for every body that fails the same
way. The 365 residue bodies are `eager-residue-2026-10-07.txt` in this folder.

## The families (the probe's own sort)

| family | bodies | what it is |
|---|---|---|
| WALLED `/protocols/pure/` | 676 + 146 | the engine's JSON protocol serializers, one copy per protocol version; the platform speaks its own protocol, so these are never served |
| TEST bodies | 428 | the roster runs them; not this probe's concern |
| WALLED `/autogeneration/` | 5 | relational-to-Pure model autogeneration, not served |
| **RESIDUE** | **365** | ours: a typer gap, a fixture file never admitted, or a file to wall by name; 319 of them in `meta::relational`, 39 in `meta::pure` |

Across all 1,620 (73 shapes): "unknown function" 611, "unknown type" 486, "unknown enumeration" 182 — the walled
families, by design (822 of the 1,620 are `meta::protocols`).

## The residue by shape (365 bodies, 45 shapes)

| bodies | shape | what it says about the compiler |
|---|---|---|
| 220 | `unknown function '_' — no function of this name in the native or user catalog (unported platform …)` | platform functions never ported: the cut list and the manifest world (D9, W4.4a), not a typer gap |
| 26 | `validate is desugared before typing (ValidateDesugar): a validate call reached the typer` | a desugar that runs before typing and misses these forms: the interleaving of desugaring and typing (W2.3a push 2a, W3.6) |
| 25 + 4 | `'_' is not a known class, mapping, runtime, connection, or database` | a function referenced as a value by its mangled name (e.g. `internalize_Class_1__Binding_1__String_1__T_MANY_`) and elements never admitted; element references by id (W2.6) and the manifest world |
| 12 + 9 | `no overload of '_' accepts N argument(s)` / `matches N argument(s) of these shapes (no candidates at all)` | the overload rule and the catalogue (W3.2, W3.3, W1.13) |
| 8 | `Unknown type: '_' is not a known primitive, class, or enum` | the manifest world |
| 6 | `expected a function-typed parameter, got LambdaFunction<Any>` | a lambda typed before its expected type is known: the solver carries the expected type into the lambda (W3.3's rule table, "reverse inference") |
| 6 | `walled body '_': the engine's compiler is the implementation` | walled by name |
| 4, 3, 2, 2, 2, 1, 1, 1 | `renameColumns expects literal pair(…)`, `generateTestData needs its query lambda and mapping reference INLINE`, `tableReference expects (database, [...])`, `flatten expects (source, ~column)`, `mayExecuteLegendTest needs a zero-arg fallback thunk`, `join expects (rel1, rel2, JoinKind, {t,v|cond})`, `from() argument N must be a mapping or runtime reference, got TypedNewInstance`, `cast expects (source, @Type)` | **shape checks**: a checker matches the argument's syntax and refuses the same value written another way (a let-bound column spec, a variable holding a mapping); a compiler types values whatever their spelling; the few that need a compile-time value belong inside D8's fence (W4.2), the rest become typed arguments (W3.1, W3.3, `CallShapes` in W2.3a push 2a's list) |
| 3, 3, 1, 1, 1, 1 | `type variable U bound to Class<Any>`, `multiplicity [*] is not compatible with [N]`, `no common supertype for U and V`, `cannot access '_' on V`, `expected Alias, got V`, `expected String, got List<String>` | inference gaps: the solver and the kernel's second half (W3.3, W3.5) |
| 1 each | `expected a Relation, got TabularDataSet`, `concatenate: column N cannot unite …`, `class ValidationResult has no property '_'`, `unknown class`, `unknown type in @Path`, `expected Boolean, got Pair<…>` | the TDS carrier (W3.1); the manifest world; single defects |

So of the 365, about 260 are the world (functions, types and elements the corpus's platform code names and lite does
not serve), about 60 are the typer's own shape (desugar order, shape checks, one-directional lambda typing), about 25
the overload rule and about 10 the inference kernel. The typer-shape bodies are the ones the plan's Phase 4 items
remove by construction; the world bodies are a decision (D9), not a fix.
