# Audit 22b — Identity Emissions + Wire/Comparator (ee28e683..f4510b93 + working tree)

**VERDICT.** The identity-by-emission machinery audited here is directionally sound and its
walls are mostly loud, but the audit sustains one HIGH and four MED findings, all of the same
family: an identity shortcut or comparator gate justified by "the contract is enforced where
the value is consumed" (or "the corpus shape doesn't need it"), where probing shows the
compensating guard covers ONE consumer of many. Concretely: (1) the rows-level `toOne()`
identity (4759f08d) is enforced only by the SCALAR shape's second-row guard — probes show a
2-row relation flows through `->toOne()` as a whole 2-row TABULAR result and `->toOne()->size()`
answers 2, both silent, and the ZERO-row lower bound is enforced nowhere (engine `toOne`
throws in every one of these); (2) the flat-cells comparator gate (ee28e683) drops the
column-name pin when EITHER side is flat and degrades two-TDS compares to a loose cell
multiset that accepts cross-row cell shuffles — both probed PASS where engine equality is
false; (3) the `'default'`-schema collapse (fab9aa24) routes through `findTableDef`'s
bare-name any-schema fallback and resolves tables the engine's
`schema('default')->table(t)->toOne()` would loudly refuse; (4) the include-direction
re-synthesis (f338b644) `bound.add` first-wins silently suppresses `findBinding`'s own
documented two-includes ambiguity wall. The working-tree self-catches are both verified
correct and complete on their own terms (the VARIANT-root gate holds; the tabular path never
used `cell()`; the `[0..1]` β-reduction gate is right), with one LOW residual: a variant-typed
ELEMENT under an Any root still decodes unconditionally, erasing the Variant identity.
No finding invalidates the FAIL-ledger claims of the audited commits; the comparator findings
weaken the gate prospectively (they can mask FUTURE wrong rows), not retroactively.

Probes: `/Users/neema/.claude/jobs/9693939d/tmp/W1.java`, `W2.java`, `W3.java`
(run against `core/target/classes`, DuckDB in-memory).

---

## F1 — HIGH — 4759f08d: rows-level `toOne()` identity is guard-covered in ONE consumer; the claim "enforced where consumed" is wrong for the rest

`Lowerer` (relation position) lowers `toOne()` over a relation-typed arg to the relation
itself, citing "the exactly-one contract is enforced where the value is CONSUMED (the
executor's scalar second-row guard)". Census of consumers, probed (W1/W3):

| consumer | probe | ours | engine | verdict |
|---|---|---|---|---|
| scalar column read `->toOne().NAME`, 2 rows | W3 | LOUD (DuckDB scalar-subquery error; executor guard is a second net) | throws | covered |
| TABULAR root `->toOne()`, 2 rows | W1 | **silent 2-row TDS** | `toOne` throws | NOT covered |
| `->toOne()->size()`, 2 rows | W1 | **silent `2`** | throws (a `[1]` can only size 1) | NOT covered — silent wrong VALUE |
| `->toOne()` over EMPTY relation | W1 | **silent empty TDS / size 0** | throws (lower bound) | NOT covered |
| zero-row scalar read | code | `Scalar(null)` — Executor.java:71-83 guard checks only a SECOND row, never zero | throws | NOT covered — the guard is upper-bound-only |
| aggregates folded to SQL (`size`/`count`/`isEmpty` over the identity) | W1 | count over the relation | 1-or-throw | NOT covered by construction (executor never sees the rows) |
| GRAPH shape (Executor.java:100-101) | code | first row only, no second-row check | n/a (type system likely prevents relation→graph) | uncovered if ever reachable |

Every divergence is of the engine-throws→ours-returns-a-value class (the two sides never both
succeed with different values), but that is exactly the class that turns a should-ERROR corpus
test into a PASS when the assert happens to match (`size()` asserts do). The commit's own
tenet — identity emission must be row/wire-identical in ALL consumers — is not met, and the
enforcement claim in both the commit message and the Lowerer comment overstates the guard.
Fix direction: either enforce cardinality in SQL for the non-scalar consumers (a windowed
count guard) or wall the identity to the consumers the guard actually covers.

## F2 — MED — ee28e683: one-sided `flatCells` drops the whole-TDS side's column-name pin

`compare` skips `gridEquals` when EITHER side is a `rows.values` spelling
(`!expected.flatCells() && !actual.flatCells()`, TestBody.java:1125-1128). Probe W2 case 2:
expected = flat cells of a TDS with columns `[A,B]`, actual = whole-TDS spelling with columns
`[X,Y]`, identical cells → **ours PASSES**; engine `assertEquals(Any[*], TDS)` is false on
type alone (control case 1, grid-vs-grid, correctly fails). The engine-verified rule
("TDSRow.values is Any[*] — column names are OUT") justifies dropping names only when BOTH
sides are flat; a mixed compare should fail. Gate the arm on the flat side only, or refuse
mixed spellings loudly.

## F3 — MED — ee28e683: flat-vs-flat compare is a LOOSE cell multiset — cross-row shuffles pass

With both sides flat, two Tabulars fall past `gridEquals` to the flat pool compare, and the
audit-9 ROW COHESION arm explicitly excludes tabular-expected sides (TestBody.java:1206-1211
`!(expected.result() instanceof Tabular)`). Probe W2 case 3: `[(1,2),(3,4)]` vs the
cross-row-shuffled `[(1,4),(3,2)]`, unsorted chain → **PASSES**. The order policy licenses row
PERMUTATIONS, not cell re-pairing across rows; engine flat-list equality (even under any row
permutation before flattening) is false here. A sorted chain compares ordered and catches it
(case 4). Extend the cohesion chunking to tabular-vs-tabular flat compares (both sides carry
their column counts).

## F4 — MED — fab9aa24: `'default'` collapse + bare-name fallback resolves tables the engine refuses

Engine ground truth pinned: top-level tables literally live in a schema named `default`
(engine RelationalParseTreeWalker.java:149; unqualified table pointers default to schema
`"default"`), and `tableReference(db, s, t)` is `$db->schema($s)->toOne()->table($t)->toOne()`
(tableReference.pure:17-20) — it looks ONLY in the named schema and throws otherwise. So the
`'default'`→bare collapse is the right namespace mapping, BUT the collapsed name goes through
`StoreCompiler.findTableDef`, whose bare arm falls back to scanning EVERY schema
(StoreCompiler.java:55-66). Probe W1: `tableReference(test::DB, 'default', 'T_SECRET')` with
`T_SECRET` declared only in `Schema hr` **resolves at compile time** (engine: loud toOne
failure) and emits the BARE name — in the probe this died as a DuckDB catalog error (loud by
accident), but when the physical table is seeded unqualified (the corpus's common setup-SQL
shape) it returns ROWS for a query the engine refuses to compile. `''` (empty schema) also
collapses and resolves (engine: loud). The 3-arg default-collapse should resolve against
top-level tables ONLY (no schema fallback); same for `''` if it is kept at all.

## F5 — LOW — fab9aa24: `tableToTDS` accepts any relation expression (engine: `Table[1]` only)

The invented-native convention IS documented at the registration site (Pure.java:1272-1275
cites the real `tableToTDS(table:Table[1]):TableTDS[1]`, tableToTDS.pure:22) and the checker
validates against the registered signature — good. But `checkTableToTds` accepts ANY
relation-typed argument: probe W1 `tableToTDS(#>{db.T}#->filter(...))` is accepted and
identity-emitted. In the engine a relation expression is not a `Table` — compile error.
Compile-leniency only (identity cannot produce wrong rows here), but per the tenet it should
be a loud wall: restrict to `TypedTableReference` sources.

## F6 — LOW — 47a8e52d: the bare-name `TabularDataSet` intercept precedes `ctx.findType`, contrary to its own comment

Typer.java:1427-1437 intercepts `"TabularDataSet"`/the FQN BEFORE `ctx.findType(name)`; the
comment says it "follows the prelude-fallback pattern below", but that pattern tries the
model space FIRST and the prelude only on miss. Bounded consequences: NameResolver FQN-izes
imported names before the Typer (NameResolver.java:337-344), so an imported user class named
`TabularDataSet` is NOT shadowed (and W3 confirms the FQN spelling `test::TabularDataSet`
resolves the user class); only a root-package class literally named `TabularDataSet` would be
shadowed, and an otherwise-unknown bare `TabularDataSet` silently becomes the platform
nominal instead of "unknown type". Move the intercept after `ctx.findType`, into the
fallback chain, to make the comment true and the exact-FQN rule (memory:
exact-fqn-identification-no-suffix-matching) hold for the bare arm.

## F7 — INFO — 47a8e52d: non-relation `cast(@TabularDataSet)` is loud only INCIDENTALLY

Probe W1: `'x'->cast(@TabularDataSet)` → "no SQL type for generic TabularDataSet<> at the
lowering boundary" — loud, as the commit claims, but the loudness comes from `GenericType`
having no SQL type, not from a cast wall, and the message does not name `cast`. Contrast
probe W3: `'x'->cast(@test::TabularDataSet)` (a CLASS target) executes as a SILENT identity
returning `'x'` (engine: runtime cast exception) — a pre-existing TypedCast behavior outside
this range, noted here because it shows the "stays loud downstream" claim is
target-type-shaped, not a property of casts.

## F8 — INFO — 47a8e52d: cast over a Relation-valued collection is loud

Probe W3: `[#>{db.T}#, #>{db.T}#]->cast(@TabularDataSet)` — the checker's identity arm fires
on the elementwise RelationType and returns the collection; lowering dies loud
("lowering not yet implemented for TypedCollection"). Not silent; no action.

## F9 — MED — f338b644: re-synthesis `bound.add` first-wins can suppress the two-includes ambiguity wall

`normalizeMapping`'s re-synthesis block (MappingNormalizer.java:339-370) seeds `bound` with
the local classes and `bound.add`-gates the closure scan: when a class is mapped in TWO
included mappings and the first in closure order qualifies (`routedTargetGainsOperation`),
it re-synthesizes a LOCAL binding — and `ClassSources.findBinding` returns local BEFORE the
include scan (ClassSources.java:475-477), so the pre-existing loud wall "class X is
ambiguously mapped via includes" (ClassSources.java:494-497) is silently bypassed;
closure-collection order becomes semantics. This contradicts findBinding's own documented
policy ("a class bound by two included mappings is a loud ambiguity … never a silent
depth-first pick") and the engine's duplicate-mapping validation error. Latent (needs the
overlap AND the Operation-gain trigger), but the fix is cheap: before re-synthesizing, check
whether ANY OTHER closure mapping also binds the class and poison/throw instead of picking.

## F10 — VERIFIED OK — f338b644 (5a): no consumer resolves the wrong synthesized fn

The re-synthesized fn is namespaced under the INCLUDING mapping
(`SynthFqn.mappingClass(md.qualifiedName(), cls)`, MappingNormalizer.java:1368-ish) — distinct
FQN from the included mapping's own synth; no collision. `findBinding` is local-first with a
loud H5 wall on local duplicates; `bound.add` prevents a local duplicate by construction.
Association resolution and set-id uses only PROPAGATE setIds (StoreResolver.java:626,736 —
no binding scan by set id); union member enumeration reads LEGACY `classMappings()`
(UnionSynthesis), which re-synthesis does not touch. No wrong-fn path found.

## F11 — VERIFIED OK + LOW residual — eca1ab92/working tree (1): the VARIANT-root gate is correct; Any-rooted variant ELEMENTS still erase

The working-tree gate (Executor.java:114-125) is verified: `toVariant(5)` as root returns the
JsonNode wire (probe W1) — the JSON-text contract holds. (b) TABULAR results never used
`cell()`: variant COLUMNS fetch through `unwrap`→`dialect.normalize` (identity for DuckDB) —
unchanged by the self-catch, contract preserved (the JsonNode IS the JSON text; a JsonNode
leaking into a numeric compare fails loud per the eca1ab92 diagnostic). (a) Residual: the
`anyRoot` branch decodes UNCONDITIONALLY — probe W1: `[1, toVariant(5)]->at(1)` → `Long 5`,
`[1, toVariant('abc')]->at(1)` → `String "abc"`: the Variant identity is erased under an Any
root. This is forced by type erasure at the Any boundary (the TO_VARIANT wrap of a variant is
idempotent — the wire cannot distinguish), and the relational engine refuses Any roots
outright ("Any is not managed yet!", PureSql.java:43-47), so no engine ROW contract is
broken — but it is NOT covered by decodeAny's documented contract ("Variant results are NOT
decoded"), which speaks only of roots. LOW: document the element-level erasure at decodeAny,
or double-encode variant elements into the Any carrier if a corpus shape ever demands
identity.

## F12 — INFO — f338b644 (6b): `tdsStringEquals` vs engine `s()` — divergences FAIL loud

Engine cell renderer pinned (core_functions_relation s.pure:23-38, toString.pure:24-35):
empty → `null` (empty VARIANT column → `"null"`), Variant → `"`-quoted with `""` doubling,
String containing `{`/`[` → quoted, else pure `toString()`; non-simple column names
single-quoted; `toString(true)` appends types/muls. Ours renders `String.valueOf` with
`null`. Checked divergences all FAIL (acceptable, loud): variant cells, empty-variant
`"null"`, brace/bracket strings, quoted column names, `toString(true)` (2-arg tail is not
stripped → loud ERROR), exotic float reprs. Matching cases match (Long↔`4`, Double↔`4.0`,
Boolean, LocalDate). Probe W2 case 5: the comma-in-cell ambiguity PASSES — but the engine's
own golden is the same ambiguous joined text, so this is parity with the engine's string
compare, not a leniency. Row multiset under unsorted chains is the standing documented order
policy; the single corpus golden of this shape (testInheritanceMultipleQueries.pure:68) sorts
first → compared exact-ordered. Structural guards (header exact, `#` framing, row count)
verified (case 6 fails correctly).

## Note — working-tree self-catch #2 (UserCallInliner) — verified in passing

The `[0..1]`→exactly-`[1]` β-reduction gate (UserCallInliner.java:391-415) is correct: a
`[0..1]` source β-reduced into a lambda body non-strict in its parameter would manufacture a
value for the EMPTY case; rebuilding the TypedMap construct is the right conservative arm.

---

### Counts

- HIGH: 1 (F1)
- MED: 4 (F2, F3, F4, F9)
- LOW: 3 (F5, F6, F11-residual)
- INFO: 3 (F7, F8, F12)
- Verified-OK: F10, F11-gate, UserCallInliner gate, plus controls (grid name pin, sorted-chain
  ordered compare, tdsStringEquals structure guards, qualified 3-arg tableReference).
