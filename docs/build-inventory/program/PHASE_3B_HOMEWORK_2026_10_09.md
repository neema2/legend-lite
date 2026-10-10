# Phase 3b homework, measured (2026-10-09)

The 3b brief (`PHASES_3B_6.md` §3b.10) asks seven questions before code. Three are answered here from the code and
two census runs on the day Phase 3 landed; the rest (H1, H2, H4, H5) need probes on the census classpath and are
listed with how to run them. Nothing here changes the brief; the 3b plan is agreed with the user from it.

## H7 — Does `//spec:manifest_world_census` pass after Phase 3? No, and it did not pass before it either

Run on Phase 3's rebased tip (1e4a2bd40) and on main's code (1c05e2946), both on 2026-10-09:

| | main 1c05e2946 | Phase 3 tip 1e4a2bd40 |
|---|---|---|
| load walls (ceiling 32) | **37** | **37** (the same 37, file for file) |
| failing bodies (ceiling 1,447) | 1,476 | **1,437** |
| bodies OK | 15,648 | 15,735 |
| walled | 32 | 32 |

The 37 walls, by kind: 18 mapping files the compiler does not support (16 model-to-model roadmap features, one
association without a column binding, one embedded property mapping whose owner is unknown; the brief's "18 files",
decision "no change"); 7 duplicate definitions (the 5 boot-layer twins and the 2 F-L1 views: 3b items 1b and 1a);
7 unknown types (4 units of measure, `Mass~Kilogram` and `meta::pure::unit::Mass`; 3 import-scope cases, `Runtime`
and `Mapping`: 3b item 5b); 5 parse walls (`m3.pure`'s top-level `^Instance`, the `;` property-mapping separator
twice, `->` in `testLambda.pure`, trailing tokens in `simpleObject.pure`: the Phase 6 brief's parser gaps).

So the ceiling of 32 was stale on main before Phase 3 (the test is `manual` and in no lane; nobody re-ran it after
the landings that added the 5 parse walls to the count, most likely W0.8's in-place island lexing and the protocol
program's parser changes, to confirm by bisection if it matters). Phase 3 changes no wall and types 39 more bodies.
**What 3b's step 1 does with it:** re-pin the ceilings to the measured 37 and 1,437 with this table as the reason, in
the 3b branch's first commit, so the census is a guard again; then the 3b items lower the walls (expected 37 − 5
twins − 2 F-L1 − 3 import scopes = 27, with the 18 mapping files, 4 units and 5 parse walls left).

## H6 — legend-pure's rule for a dot call that could be a qualified property (3b item 4; PARK-14)

Read in legend-pure 5.99.0, `m3/compiler/postprocessing/processor/valuespecification/FunctionExpressionProcessor.java`
and `m3/navigation/_class/_Class.java`:

- The parser marks a dot call WITH arguments as a qualified-property call (`_qualifiedPropertyName`). The processor
  takes the receiver's class, collects every qualified property of that name through the class's generalization
  order (`_Class.findQualifiedPropertiesUsingGeneralization`: `Type.getGeneralizationResolutionOrder`, the C3 order),
  and runs the ordinary function matcher over them (`FunctionExpressionMatcher.getFunctionMatches`) with the receiver
  as the first argument, the type-variable values next, then the call's arguments
  (`findFunctionsForQualifiedPropertyBasedOnMultiplicity`). No match: it throws "no match" (`throwNoMatchException`).
  It never looks at a plain property of the same name and never falls back to a function `name($x, args)`.
- A dot call WITHOUT arguments (`$x.name`) looks for a plain property first through the generalizations
  (`class_findPropertyUsingGeneralization`); only if none exists does it try the qualified properties, and then only
  one that takes no explicit argument (`findSingleArgumentQualifiedProperty`), else an error naming what it found
  ("requires some parameters", or the milestoning-date messages).
- A to-many receiver is rewritten first (`reprocessPropertyForManySources`: the auto-map), then processed as above.

What item 4 ports: one lookup "qualified properties named N through the generalization order", matched by the
arguments with the receiver first, independent of any same-named plain property; `$o.x('a')` is the qualified `x`,
`$o.x` the stored one; the match by arity and types is the kernel's, not a second rule. PARK-14's answer: a dot call
with arguments and no matching qualified property is refused as legend-pure refuses it (3b-O1's register row for the
difference goes away with the fallback).

## H3 — Can the name resolver rewrite a view? Yes (3b-O3: option (b))

Upstream's grammar (`RelationalParserGrammar.g4`): `viewColumnMapping: identifier (BRACKET_OPEN identifier
BRACKET_CLOSE)? COLON operation PRIMARY_KEY?`, and `operation` reaches `joinOperation: databasePointer? joinSequence
...` and the dynaFunction forms; `viewFilterMapping: FILTER_CMD (viewFilterMappingJoin | databasePointer)? identifier`.
Our parser reads the same (`DatabaseProtocolParser.parseView`: the `[db]`-qualified join-mediated filter, columns as
relational operations). A view's columns and filter can therefore name another database's joins, which the resolver
rewrites, so a resolved view is not the same object as its parsed one, and the "one line in `ModelBuilder`" fix holds
only while the flat and per-schema lists share objects. 3b-O3 takes (b): `NameResolver.resolveDatabase` resolves each
view once and rebuilds the flat list from the resolved objects; the test is a schema view whose column reaches a
`[db]`-qualified join.

## H1, H2 and H4 — measured by `//spec:phase3b_probes` (manual; `Phase3bProbesTest`; tables in `evidence/phase3b/`)

The probes load the same 27-module closure as the census (`ManifestWorldCensusTest.closure`) and parse it whole;
H4 parses each file alone, because the whole-module parse keeps one file per full name (the very bug it measures).

**H1 — the twin cause, confirmed.** 28 names are declared by both the system metamodel and the closure. The closure
holds 43 upstream versions of them: **29 have the same function id as a system version but different parameter-type
spellings** (bare `String` against `meta::pure::metamodel::type::String`, `EnumerationMapping` against
`EnumerationMapping<T>`, and the like), so `SystemMetamodel.shadows` does not see them as twins; 3 have the same id
and the same spellings (`dataTypeToSqlText`, `inferPrimaryKeyColumnNames`, one `extractDBs`), which it does; 14 are
other versions with ids of their own (`phase3b-h1-twins.tsv`). The duplicate-definition walls are the 29 that fall in
files the loader cannot split.

**H2 — the other versions, by id** (`phase3b-h2-versions.tsv`, the 14 rows with "twin of a system version" false):
`propertyMappingsByPropertyName` over `OtherwiseEmbeddedSetImplementation`, `AggregationAwareSetImplementation` and
`EmbeddedSetImplementation` (3); `inferRelationalType` with a `TranslationContext`, with a `Boolean` and a context, and
with a `Boolean` alone (3); `relationTreeAsString` with a separator and with a `Boolean` (2); `extractDBs` over
`Mapping[*]` and over two mappings (2); `superMapping` over `PropertyMappingsImplementation` (1); `resolvePrimaryKey`
over `RelationalInstanceSetImplementation`, `RelationFunctionInstanceSetImplementation` and
`InstanceSetImplementation` (3). Each is a decision in item 1b: a row ("runs as the platform's version", or upstream's
body runs), a refusal, or a signature fix in the system metamodel (`superMapping`'s return type is the known one).

**H4 — item 5b's blast radius in the closure.** 19 function names are declared in more than one file; **18 of them
from files whose `import` lines differ**, so today all their overloads resolve with the last file's imports:
`from`, `routeFunction`, `execute`, `joinStrings`, `assert`, `resolvePrimaryKey`, `toLowerFirstCharacter`,
`toUpperFirstCharacter`, `buildConcatenate`, `loadCsvDataToDbTable`, `loadValuesToDbTable2`, `setUpDataSQLs`, the
four `toDDL` statement functions, and two upstream tests (`phase3b-h4-imports.tsv`). Those are the places whose
resolution may change when the map is keyed per element; the corpus passes, PCT and the reference lane judge them.

**H5 — measured with item 3 (2026-10-09; `core/src/test/java/com/legend/exec/MissingValueInComputedColumnTest.java`).**
A computed column over a missing value (`$c.STR->in(['a'])`, `$c.N > 3` in an `extend`) is `false` on DuckDB and on
H2: the lowering guards the missing value (`coalesce(x IN (...), FALSE)`, `x IS NOT NULL AND x > 3`), which is
legend-pure's own value for its `[0..1]` body over an empty argument (read from upstream's bodies, not run);
legend-engine's plain SQL (`x IN (...)`, `x > 3`) would give NULL — a claim from its SQL shape, not run here. In a filter the rows are the same. So the `[1]`
typing changes no value against legend-pure: recorded as SEMANTICS_REGISTER S40. For `max` over integers the
difference is the result column's declared type (`Number` in legend-pure, `Integer` here; the values are the same):
recorded as S41; what Studio and DataCube display for the two type names was not measured (they show the column's
values the same way).

The census ceilings were already known to be stale (the 3b brief's line 15 and `PHASE_8.md` Short-21, "no owner");
they are re-pinned at 37 and 1,437 in this branch's first commit, and `//spec:manifest_world_census` passes again.

## Decisions these answers point at (for the 3b plan)

| Decision | Answer from the homework |
|---|---|
| 3b-O1 twins by id | (b), the "platform's own Pure" row kind (decision 1 of the plan; H1 to confirm the cause, H2 to decide each version) |
| 3b-O2 import scopes | (a), per element: three of the 37 walls are this bug, and a multi-file user project meets it |
| 3b-O3 F-L1 | (b): the grammar allows a rewritten view |
| PARK-14 | refused as legend-pure refuses it (H6) |
| the census ceilings | re-pinned to the measured 37 and 1,437 in 3b's first commit, with the table above |
