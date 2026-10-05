# Parser work — complete handoff

Written for someone with **no context**. Everything needed to pick this up is here or is
named here. Read this before `README.md` (design), `PERMISSIVENESS.md` (findings) or the
code.

Branch: **`test-corpus`**. All parser work lives there and is pushed. `main` does not have
it — a merge back was prepared but deliberately not published (see *Open decisions*).

---

## 1. What this is, in one paragraph

legend-lite is a clean-room rewrite of legend-engine's Pure parser. The question "is the
rewrite complete?" was previously answered against corpora *harvested from legend-engine's
own tests* — which can only ever cover what upstream happened to write. This work answers it
against **the grammar itself**: every keyword legend-engine declares and a user can reach.
That difference is not academic. It found fourteen constructs legend-lite does not implement,
and legend-lite's own error messages describe them as *"corpus-censused"* — its surface was
scoped to what appeared in a corpus. A corpus cannot find what it was derived from.

---

## 2. Current numbers

**2026-10-05, by Bazel over the pinned release (4.145.0; Bazel workplan P2-20):** the keyword census is
`scripts/parser/keyword-coverage.tsv` (`bazel run //scripts/parser:update_keyword_coverage`; `//:generated` fails when
it is stale) -- in scope **598 of 607**: the pinned grammars added DataSpace's `mappingProvider`, `expertise`,
`expertIds`, `coverageRegions`, `operationalMetadata`, `updateFrequency`, and tier 1 misses `trueString`,
`falseString`, `optional`. The table below is the 2026-08 measurement against a legend-engine working copy (history).

Coverage of the grammar surface (`python3 fixtures.py`):

```
TIER 1  core Legend surface     219 of 219   100%
TIER 1  embedded (GraphQL)       31 of 31    100%
TIER 2  vendor connectors       182 of 182   100%
TIER 2  extension DSLs          230 of 230   100%
ALL IN SCOPE                    574 of 574   100%   (+26 excluded as unreachable)
```

Corpus: **51 positive fixtures, 215 negative fixtures, 1636 mutants.**

Parity against legend-lite (`python3 parity.py`):

```
POSITIVES  51    agree  37 (73%)   14 missing constructs
NEGATIVES  215   agree 179 (83%)   34 over-permissive (+2 quarantined)
MUTANTS   1137   agree 1092 (96%)  32 more permissive, 7 stricter (6 quarantined)
```

> **The numbers above are only true if `core` is freshly built.** See §7, trap 1. This bit me
> and I published wrong figures for a day.

---

## 3. How to run everything

Prerequisites (neither is on `PATH` by default here):

```bash
export JAVA_HOME=$HOME/jdk/jdk-21.0.11+10/Contents/Home
export PATH=$JAVA_HOME/bin:$HOME/jdk/apache-maven-3.9.9/bin:$PATH
mvn -o -pl core install -DskipTests        # REQUIRED before any parity run
```

```bash
cd scripts/parser

python3 fixtures.py              # coverage + positive & negative harnesses; non-zero on any problem
python3 fixtures.py --gaps       # what is still uncovered, by grammar
bazel run //scripts/parser:keywords -- --tier1   # raw keyword census over the pinned grammars, no fixtures involved
python3 mutants.py               # regenerate the mutation manifest (~2 min)
python3 mutants.py --check       # fail on drift instead of overwriting
python3 mutants.py --accepted    # mutations legend-engine tolerated (review queue)
python3 parity.py                # both parsers over all three corpora
python3 parity.py --detail       # every divergence, named
```

The other `python3` lines are history (Bazel workplan P7-04): only the keyword census is kept, as a Bazel target.

The CI ratchet (this is what a build runs): `bazel test //parser-equivalence:parser_parity` (FixtureCorpusParityTest
among its classes).

The runner's view of the engine's token vocabulary is a build output; after a release bump:

```bash
bazel run //tools/engine-runner:update_vocab
bazel run //scripts/parser:update_keyword_coverage
```

---

## 4. The pieces and what each proves

| file | role |
|---|---|
| `keywords.py` | harvests every literal keyword from legend-engine's 156 `.g4` files; dead-token and version-skew detection |
| `tiers.py` | what is in scope and **why**; all exclusions with the evidence checked |
| `fixtures.py` | runs positive + negative harnesses, recomputes coverage from fixtures that parse |
| `mutants.py` | damages every positive fixture; records what legend-engine does |
| `mutants.tsv` | the manifest, committed and diffable — 1636 rows |
| `parity.py` | both parsers over all three corpora; reports divergences |
| `parity-quarantine.tsv` | known divergences, each with a reason |
| `fixtures/` | 51 positives, each declaring `// COVERS <grammar>: kw, kw` |
| `negative/` | 215 must-be-rejected fixtures, each declaring `// REJECTS: <message substring>` |
| `tools/engine-runner/…/ParseMain.java` | parses each file individually with **legend-engine** |
| `tools/engine-runner/…/LiteParseMain.java` | the twin, driving **legend-lite**; `--protocol-check` also round-trips the output into the engine's protocol classes |
| `tools/engine-runner/…/TokenDump.java` | dumps the token vocabulary the runner's jars actually have |
| `parser-equivalence/…/FixtureCorpusParityTest.java` | the CI ratchet |

**Three strengths of claim, and they are not interchangeable.**

- *Positive fixtures* prove a construct is reachable: it parsed, so the surface exists.
- *Negative fixtures* prove a construct is refused **and why** — each pins the message, so it
  cannot pass for an accidental reason.
- *Mutants* prove nothing alone. They record behaviour across 1636 damaged inputs nobody would
  write by hand, and their value is purely differential.

---

## 5. Upstream findings (legend-engine defects)

All in `docs/UPSTREAM_FINDINGS.md`; runnable repros under `scripts/corpus/repro/`. **None of
these has been reported to the legend-engine project.** They are a real contribution and
they are perishable.

### 5.1 Eight NullPointerException sites, three grammars, one shape

The strongest finding. Every one is reachable by typing into a `.pure` file, and every one
surfaces as `An exception of type 'NullPointerException' occurred, please notify developer` —
no line, no column, no construct named. The shape is always: **an optional or alternative
grammar element dereferenced without a null check.** The grammar says "optional"; the walker
assumes "present".

| # | construct | cause |
|---|---|---|
| 1 | `Class X projects Y { … }` | `visitClass` dereferences `ctx.classBody()`, null for the projection form (**F17**, `repro/projects-npe/`) |
| 2 | `HostedService actions: [ MyAction ];` | walker dereferences `spec.actionBody().actionValue()` while the grammar makes `actionBody` optional |
| 3–8 | Persistence `serviceOutputValue` | one rule, two meanings (`identifier | dslNavigationPath`); the walker picks the accessor by dataset kind and never null-checks. Six call sites: `keys` on both sides, `DeleteIndicator.deleteField`, `FieldBased.partitionFields`, `MaxVersion.versionField` (**F23**, `repro/persistence-npe/`) |

F17 is worth reporting on its own merits: `native function` is declined *cleanly* by the same
walker with "Unsupported syntax", one class away. Two unsupported constructs, one diagnostic
and one stack trace. And legend-engine **ships a model file** written in that projection
syntax (`core_relational/…/projectionTestModel.pure`) because legend-pure's M3 grammar accepts
it — the two front ends disagree about a construct in the repo's own sources.

### 5.2 F19 — `###Connection` accepts an element with no closing brace

A complete `RelationalDatabaseConnection` minus its final `}` parses. So does one followed by
a further `###Pure` section — the protocol document then contains **both** elements, so
nothing is swallowed; the parser simply does not require balance. Rejected in `###Pure`,
`###Relational`, `###Service` and `###Diagram`, so it is the Connection grammar alone.
`repro/unterminated-connection/`.

*How it was found matters:* the mutation harness drops the final delimiter of every fixture;
16 of 51 tolerated it and **all 16 were `###Connection`**. No hand-written negative would have
found it, because nobody tries the same deletion in eight different sections.

### 5.3 F20 — an unsatisfiable multiplicity compiles

`String[2..1]` and `Integer[10..3]` pass **both** parse and compile. Not general laxity —
`[1..]` *is* a grammar error, so the shape is validated and only the range check is absent.
The comparison that makes it a gap: an `Association` with one property also parses and is then
rejected at compile with `Expected 2 properties for an association`. Arity is checked one
stage later; bounds never are. `repro/inverted-multiplicity/`.

### 5.4 F21 — fields parsed and silently discarded

Three instances of one shape, no diagnostic in any:

- `mode: local` alongside `specification:` and `auth:` — the walker treats local mode as an
  if/else, so both blocks parse and are replaced by **synthesised placeholders**
  (`accountName: "legend-local-snowflake-accountName-…"`). A vault reference written by the
  author appears nowhere in the output.
- `mappings:` on a `JsonModelConnection`, `class:` on a `ModelChainConnection` — all three
  model connections share one `definition` rule and each walker extracts only what its own
  type needs, so the field is dropped outright.
- `trigger: Manual #{ whatever: 1; }#` — the `Manual` processor ignores its island body.

A round-trip through parse-and-compose therefore does not preserve the file, and the author is
never told which lines stopped existing. `repro/silently-discarded-fields/`.

### 5.5 F22 — `###FileGeneration` type validation is dead code

`FileGenerationParseTreeWalker` wraps its type extraction in a `catch (IllegalArgumentException)`
around a `substring`/`toLowerCase` that cannot throw one, so `"Generation type '…' is not
supported."` is unreachable. `CompletelyMadeUpType fx::G { }` parses and records
`"type": "completelyMadeUpType"`. `repro/generation-type-unvalidated/`.

### 5.6 Declared-but-unreachable surface

Not defects exactly, but they mean the lexer advertises surface the parser does not have — any
tool deriving completions or documentation from the grammar will offer fields nobody can type.

- **Ten dead tokens in `AuthenticationStrategyLexerGrammar`**: `host`, `port`, `name`, `mode`,
  `directory`, `account`, `warehouse`, `region`, `projectId`, `defaultDataset`. No parser rule
  references them; writing `host` in an `auth:` block is answered with
  `Valid alternatives: ['baseVaultReference', 'userNameVaultReference', 'passwordVaultReference']`.
- **Two island types with zero registered processors repo-wide**:
  `IPostDeploymentActionGrammarParserExtension` and `IDataQualityGrammarParserExtension` have no
  implementors, so `HostedService.actions` and `DataQualityValidation.persistenceStrategy`
  reject every value a user could write.
- **`###AuthenticationDemo`'s section parser ships only in `src/test/java`** — the section is
  unreachable in a deployed engine while the grammar's other 43 keywords remain reachable
  through `authentication:` islands. A grammar can be half-live and nothing in it says which
  half.

---

## 6. The legend-lite queue

### 6.1 Fourteen missing constructs (positives legend-lite rejects)

Run `python3 parity.py --detail | grep 'LITE-REJECTS positive'` for the live list. legend-lite
names each one itself, e.g.:

```
[16:3]  unsupported datasource specification: Athena (corpus-censused …)
[78:6]  unknown DSL island type: '#GQL{'
[33:18] unknown BigQuery key: proxyHost
[29:12] unknown key 'groupId' inside DataSpace
[90:7]  unknown service-mapping block: ~paramMapping
```

Mostly vendor connectors — mechanical keyword→field work, the same shape that took tier-2
coverage from 2% to 100% in one pass. Two are not mechanical: the `#GQL{` embedded island and
`~paramMapping`.

### 6.2 Thirty-four over-permissive negatives — and why only 15 are worth fixing

This is the most important analytical result in the whole effort, so it is spelled out.

Raw breakdown: 27 ordering/placement, 5 required field, 1 closed value set, 1 semantic
cross-check.

The 27 "ordering" cases look like one architectural gap: legend-lite parses key-value blocks as
unordered maps, while the engine's ANTLR rules encode order in the rule structure. **Enforcing
order globally would be a serious mistake** — the mutation data shows field order is genuinely
free in 110 of 114 cases, so a global rule would trade 27 harmless divergences for a much
larger number in the dangerous direction (rejecting models the engine accepts).

So the ordering cases were split by a measurement rather than a judgement.
`LiteParseMain --protocol-check` parses with legend-lite and then deserializes the result into
**the engine's own protocol classes**:

```
27 ORDER-class divergences
  19  produce VALID protocol — a document the engine could have produced.
      legend-lite is relaxed about keystroke order; nothing downstream can tell.
   8  produce protocol the engine CANNOT deserialize.
```

The 8 failures are `Unrecognized field "ingestMode"`, `"transactionScope"`,
`"actionIndicator"`, `"derivation"`, `"sink"`, `"endField"`, `"sourceDerivedDimension"`, and
`Could not resolve type id 'sourceSpecifiesFromDateTime'`. **All eight are Persistence**, and
none is really an ordering problem: they are legend-lite accepting a key into a node **kind**
that has no such field.

**Recommended work, in order:**

1. **Persistence permitted-keys check — 8 fixes, one mechanism. Start here.**

   `core/src/main/java/com/legend/parser/section/PersistenceSectionGrammar.java` already has
   a `REQUIRED_FIELDS` map keyed by `slot/kind` (e.g. `"ingestMode/UnitemporalDelta"`), read
   by `validateNode`, which walks the generic node tree and enforces required-and-once. A
   `PERMITTED_FIELDS` map on the *same key* plus a rejection for any entry outside it closes
   all eight. Nothing new is needed structurally — `validateNode` already iterates
   `node.entries()` for the duplicate check, so the permitted test goes in that same loop.

   The permitted sets do not have to be guessed. legend-engine states each one in its own
   rejection message:

   | slot/kind | offending key | engine's permitted set |
   |---|---|---|
   | `persister/Streaming` | `ingestMode` | `['sink']` |
   | `targetShape/Flat` | `transactionScope` | `['partitionFields', 'modelClass', 'targetName', …]` |
   | `datasetType/Snapshot` | `actionIndicator` | `['partitioning']` |
   | `transactionMilestoning/BatchId` | `derivation` | `['batchIdInName', 'batchIdOutName']` |
   | v2 service-output target | `sink` (a v1 block) | `['keys', 'deduplication', 'datasetType']` |
   | relational `sourceFields` | `endField` first | `['startField']` — ordered pair |
   | relational unitemporal | `sourceDerivedDimension` | `['processingDimension']` |
   | validity `derivation` | `SourceSpecifiesFromDateTime` | `['SourceSpecifiesInDateTime', …]` |

   Regenerate the live list any time with:
   ```bash
   java -cp tools/engine-runner/target/classes:$(cat tools/engine-runner/cp.txt) \
        perf.ParseMain scripts/parser/negative/neg-persistence-*.pure
   ```
   Confirm a fix with `LiteParseMain --protocol-check` on the same file: it must go from
   `Unrecognized field …` to `ok`. That is the real acceptance test, not the parity count.

   This is the only group that corrupts anything today — legend-lite currently emits JSON no
   downstream engine consumer can deserialize.
2. **Five required-field entries** — map entries in validation code that already exists.
3. **Two one-liners** — `Unknown database type 'Frobnicate'`, and the execution-env key
   space check.
4. **Then the 14 missing constructs** (§6.1).
5. **Do not** chase the remaining 19 ordering cases. High cost across seventy grammars, zero
   downstream effect, and a real risk of over-correcting into the worse failure direction.

### 6.3 Quarantined divergences (8) — legend-lite is RIGHT, the reference is wrong

In `parity-quarantine.tsv`, each with a reason:

- 6 mutants: `drop-final-delimiter` on `###Connection` files. That is **F19** — legend-lite
  requires the closing brace and legend-engine does not. Found from one side by mutating engine
  fixtures, confirmed from the other by parity, using two harnesses that share no code.
- 2 negatives: the Persistence NPE cases. legend-engine **crashes**; legend-lite parses
  cleanly. Matching parity would mean reproducing a NullPointerException.

---

## 6.4 Do NOT be stricter than legend-engine — read `PERMISSIVENESS.md`

Parser parity fails in two directions and only one is obvious. Accepting what the engine
rejects breaks in production. **Rejecting what it accepts looks like rigour** and surfaces as
a model that has worked for two years suddenly failing to load, reported as "your parser is
broken" rather than "your model was always malformed".

`PERMISSIVENESS.md` is the full catalogue, derived from 1636 mutants. The rules a rewrite is
most likely to get wrong:

- **Field order is free** — 110 of 114 sibling swaps accepted. Two exceptions corpus-wide:
  `ExecutionEnvironment` needs `mapping` before `runtime`; MongoDB needs `validationAction`
  before `validationLevel`.
- **Section order is free** — all 15 swaps accepted; resolution is by name, not position.
- **A file with no `###` header parses as Pure.** `Class fx::A {…}` alone is valid; a
  `Database` with no header is rejected with Pure's alternatives list.
- **`import` must precede every element in its section** — the one structural ordering rule
  that IS enforced.
- **Date literals are not validated at parse time.** `%2024-13-45` parses; the *compiler*
  says `Invalid month: 13`. Validating dates in a lexer is an obvious move and would be wrong.
- **The same element may be declared twice**, and a class may declare the same property twice.
- **`String[2..1]` and `Class A extends B, C` survive compilation** (F20) — a rewrite
  rejecting them at parse is stricter than the engine at *any* stage.
- **Almost every keyword is a legal property name** — `Class`, `let`, `all`, `toBytes`,
  `native` all work; only `true`/`false` do not.

## 6.5 Per-grammar quirks that exist nowhere else

Found during the tier-2 sweep, recorded here because they are not derivable from any `.g4`
and would otherwise be lost:

- **Validation is wildly inconsistent between sibling grammars.** Snowflake's
  `permissionScheme` is walker-checked against a closed set; `accountType` in the same element
  is unvalidated (upstream's own roundtrip test feeds it `BadOption`). MemSql's `port` is a
  STRING run through `Integer.valueOf` at parse time, so `port: 'abc'` is rejected; Databricks'
  `port` is the same field name, same type, and is not coerced. `ownership` is optional for
  `BigQueryFunction` and required for the otherwise-identical `MemSqlFunction`.
- **Identifier-only reachability is the single most common reason a keyword looks
  uncoverable.** `stage` (BigQueryFunction, MemSqlFunction),
  `SnowflakeUDFDeploymentConfiguration`, Deephaven's `tables`/`columns`/`columnDefinition`,
  MongoDB's `~distinct`/`~primaryKey`/`debug`, Persistence's `Notifier`/`serviceOutput`/
  `target`, `relation`, `FileGeneration`. In each case the rule that looks like it consumes
  the token as a field is absent or uses a different token, so the only way to type it is to
  NAME something with it. Worth testing rather than skipping — keyword-as-identifier is a
  classic parser defect.
- **Section names are not guessable from the element or grammar name.**
  `BigQueryFunctionGrammarParserExtension` registers `###BigQuery`;
  `DataQualityGrammarParserExtension` registers `###DataQualityValidation`;
  `RelationalMapper` lives in `###QueryPostProcessor`, not `###RelationalMapper`. Always read
  the `NAME` constant in the `*ParserExtension.java`.
- **Deephaven `appMultiplicity` accepts only `[n]` and `[*]`** — `[0..1]` is unreachable
  because CoreLexer lexes `..` as a single token.
- **MongoDB schemas use double-quoted strings**, and `jsonSchema:` is Jackson-deserialized at
  parse time, so `"bsonType": "object"` is mandatory.
- **ServiceStore requires `binding: X;;`** — a double semicolon.
- **Persistence's `.g4` is far weaker than its walker.** Every ingest-mode sub-block is `(x)*`
  in the grammar; the real requirements live in `PersistenceParseTreeWalker.visit*`. A parser
  generated straight from the grammar accepts a great deal the engine refuses — which is
  precisely why 16 of the 34 over-permissive cases are Persistence.
- **`derivation` is optional under `transactionMilestoning` and required under
  `validityMilestoning`** — an asymmetry invisible in upstream's own tests, which always write
  it.

## 7. Traps — every one of these cost real time

**1. A stale build gives confident wrong answers.** `core/target` was two days old and I
published parity numbers describing a legend-lite that no longer existed — wrong in the
direction that flattered the harness and slandered the parser (32/51 and 164/215 reported;
37/51 and 179/215 actual). Three probes agreed with each other on the wrong answer, which is
what a stale artifact buys you: consistency without correctness. **Run
`mvn -o -pl core install -DskipTests` before trusting any parity number.** Nothing in the build
does this for you after a merge that touches `core`.

**2. A missing grammar extension is indistinguishable from a grammar limit.** Four vendor
grammar extensions (athena, oracle, aurora, memsql) were absent from `tools/engine-runner`, and
later from `parser-equivalence` — `specification: Athena` failed with
`Unsupported Data Source Specification type`, which reads exactly like the construct being
invalid. It cuts the dangerous way too: **a missing extension makes any content in that section
look correctly rejected**, so a negative fixture would pass for a reason that proves nothing.
This is why every negative pins its exact message, and why `fixtures.py` flags any negative
whose rejection matches an environmental pattern unless the fixture says `ENVIRONMENTAL`.

**3. Baselines belong to the environment that asserts them.** `parity.py` drives
`engine-runner`'s classpath (every published extension); `FixtureCorpusParityTest` drives a
deliberately production-shaped oracle. They legitimately report different numbers for the same
corpus. Copying one into the other produces a red build with no defect behind it.

**4. The census must be harvested from the same artifact that parses.** `keywords.py` reads
`.g4` files from a legend-engine **working copy** at git HEAD; the runner parses with released
jars. Five keywords existed only in the source, so no fixture could ever cover them and "100%"
would have been a claim about a parser nobody runs. `TokenDump` + `vocab.tsv` closes this;
regenerate on any engine version bump.

**5. Composite tokens have no literal name.** `CONSTRAINT_OWNER: '~owner' CONSTRAINT_SEPARATOR;`
has no entry in ANTLR's `Vocabulary`, so a naive skew check reports five Domain keywords as
"missing from the jar" while they parse perfectly. The skew check only compares
simple-literal tokens for this reason.

**6. Two grammars can share a filename.** `SnowflakeLexerGrammar.g4` exists twice — the
`###Connection` datasource spec (20 tokens) and the `###Snowflake` function activator (17).
Keying on file stem merged them into one bucket of 37. The total was right, which is worse than
wrong. Grammar identity is now module-qualified when stems collide; Snowflake is the only
collision in the repo.

**7. XML comments cannot contain `--`.** A prose em-dash in a `pom.xml` comment produced
`Non-parseable POM`.

---

## 8. Measurement bugs I introduced and fixed

Recorded because the direction is itself a signal: **every correction moved the coverage number
DOWN**, and a correction that moves it *up* deserves suspicion.

| bug | effect |
|---|---|
| counted keywords in comments and `###Data` payloads | 186 → 122 |
| credited a keyword to a grammar whose section did not define it | 97 → 83 |
| word boundaries applied unconditionally | `->subType(@` uncoverable by construction |
| composite token `'include '[a-z]+' '` kept only its first fragment | `include` uncoverable |
| GraphQL byte-order marks harvested as keywords | demanded fixtures for a file encoding |
| grammars keyed by file stem | two Snowflake grammars merged |
| `//` inside a string literal stripped as a comment | swallowed real code to end of file |
| mutation sites not masked to code | 56 phantom "accepts" in one operator alone |
| `parity.py` truncating its detail at 40 rows | hid 6 of 46 divergences |

**A tool built and deliberately discarded:** a rule-reachability detector meant to find dead
parser rules. It claimed `UserNamePassword` was unreachable, which a passing fixture disproves —
these grammars have no `definition` rule, their entry points are invoked from Java, so
reachability cannot be derived from the `.g4` alone. A false "unreachable" silently shrinks the
denominator, the one direction this harness must never move on its own. The token-level
`tokenVocab` check survives because it only claims dead when *no* parser text mentions the
token at all.

---

## 8a. The execution corpus: feature density (added late, read this)

A separate strand from the parser work, and where the corpus now is.

An audit against `docs/MAPPING_FEATURE_CHECKLIST.md` and `docs/rosetta/` -- catalogues that
predate this corpus and that it had never been checked against -- found **94% of 397 class
mappings were plain 1:1** (`~mainTable` plus direct columns). Two databases, one view, one
filter, no schemas, and every join a single-column equality. All 24 findings to that point
had come from navigation and query shape, because that was the only dimension the corpus had.

`scripts/corpus/density.py --gate` measures and ratchets it. Current state:

```
plain 1:1          94% -> 82%
features present   17 of 35 -> 25 of 35
suite              182 tests, 161 passed, 0 unexpected
```

Landed: `dense_mapping.py` (scope blocks both arms, set ids, explicit ~primaryKey) and
`dense_store.py` (Schema, TabularFunction, MultiGrainFilter, multi-column joins,
non-equality joins, `or` joins, dynafunction joins, a View with all three directives).
`dense_store.py` INCLUDES the base database rather than duplicating it, so its objects sit
over already-seeded tables and are executable rather than compile-only.

**Verified syntax for everything else is in `scripts/corpus/verified/`** -- ~50 snippets
across four files, each run through `ParseMain --compile` before being written down. Five
spellings in our own rosetta docs turned out to be WRONG and are corrected there.

### The two blockers, precisely

Neither is syntax. Syntax is done.

1. **The reader is mapping-agnostic.** `c.columns` and `c.dyna` in `model.py` are keyed by
   CLASS, not by `(mapping, class)`. The same class mapped twice -- which `dense_mapping.py`
   does for 60 of them -- collapses to one view, so a dynafunction or join chain on a
   re-mapped class silently overwrites the flat column. This blocks executing A2 (join
   chains), A3/A4 (dynafunctions), B3 (~filter via join) and B4 (~distinct), and it is a
   change to `model.py`'s core data structure that ripples through six modules.

   The ORACLE side of that work is already done: `oracle._dynafunction` implements
   concat/toUpper/toLower independently and raises `Unsupported` for anything else, so an
   unimplemented dynafunction cannot silently take its expected value from the engine.

2. **The domain has zero class inheritance.** 210 classes, not one `extends`. This blocks
   mapping `extends [id]`, `inheritance` Operation mappings, and subtype graph fetch
   (`->subType(@)`) -- which the PARSER corpus covers and the execution corpus has never
   run. It is a DOMAIN change, and no amount of mapping generation reaches it.

## 9. Open decisions

1. **Merge to `main`.** All work is on `test-corpus`, pushed. `main` was updated from origin and
   merged *into* the branch, so nothing is stale, but the ~40-commit merge back is unpublished.
   Note another session was working on `main` concurrently — coordinate before merging.
2. **Report the upstream findings.** §5. Nothing has been sent to the legend-engine project.
3. **Message parity.** `parser-equivalence` has a `MessageParityTest`; pointing it at these 215
   negatives is natural but should wait until verdicts agree, or the prose diff buries the real
   queue.
4. **The 19 harmless ordering divergences.** Recommendation is to leave them; if that is
   rejected, the work is a per-rule sequence derivation across seventy grammars.

---

## 10. Deliberate non-goals

- **Coverage is not the constraint any more.** The surface is at 100% of what is reachable.
  Adding fixtures measures nothing new; the binding constraint moved to what legend-lite does
  about it.
- **Parse, not compile.** A fixture proving `~distinct` parses does not need a runtime, a
  connection and seeded data. Demanding compilation would make every fixture ten times the size
  and make coverage depend on the compiler.
- **Verdicts, not message text.** Two parsers can refuse the same input for the same reason and
  word it differently.
