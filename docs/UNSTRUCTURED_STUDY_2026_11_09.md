# Text in Pure: can lite's semantic layer answer over documents the way it answers over tables?

**Status: study (2026-11-09), branch `abs/unstructured-study`. Nothing is scheduled. §6 is a decision for the owner, and neither
native it proposes exists.** It answers the owner's ask: model real and synthetic documents in Pure, hold a frozen answer
key against them, and say what a text search in lite would have to be.

§0 is the answer; §1 the question and the corpora; §2 what lite and upstream have; §3 what each database can do; §4 the
evaluation discipline; §5 the results; §6 the decision; §7 the findings that stand on their own.

**Every number below comes from a committed file and names it.** The short names:

| Name | File |
|---|---|
| `summary.tsv`, `ledger.tsv` | `tools/unstructured-oracle/` |
| `answer_key.yaml`, `DECISIONS.md`, `REVIEW_2026_11_09.md` | `projects/unstructured/evals/` |
| `FINDINGS.md` | `projects/unstructured/` |
| `edgar/manifest.yaml`, `emails/README.md`, `loans/README.md` | `projects/unstructured/corpus/` |
| `pins.env` | `tools/unstructured-corpus/` |
| `sql/` | `tools/unstructured-oracle/sql/` |

---

## 0. Start here

1. **Over tables, yes.** The structured route answers 24 of 24 rows correctly through lite's server
   (`summary.tsv`: 22 PASS, 2 EMPTY_OK). These are the key's own queries, so that is the semantic
   layer answering, not a measure of retrieval.
2. **Over text, not yet well.** The best text arm passes 16 of the 40 answerable rows (`summary.tsv`, less
   the 4 text negatives of `answer_key.yaml`). keyword on Postgres and hybrid on DuckDB tie at it.
3. **The misses are mostly ranking.** Of hybrid's 37 missed accepted pointers, 24 are in the fused list but beyond
   the cut, and only 8 are outside both top-150 lists (`DECISIONS.md`, "Not tuned, candidates").
4. **The score floors are a small and fitted gain, reported as that** (§5.3).
5. **The decision is the owner's** (§6): `textSearch` has no legend-engine twin, so it would be a lite extension and a register row.

---

## 1. The question and the corpora

**Can lite's semantic layer answer over text the way it answers over tables, with the same receipts?** A receipt says where an
answer came from: source, section, date, sha256. A hit is a pointer, never the answer. An empty result means nothing matched; an
error says it is an error.

**Three sources, four corpora** (`answer_key.yaml`): EDGAR supplies two, and SALES and LENDING one each.

| Corpus | Source | Real or synthetic | What | Cases |
|---|---|---|---|---|
| `EDGAR 13F` | `EDGAR` | real, public | quarterly institutional holdings: tables with a cover page | 10 |
| `EDGAR 10-K/10-Q/8-K` | `EDGAR` | real, public | annual and quarterly reports and current reports | 11 |
| `SALES` | `SALES` | synthetic, ours | sell-side emails, with orders revised down the thread | 15 |
| `LENDING` | `LENDING` | synthetic, ours | credit agreements, each amended and restated | 14 |

**Why these.** The filings are real and public, so nobody can say the text was written to suit the test. They are a pointer
corpus: 41 files (16 raw, 25 extracted) over 13 accessions from 4 companies,
each held by sha256 in `edgar/manifest.yaml`, fetched locally and never committed. The emails and loan documents are synthetic, so
their ground truth is exact: a table row's `passageSha256` is the hash of the paragraph or the clause that states it. 60
emails in 18 threads (`emails/README.md`) and 12 agreements across 6 borrowers
(`loans/README.md`). Every firm, person and deal in them is fictional. Together those give an answer that can be checked on both
routes: a passage for the text route and a row for the structured one.

Search runs over all 2,595 passage rows, 1,769 distinct by sha256 (`FINDINGS.md`), not over the case's own corpus:
cross-corpus noise is part of the test (`DECISIONS.md`, entry 3).

---

## 2. What lite has today, and what upstream has

**Lite**, read in this repository:

| Feature | What it does for text | What it does not do |
|---|---|---|
| Variant (`meta::pure::metamodel::variant::Variant`; `get`, `fromJson`, `toMany`, in `builtin/Pure.java`) | holds and navigates a JSON document | search; there is no relevance |
| `matches` (`meta::pure::functions::string::matches`, in `builtin/Pure.java`) | a regular-expression test per row, rendered per dialect (`regexp_matches` on DuckDB) | ranking: the answer is a Boolean |
| `#SQL{...}#` over jsonb (`protocol/sql/PostgresJsonb.java`, register row S49, `sql-parity/lite/json.yaml`) | reads Postgres's JSON operators as Pure | search; `@>` and `?` are refused by name |
| MongoDB v0 (`docs/MONGODB_PLAN_2026_10_16.md` §11) | the first target that is not SQL, a render of the same IR into an aggregation pipeline, through a connector beside JDBC | `$text`, `$search` and `$vectorSearch` are not rendered: `sql/dialect/MongoDb.java` writes `$regexMatch` only |

There is no vector column and no long-text column (`FINDINGS.md`, F-U3 and F-U4): `corpus::Passage` carries no embedding, and long
text is a sized VARCHAR. The study's text arms are therefore SQL files run directly, `road` = `direct-sql` in `ledger.tsv`.

**Upstream**, read in the pinned sources (legend-engine 4.145.0, `FINDINGS.md`).

The Elasticsearch store is the module `legend-engine-xts-elasticsearch`: `-V7-grammar`, `-V7-protocol`, `-V7-pure-metamodel`,
`-V7-executionPlan`. It maps:

- **A store, not a class.** `###Elasticsearch` declares an `Elasticsearch7Cluster` of indices, each with properties typed `Keyword`,
  `Text`, `Date`, the numeric types, `Boolean`, `Object` and `Nested`. The store contract answers `supportsSetImplementation`
  with `false`, so no class mapping exists.
- **An index to a TDS.** `indexToTDS(store, indexName)` yields a `TabularDataSet`. `pure_to_elasticsearch.pure` translates `project`,
  `filter`, `sort`, `limit`, `drop`, `groupBy` and `extend` into a search request.
- **Filters, not relevance.** The translation emits term, terms, range, exists, prefix, wildcard, ids, bool, script and
  `match_phrase_prefix` queries, for `equal`, `in`, the comparisons, `contains`, `startsWith` and `endsWith`. It never reads
  `_score` and never emits a `match` query. The generated specification metamodel names `MatchQuery` and `DenseVectorProperty`, but the grammar
  has no dense-vector property type.

So upstream can filter an index and cannot rank by relevance from Pure. Searching the pinned engine and pure trees for `textSearch`,
`fullTextSearch`, `vectorDistance`, `cosineDistance`, `cosineSimilarity` and `euclideanDistance` finds no such function.

---

## 3. What each database can do

**Measured** means this study's judge ran the statement on every text case. 44 rows per arm and backend (`summary.tsv`);
the statements are in `sql/`.

| Database | Keyword | Vector | Status |
|---|---|---|---|
| DuckDB (1.4.5, `pins.env`) | `fts` extension, `match_bm25` | `array_cosine_similarity` over `FLOAT[384]` | **measured**: 0 and 0 ERROR |
| Postgres (embedded, 16.15, `ledger.tsv`) | `tsvector`, `@@`, `ts_rank`, terms ORed | pgvector `<=>`: **REFUSED** | keyword **measured**, 0 ERROR; vector and hybrid REFUSED, 88 rows |
| Snowflake | `SEARCH` | `VECTOR_COSINE_SIMILARITY` | unmeasured, recalled |
| ClickHouse | `hasToken` | `cosineDistance` | unmeasured, recalled |
| BigQuery | `SEARCH` | `ML.DISTANCE`, `VECTOR_SEARCH` | unmeasured, recalled |
| SQLite | FTS5, `MATCH`, `bm25()` | none built in | unmeasured, recalled |
| MariaDB | `MATCH ... AGAINST` | `VEC_DISTANCE_COSINE` | unmeasured, recalled |
| MySQL | `MATCH ... AGAINST` | a `VECTOR` type; a distance function is not in every edition | unmeasured, recalled |
| Trino | none recalled | `cosine_similarity` | unmeasured, recalled |
| Databricks | none recalled | `vector_search`, excluded here by the open-source-only rule | unmeasured, recalled |
| H2 | `FT_SEARCH` | none recalled | unmeasured, recalled |
| MongoDB | `$text`; `$search` on Atlas | `$vectorSearch` on Atlas | unmeasured, recalled |

**The exact refusal** (`ledger.tsv`, every vector row on Postgres):

> `pgvector: extension 'vector' is not among the 63 extensions PostgreSQL 16.15 has available; vector_postgres.sql, run: UndefinedTable 42P01: relation "co_passage_embedding" does not exist`

And on hybrid:

> `pgvector: extension 'vector' is not among the 63 extensions PostgreSQL 16.15 has available; hybrid_postgres.sql, run: UndefinedTable 42P01: relation "co_passage_embedding" does not exist`

Both statements parse and stop at name resolution, because the table's column needs the type pgvector provides. Creating that table
answers `UndefinedObject 42704, type "vector" does not exist` (`DECISIONS.md`, entry 4). Nothing was installed. Neither statement has
returned a row, and `<=>` has not been exercised.

**The score scales are not portable.** The keyword floors, measured on the same cases, are 4.5 on BM25 and 0.026
on `ts_rank` (`DECISIONS.md`, entry 5). `ts_rank` also has no inverse document frequency, and the backends do not pass the same
rows (`ledger.tsv`: 3 of the 44 keyword cases differ in verdict between them).

---

## 4. The evaluation discipline, as practised on this branch

**The key.** 50 cases in `answer_key.yaml`, written by hand from the source passages and frozen before the judge existed
(`DECISIONS.md`, entries 1 and 2; sha256 `a4dcdc34…`). A pointer names file, sha256 and character offsets, with a short
snippet for the reader; the passage is never copied. No label changes without a dated entry, and none has since the freeze.

**The independent review** (`REVIEW_2026_11_09.md`). 10 of the 50 cases, drawn at random with seed 20261109, by
Claude Fable 5.1, which read the key and the corpus only. Every recomputed hash matched. It settled 9 and left 1 unsettled:
**C14** asked by how many shares a position changed, and its expected listed the positions and left the subtraction to the reader. The
owner replaced the expected, the query now computes the change, and the re-review settled it at `[[60414]]`
(298,117 − 237,703). The other 40 cases were not sampled.

**Per-case verdicts, never a headline.** One `ledger.tsv` row per case, route, arm and backend: 288 rows. `STALE_HIT` (a superseded
pointer inside the cut, even with the accepted one there too) and `ERROR` (the search broke, never spelled as empty) are first-class, as are
`REFUSED` and `UNRESOLVED`. The gate fails on any PASS that becomes anything else, even if the counts improve.

**Receipts.** Every text PASS carries path, section, date, sha256 and rank, one per pointer found (`ledger.tsv`: 59 of
59). The 22 structured PASS rows carry none: they say the rows equal the key's, and `ledger.tsv` holds `-` in those columns.
**So the "same receipts" of §1 holds on the text route only.** Where a table's answer came from is not judged here.

**The dedupe rule** (`FINDINGS.md`, the owner's ruling): the judge dedupes by sha256 before any cut, and a result is identified by
`(path, charStart)`; sha256 is its fingerprint. A hash held at more than one date (171 of the 1,769) takes its newest, which is F-U6.

**The one tuning entry** (`DECISIONS.md`, entry 5): a floor on a result set's best score. It cost 0 PASS, and one row found
less: C04 on keyword DuckDB went from 1 of 6 to 0, still DROPPED. Entry 3 records what was decided
after a ledger was seen: the BM25 rounding, which is determinism, and the cut rule below.

| Fact of the test | Design choice |
|---|---|
| Every pointer's hash, recomputed from the files | **Passage granularity.** EDGAR merges to about 800 characters; SALES and LENDING are the paragraph or clause the ground truth hashes (`FINDINGS.md`). It shapes every rank below |
| Which passage states an answer | **No reranker.** A cross-encoder runs a model per candidate at query time, outside the engine (`tools/unstructured-oracle/README.md`) |
| Which verdict a set of rows earns under the rules | **`match: all`.** A case needing every pointer is harder than one needing any. Under `match: any`, 5 of hybrid's DROPPED rows, each with part of its answer, would pass: C01, C03, C31, C56, C59 (`ledger.tsv`) |
| What the pinned statements returned | **The cut.** Top max(5, accepted pointers), set after the first ledger was seen (`DECISIONS.md`, entry 3) |
| | **Search over all corpora**, the embedder, RRF's k, and the recency blend |

---

## 5. Results

### 5.1 The summary

`summary.tsv`, generated from `ledger.tsv` and diff-tested (ledger sha256 `20afd1c9…`, `DECISIONS.md`, entry 5):

| route | arm | backend | road | rows | PASS | DROPPED | STALE_HIT | EMPTY_OK | FALSE_HIT | REFUSED | ERROR | UNRESOLVED |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| structured | pure | duckdb | lite | 24 | 22 | 0 | 0 | 2 | 0 | 0 | 0 | 0 |
| text | hybrid | duckdb | direct-sql | 44 | 16 | 22 | 2 | 2 | 2 | 0 | 0 | 0 |
| text | hybrid | postgres | direct-sql | 44 | 0 | 0 | 0 | 0 | 0 | 44 | 0 | 0 |
| text | keyword | duckdb | direct-sql | 44 | 15 | 20 | 5 | 2 | 2 | 0 | 0 | 0 |
| text | keyword | postgres | direct-sql | 44 | 16 | 19 | 5 | 2 | 2 | 0 | 0 | 0 |
| text | vector | duckdb | direct-sql | 44 | 12 | 24 | 4 | 0 | 4 | 0 | 0 | 0 |
| text | vector | postgres | direct-sql | 44 | 0 | 0 | 0 | 0 | 0 | 44 | 0 | 0 |

Three fresh runs wrote byte-identical ledgers (`DECISIONS.md`, entry 5).

### 5.2 The structured route: 24 of 24, through lite's server

22 PASS and 2 EMPTY_OK, `road` = `lite`, each through `//core:server`'s `/api/pure/v1/execution/execute` (`summary.tsv`).
The queries are the key's own, over a model mapped for these corpora. That shows lite answering them correctly, and it is not a retrieval result:
nothing chose those queries from a question.

### 5.3 The text route's baseline and its ceiling

Baseline against now, from `DECISIONS.md`, entry 5:

| Arm, backend | Floor | PASS | DROPPED | STALE_HIT | EMPTY_OK | FALSE_HIT |
|---|---|---|---|---|---|---|
| keyword, DuckDB (BM25) | 4.5 | 15 → 15 | 20 → 20 | 5 → 5 | 0 → 2 | 4 → 2 |
| keyword, Postgres (`ts_rank`) | 0.026 | 16 → 16 | 19 → 19 | 5 → 5 | 0 → 2 | 4 → 2 |
| vector, DuckDB (cosine) | none | 12 → 12 | 24 → 24 | 4 → 4 | 0 → 0 | 4 → 4 |
| hybrid, DuckDB (fused) | 0.89 | 16 → 16 | 22 → 22 | 2 → 2 | 0 → 2 | 4 → 2 |
| vector and hybrid, Postgres | none, 0.89 | REFUSED 44 each, unchanged, details included | | | | |

**The baseline.** With no floor, every arm that ran returned something for every negative: 4 FALSE_HIT each (`DECISIONS.md`, entry 3).
PASS is unchanged since. The best arm passes 16 of 40, and hybrid does not beat keyword: 16 on DuckDB against keyword's
15 on DuckDB and 16 on Postgres (`summary.tsv`).

**The floors were read from the same 50 cases they are judged on, with 4 negatives among them. Any value at or under the lowest
PASS's best score passes the gate by construction, so the gate passing here is not independent evidence** (`DECISIONS.md`, entry 5). Each floor is also
tied to this corpus and these pins.

**The ceiling. 24 of hybrid's 37 missed accepted pointers are in the fused list but beyond the cut, which is the ceiling a reranker outside
the engine would address, against only 8 outside both top-150 lists** (median rank 30, deepest 141; the other 5
are inside the cut, on rows missing others; `DECISIONS.md`, "Not tuned, candidates"). These are pointers, not rows. A `match: all` row passes only when all
its pointers are in, and those 8 stay out whatever reorders the list. No row count for a reranker is claimed here.

**The vector arm has no floor, because none exists that costs no PASS.** Every negative's best cosine is above the lowest PASS's (0.6314, C32),
so any value that empties a negative costs a PASS, and any value that costs no PASS empties no negative (`DECISIONS.md`, entry 5). A negative on vector
cannot be EMPTY_OK: all 4 are FALSE_HIT.

A fused score has the same limit from another side. It is rank agreement and not relevance, so C60, first in both lists on a passage that answers nothing, scores
1.0000 and no fused floor can reach it (`DECISIONS.md`, entry 5).

### 5.4 The hybrid's one job: STALE_HIT from 5 to 2

Recency exists for the 9 time-aware cases of `answer_key.yaml`. On DuckDB, keyword is STALE_HIT on 5 (C43, C45, C63, C64, C68) and vector on
4; hybrid on 2 (C43, C45; `ledger.tsv`). That is the one thing hybrid clearly improves.

It is half of the job. **The 3 cases that left STALE_HIT, C63, C64, C68, are DROPPED on hybrid and none became PASS** (`ledger.tsv`): the stale passage
was pushed out of the cut and the current one did not come in. The ledger cannot separate what fusion did from what the recency blend did.

### 5.5 Why the retriever is weak, and what addresses which rows

- **Ranking first.** 24 of 37, as above.
- **A small general embedder.** `BAAI/bge-small-en-v1.5`, 384 dimensions (`pins.env`). Vector has the fewest PASS (12) and the most DROPPED (24)
  of the arms that ran (`summary.tsv`).
- **Vector is weakest on SALES and 13F.** It passes 1 of the 15 SALES cases and 0 of the 4 `EDGAR 13F` text cases (`ledger.tsv`). SALES has the
  shortest passages (median: EDGAR 717 characters, LENDING 152, SALES 58; `DECISIONS.md`), but that the length is the cause is not measured.
- **The noise is on purpose.** All corpora are searched for every question.

By corpus, on the arms that ran on DuckDB (PASS per arm, `ledger.tsv` and `answer_key.yaml`):

| Corpus | Text cases | keyword | vector | hybrid |
|---|---|---|---|---|
| `EDGAR 13F` | 4 | 0 | 0 | 1 |
| `EDGAR 10-K/10-Q/8-K` | 11 | 5 | 6 | 6 |
| `SALES` | 15 | 4 | 1 | 3 |
| `LENDING` | 14 | 6 | 5 | 6 |

Nothing below has been run (`DECISIONS.md`, "Not tuned, candidates"):

| Candidate | Rows it addresses | Strength of the evidence |
|---|---|---|
| A reranker outside the engine | the 24 pointers beyond the cut; C60 | the strongest; stays outside the engine |
| A larger embedder | vector's 24 DROPPED; SALES; the negatives' cosines, 0.6537 to 0.7488, inside the PASS rows' range, 0.6314 to 0.8449 | strong for vector alone |
| k per arm | the 8 outside both lists; a deeper vector list reaches C04, C16, C24 and C59 | modest; keyword's deepest listed miss is rank 69 of 150 |
| Per-location recency (F-U6) | C63, C43, C64 | thin: no accepted or superseded pointer of a time-aware case is in a repeated-date hash |
| Smaller EDGAR passages | hybrid's EDGAR DROPPED: C09, C10, C16, C20, C24, C31, C36 | the weakest: accepted passages have a median of 772 characters on PASS rows and 752 on DROPPED |

---

## 6. The decision for the owner

### 6.1 The situation

`textSearch`, and later `vectorDistance`, have no legend-engine twin (§2). Under the upstream-fidelity tenet the first question is "can we do exactly as pure and engine
do?", and here there is nothing to do exactly: upstream has no native for relevance. So these would be lite extensions, posted before they land and ledgered, as `#IQ{...}#`
was (S50).

### 6.2 The proposed register rows, in full, the number left to the owner

Not added to `docs/SEMANTICS_REGISTER.md`.

| id | kind | legend-pure / legend-engine | lite | why, decided by | evidence | owner |
|---|---|---|---|---|---|---|
| (the owner's) | scope (extension) | no such native. legend-engine 4.145.0 and legend-pure hold no `textSearch` or `fullTextSearch`; the nearest are `matches`, a regular expression answering a Boolean, and the Elasticsearch V7 store, which filters an index and never reads `_score` | `meta::legend::lite::textSearch(document:String[0..1], query:String[1]):Float[0..1]`: the database's own relevance of the document for the query, empty where no term matches, larger is better. **The score's scale belongs to the database and is not portable**, so databases may rank and floor differently. The database computes it; Java computes no value. The index is data preparation, as the vectors are, and the engine never builds it. DuckDB: `fts`, `match_bm25`. Postgres: `to_tsvector`, `ts_rank`, the terms ORed, because `plainto_tsquery` ANDs them. Every other dialect refuses by name with `DialectCapability` until it has been measured | proposed 2026-11-09, awaiting the owner's ruling; the study's §6 | `docs/UNSTRUCTURED_STUDY_2026_11_09.md` §3 and §5; `tools/unstructured-oracle/sql/keyword_duckdb.sql`, `keyword_postgres.sql`; `tools/unstructured-oracle/summary.tsv`, the keyword rows; the pinned trees searched for the names above | the text search line |
| (the owner's) | scope (extension) | no such native. legend-engine 4.145.0 and legend-pure hold no `vectorDistance`, `cosineDistance`, `cosineSimilarity` or `euclideanDistance`, and no vector type; the Elasticsearch V7 grammar has no dense-vector property | `meta::legend::lite::vectorDistance`, its signature to be fixed when the owner rules on F-U3, since lite has no vector column and Pure no embedding type: the database's cosine distance between a stored vector and a query vector. **Lite reads vectors and never makes them:** embedding is data preparation. DuckDB: `array_cosine_similarity`. Postgres: pgvector's `<=>`, which has never run here. Every other dialect refuses by name | proposed 2026-11-09, awaiting the owner's ruling, after `textSearch`; the study's §6 | `docs/UNSTRUCTURED_STUDY_2026_11_09.md` §3 and §5; `tools/unstructured-oracle/sql/vector_duckdb.sql`, `vector_postgres.sql`; `projects/unstructured/FINDINGS.md` F-U3; the refusal quoted in §3 | the text search line |

### 6.3 The spike that would carry them

`textSearch` alone, smallest first:

- **One MIR variant**, a new `SqlExpr` record, with its render arm in `AnsiSqlRenderer` and each dialect (invariants 3 and 3a: no `FunctionCall`, no `default ->`).
- **An arm in every dialect, throwing where unsupported.** DuckDB and Postgres render; every other dialect throws `DialectCapability` by name until a row of §3 is measured.
- **The first thing it has to settle.** `match_bm25` takes the row's key and an index named for the table, not a document, while `ts_rank` takes the document. A dialect consults no model, so the variant must carry the document column and the row's key, both resolved at lowering from the mapping, and an argument that is not a stored column of an indexed table is refused by name.
- **Judged by this ledger, on the lite road.** The keyword arm gains rows with `road` = `lite`, through `//core:server`, and `road` joins the ledger's key columns. The existing `direct-sql` rows are the oracle: a lite row must return the same ranked sha256 list, on both backends, and the gate applies as it does now.
- **Not in the spike:** hybrid, the floors, `vectorDistance`. Hybrid's fusion and the floors are ranks, sums and comparisons the database already does, so they follow once a score exists.

### 6.4 The door after that

An OpenSearch store, done the MongoDB-v0 way (`docs/MONGODB_PLAN_2026_10_16.md` §11): a render target over the same `SqlQuery` IR, a connector beside JDBC, an embedded test server, with a store grammar read from upstream's `Elasticsearch7Cluster` as the spec. Upstream's module filters; `textSearch` is what would give such a store a relevance to carry.

### 6.5 Needs a ruling

1. **Is `textSearch` a lite extension at all?** Recommendation: yes, as a spike judged by this ledger. The text route's result is weak, but the spike tests whether lite can carry the statement with receipts, which does not depend on how good the retriever is.
2. **The register rows above**, their wording and their numbers.
3. **`vectorDistance`:** recommendation, not until F-U3 is ruled and pgvector, or another real `<=>`, has returned a row.
4. **Whether the ranking gap is worth a reranker outside the engine**, which would sit beside lite and not in it.

---

## 7. Findings that stand on their own

From `FINDINGS.md`. The pins passed on this branch, `bazel test //core:core_tests_compiler --test_filter=com.legend.compiler.OpenMappingGapsPinTest`.

| Id | Finding | Against upstream | Test |
|---|---|---|---|
| F-U1 | A mapping can name a column its table does not have; `compileModel` accepts it | **lite laxer.** legend-engine refuses at compile: `Can't find column 'NO_SUCH_COLUMN'` | `OpenMappingGapsPinTest.fU1_aMappingToAColumnThatDoesNotExistCompiles` |
| F-U2 | An `AssociationMapping` can name a set id that does not exist; it compiles and plans | **lite laxer.** legend-engine refuses at planning: `Expected 1 class mapping for 'B' in 'M', found 0` | `OpenMappingGapsPinTest.fU2_anAssociationMappingToASetIdThatDoesNotExistPlans` |
| F-U5, the enum-NULL pin | A stored value outside an `EnumerationMapping` reads back as NULL, even on a `[1]` property | **matches upstream**, the same answer and the same SQL, by the owner's ruling | `OpenMappingGapsPinTest.fU5_aValueOutsideTheEnumMappingReadsBackAsNullOnARequiredProperty` |
| F-U6 | A repeated passage takes its newest date for recency: 171 of the 1,769 hashes are held at more than one date, the largest gap 1,015 days | not an upstream question | **no test:** it is measured in `FINDINGS.md` and stated in `sql/hybrid_duckdb.sql`'s comment; it is a candidate in §5.5, not a pinned behaviour |

F-U1 and F-U2 are defects under the upstream-fidelity tenet, and `FINDINGS.md` recommends closing both to match upstream, in a separate PR. Neither changes on this branch.
