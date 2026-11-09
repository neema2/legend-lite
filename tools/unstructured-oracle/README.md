# tools/unstructured-oracle: the judge of the unstructured study

Runs the frozen answer key (`projects/unstructured/evals/answer_key.yaml`) and writes one ledger row per case, route, arm
and backend. There is no headline score.

## The arms are SQL

`sql/` holds one file per arm and backend. These are the statements a future `textSearch` must render from Pure, so the
retrieval lives there and not in Python: the judge passes in the question and reads back rows.

| File | What |
|---|---|
| `keyword_duckdb_setup.sql` | the index: `fts`, over one row per distinct passage |
| `keyword_duckdb.sql` | BM25 (`match_bm25`), any term may match, top 150 |
| `keyword_postgres.sql` | `tsvector`, `@@`, `ts_rank`, any term may match, top 150 |
| `vector_duckdb.sql` | `array_cosine_similarity` over `FLOAT[384]`, top 150 |
| `vector_postgres.sql` | pgvector `<=>`, top 150. Never run: the embedded Postgres has no pgvector |
| `hybrid_duckdb.sql`, `hybrid_postgres.sql` | reciprocal rank fusion (k = 60), one row per hash, recency blend on time-aware cases, top 5 |

Every arm ranks distinct passages (one row per `sha256`) and each row carries every `(source, path, charStart)` holding
that hash, so duplicates cannot use up the list.

Each hybrid statement contains its keyword and vector arms verbatim, and the judge refuses a run if they have drifted.
`hybrid_postgres.sql` has never run whole; its fusion half has, with its vector arm replaced by the keyword list.

**No reranker: that is the boundary.** A cross-encoder runs a model per candidate pair at query time, which is outside the
engine. Everything here is either data preparation (the vectors) or a statement the database executes.

Hybrid's relevance is the fused score divided by its largest possible value, `2 / (60 + 1)`, a constant that needs no
look at the results. It is needed only for the blend `0.7 * relevance + 0.3 * recency`, which would otherwise be decided by
recency alone: a fused score never exceeds 0.033.

## The structured route

A structured query always goes through lite: the judge starts `//core:server` and POSTs to
`/api/pure/v1/execution/execute` with the model as text and `runtime::DuckDb`. It never runs one itself. Rows are compared
as a set, numbers by value (`4`, `4.0` and `4.00` are one value, and a boolean is not a number). A refusal or a failure is
an ERROR, never an empty result.
