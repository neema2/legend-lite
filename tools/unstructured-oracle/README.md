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

Every arm ranks distinct passages (one row per `sha256`) and each row carries every `(source, path, charStart)` holding
that hash, so duplicates cannot use up the list.
