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
| `hybrid_duckdb.sql`, `hybrid_postgres.sql` | reciprocal rank fusion (k = 60), one row per hash, recency blend on time-aware cases, cut as below |

Every arm ranks distinct passages (one row per `sha256`) and each row carries every `(source, path, charStart)` holding
that hash, so duplicates cannot use up the list.

Each hybrid statement contains its keyword and vector arms verbatim, and the judge refuses a run if they have drifted.
`hybrid_postgres.sql` has never run whole; its fusion half has, with its vector arm replaced by the keyword list.

**No reranker: that is the boundary.** A cross-encoder runs a model per candidate pair at query time, which is outside the
engine. Everything here is either data preparation (the vectors) or a statement the database executes.

Hybrid's relevance is the fused score normalised by its theoretical maximum, `2 / 61`, a constant that needs no look at
the results. It is needed only for the blend `0.7 * relevance + 0.3 * recency`, which would otherwise be decided by
recency alone: a fused score never exceeds 0.033.

Postgres's `plainto_tsquery` ANDs its terms by default. `keyword_postgres.sql` turns them into ORs, because a whole-sentence
question almost never contains all of its terms in one passage and DuckDB's `match_bm25` is already any-term.

## The structured route

A structured query always goes through lite: the judge starts `//core:server` and POSTs to
`/api/pure/v1/execution/execute` with the model as text and `runtime::DuckDb`. It never runs one itself. Rows are compared
as a set, numbers by value (`4`, `4.0` and `4.00` are one value, and a boolean is not a number). A refusal or a failure is
an ERROR, never an empty result.

## Running it

```
UNSTRUCTURED_ORACLE_PYTHON=<venv>/bin/python UNSTRUCTURED_MODEL_DIR=<model> tools/unstructured-oracle/run.sh --write
```

`run.sh` starts the embedded Postgres in a throwaway directory, loads the corpora, computes the vectors, builds the index
and runs the judge. The judge stops, naming the cause, if the key is not the one `DECISIONS.md` last recorded, a pointer of
the key does not match the corpus, the vectors were not made on the pins, or a hybrid statement has drifted from its arms.
`--check` is a fresh run against the committed `ledger.tsv`; `judge.py --compare OLD NEW` is that gate over two files.

## The ledger

One row per (case, route, arm, backend), with no headline score. **The cut** is the top `max(5, accepted pointers)` of each
case, so that a `match: all` case can pass in principle (`projects/unstructured/evals/DECISIONS.md`, entry 3).

| Verdict | Means |
|---|---|
| `PASS` | every accepted pointer inside the cut (`match: all`), or at least one (`match: any`). Carries path, section, asOfDate, sha256 and rank, `\|`-joined, one per pointer found |
| `DROPPED` | not that. On the structured route, the rows differ from the key's, missing or extra |
| `STALE_HIT` | a superseded pointer inside the cut, even with the accepted one there too; it outranks `DROPPED` |
| `EMPTY_OK` | a negative, and nothing returned |
| `FALSE_HIT` | a negative, and something returned |
| `REFUSED` | the arm is not available on that backend, named |
| `ERROR` | the search broke; never spelled as an empty result |
| `UNRESOLVED` | the case's status is not `resolved` |

`road` says how a row was reached: `lite` for the structured route, `direct-sql` for the text arms, which have no Pure
spelling yet. `summary.tsv` is the counts per route, arm and backend, generated from the ledger.

**The gate:** a run that turns any row from PASS to anything else, or loses it, fails, even if the counts improve.

## What this measures, and what it does not

Search is over all of `CO_PASSAGE`, not scoped to the case's corpus, and a text arm has no score cut, so a vector search
always returns neighbours: a negative on it cannot be `EMPTY_OK`.
