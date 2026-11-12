# tools/unstructured-oracle: the judge of the unstructured study

Runs the frozen answer key (`projects/unstructured/evals/answer_key.yaml`) and writes one ledger row per case, route, arm
and backend. There is no headline score.

The study this judge serves, its results and the decision they lead to: `docs/UNSTRUCTURED_STUDY_2026_11_09.md`.

## The arms are SQL

`sql/` holds one file per arm and backend. These are the statements a future `textSearch` must render from Pure, so the
retrieval lives there and not in Python: the judge passes in the question and reads back rows.

| File | What |
|---|---|
| `keyword_duckdb_setup.sql` | the index: `fts`, over one row per distinct passage |
| `keyword_duckdb.sql` | BM25 (`match_bm25`), any term may match, top 150 |
| `keyword_postgres.sql` | `tsvector`, `@@`, `ts_rank`, any term may match, top 150 |
| `vector_duckdb.sql` | `array_cosine_similarity` over `FLOAT[384]`, top 150 |
| `vector_postgres.sql` | pgvector `<=>`, top 150. Has never returned a row: see below |
| `hybrid_duckdb.sql`, `hybrid_postgres.sql` | reciprocal rank fusion (k = 60), one row per hash, recency blend on time-aware cases, cut as below |

Every arm ranks distinct passages (one row per `sha256`) and each row carries every `(source, path, charStart)` holding
that hash, so duplicates cannot use up the list.

Each hybrid statement contains its keyword and vector arms' lists verbatim, and the judge refuses a run if they have drifted.

**The floors** (`projects/unstructured/evals/DECISIONS.md`, entry 5). A floor is a minimum on a result set's best score: below
it the set is empty. BM25 on `keyword_duckdb.sql` is 4.5, `ts_rank` on `keyword_postgres.sql` is 0.026, and each hybrid
statement's is 0.89 on the fused relevance, before the recency blend. `hybrid_postgres.sql` carries DuckDB's value and has never
run. The vector arm has no floor: every negative's best cosine is above the lowest PASS's, so no value could turn a negative
empty without costing a PASS. Hybrid fuses the arms' lists without the keyword floors. A fused score is rank agreement, not
relevance: a passage first in both lists scores 1.0 whatever it says, and C60 does, so no fused floor can catch it.

**The Postgres vector and hybrid statements are run on every case and refused.** The embedded Postgres has no pgvector, and
nothing is installed into it. Both statements parse; the server stops at name resolution with
`relation "co_passage_embedding" does not exist` (42P01), because that table's column needs the type pgvector provides: the
store step itself answers `type "vector" does not exist` (42704). The ledger's REFUSED rows carry the catalogue's reason and
the statement's exact error, and any other failure, such as a syntax error, is an ERROR. So `<=>` has never been exercised
here. Hybrid's fusion half has run on its own, with the vector arm replaced by the keyword list.

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

Before the first run, in this order:

1. The pinned venv and the model: the commands under "Embeddings" in `projects/unstructured/corpus/README.md`.
2. The EDGAR corpus, which is never committed: `fetch.py` and `extract.py`, as `projects/unstructured/corpus/edgar/README.md` gives them.
3. Nothing for `SALES` and `LENDING`: they are committed. Regenerating the loan documents needs `reportlab` at the version `tools/synth-corpus/gen_loan_docs.py` enforces (`projects/unstructured/corpus/loans/README.md`); the judge does not.

```
UNSTRUCTURED_ORACLE_PYTHON=<venv>/bin/python UNSTRUCTURED_MODEL_DIR=<model> tools/unstructured-oracle/run.sh --write
```

`run.sh` starts the embedded Postgres in a throwaway directory, loads the corpora, computes the vectors, builds the index
and runs the judge. The judge stops, naming the cause, if the key is not the one `DECISIONS.md` last recorded, a pointer of
the key does not match the corpus, the vectors were not made on the pins, or a hybrid statement has drifted from its arms.
`--check` is a fresh run against the committed `ledger.tsv`; `judge.py --compare OLD NEW` is that gate over two files.

`--write` also writes `evidence/ranks.tsv` and `evidence/passages.tsv`, the numbers a ledger row does not carry (`evidence/README.md`). A run is
reproduced by running `--write` and then `git diff --exit-code tools/unstructured-oracle/ledger.tsv tools/unstructured-oracle/evidence`.

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

Search is over all of `CO_PASSAGE`, not scoped to the case's corpus. The vector arm has no floor, so a vector search always
returns neighbours: a negative on it cannot be `EMPTY_OK`. The floors were read from the same 50 cases they are judged on,
four negatives among them, so a floor passing the gate says little; `DECISIONS.md` entry 5 says what would reverse them.

## Licences

Read on 2026-11-11 from each distribution's own metadata in the pinned venv: its `License-Expression`, else its `License` field, else its
licence classifier, as the "read from" column says. The repository holds none of these packages, nor the model: it names them and their
versions (`tools/unstructured-corpus/requirements.txt`, `pins.env`), and everything is fetched locally.

**The venv.** `requirements.txt` resolves to 52 distributions, and each is reachable from it:

| Distribution | Version | Licence, as declared | Read from | Pulled in by |
|---|---|---|---|---|
| `chardet` | 7.7.0 | 0BSD | License-Expression | requirements.txt (a floor, `>=7.0.0`: below) |
| `duckdb` | 1.4.5 | MIT License | classifier | requirements.txt |
| `huggingface_hub` | 2.4.0 | Apache-2.0 | License | requirements.txt |
| `numpy` | 2.5.4 | BSD-3-Clause AND 0BSD AND MIT AND Zlib AND CC0-1.0 | License-Expression | requirements.txt |
| `pg8000` | 1.31.6 | BSD 3-Clause License | License | requirements.txt |
| `PyYAML` | 6.0.3 | MIT | License | requirements.txt |
| `requests` | 2.34.2 | Apache-2.0 | License | requirements.txt |
| `safetensors` | 0.8.0 | Apache Software License | classifier | requirements.txt |
| `sentence-transformers` | 6.2.0 | Apache-2.0 | License-Expression | requirements.txt |
| `tokenizers` | 0.23.3 | Apache Software License | classifier | requirements.txt |
| `torch` | 2.15.0 | Apache-2.0 AND Apache-2.0 WITH LLVM-exception AND BSD-2-Clause AND BSD-3-Clause AND BSL-1.0 AND ISC AND MIT AND Zlib | License-Expression | requirements.txt |
| `transformers` | 5.22.2 | Apache 2.0 License | License | requirements.txt |
| `annotated-doc` | 0.0.5 | MIT | License-Expression | typer |
| `anyio` | 4.16.0 | MIT | License-Expression | httpx2 |
| `asn1crypto` | 1.5.1 | MIT | License | scramp |
| `certifi` | 2026.10.23 | MPL-2.0 | License | requests |
| `charset-normalizer` | 3.5.2 | MIT | License | requests |
| `click` | 8.5.1 | BSD-3-Clause | License-Expression | huggingface_hub |
| `cloudpickle` | 3.1.2 | BSD-3-Clause | License | joblib |
| `filelock` | 4.2.9 | MIT | License-Expression | huggingface_hub, torch |
| `fsspec` | 2026.9.0 | BSD-3-Clause | License-Expression | huggingface_hub, torch |
| `h11` | 0.16.0 | MIT | License | httpcore2 |
| `hf-xet` | 1.8.0 | Apache-2.0 | License-Expression | huggingface_hub |
| `httpcore2` | 2.16.0 | BSD-3-Clause | License-Expression | httpx2 |
| `httpx2` | 2.16.0 | BSD-3-Clause | License-Expression | huggingface_hub |
| `idna` | 3.20 | BSD-3-Clause | License-Expression | anyio, httpx2, requests |
| `Jinja2` | 3.1.6 | BSD License | classifier | torch |
| `joblib` | 1.6.0 | BSD-3-Clause | License-Expression | scikit-learn |
| `markdown-it-py` | 4.2.0 | MIT License | classifier | rich |
| `MarkupSafe` | 3.0.4 | BSD-3-Clause | License-Expression | Jinja2 |
| `mdurl` | 0.1.2 | MIT License | classifier | markdown-it-py |
| `mpmath` | 1.3.0 | BSD | License | sympy |
| `narwhals` | 2.28.0 | MIT | License-Expression | scikit-learn |
| `networkx` | 3.7 | BSD-3-Clause | License-Expression | torch |
| `packaging` | 26.3 | Apache-2.0 OR BSD-2-Clause | License-Expression | huggingface_hub, transformers |
| `Pygments` | 2.21.0 | BSD-2-Clause | License-Expression | rich |
| `python-dateutil` | 4.0.0 | BSD-3-Clause | License-Expression | pg8000 |
| `regex` | 2026.11.9 | Apache-2.0 AND CNRI-Python | License-Expression | transformers |
| `rich` | 15.0.0 | MIT | License | typer |
| `scikit-learn` | 1.9.1 | BSD-3-Clause | License-Expression | sentence-transformers |
| `scipy` | 1.18.1 | BSD License | classifier | scikit-learn, sentence-transformers |
| `scramp` | 1.5.0 | MIT-0 | License-Expression | pg8000 |
| `setuptools` | 84.2.1 | MIT | License-Expression | torch |
| `shellingham` | 1.5.4 | ISC License | License | typer |
| `sympy` | 1.14.0 | BSD | License | torch |
| `threadpoolctl` | 3.8.0 | BSD-3-Clause | License-Expression | scikit-learn |
| `tqdm` | 4.70.1 | MPL-2.0 AND MIT | License | huggingface_hub, sentence-transformers, transformers |
| `truststore` | 0.10.5 | MIT | License-Expression | httpcore2, httpx2 |
| `typer` | 0.28.0 | MIT | License-Expression | transformers |
| `typing_extensions` | 4.16.0 | PSF-2.0 | License-Expression | anyio, httpx2, huggingface_hub, sentence-transformers, torch |
| `tzdata` | 2026.6 | Apache-2.0 | License | python-dateutil |
| `urllib3` | 2.8.0 | MIT | License-Expression | requests |

**Not Apache-2.0, MIT or BSD:**

- `psycopg` and `psycopg-binary` 3.3.8, LGPL-3.0-only, were the Postgres driver until 2026-11-11. They are replaced by `pg8000` 1.31.6,
  BSD-3-Clause (`projects/unstructured/evals/DECISIONS.md`, entry 7).
- `certifi`, MPL-2.0, pulled in by `requests` alone, and `tqdm`, MPL-2.0 AND MIT, pulled in by `huggingface_hub`, `sentence-transformers` and
  `transformers`. **Kept (ruled 2026-11-11):** MPL-2.0 is file-level copyleft, compatible with an Apache project, and they are tool-time
  dependencies of the Python environment, never redistributed with lite.
- Permissive, though none of the three: `regex` (Apache-2.0 AND CNRI-Python), `typing_extensions` (PSF-2.0), `shellingham` (ISC), `scramp`
  (MIT-0), and parts of `torch` and `numpy` (Zlib, BSL-1.0, ISC, CC0-1.0, 0BSD, Apache-2.0 WITH LLVM-exception).

**The generators.** `tools/synth-corpus/gen_emails.py` and `tools/edgar-corpus/extract.py` use the standard library and this repository's
own modules; `tools/edgar-corpus/fetch.py` uses `requests`, above. `tools/synth-corpus/gen_loan_docs.py` needs `reportlab` 4.2.5, the release
it enforces, which declares a BSD licence ("BSD license (see license.txt for details)" and its classifier, read from PyPI). reportlab requires
`pillow>=9.0.0` and `chardet` with no version. A fresh resolve on 2026-11-11 takes pillow 13.0.0 (MIT-CMU) and chardet 7.7.0 (0BSD). **chardet
before 7.0.0 is LGPL** (5.2.0 "LGPL", 6.0.0 LGPL-2.1-or-later, read from PyPI), so `requirements.txt` holds it at `chardet>=7.0.0`
(ruled 2026-11-11), and an environment made from it cannot take an LGPL release: 7.0.0 is MIT, 7.7.0 0BSD. chardet 7 requires Python 3.10 or
later (its `Requires-Python`, read from PyPI), so a Python 3.9 environment cannot resolve the floor at all, and refuses rather than taking an
old one. Only regenerating the loan documents needs reportlab; the judge does not.

**The model.** `BAAI/bge-small-en-v1.5` at revision `5c38ec7c405ec4b44b94cc5a9bb96e735b38267a` (`pins.env`): MIT, in its model card's front
matter (`license: mit`) and in Hugging Face's API for that revision.

**The databases.** DuckDB 1.4.5 is MIT, and so is its `fts` extension (the licence of `duckdb/duckdb-fts`, read from GitHub). Postgres 16.15
is the repository's `@embedded_postgres`: zonky's embedded-postgres binaries (Apache-2.0, read from GitHub), downloaded from Maven Central by
`tools/postgres/postgres.bzl`, which predates this study (2026-10-04). PostgreSQL itself is under the PostgreSQL Licence, its `COPYRIGHT` file.
