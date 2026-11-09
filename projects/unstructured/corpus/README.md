# projects/unstructured/corpus: the study's three corpora

The study: `docs/UNSTRUCTURED_STUDY_2026_11_09.md`. Its §1 says why these.

| Source | Directory | What | In git |
|---|---|---|---|
| `EDGAR` | `edgar/` | Real SEC filings: a pointer corpus, rebuilt locally and verified by sha256 | the manifest and README only |
| `SALES` | `emails/` | Fictional sell-side emails | everything |
| `LENDING` | `loans/` | Fictional credit agreements | everything |

`tools/unstructured-corpus/load.py` loads all three into `unstructured.duckdb`, which is built here and never committed,
and into Postgres if `PG_URL` is set.

## Embeddings

`tools/unstructured-corpus/embed.py` computes one vector per distinct passage with `BAAI/bge-small-en-v1.5` (384
dimensions, MIT licence), held by commit and weights sha256 in `tools/unstructured-corpus/pins.env`, on the libraries pinned
there; a run on anything else stops with `VERSION`. **Embedding is data preparation and is never done by the engine: lite
reads vectors, it does not make them.** The DuckDB file keeps them in `CO_PASSAGE_EMBEDDING` (`FLOAT[384]`, keyed by the
passage's `sha256`), and `CO_EMBEDDING_PIN` records what made them. `load.py` replaces the file, so run it first.

```
export $(grep -E '^[A-Z_]+_VERSION=' tools/unstructured-corpus/pins.env | xargs)     # the versions alone: pins.env's query instruction holds spaces, so the file cannot be sourced
uv venv --python "$PYTHON_VERSION" <venv>
<venv>/bin/python -c 'import os,sys; sys.stdout.write(os.path.expandvars(open(sys.argv[1]).read()))' tools/unstructured-corpus/requirements.txt | uv pip install --python <venv>/bin/python -r -     # uv does not expand ${VAR} in a version
UNSTRUCTURED_MODEL_DIR=<an empty directory> <venv>/bin/python tools/unstructured-corpus/embed.py --fetch    # the one network step
UNSTRUCTURED_MODEL_DIR=<that directory> <venv>/bin/python tools/unstructured-corpus/embed.py                # offline
```

Measured 2026-11-09: 1,769 distinct passages, one vector each, none longer than the model's 512 tokens.

**Postgres holds no vectors, and that is a measurement.** The repository's embedded Postgres (zonky's PostgreSQL 16.15)
offers 63 extensions and `vector` is not one of them. `embed.py` reads `pg_available_extensions`, reports
`REFUSED pgvector: ...` and installs nothing; the judge's vector and hybrid arms on Postgres are REFUSED rows.

## The passage rule differs by corpus, by design

| Source | A passage is | Why |
|---|---|---|
| `EDGAR` | whole lines merged to about 800 characters, a longer paragraph or table row standing alone | filings have no natural unit |
| `SALES` | one paragraph of one email | the ground truth hashes exactly that unit |
| `LENDING` | one clause of one agreement | the ground truth hashes exactly that unit |

A `SALES` or `LENDING` passage is one blank-line-delimited block of its file, never merged and never split, however short or
long. A row's `passageSha256` is the hash of that block, so an allocation request points at the paragraph stating its size
and limit, and a covenant at its clause.

## Identity

Paths are relative to the corpus's own directory. A passage is identified by `(path, charStart)`, across all three corpora,
and `load.py` refuses a duplicate. `sha256` is its fingerprint: identical text in two places has the same one, and the judge
dedupes by it before any cut.
