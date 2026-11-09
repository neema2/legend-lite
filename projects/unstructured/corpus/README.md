# projects/unstructured/corpus: the study's three corpora

| Source | Directory | What | In git |
|---|---|---|---|
| `EDGAR` | `edgar/` | Real SEC filings: a pointer corpus, rebuilt locally and verified by sha256 | the manifest and README only |
| `SALES` | `emails/` | Fictional sell-side emails | everything |
| `LENDING` | `loans/` | Fictional credit agreements | everything |

`tools/unstructured-corpus/load.py` loads all three into `unstructured.duckdb`, which is built here and never committed,
and into Postgres if `PG_URL` is set.

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
