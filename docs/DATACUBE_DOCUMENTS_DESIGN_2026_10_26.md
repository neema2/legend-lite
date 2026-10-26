# DataCube: documents as data (design, 2026-10-26)

**Status: proposed.** The decisions the user took on 2026-10-25 and 2026-10-26 are in §1; the rest waits on a go. This
supersedes the order of `docs/DATACUBE_UI_REVIEW_2026_10_24.md` §8 (files in the text box), whose step 1 is built.

**What the user asked** (2026-10-25): more file types first, because "the real thing we want to do with docs is
chunk/embed/vectorize/text extract for search and join with other data."

So a document is no longer only something a text box previews. Its text becomes rows a grid reads, searches and joins.

## 1. Decided

| Question | Decision |
|---|---|
| Formats | PDF, Word (`.docx`), email (`.eml`, later `.msg`) and saved web pages (`.html`), PowerPoint (`.pptx`), beside the Markdown, text, CSV and JSON a text box reads today |
| Joins | All three kinds: by the file's details, by what the text mentions, and by meaning |
| Scale | Both: tens to hundreds of files dropped on a page in the browser, and thousands from folders loaded from Python, into the same kind of table |
| Embeddings | A local model, developed in the US (the user: "make sure it's US model not foreign model"). Proposed: Snowflake's `snowflake-arctic-embed-s` (§5) |
| Where files live | Pointed to, never copied into the page (carried over from the UI review §8): a page keeps each file's name, size and SHA-256 |

## 2. What exists today

Measured or read on 2026-10-26; file references are to `main` at `7b2511a43`.

- **No file of these kinds enters the database.** A text box's files are previews only (`text-files.ts`: name, size,
  SHA-256). A grid's file is CSV, Parquet or JSON (`upload.ts` `formatOf`; `cube-document.ts` `FileSource` refuses any
  other format).
- **A file becomes a grid through Java's host operations:** the page writes no SQL; `DuckDbHostOperations.java` spells
  `CREATE OR REPLACE TABLE … AS SELECT * FROM read_csv(…)`, the model writer types the catalog, and the grid reads the
  table (`upload.ts` `readFileInto`). Python's pages run the same TypeScript in V8 against Python's DuckDB
  (`headless/data-in.ts`).
- **Join takes only tables of grids over a file, a remote file or a Python frame** (`join-open.ts` `TableKind`). A new
  source kind must be added there, in `join-sources.ts` and in headless `host-tables.ts`.
- **A vector column would be typed as an untyped Variant** (`DuckDb.java` maps `LIST` and `ARRAY` to `SEMISTRUCTURED`).
- **The browser's DuckDB serves four extensions** (`icu`, `json`, `parquet`, `httpfs`, pinned in `MODULE.bazel`);
  browser tests block DuckDB's own extension site, so any other must be vendored the same way. Python carries
  `httpfs`, `iceberg` and `avro`.
- **No document parser and no embedding runtime** is a dependency anywhere (no pdf.js, mammoth, pypdf, ONNX Runtime,
  transformers). `fflate` (already a DataCube dependency) unzips, which `.docx` and `.pptx` need.

**The browser spike (2026-10-26),** DuckDB-WASM 1.33 (DuckDB 1.5.4, the version the pages ship) in headless Chromium:

| Test | Result |
|---|---|
| Load `vss` (vectors) and `fts` (full text) from DuckDB's `wasm_eh` builds, served locally | about 0.1 s each; 403 KB and 474 KB |
| 50,000 passages of 384 numbers each | made in 0.7 s |
| The 5 nearest by cosine distance, no index | **89 ms** |
| Build `vss`'s HNSW index over the 50,000 | **158 s** |
| The same 5 through the index | 45 ms, the same 5 |
| `fts` BM25 over a few passages | ranks correctly |

So in a tab the nearest passages are found by a plain scan, fast at page sizes, and no index is built; `vss`'s index
is for Python's large folders, where DuckDB is native and uses every core. `vss` searches vectors; it does not make
them.

## 3. The shape: a documents source

A page gains a source kind beside a file, a remote file, a frame and a class:

```
{ _type: 'documents', name, files: [{ name, size, sha256 }],
  passages: { words: 200, overlap: 40 },          // how text is cut into passages
  vectors?: { model: 'snowflake-arctic-embed-s', revision, dims: 384 } }
```

It reads into three tables in the page's DuckDB, named from the source:

| Table | One row per | Columns | Made by |
|---|---|---|---|
| `<name>_files` | file | sha256, name, folder (Python), format, size, title, author, created, modified, pages; for an email: from, to, cc, subject, sent | the file's reader (§4) |
| `<name>_parts` | piece of text the file holds | sha256, part, kind (page, heading, paragraph, table row, slide, notes, header, body), page, heading, text | the file's reader |
| `<name>_passages` | passage | sha256, passage, page, heading, text, words; plus the file's columns | SQL over the parts (§4) |

A vector per passage is kept beside it, keyed by (sha256, passage), never shown in a grid.

**What a person sees.** Open Documents… (the page's ≡ and Create ▸), or files dropped on the page, makes a grid of
passages, one row each: the file's name and title, its page, its heading, the passage. Create ▸ adds a grid of the
files, one row each. A text box still previews a file; its tab can now say "Search these files" to make the grid.
Python: `page.documents(folder_or_paths)`.

**Joins** (the user's three kinds), all through the Join window:

1. **By the file's details:** a passages or files grid is an ordinary table, so it joins on a column as any grid does
   (a deal id in the file name, a folder, a date). The source kind joins `join-open.ts`'s list.
2. **By what the text mentions:** Find Mentions… on a documents grid makes a mentions table (sha256, passage, kind,
   value, where in the passage) from patterns: built-in ones (emails, URLs, ISINs, CUSIPs, tickers, amounts, dates) or
   one a person writes. Or, the strongest form, **mentions of another grid's values**: which passages name which of
   the counterparties in that grid. Either joins on `value`.
3. **By meaning:** a new join kind, *nearest passages*: each row of a grid, by a text column, matched to its k nearest
   passages above a similarity a person sets (each trade's notes to the policy paragraph that covers it).

**Search,** a box on a documents grid: by words (BM25, ranked), by meaning (nearest passages to what was typed), or
both (the two rankings fused). It adds a score column and keeps the rows that match.

## 4. Who computes what

DataCube's rule is "the database computes, the browser draws" (the charts and BI designs), and the engine's is
"Java orchestrates, the database executes" (`docs/TENET_CHARTER.md`). This design keeps every step it can in SQL and
names the one it cannot.

| Step | Where | Why |
|---|---|---|
| Read the file: PDF pages, Word paragraphs, slides, mail parts, into `_files` and `_parts` rows | the file's reader, one TypeScript implementation, run in the tab and, for Python, in its V8 (as `readFileInto` is) | **Ingress** (charter clause 5): decoding a file's format into values, as `read_csv` decodes a CSV. DuckDB has no reader for these formats. The rows go in bound (Arrow), never as SQL text the host composed |
| Cut parts into passages: words counted, overlap kept, never across files, headings carried | **SQL**, a host operation in `DuckDbHostOperations.java` | The database computes |
| Full-text index and BM25 scores | **SQL**, DuckDB's `fts` extension | The database computes |
| Mentions by pattern; mentions of another grid's values | **SQL**: `regexp_extract_all`, a join on the found values; the second through `fts` | The database computes |
| Nearest passages, scores, the fused ranking, *nearest passages* joins | **SQL**: `array_cosine_distance` over `FLOAT[384]`, a lateral top-k; `vss`'s HNSW in Python only | The database computes |
| **Vectors:** a passage's, a search's words, a grid column's values | **the model**, where the reader runs | **The named residue.** No DuckDB we ship can run a model (`vss` searches vectors; DuckDB publishes no browser build of an extension that makes them). A passage's vector is ingress, made as the file is read; a search's vector is a parameter, bound. A grid column's vectors (the *nearest passages* join) are made from values the database returned, which is the one place this design computes from results, and so it is said plainly: those vectors are written back as a table keyed by the value, never shown, and every comparison is then the database's |

One reader in TypeScript for both hosts keeps Python from having rules of its own (the twins program). Whether pdf.js
runs in Python's V8 (mini-racer) is the first thing to try (§9, D0); if it cannot, the reader runs in the tab only and
Python reads PDFs through a pinned reader of its own, with a parity test over a shared set of files.

## 5. The model

**Proposed: Snowflake's `snowflake-arctic-embed-s`.**

| | |
|---|---|
| Who made it | Snowflake (US), fine-tuned from Microsoft's E5 small (`intfloat/e5-small-unsupervised`; Microsoft, US) |
| Licence | Apache 2.0, commercial use free (its model card) |
| Size | 33M parameters; 384 numbers per passage; the 8-bit ONNX file is 34 MB |
| Quality | MTEB retrieval NDCG@10 51.98, the best of its size on its card (BGE small 51.68, E5 small v2 49.04) |
| Inputs | up to 512 tokens a passage; a search is prefixed "Represent this sentence for searching relevant passages: "; the [CLS] vector, normalised |

Checked on Hugging Face on 2026-10-26:

| Model | Maker (country) | Licence | ONNX ready | MTEB retrieval | Why not first |
|---|---|---|---|---|---|
| **arctic-embed-s** | Snowflake (US), on Microsoft's E5 | Apache 2.0 | 8-bit, 34 MB | 51.98 | — |
| arctic-embed-xs | Snowflake (US), on all-MiniLM-L6-v2 (Microsoft base; tuned in Germany by the sentence-transformers project) | Apache 2.0 (in its card's text) | 8-bit, 23 MB | 50.15 | Slightly weaker; mixed lineage |
| Granite Embedding 30M English | IBM (US), trained from scratch on data IBM vetted | Apache 2.0 | 32-bit only, 121 MB | lower | Bigger download until quantised ourselves; the choice if trained-from-scratch-by-one-US-company matters most |
| nomic-embed-text v1.5 | Nomic (US) | Apache 2.0 | 8-bit, 137 MB | ≈53 | Four times the download, 768 numbers |
| EmbeddingGemma 300M | Google (US) | Gemma terms, not Apache | — | — | Use restrictions; large |
| BGE, GTE | BAAI, Alibaba (China) | — | — | — | Excluded by the user's rule |

**How it runs.** ONNX Runtime (Microsoft, MIT): `onnxruntime-web` in the tab, fetched with the first document, never
by a grid-only page (`bundle-budget.test.ts`); `onnxruntime` in Python. The model file is pinned by Bazel at a fixed
revision and SHA-256, served from the site's `vendor/` as the DuckDB files are, and carried for Python (or fetched
once, pinned, if the wheel should stay small; to decide). The tokenizer is BERT's WordPiece (uncased, 30,522 words);
one TypeScript implementation, with a parity test against the reference tokenizer over a fixed text set, and the
vectors of a fixed set of passages pinned in the tests within a tolerance.

**Cost.** In a tab: tens of milliseconds a passage, so a few hundred documents take a minute or two, with a progress
bar; in Python, much faster. A file's vectors are kept by (model, revision, passage settings, SHA-256), in the
browser's own storage and in a cache folder for Python, so an unchanged file is never embedded twice.

**One model per page.** A page records the model and revision that made its vectors; vectors from different models
cannot be compared, so a page refuses a mix rather than give wrong scores.

## 6. Python and large folders

`page.documents("contracts/")` reads a folder (or a list of paths, or a bucket through `httpfs`) in batches with
progress, writes the same three tables, embeds, and builds `vss`'s HNSW index and an `fts` index once the passages pass
a size where a scan is slow (to be measured: in the browser a scan of 50,000 took 89 ms). The tables can be saved as
Parquet so a page opens them later without reading the files again; a page in the browser opened on Python's tables
sees the same grid.

## 7. The formats

| Order | Format | Reader | Notes |
|---|---|---|---|
| 1 | PDF | Mozilla's pdf.js (Apache 2.0), its text per page in reading order, the document's info for title, author, dates | A chunk and its worker fetched with the first PDF. A scanned PDF has no text to read: said, never guessed (no OCR) |
| 2 | Word `.docx` | unzipped with fflate; `word/document.xml` read by a small XML reader of our own (no DOM, so it runs in Python's V8 too): headings by style, paragraphs, table rows; `docProps/core.xml` for title, author, dates | The document's HTML is never made or run |
| 3 | Email `.eml`, then `.msg` | MIME read by our own reader: headers (RFC 2047 words), parts, quoted-printable and base64, charsets; the text part, else the HTML part as text; attachments listed (and readable as files of their own later) | No remote image is ever fetched. `.msg` (Outlook's compound file) follows `.eml` |
| 3 | Saved web page `.html` | text from a small tokenizer of our own: scripts and styles dropped, headings and paragraphs kept | Never run, never fetched |
| 4 | PowerPoint `.pptx` | unzipped; each slide's text frames in order, then its notes | A slide's text, not a rendering of it |

A text box's preview of each format is its text, drawn as the box draws a file today; a PDF's pages drawn as pictures
can follow.

## 8. What changes where

- **`datacube/`:** the source kind (`cube-document.ts`), the readers (one module a format, each a lazy chunk), Open
  Documents…, the grids, Search, Find Mentions…, the *nearest passages* join, the text box's link to a grid, the
  model's runner and cache, budgets.
- **`engine-client/`:** loading `fts` and `vss`; inserting Arrow with a `FLOAT[384]` column.
- **`core/` (announced in `docs/IN_FLIGHT.md` before it is touched):** host operations for passages, the `fts` index,
  mentions and vectors in `DuckDbHostOperations.java`; whether a search's score is a native the planner lowers or a
  table a host operation makes is open (§10).
- **`python/`:** `page.documents`, batches, the model through `onnxruntime`, indexes, Parquet.
- **Bazel:** `fts` and `vss` for the tab (`wasm_eh`) and Python, pdf.js, `onnxruntime-web`, the model file, each
  pinned by SHA-256.

## 9. Steps

| Step | What | Done when |
|---|---|---|
| **D0** spike, small | pdf.js reads text in the tab and in Python's V8; the model runs in the tab through `onnxruntime-web` (time a passage) and in Python through `onnxruntime`, with the same vectors within 1e-4 | Numbers in this document; a go or no-go on one reader for both hosts |
| **D1** documents as data | The source kind; readers for PDF, `.docx`, `.eml` and `.html`, `.pptx`; passages in SQL; the passages and files grids; previews; joins by the file's details; Python's `page.documents` | A folder of mixed files becomes a grid in the tab and from Python, and joins a grid on a key in the file name |
| **D2** words | `fts` vendored; Search by words; Find Mentions… by pattern and by another grid's values | "Which contracts mention which of these counterparties" is a join |
| **D3** meaning | The model, its cache; Search by meaning and fused; the *nearest passages* join | Each trade's notes find their policy paragraph |
| **D4** scale | Python's batches, `vss` and `fts` indexes over large folders, Parquet out and in | Thousands of files, searched in under a second |

Each step lands as the branch's earlier work did: tests that fail without it, the browser checks, an independent
review, CI green.

## 10. Open

1. **A search's score: a native or a table?** As natives (`bm25`, cosine similarity) a search is part of the query the
   planner writes, typed, on any database that has them (Postgres with pgvector, DuckDB); that needs a vector type in
   the model and a lowering each. As a table a host operation rebuilds per search, nothing in the planner changes, at
   the cost of a rebuild per search (milliseconds at page sizes). Proposed: a table first (D2, D3), natives when a
   second database needs them.
2. **The model file in the Python wheel, or fetched once.** 34 MB in the wheel keeps Python offline; fetched once
   (pinned) keeps the wheel small.
3. **pdf.js in Python's V8** (D0). If not, Python's PDF reader is its own, pinned and held to the tab's by a parity
   test, a twin this design would rather not have.
4. **Passage settings:** 200 words with 40 overlapping is a common start; whether a page lets a person change them,
   and whether a heading starts a new passage.
5. **`.msg`** after `.eml`; **OCR** for scanned PDFs is not planned.
