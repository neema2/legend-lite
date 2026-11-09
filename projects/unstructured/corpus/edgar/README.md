# projects/unstructured/corpus/edgar: a pointer corpus of SEC EDGAR filings

**This is a pointer corpus: rebuilt locally, never redistributed.** The filings themselves are never committed, the
same way `tools/neo4j-oracle` treats its Neo4j server: a person runs the real thing locally, CI never does. Only
`manifest.yaml` and this README are in git (`.gitignore`); `tools/edgar-corpus/fetch.py` and `extract.py` rebuild
everything else, and the manifest proves the rebuild is the same bytes.

## Provenance

- **Source:** SEC EDGAR (`data.sec.gov`, `www.sec.gov`). EDGAR filings are public-domain U.S. government records; this
  repository still does not redistribute them.
- **Date fetched:** 2026-11-08. The selection is pinned twice over: `AS_OF = "2026-11-08"` in `fetch.py`, and the
  accessions in the manifest. A filing published later never enters the corpus.
- **Access:** the User-Agent the SEC's fair-access policy asks for, given by whoever runs the fetch (below); at most 10
  requests a second (0.15 s between calls), those two hosts only, redirects not followed.

## Rebuild and verify

From the repository root, with Python 3.9+ and `requests`:

```sh
EDGAR_USER_AGENT="Name email@example.org" python tools/edgar-corpus/fetch.py   # download every raw file the manifest pins
python tools/edgar-corpus/extract.py                                           # rebuild the extracted text and tables
python tools/edgar-corpus/fetch.py --verify                                    # check every file on disk against the manifest
```

- **`EDGAR_USER_AGENT`** is the SEC's required User-Agent: your own name and address. It has no default; `fetch.py`
  refuses to touch the network without it and says so, and the value is never written to the repository or the manifest.
  `--verify` makes no requests, so it does not need it.
- **Re-fetching is verified by sha256.** With a manifest present `fetch.py` selects nothing: it downloads each raw
  row's `source_url` again and compares it to the manifest and to the disk. A missing file is written only if its
  sha256 matches; any mismatch is reported, nothing is overwritten, and the exit status is 1.
- **`fetch.py --verify`** is local and makes no requests. It checks each raw and extracted row's bytes and sha256, and
  reports a file the manifest lists that is absent (MISSING), one that differs (MISMATCH), and one on disk the manifest
  does not list (UNLISTED). Exit status 0 only if all of them agree.
- **`extract.py`** is a pure function of the raw bytes. It refuses a raw file that does not match its manifest row, and
  a section it should find and cannot is an error, never an empty file.

## The manifest

`manifest.yaml` is the repository's record of the corpus: one row per file, sorted by path, no timestamps. Every row,
raw and extracted, carries `cik`, `company`, `form`, `accession`, `filed`, `source_url`, `bytes` and `sha256`. An
extracted row also carries `derived_from`, the raw file it came from.

## What is in it

13 filings, 16 raw files, 25 extracted files; 31,457,686 bytes in all. `extract.py` exits 1 above `DISK_LIMIT_BYTES`
(60 MB), a limit on the local disk a rebuild takes and on nothing else: none of this reaches the repository.

| Filer | CIK | Filings |
|---|---|---|
| Berkshire Hathaway | 1067983 | 2 x 13F-HR |
| Renaissance Technologies | 1037389 | 1 x 13F-HR |
| Apple | 320193 | 1 x 10-K, 2 x 10-Q, 3 x 8-K |
| JPMorgan | 19617 | 1 x 10-K, 1 x 10-Q, 2 x 8-K |

| Form | Raw | Extracted, beside the raw file |
|---|---|---|
| 13F-HR | `primary_doc.xml`, the information table XML | `holdings.csv` (`issuer,cusip,value,shares,put_call,discretion`), `cover.txt` |
| 10-K | the primary `.htm` | `item_1a.txt`, `item_7.txt`, `item_7a.txt` |
| 10-Q | the primary `.htm` | `item_1a.txt` (Part II), `item_2.txt`, `item_3.txt` (Part I) |
| 8-K | the primary `.htm` | `item_2.02.txt`, `item_8.01.txt`, whichever the filing has |

Reading it correctly:

- A 10-Q has no Item 7 or 7A. Its Part I Items 2 and 3 are the same disclosures, and those rows say so
  (`ten_k_item: "7"`, `"7A"`).
- JPMorgan files its MD&A and market-risk discussion as named parts of the report and puts only a pointer under Items
  7/7A (10-K) and 2/3 (10-Q). `extract.py` follows those pointers within the document, by the headings the filing gives
  those parts, and the row says so (`followed_pointer: true`). Its 10-Q's Item 1A points outside the document, to the
  10-K, so that file is the pointer and nothing more.
- `value` in `holdings.csv` is as filed. Each file's row count and value total equal its own `cover.txt`.
- An 8-K's press release is an exhibit, and exhibits are not kept, so `item_2.02.txt` is the short announcement. JPMorgan's
  8-K of 2026-10-13 declares items 7.01 and 9.01 only, so it has no extracted file.
- Raw `.htm` files are the bytes as filed: `www.sec.gov` appends a bot-defence `<script>` when it serves a page, and
  `fetch.py` removes it, proving the result against the size the filing index states. A sha256 over that tag would stop
  verifying when the CDN rotates it.

## Tests

A test that needs this corpus runs in a manual lane, outside `//gates:local` (a checkout has no filings until someone
rebuilds them), and **fails loudly when the corpus is absent, naming the rebuild command**, the same way
`//core:iq_corpus_test` fails without its corpora. It never skips: `SkipCensusTest` pins skips at zero.
