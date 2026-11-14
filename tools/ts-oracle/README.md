# tools/ts-oracle: the first gs-quant oracle, retired

The first oracle (docs/TS_GSQUANT_ORACLE_2026_11_01.md) held the gallery's and the markets example's drawn series to
gs-quant: `oracle.py` read each series' tree with an evaluator of its own (and B5's TA-Lib, statsmodels and pandas
references for the words gs-quant lacks), at 1e-6 relative; `calls.py` ran every row of Show Python through gs-quant's
signatures. Both were retired on 2026-11-09 (docs/TS_DECISIONS_2026_11_07.md item 14f, steps 4b and 4c): their checks
are `tools/gs-quant-oracle`'s -- one oracle, one judge -- where every gallery series is a case (`gallery.jsonl`), its
gs-quant side Show Python's own call run over the pages' samples, its references `references.py`, its verdict held by
`//core:ts_gsquant_parity_test`. Every series the first oracle checked is checked there, at the one tolerance; its four
departures are the same, each ruled.

What stays here:

- **`signatures.py`**: gs-quant's own signatures, read from gs-quant itself, into
  `projects/timeseries/gs-quant-signatures.json` -- what the builders' names, parameters and defaults are generated from
  (`datacube/tools/ts-builder/make.mts`).
- **`report-2026-11-01.txt`**, **`report-2026-11-06.txt`**, **`report-2026-11-08.txt`**: the first oracle's reports,
  the evidence the register's rows of those days cite (S145's among them).
