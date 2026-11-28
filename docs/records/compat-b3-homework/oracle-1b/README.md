# B3 step 1b: legend-engine 4.145.0's own answers to the 19 tests S15 and S132 carried (2026-11-24)

Asked of legend-engine itself (the engine runner's jars: `planoracle.GoldenRunMain` and `SeedDuckDbMain`, test tooling,
never core), on its own DuckDB 1.3.0, on lite's DuckDB 1.5.6 and on its own H2 2.1.214, from the tests' own model, data
and lambdas (`models/`, `requests/`, prepared by `tools/make_models.py` from the pinned 4.145.0 sources). `run.sh` asked
it; `tools/classify.py` classed each test; `classes.txt` is the whole answer; `tests.md` the preparation (each test, its
golden, how upstream compares and prints, read from the sources). Files that were scripts are kept as `.txt` (never
compiled or run from here); `<oracle1b>` stands for the scratch folder they ran in.

The result (the progress record's step 1b):
- the 14 DuckDB calendar tests: legend-engine FAILS each on DuckDB (1.3.0 and 1.5.6) with the digits lite gets --
  agrees with lite (H2-specific goldens);
- H2 `testAnnualizedValue`, `testPmaValue`: a printing difference; H2 `testCmeValue`: Campus printing, Lateral a real
  difference (lite's IEEE division, the user's 2026-09-19 "keep it ieee");
- DuckDB `log::testProject`, `log::testFilter`: legend-engine PASSES; lite's difference was a lite bug -- its expected
  Float literal written `CAST(0.09531017980432493 AS DOUBLE)`, which DuckDB converts one ULP low (fixed in DuckDB's
  dialect: the literal written as the double itself, `0.09531017980432493E0`).
