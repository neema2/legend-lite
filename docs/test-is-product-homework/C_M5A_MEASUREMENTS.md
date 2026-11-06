# C5a. M5a's measurement: the assert-free bodies, and whether the test door plans each whole (2026-11-01)

M5 (`docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md` §6 B1) moves the first class of bodies onto the program road whole:
assert-free programs. Before building the router, M5a asked, of every body the test road runs on 9a0fe445 (MR's
checkpoint):
- Does it hold an assert, judged by the query door's scan?
- If not, does `TestDoor.plan` plan it whole, or refuse it, and with what message?

The probe is never committed as code: `probes/m5a-hooks.diff.txt`, with its runner `probes/m5a-run.sh.txt`, a hook at
`BodyDriver.execute`'s entry. It ran over every core test package, both corpora (both passes) and the five channel B
suites, all green.

## Counts

| where | bodies with an assert | assert-free, planned whole | assert-free, refused |
|---|---|---|---|
| each corpus pass (DuckDB and H2, database and host) | 2,559 | 623 (EFFECT 616; frame lets 2; none 5) | 5 |
| channel B (five suites) | 1,249 (all) | 0 | 0 |
| core's test packages | ≈145 | 162 (integration 90, exec 49, sql 6, judge 5, root 4, compiler 3, planner 3, resolver 1, server 1), all answered to a Java test's sink | 8 |

## The refusals, each a kind a later step plans (none is a body the router may move at M5)

- **Corpus, per pass.**
  - 3 effectful maps (T9, M9).
  - 2 bodies of a test that calls a function the catalog does not hold (`meta::pure::tds::toRelation::transform`):
    a compile error. The walk fails those bodies too.
- **Core.**
  - A staged let used as a statement: `StagedRowsTest`.
  - A system-store read (M4's): `MetamodelStoreTest`.
  - A raw grid read as its cells and by a column's name, and `toString` over a raw grid: `TaggedListTest`.
  - A computed String argument of `executeInDb` (T9): `ExecuteInDbDuckDbTest`.
  - An `executeInDb` result read by a let (T8): `ExecuteInDbDuckDbTest`.
  - An `execute(...)` read where it stands, and `values->at(k>0)` over a relation (M9): `ExecuteFrameTest`.

## Reading

- The class that moves at M5 is the corpus's effect setup programs (616 per pass), plus core's assert-free bodies
  answered to a test's sink (162), less the shapes above.
- The router must name those shapes by a rule over the body (R16: "never by catching a failure"), not by trying the
  test door.
- Channel B moves nothing at M5: every one of its bodies holds an assert (M6-M7).
