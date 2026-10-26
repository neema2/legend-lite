# The `#IQ{...}#` island's realistic report suite

The IQ scale program (2026-10-25, `docs/IQ_SCALE_PROGRAM_2026_10_22.md`): queries as analysts write them -- complex
joins, filters, aggregations, subqueries and windows, string and date manipulation, calculations -- over a small
retail database, judged by the live SAP IQ 17.1 as the parity cases are.

- `gen_schema.py` writes the schema: regions, employees, customers, products, orders, their lines and payments, with
  the data real tables hold (NULLs, mixed-case e-mails, padded names, cancelled and pending orders, partial payments).
  Deterministic.
- `queries_a.py` to `queries_v.py` hold the queries by group (parts E and I over the tables `gen_schema.py` adds for
  them: exchange rates, promotions, web events, support tickets, inventory; suppliers and their products);
  `build.py` writes them as
  `core/src/test/resources/iq-reports/lite/reports_<group>.yaml` (one line each, profile iq17-default) and as a corpus
  suite for `IqCorpusTest`'s coverage run.
- The root `core/src/test/resources/iq-reports` is a second parity root (`tools/iq-oracle/README.md`, "A second
  root"): snapped with `snap.sh --oracle iq --root core/src/test/resources/iq-reports --write`, judged by
  `//core:iq_reports_parity_test` and `//core:iq_reports_parity_postgres_test`.

Each query is deterministic for the parity harness's lint (`IqParity.determinism`): an ORDER BY or a window order that
reaches each source's unique key (`id`), or a grouped derived table's keys.
