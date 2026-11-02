# The conformance suite's corpus

The cases every host is held to (`docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md`, principle P5 and phase R1). The same
plans, on the same data, must answer the same values, the same Arrow and the same text on Java, Python and the browser
tab.

| File | What |
|---|---|
| `model.pure` | One DuckDB database: a table of every type with its edge values and nulls, and a table of trades. Its rows are in a `###Data` element, so every host loads them by the planner's own statements (`projectData`). |
| `queries.tsv` | One case a line: its name, a tab, the query (run with `->from(conf::RT)`). Every answer is sorted. A name that starts with `err` is a refusal. |
| `ledger.json` | The differences the hosts answer today. Each entry gives the case, the check (`json`, `arrow`, `arrow-schema`, `arrow-values`, `grid`), the two hosts and what differs. |

The suite is `bazel test //datacube:conformance_test`. It asks every case of:
- the JVM server, through legend-engine's execute, in JSON and in Arrow;
- Python's engine, the same two;
- the tab's own execute, in JSON;
- the tab's grid path.

It compares the answers. A difference not in the ledger fails it, and so does a ledger entry that no longer differs. The
ledger only shrinks: an entry leaves with the fix that removes the difference, or becomes a row of
`docs/SEMANTICS_REGISTER.md` the user ruled. An expected answer is never edited to make the suite pass. On a failure,
the suite prints the differences it found, in the ledger's own form.
