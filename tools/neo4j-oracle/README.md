# tools/neo4j-oracle: Neo4j Community's answers for the `#Cypher{...}#` island

The `#Cypher{...}#` island (docs/IN_FLIGHT.md, the eleventh line; docs/CYPHER_STUDY_2026_10_15.md §3.3 and §8) is
judged against the real thing: a Neo4j Community server, run locally. This tool runs every parity case on it and
snaps the answers into `core/src/test/resources/cypher-parity/neo4j-answers.jsonl`.

**CI never runs Neo4j.** Neo4j Community is GPLv3: it is never on a classpath, never a test dependency, never in CI.
The Cypher parity lanes (`//core:cypher_parity_test` on DuckDB, `//core:cypher_parity_postgres_test` on the embedded
Postgres) judge each case against the committed answers file, and they fail a case that has no answer (UNSNAPPED),
whose answer predates its query, setup or parameters (STALE), an answer whose case is gone (GONE), and a file snapped
on another Neo4j release than the pinned one (VERSION, `CypherParity.PINNED_SERVER`). Only a person runs the live
check (`--check`), locally: after editing cases, after changing this tool, and before landing a change to the
answers file.

## Prerequisites

- **Neo4j Community**, the release `CypherParity.PINNED_SERVER` names (2026.09.0 at this writing: the newest
  Community release at each landing, C9), running locally with authentication off, Bolt on `127.0.0.1:7687`. The
  tarball from `https://dist.neo4j.org/neo4j-community-<version>-unix.tar.gz` on a JDK 21, with these lines appended
  to `conf/neo4j.conf`:

  ```
  dbms.security.auth_enabled=false
  server.default_listen_address=127.0.0.1
  ```

  then `bin/neo4j start`. Or the official image, `docker run -p 7687:7687 -e NEO4J_AUTH=none neo4j:<version>`
  (Community tags carry no suffix). **The tool empties the graph before every case**: run it against a server that
  holds nothing you want to keep.
- **Python 3** with the Apache 2.0 driver: `pip install neo4j`. `snap.sh` runs `python3 -I` (isolated); `snap.py` is
  the standard library plus that driver. `NEO4J_ORACLE_PYTHON` overrides the interpreter, `NEO4J_URL` the Bolt URL.

## Commands

```sh
tools/neo4j-oracle/snap.sh --write                    # every case: write the answers file
tools/neo4j-oracle/snap.sh --write --only 'lite/*'    # one glob of case keys, merged into the file
tools/neo4j-oracle/snap.sh --check                    # the live drift check: DRIFT, UNSNAPPED, STALE, GONE, VERSION, FUNCTIONS
tools/neo4j-oracle/snap.sh --functions                # write cypher-parity/cypher-functions.txt: SHOW FUNCTIONS's names
tools/neo4j-oracle/snap.sh --census                   # the cases by what they expect; no server needed
bazel run //tools/neo4j-oracle:snap -- --check        # the same through Bazel
```

## What a run does

For each case, in key order: `MATCH (n) DETACH DELETE n`; the case's setup statements (a TCK scenario's named graph
or `having executed` blocks, a lite case's `graph`), each under `CYPHER 5` unless it names its version (the TCK's
setups are openCypher 9's text, which Cypher 5 reads as written; Cypher 25 refuses a variable created in a `CREATE`
read in a property map of the same `CREATE`, the three setups of WithSkipLimit1/[1], WithSkipLimit2/[2] and With2/[1],
measured; a setup only builds the graph the case is judged over); the graph DUMPED (every node's labels and properties, every
relationship's ends, type and properties, in the TCK's value grammar; a date-time of a named zone with the zone's
name in brackets after its offset, `DateTime('1984-11-11T12:31:14.645876123+01:00[Europe/Stockholm]')`, Java's
spelling, since the driver's text is the offset alone and the lanes store the zone the value names) -- that dump is
what the lanes seed their tables from, so CI never interprets a `CREATE`; then the query, under the Cypher version the case names (its own
`CYPHER 5` or `CYPHER 25` prefix, a lite case's `version:`) or lite's default (25, C1), with the case's parameters,
within 60 seconds. Its rows go to the file in the value grammar, or Neo4j's error by its code (`Neo.ClientError...`)
and its message's first line. A setup Neo4j refuses is recorded as `setupError`, and the lanes mark the case SKIP.

## The cases

- **The openCypher TCK** (`cypher-parity/tck`, Apache 2.0; its `README.md` names the commit): every scenario, a
  Scenario Outline once per Examples row, keyed `tck/<feature path>/<scenario name>` (`#<row>` for an outline).
  The reader is the Gherkin subset the TCK uses; `CypherTck.java` mirrors it step for step, and the sha guards that
  the two agree. A scenario with a control query, a procedure, or a step neither reader knows is skipped by both.
- **lite's own** (`cypher-parity/lite/*.yaml`, keyed `lite/<file>/<id>`): `- id:` items with `cypher:` (a quoted
  line or a `|` block), `graph:` (a `graphs/<name>.cypher` file's name, or `CREATE` statements), optionally
  `version: 5`, `params:` (a map in the value grammar), `skip:`, and a decided case's `decision:` and `expect:`.

## The answers file

One JSON object per line. The first is the metadata: this tool's name, the format, the server version and edition
(`CypherParity.PINNED_SERVER` must be the server), the default Cypher version. Then one per case, keyed by `id`:
`cypher` (the version it ran under), `sha` (over the query, the version, the setup, the parameters as written, and a
lite case's decision and expect: `CypherAnswers.sha` computes the same), `graph` (the dump), and `columns` and
`rows` (each cell in the value grammar) or `error` (`code`, `message`); `setupError` instead when the setup failed.
Keys are sorted, so a re-snap diffs by case.

## What the oracle cannot say

- A query whose result depends on Neo4j's internal order (no `ORDER BY`) is compared as a bag of rows, as the TCK
  compares it; `in order` scenarios in order.
- Nodes, relationships and paths in a result compare by labels, type and properties, never by an id (C5).
- A GDS algorithm's answer is PR6's: the oracle will run the procedure where GDS is installed and the answer is a
  function of the graph (docs/CYPHER_STUDY_2026_10_15.md §7).
