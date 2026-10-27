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
tools/neo4j-oracle/snap.sh --check                    # the live drift check: DRIFT, UNSNAPPED, STALE, GONE, VERSION, FUNCTIONS, SIDE EFFECTS
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
within 60 seconds, in an explicit transaction that is committed. Its rows go to the file in the value grammar, or
Neo4j's error by its code (`Neo.ClientError...`) and its message's first line. A setup Neo4j refuses is recorded as
`setupError`, and the lanes mark the case SKIP.

**Writes** (docs/WRITES_DESIGN_2026_10_18.md §6). When Neo4j's result summary marks the query as writing (its query
type is not `r`), the graph is dumped again after the commit, as `after`: an element the first dump held keeps its
ordinal there, and a new one takes the next free (element ids outlive the transaction), so the two dumps name each
element alike and the side effects are their diff. The transaction is committed, not rolled back, because Neo4j
checks some writes only at commit (a node deleted with its relationships left raises then, the error the TCK's
DELETE scenarios expect); the next case empties the graph, so nothing leaks. A case's control query (a TCK scenario's
`executing control query`, a lite case's `control:`) then runs over that graph, its answer recorded as `control`.

**The side effects** (`side_effects`) are the TCK's, as a state diff of the two dumps: the nodes and relationships
created and deleted, the label names the graph gained and lost (distinct names, not per node), and the (element, key,
value) triples it gained and lost (an overwrite is one of each). `--check` holds the diff to every TCK scenario's
side-effect table (SIDE EFFECTS: a disagreement); the lanes judge lite's diff of its own two reads of the graph
against Neo4j's, computed the same way (`CypherParity.sideEffects`).

## The cases

- **The openCypher TCK** (`cypher-parity/tck`, Apache 2.0; its `README.md` names the commit): every scenario, a
  Scenario Outline once per Examples row, keyed `tck/<feature path>/<scenario name>` (`#<row>` for an outline).
  The reader is the Gherkin subset the TCK uses; `CypherTck.java` mirrors it step for step, and the sha guards that
  the two agree. A scenario's control query and its side-effect table are read (the control query's own expectation
  is not: Neo4j's answer to it judges); a step neither reader knows makes the case a skip in both.
- **lite's own** (`cypher-parity/lite/*.yaml`, keyed `lite/<file>/<id>`): `- id:` items with `cypher:` (a quoted
  line or a `|` block), `graph:` (a `graphs/<name>.cypher` file's name, or `CREATE` statements), optionally
  `version: 5`, `params:` (a map in the value grammar), `skip:`, a decided case's `decision:` and `expect:`, and a
  write case's `control:` query.

## The answers file

One JSON object per line. The first is the metadata: this tool's name, the format, the server version and edition
(`CypherParity.PINNED_SERVER` must be the server), the default Cypher version. Then one per case, keyed by `id`:
`cypher` (the version it ran under), `sha` (over the query, the version, the setup, the parameters as written, a
lite case's decision and expect, the procedures a case declares, and its control query: `CypherAnswers.sha`
computes the same), `graph` (the dump), and `columns` and `rows` (each cell in the value grammar) or `error`
(`code`, `message`); `setupError` instead when the setup failed; for a write, `after` (the dump after it) and, with a
control query, `control` (its `columns` and `rows`, or its `error`). Keys are sorted, so a re-snap diffs by case,
and `--write` keeps an answer the file holds where it agrees with the live one (Neo4j's row order where nothing
orders, and its elements' ordinals, are the run's): a re-snap moves only what moved.

## What the oracle cannot say

- A query whose result depends on Neo4j's internal order (no `ORDER BY`) is compared as a bag of rows, as the TCK
  compares it; `in order` scenarios in order.
- Nodes, relationships and paths in a result compare by labels, type and properties, never by an id (C5).
- A GDS algorithm's answer is PR6's: the oracle will run the procedure where GDS is installed and the answer is a
  function of the graph (docs/CYPHER_STUDY_2026_10_15.md §7).
