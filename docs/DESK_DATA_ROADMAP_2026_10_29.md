# The desk's data, end to end: the roadmap (2026-10-29)

**Status: AGREED 2026-10-29 but for decision P (§2); work under way.** Written to read with the user first (the
user, 2026-10-29: "Let's write down full roadmap so we can start after we look at it together").

This orders the work of the data products design, docs/DEEPHAVEN_TABLES_AND_SOURCES_2026_10_29.md ("the design"). It
also takes in what is left of the one-connection plan (docs/ONE_CONNECTION_DESK_2026_10_28.md: steps 6 and 7, "Next
(D6)", "Last") and the open Deephaven items (docs/DEEPHAVEN_START_HERE.md). Sizes are estimates, not measures.

## 0. Where it ends

1. **Declared once.** Every table the desk reads is declared in the model by an ingest: its columns, its key, where
   its rows come from, how they land, how much history it keeps.
2. **Made by the platform.** The platform makes those tables wherever the environment says, a Deephaven server or the
   tab's DuckDB. No table is written by hand on either.
3. **Rows from real sources.** Rows come from where production's come from:
   - topics, through Kafka;
   - the masters (security master, risk, organisation), through scheduled pulls;
   - position keeping's morning files.

   In a demo, one simulated world, written in TypeScript, stands in for all of them:
   - **on Deephaven**, behind our own Kafka door, so Deephaven runs production's path with only the address changed;
   - **in the tab**, writing the tables directly, with no server and no Kafka.
4. **The desk is a data space made of data products**, one per domain.
5. **`market.groovy` is deleted** (2,351 lines). The simulation is the TypeScript world's; the tables are the generated
   startup program's.
6. **It runs for hours.** The desk stays up on Deephaven without its memory growing or its cycles slowing.

**The door** is our own Kafka API: a small server of ours that answers the requests Kafka's clients send, here only the
four Deephaven's reader needs (design §A.5, `experiments/kafka-door-spike`).

## 1. Where it starts

| | Today |
|---|---|
| The desk's tables | written by hand in the trading model's store, `core/src/test/resources/stress/30-store.pure` |
| On Deephaven | made by `market.groovy`, which also simulates the market and writes the rows; DH16 checks each table against the store |
| In the tab | made by the live DuckDB engine from `market-sim/tables.ts`'s copies of the columns; the TypeScript world writes them |
| The desk's data space | two contexts, "In this tab" and "Deephaven", both over `stress::AllMapping` (`environments.pure`) |
| Kafka | none. A spike showed the pinned Deephaven reads our door: every row once, at 2,000 and 20,000 rows a second |

## 2. Decisions

The user, 2026-10-29: "Retention, door for sure, no need for tansu just use door, order do all in order you think but
all together in same PR don't worry about license for the moment".

| | Decision | Needed before | Decided |
|---|---|---|---|
| **W** | The words (below) | A1 | `retention=` (the user). The other two as recommended below, taken under "do all in order you think"; still the user's to change |
| **K** | The Deephaven demo's Kafka: our door, or a real broker | B1 | the door ("door for sure") |
| **S** | Decision 3 of the design said "the demo's simulator stays in the server, writing rows only". With the door, the simulation runs as its own process instead | B1 | yes: it follows from K |
| **P** | How pulled reference data reaches Deephaven (below) | B3 | **open**: the user asked for the best options; recommended (c) |
| **X** | A comparison lane against Tansu or Apache Kafka | B5 | no ("no need for tansu just use door"): B5 dropped |
| **O** | The order | after A | the recommended order (§4) |
| **L** | Where it lands | -- | one PR for all of it; the licence set aside for now (§5) |

**W, the words:**

1. **How much history a table keeps: `retention=`** (the user). `retention=Last('PT8H')`, `retention=LastRows(200000)`.
   It is Kafka's and Deephaven's word for exactly this; "history" in Legend usually means milestoning.
2. **Where a topic's format is written:**
   - On the Kafka connection (`format: Json;`, `format: Avro;`) when the ingest reads someone else's topic: the
     producer chose the format, so it belongs to where the data is.
   - On the ingest (`Stream<Json>`) only when producers write into the ingest's own topic, as the world's status does.
3. **The tab's word for "the world writes these tables":** `Simulated` (design §9 question 5). Leaving the stores
   unbound in the tab's environment would look the same as a forgotten binding.

**P, how pulled reference data reaches Deephaven.** Two questions hide in "the pull":

1. **How a master publishes.** That is the owner's choice, written as its ingest's read mode:
   - `Pull`: the platform queries the master's database on a schedule (the desk's masters, in the design);
   - `Stream`: the master publishes each change to a topic (change data capture); fresher, no schedule;
   - `Snapshot<Parquet>`: the master drops snapshot files (Legend's own form).

   The last two need no one to run a query. The rest of this is about `Pull`.
2. **For a `Pull`: who runs the query, and how its rows reach Deephaven.** Four ways:

| | Who runs the query | How the rows reach Deephaven | For | Against |
|---|---|---|---|---|
| (a) | Deephaven itself: the generated program, over JDBC (`deephaven-extensions-jdbc-42.7`) | Deephaven reads the result into a table | no other process; the master's database does the query | every Deephaven server needs each master's JDBC driver (42.7 ships none), network route and password; the schedule, retries and errors written as Groovy; the query runs once per server; it serves Deephaven only |
| (b) | the platform | written into a Deephaven table, as an Arrow upload into an input table | the passwords stay with the platform | a writer lite does not have; input tables are what the licence's section 4 restricts; after a server restart the table is empty until the next run |
| (c) | the platform | published to a keyed topic (the door in the demo, Kafka in production), read like every stream | Deephaven has one way in for all its data; the query runs once however many servers read it; a restarted server reads the topic again; the passwords stay with the platform; no input tables | the runner must publish (into the door in the same process in the demo; Kafka's Java client in production); a key a run no longer returns needs Kafka's delete marker, and how Deephaven's reader takes one is to be measured |
| (d) | the platform | written as a Parquet snapshot that Deephaven reads again, or an Iceberg table it refreshes from (both readers in 42.7) | each run's snapshot replaces the last, so keys that vanish vanish | storage shared by the runner and every server; a second way into Deephaven beside Kafka |

**Recommended: (c).** It reverses the earlier lean to (a), for three reasons:

- the masters' drivers and passwords stay in one place, the platform's, with every other connection's;
- Deephaven gets all its data one way: the subscriptions the generated program makes;
- a restart, or a second server, costs nothing.

In the demo, the masters are a small DuckDB database the world writes at start. The runner plans each pull with the
planner, runs it on that database on its schedule, and publishes the rows to the door. It is the platform's piece,
kept apart from the world's even though they share the demo's process. Production's runner (the platform's server,
real Kafka) is later work.

## 3. The stages

### A. The foundation (does not touch Deephaven)

**A0. Settle the words** (W) on the desk's example. The design's §3a made final, and the
`docs/SEMANTICS_REGISTER.md` rows drafted. *Small.*

**A1. Ingests in lite's compiler.**

- **Build:**
  - The `###Lakehouse` `Ingest` element, parsed:
    - Legend's forms: `Snapshot` or `Delta` files, `CSV` or `Parquet`; `owner=AppDir(...)`; datasets with columns
      and `pk=`.
    - Lite's: `Stream`, `Pull<schedule>`, datasets made by a function, `writeMode=`, the history setting.
  - Its JSON, tested against Studio's round-trip fixtures (test input only, never run by the platform: the
    reference-checkout tenet).
  - `include Ingest` in `###Relational`: an ingest's datasets become the store's tables.
  - The design's §7 checks.
- **A design point:** a dataset made by a function takes its columns from the function's result.
  - For a plain read of another store's table (`|#>{tickerplant::Topics."..."}#`, every stream and pull on the desk),
    they are that table's columns, known while the elements are compiled.
  - Any other function's columns need its body type-checked, which compiling elements must not do (AGENTS.md
    invariant 6). Those tables are typed on demand, when a query first reads them.
- **Done when:** core tests of each form and each refusal pass, and the JSON round trips.
- *Medium to large.*

**A2. The desk's tables declared by ingests.**

- **Build:** in the trading model:
  - the topic stores (`tickerplant::Topics`, `newswire::Topics`, `oms::Topics`);
  - the masters' stores (`secmaster::DB`, `riskdb::DB`, the others);
  - the ingests of the design's §4.2.

  `store::DB` includes the ingests.
- **Delete:** the hand-written declarations of those tables in `30-store.pure`.
- **Done when:** a test shows the compiled store's tables are the same as before (columns, types, keys), and every
  lane over the trading model is green: core, corpus, DataCube, the desk lanes.
- *Medium.* The trading model is the stress corpus, and many tests read it.

### B. Deephaven made by the platform (D6, the root cause; on the Deephaven branch until the licence answer)

**B1. The world as a feed, behind our Kafka door.**

- **Build:**
  - The door as product code, from the spike. Each table a stream reads is a topic, named as its topic store's table:
    - an appended table's topic drops records past its retention;
    - a keyed table's topic keeps the latest record of each key (Kafka's "compacted" topic).

    Records are JSON with the dataset's columns.
  - A second destination for the world's writes (`MarketSink`), appending to the door.
  - A feed process in Node (the world, the Coinbase feed for the crypto names, the door), and a Bazel target that
    starts it beside Deephaven.
- **Measure first:** every column kind Deephaven holds (the twelve DH16 measured) through JSON into Deephaven's reader,
  instants, dates and decimals above all.
- **Done when:**
  - the protocol tests pass: encoding, the checksum, an unknown request refused by name;
  - the pinned Deephaven reads every table the world writes, every row once.
- *Medium.*

**B2. The generated startup program: streams.**

- **Build:** from the model, for each dataset that lands on a Deephaven environment, a Kafka subscription with the
  dataset's columns:
  - **Appended with a retention:** at start, read from the record at "now less the retention", which Kafka finds by
    time (the door already answers that lookup). After that, rows past the retention are dropped.
  - **Latest by key:** read from the beginning, one row per key.
  - **The world's status table:** the same way.
- **Rules it keeps:**
  - The program is Groovy text for Deephaven, so it is a render target of the one compiler (the execution tenet's
    decision rule), never strings assembled where they are used.
  - DH16 stays and checks the generated tables.
  - It is installed by the test fixture (as its application) and, on a real server, by an internal user. The licence's
    section 4 says no third party may define input-table schemas.
- **Done when:**
  - `deephaven_market_test` and the desk lane on Deephaven pass;
  - the screens still show the same rows on DuckDB and Deephaven (P4);
  - after a restart, every table is made again.
- *Medium to large.*

**B3. Reference data and positions on Deephaven.**

- **Build:**
  - The pulls, as decided in P: each runs on its schedule, and once at start.
  - Position keeping's files, read from their write location. Deephaven reads Parquet (`deephaven-extensions-parquet-table`).
  - In the demo, the world writes the masters' small database and the files at start.
- **Done when:** each table's rows on Deephaven equal the world's, and a test with a short interval sees a pull run on
  its schedule.
- **Replaces:** P5, "reference data and world status written by the platform".
- *Medium.*

**B4. `market.groovy` deleted.**

- **Delete:** the whole file: the simulation, its Groovy Coinbase feed, and the table making. Two of the open
  Deephaven items go with it: the feed's spliced messages after a sleep, and the 148 conversions that throw.
- **Decide then:** whether the corpus's own `corpus.groovy` (76 lines) is generated the same way or kept as a fixture.
- **Done when:** every Deephaven lane is green with only the feed process and the generated program.

**B5. Dropped** (decision X: "no need for tansu just use door"). The door is the only Kafka the desk runs against.

### C. The tab (does not touch Deephaven; the one-connection plan's steps 6 and 7)

**C1. The tab's tables from the ingests.**

- **Build:**
  - The live engine's tables made from the ingests: an appended table with a retention becomes a bounded stream, a
    latest-by-key table becomes a keyed table.
  - The tab environment's topic and master stores become `type: Simulated { world: 'market' }`. The market is the
    connection's (decided 2026-10-29).
  - The world writes the tables directly.
- **Delete:** `market-sim/tables.ts`'s column lists, and the desk host's start of the simulator.
- **Done when:** the in-tab desk lane and the live desk check pass, and the stage A measurement of the tab is
  unchanged.
- *Medium.*

**C2. The tests.**

- The in-tab desk lane runs on the `Simulated` connection.
- The Deephaven desk lane runs by its context, its ports (Deephaven's and the door's) taken from a test copy of
  `environments.pure`.
- *Small.*

### D. The API: data products (does not touch Deephaven)

**D1. `###DataProduct` in lite.**

- Legend's forms: title, description, groups of access points.
- Lite's spelling of a model access group: its mapping, its compatible runtimes, its default runtime.
- `include dataproduct` in a mapping.
- Its JSON, tested against Studio's fixtures, and the register rows.
- *Medium.*

**D2. The desk's products and its data space over them.**

- **Build:**
  - The ten products of the design's §4.1, each over its domain's mapping.
  - `desk::Desk` and `desk::DeskMapping`, which includes the products and carries the cross-domain links
    `stress::AllMapping` carries today.
  - The data space's contexts take `mappingProvider: { element: desk::Desk; keys: [ 'model' ]; }`.
- **Delete:** the data space's `mapping: stress::AllMapping`. The corpus keeps `AllMapping` for its own tests.
- **Done when:** the saved desk opens unchanged on both contexts, and Open With works.
- *Medium.*

### E. The long run (the one-connection plan's "Last")

**E1. Memory over hours on Deephaven.**

- The tape's retention is now a model setting.
- Compute, then bound: the tape with its EMA made on the server, and its history capped after that (the "desk under
  load" items 1-3).

**E2. The bugs a long run finds:**

- the page not resuming after a restart (item 4);
- a page's server state held five minutes after it closes (item 6);
- a linked view not following a pick within 30 seconds (item 10);
- the desk's open speed.

*Medium, each measured first.* Last on purpose: measuring leaks in code about to be replaced wastes the measurement.

### F. Later, on the user's word

- Shared views as materialized views, kept live (the design's decision 4).
- Access points read as `#P{...}#`, where a reader wants a function rather than classes.
- Production's formats: Avro with a schema registry, on the Kafka connection.
- Deephaven in DataCube's Database source: a server's tables listed and opened live.

## 4. The order

```
A0 -> A1 -> A2 -+-> B1 -> B2 -> B3 -> B4
                +-> C1 -> C2
          A1 ---+-> D1 -> D2 (D2 after A2)
                         all of it -> E
```

**Recommended:** A0, A1, A2, B1, B2, B3, B4, C1, C2, D1, D2, E.

- Deephaven first, as the user asked (D6 ahead of step 6).
- The tab next: it deletes the last hand-copied column lists.
- Then the API layer.
- E last, on the finished shape.

Stages B, C and D share only A, so two of them can run side by side if wanted.

## 5. Landing

- **One PR for all of it** (the user, 2026-10-29: "all together in same PR"), from `claude/deephaven-3-browser`,
  opened when the user asks.
- **The licence is set aside for now** (the user, 2026-10-29: "don't worry about license for the moment"). The
  question itself stays open (docs/DEEPHAVEN_START_HERE.md, open item 1).
- Main is merged into the branch as it moves (merge, never rebase).
- No push to main without the user's word, in the session that pushes.

## 6. What this replaces, and what it leaves out

**Replaces:**

- the design's §8, its order of work;
- the one-connection plan's steps 6-7 (stage C), its "Next (D6)" (stage B) and its "Last" (stage E);
- the open tasks:
  - P5 (B3);
  - the Market data source and its tests (C);
  - compute then bound (E1);
  - finding 10 and the open speed (E2).

**Leaves out:**

- the licence question;
- sign-in and authorization for the whole system (START_HERE, open item 3);
- D5's page half (the one-connection plan's §6);
- Python's `model_elements`;
- the Deephaven features ranked by the corpus's needs.
