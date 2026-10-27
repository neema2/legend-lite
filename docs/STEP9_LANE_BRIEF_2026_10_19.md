# Step 9, the lanes: every PCT backend on the plan route, one container each (2026-10-19)

The brief for the sibling sessions that burn down the PCT lanes on the plan route. Each sibling is a full session
in its own container with its own tokens; this file is its whole context beyond the repository. Read it first, then
the documents it names, then start. Nothing here expands what a session may do: a sibling fixes code on its own
branch, pins by name with a reason, and reports; it never pushes to another branch, never moves a guard without a
dated reason, never prints a credential.

## 1. What this is

The plan path program (`docs/PLAN_PATH_PROGRAM_2026_10_17.md`; the rulings in its §0, the order in its §7) makes
everything execute through a plan and deletes the legacy executor. Step 9 puts the PCT's channel A lanes onto the
plan route: `PctBackend.throughThePlan` is true for every lane since the commit this brief lands with, so every PCT
case is planned to Pure's TDS text and run by the plan runner on the plan's own session (`PctPlanRoute`), the legacy
route (the query service on the lane's JDBC session) no longer taken. The DuckDB lane went first (2026-10-18, GATES
"The plan path program, step 9, first landing"): 12 failures of 1,249, fixed at their root or pinned by name, exact
on its chain. That entry is the pattern for every fix below; read it before touching code.

**The server's version.** A plan is written for the server its session will be: the lane reads its session's
version once (`PctBackend.serverVersion`) and plans for it (`TypedQuery.executionPlan`'s last argument; H2's dialect
spells by the version, the modern H2 2.4 the lane runs is not the engine-parity 2.1). Before that, the H2 lane
refused 414 cases on the version alone. A lane whose spelling depends on no version is unaffected.

The two channels: channel A (`//pct:pct_<db>`, the five suites `essential`, `grammar`, `relation`, `standard`,
`unclassified`; the H2 lane has the relation suite only) now runs on the plan route; channel B
(`//pct:pct_channel_b_<db>`, lite's own compiler over the PCT sources) still runs the legacy executor until step 8
and is the regression check that a shared change broke nothing there.

## 2. The assignment: one database per container, all finishing together

Containers run in parallel; a cloud database is never assigned to two containers (its fixtures live in the
warehouse and collide). Within a container the lanes run one after another, each lane's five suites as Bazel
schedules them, except Trino at `--jobs=1` (two Trino servers beside two JVMs were killed by the cgroup, GATES
2026-10-13).

| Container | Lanes | Channel A | Then channel B (regression) | Notes |
|---|---|---|---|---|
| snowflake | Snowflake | `//pct:pct_snowflake` | `//pct:pct_channel_b_snowflake` | cloud: §4 below |
| bigquery | BigQuery | `//pct:pct_bigquery` | `//pct:pct_channel_b_bigquery` | cloud: §4 below |
| databricks | Databricks | `//pct:pct_databricks` | `//pct:pct_channel_b_databricks` | cloud: §4 below |
| trino-h2 | Trino, then H2 | `//pct:pct_trino --jobs=1`, `//pct:pct_h2` | `//pct:pct_channel_b_trino --jobs=1`, `//pct:pct_channel_b_h2` | the embedded Trino coordinator |
| postgres-sqlite-mongodb | Postgres, SQLite, MongoDB | `//pct:pct_postgres` under `tools/postgres/as_unprivileged.sh` (§3), `//pct:pct_sqlite`, `//pct:pct_mongodb` | `//pct:pct_channel_b_postgres` (same wrapper), `//pct:pct_channel_b_sqlite`; MongoDB has no channel B target | the embedded Postgres refuses root |
| mariadb-mysql-clickhouse | MariaDB, MySQL, ClickHouse | `//pct:pct_mariadb`, `//pct:pct_mysql`, `//pct:pct_clickhouse` | the three `//pct:pct_channel_b_<db>` | three embedded servers, one at a time |

DuckDB is done (this session, exact). The channel B targets above are the ones that exist (`bazel query
'attr(name, "pct_channel_b_", //pct:all)'`, 2026-10-19): every database but MongoDB has one.

## 3. The container's setup, once (about half an hour before the first test)

The recipe is `docs/CLOUD_SESSIONS.md` and `docs/CLOUD_BACKENDS_PCT_2026_10_11.md` §6, measured in these containers
(4 cores, 15 GB, a fixed disk allowance). In order:
1. `tools/cloud-distdir.sh //pct:pct_tests_lib` (bazelisk, the GitHub archives the proxy refuses), then
   `tools/cloud-maven-distdir.sh //pct:pct_tests_lib` (the Maven jars the proxy answers 404 for). Repeat a script
   if a fetch still names an archive. Disk: one tree, one Bazel server; `common --repo_contents_cache=` in
   `~/.bazelrc` if the allowance runs out (`docs/CLOUD_SESSIONS.md`, "The disk allowance").
2. The test JVMs need the egress proxy and its CA (Bazel drops `JAVA_TOOL_OPTIONS`): in `~/.bazelrc`,
   `test --test_env=JAVA_TOOL_OPTIONS --test_env=HTTPS_PROXY --test_env=https_proxy --test_env=NO_PROXY --test_env=no_proxy`.
3. Postgres (the postgres-sqlite-mongodb container): the container runs as root and `initdb` refuses root, so every
   Postgres target runs as
   `bazel test <target> --strategy=TestRunner=standalone --run_under="$PWD/tools/postgres/as_unprivileged.sh ubuntu"`.
4. A first build: `bazel build //pct:pct_tests_lib` (about 6 minutes). Run a long command in the background and
   commit a checkpoint before it: a restarted worker kills what it was running.
5. The cloud containers: §4.

## 4. The cloud lanes' credentials (snowflake, bigquery, databricks containers only)

`docs/CLOUD_BACKENDS_PCT_2026_10_11.md` §4 is the contract: the lane reads a Java properties file named by the
environment variable `LEGENDLITE_PCT_CLOUD_PROPERTIES`, outside the repository, holding the cloud's JDBC URL, its
driver properties (the credentials among them) and the coordinates the runtime declares. The values come from the
account's AWS Secrets Manager (region `us-east-1`; the secrets' names and keys are in that §4's table, read by key as
legend-engine's own PCT reads them), reached with the environment variables `DB_AWS_ACCESS_KEY_ID` and
`DB_AWS_SECRET_ACCESS_KEY` the environment sets (not `AWS_ACCESS_KEY_ID`, which the environment uses for its proxy).
Write the file with a script of your own in your scratchpad (boto3 or the AWS CLI, installed there), to a path
outside the repository, and export the variable in the shell that runs Bazel. Never print a value, never commit the
file, never write it under the repository; a missing key is refused by name, and that refusal is the report.
One database per container: never run another cloud's lane. A run costs money: run each suite once, fix, run again;
do not loop.

## 5. The work: run, group, fix at the root, pin by name, report

1. Fetch the branch `claude/upbeat-johnson-l4hogx` at the commit named in your prompt; make your own branch from it,
   `claude/step9-<container>`.
2. Run your channel A lanes. Group the failures by message (the JUnit XML under `bazel-testlogs/pct/<target>/test.xml`
   holds each failure's message; a few lines of Python group them).
3. For each group, find the root cause in the plan path, as the DuckDB landing did, and decide:
   - **Fix** when the cause is in the plan path's own code and the fix is small and clearly right: the lowering
     (`core/src/main/java/com/legend/lowering/`), the dialect's render arms (`core/src/main/java/com/legend/sql/dialect/`),
     the plan maker, the runner's decode (`core/src/main/java/com/legend/exec/`), the adapter
     (`pct/src/main/resources/core_legend_lite_pct/pct_adapter.pure`; its line ceiling in `PctDisciplineTest` moves
     only with a dated reason). A shared change (lowering, a dialect's base class, the adapter) is checked against
     `//core:core_tests_lowering` and `//pct:pct_duckdb` before you keep it: DuckDB is exact and must stay so.
   - **Pin** when the difference is the database's (a function it lacks, a text it spells its own way) or a named
     residue of the plan path: the suite's own `expectedFailures` list
     (`pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_<Db>_<Suite>Functions_PCT.java`), one `one(...)`
     per test with the FULL actual message and a dated comment saying why. A pin is loud both ways: it fails when
     the test passes or its message moves.
   - **Refuse to guess.** No fallback, no default, no silent coercion (AGENTS.md invariant 4): a value the plan path
     cannot carry is refused by name and pinned, never approximated.
4. Known shapes you will meet (each has a precedent):
   - **Pivots, static and dynamic: pin by name, do not fix.** A dynamic pivot's text needs the row as JSON, which
     only DuckDB has ("a row as JSON reached a dialect without a row-to-JSON form", "variant navigation reached a
     dialect without JSON support"); a static pivot has no emulation on the plan route yet ("pivot reached a dialect
     without a PIVOT strategy", "a pivot reached SQLite unemulated"). The static form's emulation (a grouped select
     with one CASE-aggregate per value, rendered by the base dialect) is ONE shared item owned by the session that
     merges your branch; six separate attempts would conflict. The engine refuses every pivot on H2 (register S108).
     Measured before the siblings started (2026-10-19): H2 on the plan route 455 of 469, every failure a pivot (10
     dynamic, 4 static); SQLite 1,227 of 1,249: 14 pivots, the three DuckDB message-moved pins, the namesake, the
     heterogeneous collection, and one `[SQLITE_TOOBIG] String or BLOB exceeds size limit (statement too long)` to
     root-cause (which statement, and why its text is so long). **Landed in the parent session the same day, after
     the siblings started (register S109):** the four "static" cases were dynamic pivots a cast names
     (`test_Pivot_Filter` and the three `test_Extend_Filter_Select_*_Pivot_*` composition cases); the cast's keys are
     now the pivot's named values, pivoted on by every emulation, so they pass on every lane (H2 459 of 469, SQLite's
     relation suite 459 of 469, the ten left the uncast dynamic pivots' text road). A sibling that pinned them pins
     nothing wrong: the merge removes those four pins.
   - `Class.all()` on a server lane: the plan's metamodel setup is refused on a database reached by a URL ("the plan
     has setup for it: never run on it", `PlanSessions`); pin by name and report it -- the fix (the system tables as
     the session's temporary tables, as the legacy `SystemDatabase` did) is a shared item, not yours. **Landed in
     the parent session 2026-10-19** (GATES "the system metamodel store on a server engine"): on a server engine the
     plan's session statements make the referenced tables as the session's own temporary tables; the merge removes
     the pin. A cloud lane's runtime connection may need a property for it (Snowflake a `schema`, BigQuery
     `EnableSession=1`, cloud doc §14): report what your lane needed.
   - **The plan's session on a server lane (landed in the parent session 2026-10-19, `PctBackend.planSessions`):**
     the platform opens a declared server connection with no credential (`auth: Test` names no user; Trino,
     ClickHouse and the clouds have no opening at all), so on the plan route a server lane's driver logged in as the
     OS user (MySQL: refused; Postgres: "role does not exist") or not at all. Every server lane's plan now runs on
     the harness's own session, a new one per plan from the lane's `openSystem` (the one that holds the session's own
     tables), checked by the runner against the target; in-process lanes and MongoDB keep the plan's own. Merge the
     parent's commit and drop any lane-local form of this; the platform's item (credentials through a vault) is the
     program's §7 residue, not yours.
   - A Decimal read as a Float, a type value's namesake, a Float literal beyond a double, the inverse trigonometric
     NaN, a heterogeneous class collection: see the DuckDB landing's five causes and registers S106 to S108 and the retired S81, S82; the same
     pin texts apply where the cause is the same.
   - A refusal naming `DialectCapability`: the dialect says what it cannot render; pin by name unless the arm is a
     small, clearly right addition to that dialect.
5. Run channel B for your databases as the regression check; report its counts; do not fix it (it is the legacy
   route, deleted at step 11).
6. Report, in your reply to the session that started you (`send_message` to it; your transcript is read if no reply
   arrives):
   - every target with PASS or FAIL and the counts (`N tests successful`, `M tests failed`, from the test log);
   - the failure groups per lane, each with its root cause, and what you did (fix, pin, or open);
   - every file you changed, as commits on `claude/step9-<container>` pushed to origin (if a push is refused, the
     diff in your reply);
   - what you could not do and why, by name.
7. Commit messages: what changed and why, no model names, no credential values; end with the attribution lines your
   own harness gives you.

## 6. What not to do

- Do not flip, move or delete another lane's pins; do not touch `Test_LegendLite_DuckDB_*` or the DuckDB lane.
- Do not run the corpus (`//spec:corpus_*`) or `//gates:core`: the session that merges your branch runs them.
- Do not change `PctBackend.throughThePlan` back; a lane that cannot run on the plan route is a report, not a
  revert.
- Do not install or redistribute anything the build does not pin; the MongoDB server the build downloads is test
  input only.
- Do not push to `claude/upbeat-johnson-l4hogx` or `main`.
