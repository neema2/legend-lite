# tools/iq-oracle: SQL Anywhere 17's and real SAP IQ 17.1's answers for the `#IQ{...}#` island

The `#IQ{...}#` island (docs/IN_FLIGHT.md, the tenth line) is judged against two oracles, both snapped into
`core/src/test/resources/iq-parity/iq-answers.jsonl`:

- **Real SAP IQ 17.1** judges the `iq17-*` profiles (IQ island, landing 4, WP1, 2026-10-14): the user's HANA Cloud
  data lake, reached over the network by `snap.sh --oracle iq` ([The live IQ](#the-live-iq)).
- **SQL Anywhere 17** judges the `iq16-*` and `sqla17-*` profiles. No IQ 16 server can run here; IQ uses SQL Anywhere
  as its front end and catalog store, and local databases approximate IQ 16's settings (ISO_BINENG, ISO-8859-1, which
  the live IQ cannot be). The `iq16-*` profiles are not verified on a real IQ 16.

**CI never runs either oracle.** The IQ parity lanes judge each case against the committed answers file, needing no
network, and they fail a case that has no answer (UNSNAPPED), whose answer predates its SQL (STALE), an answer whose
case is gone (GONE), or an answer of the other oracle (ORACLE). Only a person runs the live checks (`--check`, and
`--oracle iq --check`), locally on a Mac with SQL Anywhere installed: after editing cases or the schema, after changing
this tool, and before landing a change to the answers file.

## Prerequisites

- **SQL Anywhere 17, x86_64**, at `/Applications/SQLAnywhere17`. The answers were snapped on 17.0.11.7254, and each
  answer records the build it came from. Each run sources `System/bin64/sa_config.sh`; `SA_CONFIG` overrides that
  path.
- **Rosetta 2** on Apple silicon. SQL Anywhere's client library (`libdbcapi_r.dylib`) is x86_64 only, so the client
  must run as an x86_64 process.
- **The system's universal Python 3**, `/Library/Developer/CommandLineTools/usr/bin/python3`, from the Xcode Command
  Line Tools. `snap.sh` runs it as `arch -x86_64 … -I`, which is isolated (no site packages, no `PYTHON*`
  variables). `snap.py` needs only the standard library and runs on Python 3.9. `IQ_ORACLE_PYTHON` overrides the
  path; the interpreter must be able to run as x86_64.

## Commands

```sh
tools/iq-oracle/snap.sh --write               # run every case, rewrite iq-answers.jsonl
tools/iq-oracle/snap.sh --check               # run every case, compare with iq-answers.jsonl
tools/iq-oracle/snap.sh --check --only 'lite/basics/*'   # a glob on the key (lite/<file>/<id>) or the bare id
tools/iq-oracle/snap.sh --write --only where_string_equality_and_case
bazel run //tools/iq-oracle:snap -- --check   # the same, through Bazel (it runs the checkout's own snap.py)
```

`--verbose` logs the setup. `--repo DIR` points the tool at another checkout's `iq-parity` directory and its
`tools/iq-oracle/known_crashers.txt`.

- **`--write`** writes the answers for the selected cases. It keeps the other cases' committed answers and drops any
  answer whose case is gone or skipped. The output is sorted and byte-stable: writing twice gives the same file.
- **`--check`** reports one line per finding, then exits 1 if there was any:

| Finding | Meaning |
|---|---|
| `DRIFT` | SQL Anywhere's answer now differs from the committed one. The line names the fields that differ (`rows`, `columns`, `error`, `sa_build`, `db`). `DRIFT meta` means the databases, server flags or profiles changed. |
| `UNSNAPPED` | A case (or one of its profiles) has no committed answer. |
| `STALE` | The committed answer's `sqlSha` no longer matches the case's SQL, its profile, its `decision` and `expect`, its `params`, its `settings`, or `schema.yaml`. |
| `GONE` | A committed answer has no live case: the case was deleted, renamed, or is now skipped. |
| `CRASH` | The case took the server down twice. List it in `known_crashers.txt` or mark it `sa_defect`. |
| `FAILED` | The tool could not snap the case, for example because the timestamp re-fetch came back with a different shape. |

A run takes about ten seconds; most of that is SQL Anywhere starting its TCP listener. Exit codes:

| Code | Meaning |
|---|---|
| 0 | clean |
| 1 | findings |
| 2 | a bad file or setup, such as an unknown profile, a case key the reader doesn't know, a statement that isn't a SELECT, or a database that reports unexpected creation settings |

## The live IQ

IQ island, landing 4 (WP1), 2026-10-14. `--oracle iq` snaps the `iq17-*` runs from a real SAP IQ 17.1 (`live_iq.py`):

```sh
export IQ_ORACLE_HOST=<instance>.iq.hdl.<region>.hanacloud.ondemand.com:443   # or --iq-host
tools/iq-oracle/snap.sh --oracle iq --iq-user HDLADMIN --write                # every iq17-* run
tools/iq-oracle/snap.sh --oracle iq --check --only 'lite/m1d/*'               # drift against the live IQ
```

- **Which runs.** Every case that runs under an `iq16-*` profile also runs under the matching `iq17-*` one (`twins` in
  `snap.py`, `IqParity.runs` in the Java harness), and a case may name an `iq17-*` profile itself. A case's
  `decision` and `expect` do not judge an `iq17-*` run: real IQ does, so its `sqlSha` has an empty decision and expect.
- **The credential.** The password is read from the macOS Keychain (service `legend-lite-iq`, account = the IQ user,
  stored once with `security add-generic-password -U -a HDLADMIN -s legend-lite-iq -w`) by `security
  find-generic-password -w`, straight into the driver's connection string in memory. It is never printed, logged,
  written to a file or passed on a command line, and nothing in the repository holds it or the instance's host.
- **The connection.** SQL Anywhere 17's dbcapi (the same client library as the SQL Anywhere oracle) over TLS, as
  `CON=legend_lite_iq_oracle`. SA 17's TLS library crashes on macOS's whole `/etc/ssl/cert.pem`, so it trusts a minimal
  CA chain: `--iq-ca <pem>` (`hdl_ca.pem` here is the public DigiCert chain a HANA Cloud host presented on 2026-10-14),
  or by default one built from `openssl s_client -showcerts`: the certificates the server sends after its own, and the
  system root that signed the last of them.
- **The server must be the presets'.** It must report CaseSensitive On, BlankPadding On, Collation UTF8BIN and CharSet
  UTF-8 (the `iq17-*` presets, `IqProfile`); the meta line records its version, its database properties, its PUBLIC
  precision and scale, and the user's own option values (none, on 2026-10-14).
- **The tables.** The parity schema as IQ-store tables `ll_<table>` owned by the user. Each run checks each one --
  in the IQ store, its columns' names and types, its rows (a CHAR's pad aside), against the server's own reading of
  the schema's literals -- and re-makes only one that is not as `schema.yaml` says. A case's table references are
  rewritten to them (`live_iq.rewrite`): `FROM t` becomes `FROM ll_t AS t`, `DBA.t` `HDLADMIN.ll_t AS t`, so names,
  derived column names and errors read as before; a query naming `owner.table.column` keeps no correlation name and
  has those references rewritten too, its derived names read back as written. A CTE's name is never rewritten (a
  CTE named like a table hides it). A rewrite that would not be faithful is not sent.
- **The profiles.** One connection per profile: `iq17-default` at the connection's defaults, `iq17-odbc` with the ODBC
  driver's formats, `iq17-tds` with the TDS login's options. **dbcapi cannot log in over TDS**, so the TDS login's
  options are set as temporary options on a CmdSeq connection; each `iq17-tds` answer says so (`"login":"tds
  emulated"`), and so does the meta line.
- **A case's settings and SET statements** run on a connection of the case's own, closed after it (every option it
  sets is temporary): the login's options, each query-level setting as `SET TEMPORARY OPTION`, the variables, the SET
  statements, the SELECT. Nothing persistent is ever set on the shared server. A run the live IQ cannot judge is
  answered `"not_judged": "<reason>"`, which the lanes count SKIP:
  - a database property other than the server's (CaseSensitive Off, Collation ISO_BINENG, ...): fixed when a database
    is made;
  - a database-level option other than the server's PUBLIC value: HDLADMIN may not set a shared server's PUBLIC
    options;
  - a `SET OPTION` at the user's or PUBLIC's level, or `SET TEMPORARY OPTION PUBLIC.x`: it outlives the case;
  - an option HDLADMIN may not set (SQLCODE -121): the environment's restriction, not IQ's answer;
  - a case `iq_crashers.txt` names, by key or by shape: it hung IQ 17.1 or dropped the connection, so it is never
    sent;
  - a statement still running after 60 s: HDLADMIN may not set `request_timeout` (-121), so the client cancels it
    (`sqlany_cancel`).
- **Lost connections** are never retried on the shared server: the run is reported `CRASH` (add it to
  `iq_crashers.txt`), and the tool reconnects, waiting up to five minutes.
- **Text that is not UTF-8.** IQ answers some cells as bytes that are not UTF-8 (`CHAR(233)` is the one byte 0xE9 in a
  UTF-8 database); each such byte is snapped as U+FFFD and the answer carries a `note`, so lite's text never matches.
- **Stability.** On `--write`, a run whose answer differs from its `iq16-*` twin's SQL Anywhere answer (its error's
  code, its column names or its rows) is run a second time and must answer alike (rows in any order: IQ returns rows a
  query does not order in its plan's order); one that does not is reported `UNSTABLE` and not written.
- **`--check`** re-runs the selected `iq17-*` runs and reports DRIFT (now with the fields `oracle`, `iq_version`,
  `db_properties`, `login` and `not_judged`), UNSNAPPED, STALE and GONE as for SQL Anywhere; `DRIFT meta` names the
  parts of the live IQ's meta that changed. A `--write` or `--check` of one oracle keeps the other's answers and meta
  as they are.

An answer of the live IQ:

```json
{"id":"lite/basics/alias_read_in_where","profile":"iq17-default","oracle":"iq","db":"hdl_iq17",
 "db_properties":{"CaseSensitive":"On","BlankPadding":"On","Collation":"UTF8BIN","CharSet":"UTF-8"},
 "iq_version":"17.1.263.7761","sqlSha":"…","columns":[["x","integer",0,0]],"rows":[["1"]]}
```

The lanes hold the `iq17-*` runs' statuses in the ledger like any run's. Where lite follows SQL Anywhere's semantics
and IQ's differ, an `iq17-*` run FAILs (or is a BUG or an ERROR); each such run is listed, with its cause, in
`iq-parity/iq17-divergences.tsv`, the known-divergence allowlist (`IqParity.divergences`): a FAIL, BUG or ERROR not
listed fails the lane (and a run SQL Anywhere judges may never be listed), and so does a listed run that no longer
diverges (the list only shrinks).

## What a run does

1. **Creates two databases afresh** in a temporary directory (and one more for each combination of database
   properties the cases' settings need: [Settings](#settings)).
   - `lite_iq.db`, the IQ-like store: `dbinit -c -b -z ISO_BINENG`, then `PUBLIC.precision = 126` and
     `PUBLIC.scale = 38`. This gives a case-sensitive, blank-padded database with a binary ISO-8859-1 collation and
     IQ's sizes for an unqualified NUMERIC.
   - `lite_sa.db`, plain `dbinit`: SQL Anywhere's defaults.

   `dbinit` picks its default collation from the locale, so the tool pins `en_US.UTF-8`, which gives UTF8BIN. Each
   database's creation settings are read back and checked: collation, charset, case sensitivity and blank padding.
   They are recorded in the answers file's first line.
2. **Starts its own server**:
   - The engine is named `iq_oracle_<pid>`.
   - It listens on 127.0.0.1 at a free port and ignores broadcasts.
   - `-sf all` disables every secured feature: no file access, shell, mail, web or external calls.

   Every connection names this server and this port, so the tool never reaches another server. It uses TCP rather
   than shared memory because a shared-memory client waits forever on a server that died, so a crash would look
   like a slow query.
3. **Seeds both databases** from `schema.yaml`.
4. **Runs each case** on one connection per profile.
   - The profile's options are set as temporary options, and each one is read back to confirm it took.
   - `request_timeout` is 15 seconds.
   - Only the case's SQL runs. It must be a single `SELECT` or `WITH` (or start with a parenthesis) and must contain
     no `;` anywhere but after each of the `SET` statements it may begin with. Each case is followed by a `ROLLBACK`.
   - A case with `settings`, or whose SQL begins with `SET` statements, runs on a connection of its own instead
     ([Settings](#settings)).
5. **Recovers from a crash.** When the connection is lost (SQLCODE -101, -308, -85, -100, -832 or -1068, or the
   server process exits), the tool rebuilds the server and databases and retries the case once. A second loss is
   reported as a CRASH.
6. **Cleans up.** It stops the server and deletes the directory on exit, including on Ctrl-C.

## Profiles

A case names the client profile it runs under. The profile chooses the database and the options a client of that
kind would have after connecting (the `iq17-*` profiles are the live IQ's: [The live IQ](#the-live-iq)):

| Profile | Database | Options set after connecting |
|---|---|---|
| `iq16-default` | `lite_iq` | none: the database's defaults, as an embedded-SQL or dbcapi client sees them |
| `iq16-odbc` | `lite_iq` | what IQ's ODBC driver sets: `date_order` ymd, `date_format` yyyy-mm-dd, `time_format` hh:nn:ss, `timestamp_format` yyyy-mm-dd hh:nn:ss.ssssss, `timestamp_with_time_zone_format` with the offset. The isolation level the driver also passes is the application's choice, so it stays at the default. |
| `iq16-tds` | `lite_iq` | what a TDS login gets from the login procedure: `ansinull` Off, `tsql_variables` On, `ansi_blanks` On, `chained` Off, `quoted_identifier` Off, `allow_nulls_by_default` Off, `on_tsql_error` Continue, `isolation_level` 1, `date_format` YYYY-MM-DD, `timestamp_format` YYYY-MM-DD HH:NN:SS.SSS, `time_format` HH:NN:SS.SSS, `date_order` MDY, `escape_character` Off |
| `sqla17-default` | `lite_sa` | none |
| `sqla17-tds` | `lite_sa` | the TDS set above |
| `iq17-default` | the live IQ | none (judged by real IQ 17.1, `--oracle iq`) |
| `iq17-odbc` | the live IQ | `iq16-odbc`'s |
| `iq17-tds` | the live IQ | the TDS set, as temporary options (dbcapi cannot log in over TDS) |

The exact values are in `PROFILES` in `snap.py` and in the `meta` line of the answers file.

## Settings

IQ island, 2026-10-14 (the options framework, docs/IQ_SQL_STUDY_2026_10_13.md §9.17). A case's `settings` are IQ's
settings by IQ's own names, each given at a level, as Pure's tagged values will give them: `database.<name>=<value>` (a
Database element's: a database property, an option's PUBLIC value, the clients' `CommProtocol`) and
`query.<name>=<value>` (the enclosing function's: the connection's temporary value), separated by commas, a value quoted
(`''` a quote in it) or bare. The Java harness gives them to `IqSettings.resolve` and translates the case's SQL under
the result. This tool runs such a case, and a case whose SQL begins with `SET` statements, on a connection of its own:

1. **The database** comes from the database properties given for the database, over the profile's base:
   `CaseSensitive`, `BlankPadding` (`On`/`Off`), `Collation` (`ISO_BINENG` or `UTF8BIN`) or `CharSet`
   (`ISO_8859-1:1987` or `UTF-8`, which picks the other). The base's own combination is `lite_iq` or `lite_sa`; any
   other is a database of its own, `lite_<iq|sa>_<cs|ci>_<bp|nobp>_<latin1|utf8>`, made by `dbinit` with `-c`, `-b`
   and `-z <collation>` (with IQ 16's PUBLIC precision 126 and scale 38 on the IQ base), seeded like the others, and
   checked for the properties it must report. The tool makes every database the live cases need, whichever cases a run
   selects, and lists each in the answers file's `meta` line. `AccentSensitive` other than `Off` is refused: the
   databases are binary-collated.
2. **The login** is the profile's, or the database's `CommProtocol`'s: `'TDS'` a TDS login's options, `'CmdSeq'` none.
3. **Every other setting**: a database-level option `SET OPTION PUBLIC.name = 'value'`, a query-level one (a database
   property's too, which is then SQL Anywhere's error) `SET TEMPORARY OPTION name = 'value'`, after the login's. The
   first error is the case's answer.
4. **The case's variables**, then **its `SET` statements**, each run on the connection in turn (the case's SQL cut at
   its statement separators), then its `SELECT`. The first error is the answer.
5. **The answer's DATE and TIMESTAMP columns** are fetched again at `YYYY-MM-DD` and six places wherever the
   connection's own `date_format` or `timestamp_format`, read from it, shows them otherwise.
6. **Every option the case set is restored**: the catalog's `SYSOPTIONS` (the user's and PUBLIC's values, and any
   option a `SET OPTION PUBLIC.x` made) as it was before the case, a temporary PUBLIC value reset.

The `sqlSha` of a case with settings has them as one more part, `settings=<text>`, after its params'.

## The files

**A second root** (the IQ scale program, 2026-10-25): `core/src/test/resources/iq-reports` holds the realistic report
suite -- its own `schema.yaml` (a small retail database), `lite/reports_*.yaml`, `iq-answers.jsonl`, `ledger.tsv` and
`iq17-divergences.tsv`, the same formats as below. Its cases are all iq17-default, so its answers carry the live IQ's
meta part alone. Snap it with `--root core/src/test/resources/iq-reports` (`snap.sh --oracle iq --iq-user HDLADMIN
--root core/src/test/resources/iq-reports --write`); the live IQ seeds its tables as `ll_<name>` beside the parity
schema's, whose names differ. `//core:iq_reports_parity_test` and `//core:iq_reports_parity_postgres_test` judge it.

### `iq-parity/schema.yaml`

The tables every case reads by bare name. The YAML is the subset `SqlParity.Yaml` reads, so the Java harness reads it
too:

```yaml
tables:
  - name: t
    columns:
      - {name: id, type: integer}
      - {name: nm, type: 'numeric(10,2)'}
    rows:
      - [1, '12345678.99']
      - [2, null]
```

- **Types** use IQ's names: `smallint`, `integer`, `bigint`, `tinyint`, `numeric(p,s)`, `decimal(p,s)`, `double`,
  `real`, `float`, `char(n)`, `varchar(n)`, `date`, `timestamp`, `bit`. Quote a type that contains a comma.
- **Values:**
  - Integers and bits are plain.
  - Every other value is quoted text, and the column's type reads it. A decimal or a double therefore keeps exactly
    the digits written. A plain decimal is refused.
  - `null` is NULL.
- **Columns** all allow NULL.
- **Text** must fit ISO-8859-1, `lite_iq`'s charset.

### `iq-parity/lite/*.yaml`

The cases. A `tests:` list of one-line maps:

```yaml
tests:
  - id: where_string_equality_and_case
    sql: "SELECT id, vc FROM t WHERE vc = 'hello' ORDER BY id"
    profile: iq16-default, sqla17-default
```

| Key | Meaning |
|---|---|
| `id` | Letters, digits, `_`, `.` or `-`. The case's key is `lite/<file path without .yaml>/<id>`, matching `SqlParity.key`. |
| `sql` | The island's body as a user writes it, on one line. In double quotes, a backslash in the SQL is written `\\`. |
| `profile` | One profile or a comma-separated list. Default: `iq16-default`. Each profile gets its own answer. |
| `engine` | `iq`, `sa` or `auto`, for the translator. The oracle runs the same either way. |
| `skip` | A reason. The case is not run and gets no answer. |
| `sa_defect` | A reason SQL Anywhere's answer is wrong or unusable here. Treated like `skip`. |
| `decision` | `D-xx`: the case is judged by that provisional decision rather than by SQL Anywhere. The oracle still snaps SQL Anywhere's answer as evidence. |
| `expect` | With `decision`: the decided rows as one line of JSON, a list of rows of text cells or null, e.g. `[["1"], [null]]`. |
| `settings` | IQ island, 2026-10-14: IQ's settings by IQ's names, one line of `database.<name>=<value>, query.<name>=<value>, ...`; see [Settings](#settings). |
| `params` | The values of the case's host variables (`@name`, `:name`), one line: `name = value; ...`, each value an integer (`5`), a float (`1.5`), a decimal (`0.50D`), a `'string'` (no quote or backslash inside), a date (`%2026-03-04`), a timestamp (`%2026-03-04T05:06:07.123456`), or a NULL of a type (`null:Integer`, `null:Float`, `null:String`, `null:StrictDate`, `null:DateTime`, `null:Decimal(38,2)`), as Pure writes them; names distinct without case. The Java harness lets each before the island (the variables the island reads; a NULL is an empty value, `[]->cast(@Integer)`). This tool makes each a real SQL Anywhere variable on the case's connection before it runs (`CREATE VARIABLE @name` of `BIGINT`, `DOUBLE`, `NUMERIC(38,s)`, `LONG VARCHAR`, `DATE` or `TIMESTAMP`, then `SET @name =` the value, a NULL left unset) and drops it after; the case's SQL runs as written but for `:name`, written `@name` (SQL Anywhere reads `:name` in dynamic SQL only as a placeholder a client binds: -188 with none). Under a TDS profile (`tsql_variables` On, where `@name` too is such a placeholder) the case runs with `tsql_variables` Off. IQ island, 2026-10-14 (review 6's m1 and M1): a column or an alias of a variable's name is read before the variable, never before a placeholder, so a case whose SQL names one spelled like a param (`"@lo"`, `[@lo]`) is rejected where it writes `:lo`, or where it runs under a TDS profile; rewrite it, or skip it (`skip:`). A value its type cannot hold is the case's answer: SQL Anywhere's error. IQ island, 2026-10-14 (review 5's M5): the tool wrote each value into the SQL as a constant before, which SQL Anywhere reads otherwise in measured places (`x = @v` two-valued under ansinull Off, a name matched without case, a select-list alias before a variable, `TOP @n`). The answers file's `meta` line says so. |
| `nondeterministic` | A reason: the case tests a refusal or an error, so its SQL may leave the rows or their order to the plan. The Java harness's determinism lint (`IqParity.determinism`, IQ island, 2026-10-14, review 7) fails any other case whose SQL (its SELECT, read under the settings and SET statements before it) has a TOP, FIRST or START AT without an ORDER BY that reaches a unique column of its source (each table's `id`), or a LIST without its own ORDER BY (but of a GROUP BY key or a constant); a case that opts out must not PASS, and one with nothing to opt out of may not. This tool snaps it as any case (its answer may be one plan's). |
| `member` | A reason: a string the case shows is one value of a group of values IQ's collation holds equal (a GROUP BY key shown, a DISTINCT's string), and which one IQ shows is its plan's choice (IQ island, 2026-10-15: any value of the group is an answer IQ could give). The Java harness (`IqParity.compare`) compares the case's string cells by the collation under the run's settings (trailing blanks dropped where blank padding is on, ASCII case folded where the database is case-insensitive). This tool snaps it as any case. |

Any other key is an error, so a misspelt key fails loudly.

### `iq-parity/iq-answers.jsonl`

One JSON object per line, sorted by key then profile, UTF-8, compact. The first line is `{"meta": …}`: the tool's
format version, the server flags, the pinned locale, each database's `dbinit` flags, PUBLIC options and measured
creation settings, and each profile's database and options. A Java reader skips the line without an `"id"`.

Every other line is one answer:

```json
{"id":"lite/basics/where_string_equality_and_case","profile":"iq16-default","db":"lite_iq",
 "sa_build":"17.0.11.7254","sqlSha":"…","columns":[["id","integer",0,0],["vc","varchar",20,0]],
 "rows":[["2","hello"]]}
```

or, when SQL Anywhere refuses the statement:

```json
{"id":…,"profile":…,"db":…,"sa_build":…,"sqlSha":…,"error":{"sqlcode":-143,"sqlstate":"52003","message":"Column 'nope' not found"}}
```

**`sqlSha`** is `sha256(utf8(sql) || 0x00 || utf8(profile) || 0x00 || utf8(decision) || 0x00 || utf8(expect) || 0x00 ||
bytes of schema.yaml)`, in lowercase hex (format 2; `decision` and `expect` are empty for a case SQL Anywhere judges;
a case with `params` has them as one more part, `utf8(params) || 0x00`, after `expect`'s, so editing them makes its
answer STALE too; a case with `settings` one more, `utf8("settings=" settings) || 0x00`, after those).
`sql`, `decision` and `expect` are the YAML scalars after unquoting. So editing a decided case's rows makes its answer
STALE, as editing its SQL does.

**`columns`** is one `[name, type, precision, scale]` per column, as SQL Anywhere describes the result.
- `name` is the column's name as SQL Anywhere derives it, including the expression text for an unnamed column.
- `type` is SQL Anywhere's native type: `integer`, `smallint`, `bigint`, `numeric`, `double`, `char`, `varchar`,
  `date`, `timestamp`, `bit`, and so on.
- For `numeric`, precision and scale are the declared ones. For `char`/`varchar`, precision is the length in
  characters (the client uses the database's own charset). For `date`/`timestamp`, precision is the length of the
  text.

**`rows`** keeps SQL Anywhere's row order, with every cell as text or `null`:

| Type | How a cell is written |
|---|---|
| integers, bits | decimal digits |
| numeric | SQL Anywhere's own text, with no leading zero before the point: `.01`, `-.50` |
| double | the shortest text that reads back as the same double (`1.5`, `0.0`, `-0.1`, `1e+300`; `NaN`, `Infinity`) |
| date | in the profile's `date_format` (every profile has year-month-day) |
| timestamp | always `YYYY-MM-DD HH:NN:SS.SSSSSS`. SQL Anywhere sends a timestamp to the client as text in `timestamp_format`, which shows three fractional digits by default. So a TIMESTAMP column is fetched a second time at six digits, and only that column's cells are taken from the second fetch. |
| char, varchar | as stored, with SQL Anywhere's trailing blanks |
| binary | `0x` and hex |

## Known crashers

`known_crashers.txt` lists case keys whose SQL crashes SQL Anywhere 17. They are skipped under every SQL Anywhere
profile. Prefer `sa_defect: <reason>` on the case itself; the list exists for cases generated in bulk.

`iq_crashers.txt` lists what is never sent to the live IQ: case keys (`key <case key>  # reason`) and SQL shapes
(`shape <Python regex over the case's SQL as written>  # reason`) that hung IQ 17.1 or took its connection down. A
matching `iq17-*` run is answered `not_judged`.

## What the oracle cannot say

SQL Anywhere is not IQ. It judges only behaviour the two share, and the island's `engine` flag says which rules a
query follows. (Real IQ 17.1 judges the `iq17-*` runs; the `iq16-*` runs stay SQL Anywhere's, against the decisions.) Where the IQ engine is documented to differ, a case names its decision (`decision: D-xx`) instead of
trusting SQL Anywhere. One example: SQL Anywhere returns a CHAR(n) value unpadded even in a blank-padding database,
while IQ pads CHAR(n) to n.
