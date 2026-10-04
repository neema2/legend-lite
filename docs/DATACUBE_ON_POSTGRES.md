# DataCube on Postgres: a user guide

DataCube lets you browse, group, filter and pivot a database table in your browser. Pointed at a
Postgres database, it lists the tables you may read. Click one, and every grouping, filter and pivot
you make runs **in Postgres**, as SQL that DataCube writes for you. Nothing is copied unless you ask,
and nothing in Postgres is changed.

```bash
bazel run //datacube:app -- postgresql://reader@db.example.com:5432/shop
```

That one command builds the app, connects to the database, and opens your browser on its tables.
This guide takes you from a fresh checkout to a working cube. Every message quoted here is one the
app actually prints.

---

## 1. What you need

| | |
|---|---|
| **OS** | macOS (Apple silicon or Intel), Linux (x86-64 or ARM64), or Windows 11 (x64). |
| **Bazelisk** | Installed as `bazel` ([github.com/bazelbuild/bazelisk](https://github.com/bazelbuild/bazelisk)). It runs the Bazel version the repository pins. Bazel fetches everything else: the JDK, GraalVM, DuckDB and its Postgres extension, Node and the web app's packages. |
| **A C toolchain (macOS and Windows)** | The app is compiled to a native binary by GraalVM's `native-image`. On Linux, Bazel fetches the compiler, linker, C library and zlib itself; the one host library its linker needs is `libxml2` (present on most systems; Debian/Ubuntu: `sudo apt install libxml2`). On macOS: Apple's Command Line Tools, `xcode-select --install`. On Windows: Visual Studio 2022 Build Tools with "Desktop development with C++" (`winget install --id Microsoft.VisualStudio.2022.BuildTools --override "--wait --passive --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended"`). The first build checks for it and, if it is missing, stops and says what to install. On Windows, if Bazel ran before you installed it, also run `bazel fetch --configure --force` once: Bazel keeps the C++ toolchain it found first. |
| **On Windows, also** | Developer Mode on (Settings → System → For developers) and Git for Windows at its default path; see the README's Windows prerequisites. Run the commands below in PowerShell. |
| **A browser** | Any current one. |
| **Postgres 16 or newer** | Reachable over TCP from your machine. Older servers are refused at start. See section 3 for a throwaway one in Docker. |
| **A Postgres login that can read** | `USAGE` on the schemas and `SELECT` on the tables you want to see. Nothing else: DataCube only reads. |

## 2. Get the code and build the app

```bash
git clone https://github.com/neema2/legend-lite.git
cd legend-lite
bazel build //datacube:app
```

The **first** build downloads the toolchains and compiles a native binary, which takes several
minutes. Later builds take seconds. `bazel run` (section 4) builds too, so this step only gets the
wait out of the way.

## 3. Have a Postgres to point it at

**If you already have one,** skip to section 4. You need its host, port, database name and a login
that can read (see the last row of the table above).

**To try it without touching a real database,** start Postgres 16 in Docker and load a small
sample: 5,000 orders in a `sales.orders` table, readable by a `reader` login. The sample is
[`datacube/demo/sample-shop.sql`](../datacube/demo/sample-shop.sql); run these from the repository's root.
`//datacube:verify_app_test` loads the same file into its own Postgres.

```bash
docker run -d --name datacube-pg -e POSTGRES_PASSWORD=admin -p 5432:5432 postgres:16
until docker exec datacube-pg pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; do sleep 1; done

docker exec -i datacube-pg psql -U postgres < datacube/demo/sample-shop.sql
```

**In PowerShell** (Windows), the same:

```powershell
docker run -d --name datacube-pg -e POSTGRES_PASSWORD=admin -p 5432:5432 postgres:16
do { Start-Sleep 1; docker exec datacube-pg pg_isready -h 127.0.0.1 -U postgres *> $null } until ($LASTEXITCODE -eq 0)

Get-Content -Raw datacube/demo/sample-shop.sql | docker exec -i datacube-pg psql -U postgres
```

If port 5432 is already taken on your machine, use `-p 5433:5432` and put `5433` in the URL below.
When you are done with it: `docker rm -f datacube-pg`.

## 4. Run it

```bash
bazel run //datacube:app -- postgresql://reader@127.0.0.1:5432/shop
```

The `--` separates Bazel's own arguments from the app's. Everything after it goes to the app.

**The password.** Leave it out of the URL. The app reads it the way `psql` does:
- from `~/.pgpass` (a line `127.0.0.1:5432:shop:reader:secret`, file mode `0600`), or
- from the `PGPASSWORD` environment variable (`PGPASSWORD=secret bazel run //datacube:app -- ...`), or
- if neither has it, the app asks in the terminal: `Postgres password for shop:`.

On Windows, libpq's password file is `%APPDATA%\postgresql\pgpass.conf` (same line format; no file
mode to set), and the variable is set in PowerShell with
`$env:PGPASSWORD = 'secret'; bazel run //datacube:app -- ...`. Unlike bash's one-command form, that
lasts for the whole PowerShell session, so the password prompt never appears afterwards;
`Remove-Item Env:PGPASSWORD` clears it.

You *can* write `postgresql://reader:secret@...`, but then the password sits in your shell history.

**What you see in the terminal:**

```
warehouse listening on 127.0.0.1:8765, catalogs [main, shop]
DataCube: http://127.0.0.1:8765/#key=3vQ…
Press Ctrl+C to stop.
```

Your browser opens that address. If it doesn't (for example over SSH), copy the `DataCube:` line
into a browser on the same machine. The `#key=…` part signs you in: keep it to yourself, as you
would a Jupyter token. It works for as long as the app runs.

**To stop,** press Ctrl+C. The app keeps nothing between runs. The next run starts fresh, with a new
key.

## 5. Use it

1. **Pick a table.** The page opens on *Open a source → Database*. It lists every table and view the
   login may read, each with its column count. Type in the box to filter the list, and click one.
2. **You are Live.** The title bar shows the table's name and a **Live** button, and the status bar
   says `the warehouse at 127.0.0.1:…`. Everything you do from here is a query on Postgres.
3. **Group, pivot, filter.**
   - Drag a column into **Row groups** (or right-click its header → *Pivot* → *Vertical Pivot on …*)
     to group by it.
   - Drag one into **Column labels** to pivot.
   - Use *Filter* at the bottom left to filter.
   - The **Columns** panel on the right shows each column's type, and lets you hide columns.
4. **Snap (optional).** Click **Live** to copy the table into the browser tab. From then on every
   query runs in the tab, with no round trip to Postgres. That is useful for quick exploration of a
   table of moderate size. The copy is the table as it was when you clicked.

**Open a table directly** by naming it when you start:

```bash
bazel run //datacube:app -- postgresql://reader@127.0.0.1:5432/shop --table sales.orders
```

## 6. Connection options

**The URL** has the same form `psql` takes:
`postgresql://USER@HOST:PORT/DATABASE?param=value&param=value`.
- `postgres://` works too.
- One host per URL.
- Parameters are libpq's own, for example:
  - `?sslmode=require`
  - `?sslmode=verify-full&sslrootcert=/path/ca.pem`
  - `?connect_timeout=10`

**Several databases at once.** Give several URLs. Each becomes a catalog named after its database,
and the table list shows them all:

```bash
bazel run //datacube:app -- postgresql://reader@db1/shop postgresql://analyst@db2/finance
```

Each database connects as its own URL's user, with its own password lookup.

**A database whose name has capitals or dashes.** A catalog name must look like `[a-z][a-z0-9_]*`,
so `My-Shop` is refused. Name the catalog yourself with a libpq connection string:

```bash
bazel run //datacube:app -- --postgres "myshop=host=127.0.0.1 port=5432 dbname=My-Shop user=reader"
```

**Statement timeout.** Every query is stopped by Postgres after 60 seconds. To change that, set
libpq's `options` in the URL, URL-encoded. For 5 minutes:
`...?options=-c%20statement_timeout%3D300000`.

**Another port.** The app listens on `127.0.0.1:8765`. If that is taken (say, by a second copy of the
app), use `--port 8766`, or `--port 0` for any free port. The printed address always has the real one.

## 7. What DataCube can read

**Which objects:**
- Every table, partitioned table, view, materialized view and foreign table whose columns your login
  may `SELECT`.
- System schemas (`pg_catalog`, `information_schema`) are left out.
- Column-level grants are respected: a column you may not read is not shown.

**How each column type is read** (the type names are the ones the page's Columns panel shows):

| Postgres type | Shown as |
|---|---|
| `smallint`, `integer`, `bigint` | Integer |
| `numeric(p,s)`, including a domain over it | Decimal, with that precision and scale |
| `numeric` with no precision | Float |
| `real`, `double precision` | Float |
| `boolean` | Boolean |
| `text`, `varchar`, `char`, `name` | String |
| `date` | StrictDate |
| `timestamp` | DateTime |
| `timestamptz` | DateTime **in UTC**: what `AT TIME ZONE 'UTC'` gives in `psql` |
| `json`, `jsonb` | Variant: you can read inside it |
| `bytea` | Left out of the table |
| everything else (`uuid`, `inet`, `interval`, `time`, arrays, enums, ranges, `money`, `xml`, composites, …) | String: its Postgres text form, which you can search, group and filter as text |

## 8. What it does and does not do

**It does not:**
- write to Postgres;
- create anything in Postgres;
- keep any data on disk after it stops.

Every query is a read, run in Postgres through DuckDB's Postgres extension, and bounded by the
statement timeout.

**It is for one person on one machine.**
- It listens on `127.0.0.1` only.
- It serves the page only to a loopback address.
- The sign-in key travels in the address's `#fragment`, which browsers never send over the network.

There is no multi-user mode in this command.

## 9. When something goes wrong

The app prints one line saying what failed, and stops (exit code 2). The messages:

| You see | What it means | Do this |
|---|---|---|
| `fe_sendauth: no password supplied` then `put the password in ~/.pgpass or PGPASSWORD, or start it from a terminal` | No password was found, and there was no terminal to ask in. | Add a `~/.pgpass` line or set `PGPASSWORD` (section 4). |
| `password authentication failed for user "reader"` | Wrong password, or the login doesn't exist. | Check with `psql "postgresql://reader@HOST:PORT/DB"`. |
| `Connection refused` … `Is the server running on that host and accepting TCP/IP connections?` | Nothing is listening at that host and port. | Check the host and port. For Docker, check `docker ps` and the `-p` mapping. |
| `Postgres catalog shop is PostgreSQL 15; DataCube needs 16 or newer` | The server is too old. | Use Postgres 16 or newer. |
| `the database 'My-Shop' is not a catalog name ([a-z][a-z0-9_]*): name it with --postgres NAME=DSN` | The database name can't be a catalog name. | Use `--postgres` (section 6). |
| `--table takes schema.name, with --site and --single-user` | `--table` was given a bare name. | Write it as `schema.name`, e.g. `sales.orders`. |
| `catalog shop is named twice` | Two URLs for databases with the same name. | Name one with `--postgres`. |
| `java.net.BindException: Address already in use` (a stack trace) | Port 8765 is taken, usually by another copy of the app still running. | Stop the other copy, or add `--port 0`. |

**In the page:**
- **The table list is empty.** The login can't read any table. Grant `USAGE` on the schema and
  `SELECT` on the tables, then restart the app.
- **`sign-in failed — AUTH_INVALID: wrong launch key`.** The address belongs to an earlier run.
  Use the address the current run printed.
- **`… is not a table you may read here`.** The `--table` name is wrong, or the login can't read it.
- **A grouping fails with *Data Fetch Failure*.** The dialog ends with Postgres's own error message.
  - `canceling statement due to statement timeout` means the query ran past the timeout: filter
    first, or raise the timeout (section 6).
  - Anything else is worth reporting, with that message.

## 10. Check your setup end to end (optional)

One command drives the whole thing in a headless browser against your database. It opens a table
Live, groups it in Postgres, snaps it, reloads, and checks the error pages:

```bash
bazel run //datacube:install_browser        # once: the headless Chromium it drives
DATACUBE_APP_PG=postgresql://reader:secret@127.0.0.1:5432/shop \
DATACUBE_APP_TABLE=sales.orders DATACUBE_APP_GROUP=channel \
bazel run //datacube:verify_app
```

In PowerShell:

```powershell
bazel run //datacube:install_browser
$env:DATACUBE_APP_PG = 'postgresql://reader:secret@127.0.0.1:5432/shop'
$env:DATACUBE_APP_TABLE = 'sales.orders'; $env:DATACUBE_APP_GROUP = 'channel'
bazel run //datacube:verify_app
```

Unlike bash's one-command form, these variables stay set for the whole PowerShell session (the
password in `DATACUBE_APP_PG` with them). Clear them with
`Remove-Item Env:DATACUBE_APP_PG, Env:DATACUBE_APP_TABLE, Env:DATACUBE_APP_GROUP`.

Every line should start with `ok:`. Against the sample database of section 3, the grouping step
reports `grouped by channel: 3 groups`.

## 11. On Windows

The app on Windows is the same native binary, started by a small native launcher
([hermetic-launcher](https://github.com/hermeticbuild/hermetic-launcher)) instead of the bash script
macOS and Linux use. Three differences:

- **The app runs in Bazel's folder, not yours.** A relative path among your arguments (say
  `?sslrootcert=ca.pem`) is read from Bazel's runfiles folder: give it as an absolute path, or run
  `bazel run --run_in_cwd //datacube:app -- ...`, which starts the app where you are. A relative
  `--data` is the exception: it is always where you ran `bazel run`.
- **An argument containing `"` arrives changed**, and so does one that holds a space and ends in
  `\` (`a b\` arrives as `a b\\`; `ab\` arrives intact).
  Postgres URLs and connection strings, which quote with `'`, are unaffected.
- **x64 only.** Windows on ARM is not supported: there `bazel build //...` skips the app.
- **Your account name becomes your user name in the app**, with each character a user name cannot
  hold written as `_`: an account `John Madsen` signs in as `John_Madsen`.

Ctrl+C stops the app as on the other platforms; PowerShell then shows the exit code as `-1073741510`,
which is `0xC000013A` (stopped by Ctrl+C) and not an error.
