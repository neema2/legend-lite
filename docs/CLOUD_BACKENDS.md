# Cloud backends — can a solo developer get Databricks and Snowflake for free?

> **Question asked:** legend-lite needs a JDBC-reachable Databricks and Snowflake to run a
> capability-probe suite (~200–500 short statements, small DDL, a few hundred rows) once, and then
> to keep running it in CI. Is there a **free, no-cost** path to each — cloud tier, trial, or a
> local engine that speaks the same dialect?
>
> **Companions:** `H2_BACKEND.md` (the method — probe the real engine, let execution beat
> documentation), `ARCHITECTURE_REMEDIATION.md` D1 (N backends is a hard requirement),
> `AUDIT_PROGRAM.md`. `ConnectionDefinition.DatabaseType` already declares Snowflake and BigQuery,
> so this question was always coming.
>
> **Out of scope by instruction:** MariaDB/MySQL (handled locally with MariaDB4j).

**Evidence standard.** This document is **desk research against vendor pages fetched on
2026-07-31**, not execution. That is a *weaker* standard than `H2_BACKEND.md`, which probed a real
jar. Nothing here has been run. Every factual claim carries an inline source URL; every URL was
fetched or searched on **2026-07-31** unless noted. Where a vendor page and a secondary source
disagreed, the vendor page won. Where I could not find a vendor statement, the claim is marked
**UNVERIFIED** and is *not* load-bearing for any verdict. Maven versions were read from
`maven-metadata.xml` on `repo1.maven.org`, not from vendor docs — **the vendor docs are stale on
every one of the three drivers checked** (§6.1).

---

## 1. Verdict

**Both are obtainable for free. They fail at different bars, and for opposite reasons.**

| Path | (i) One-time probe run | (ii) Continuous CI backend | Why |
|---|---|---|---|
| **Databricks Free Edition** | **YES** | **YES, with caveats** | Perpetual, no card, no cloud account, one 2X-Small SQL warehouse. Quota is a *daily* fair-use cap, not an expiry. Non-commercial-use clause and no SLA. |
| **Databricks local (OSS Spark)** | **YES** | **YES** | Free, hermetic, JDBC via Thrift server. But it validates **Spark SQL**, not **Databricks SQL** — a strictly smaller dialect (§2.4). Good as a *regression* backend, not as a *conformance oracle*. |
| **Snowflake cloud trial** | **YES — comfortably** | **NO** | 30 days wall-clock, $400 credits. The credits are not the binding constraint; the calendar is. A CI backend that dies every 30 days is not a CI backend. |
| **Snowflake local emulator** | **NO** | **NO** | Neither candidate is an acceptable correctness oracle (§3.6). `fakesnow` is DuckDB-backed — it cannot be an independent oracle for a project whose primary backend *is* DuckDB. |

**Three findings that change the shape of the problem:**

1. **The Databricks free story got much better and the Snowflake free story did not change.**
   Databricks Community Edition — the notebook-only tier everyone remembers — **was retired on
   2026-01-01** and replaced by **Free Edition**, which is *perpetual*, needs **no credit card and
   no cloud account**, and includes **a SQL warehouse**
   ([community.databricks.com PSA](https://community.databricks.com/t5/announcements/psa-community-edition-retires-on-january-1-2026-move-to-the-free/td-p/141888),
   [databricks.com/try-databricks](https://www.databricks.com/try-databricks), accessed 2026-07-31).
   The decisive notebook-only objection **no longer applies**.

2. **Snowflake has no perpetual free tier, but trials are re-creatable — including on the same
   email.** Snowflake University's FAQ: *"You can sign up for a new trial and you can often use the
   same email address as the one you used before"*
   ([learn.snowflake.com](https://learn.snowflake.com/en/pages/university-faqs/), accessed
   2026-07-31). That converts "the trial lapses" from a wall into a **30-day maintenance chore** —
   which is survivable for a probe run and corrosive for CI.

3. **Cost is not the Snowflake constraint — time is.** An X-Small warehouse burns **1 credit/hour**
   with **per-second billing and a 60-second minimum per resume**
   ([docs.snowflake.com/warehouses-overview](https://docs.snowflake.com/en/user-guide/warehouses-overview),
   accessed 2026-07-31). §3.5 does the arithmetic: a 500-statement probe suite costs **well under a
   dollar**. $400 is roughly 133 credit-hours at Enterprise rates. You will hit day 31 long before
   you hit credit zero.

---

## 2. Databricks

### 2.1 What free options exist today

| Option | Status | Card | Cloud account | Expiry |
|---|---|---|---|---|
| **Community Edition** | **RETIRED 2026-01-01** | — | — | Accounts no longer accessible |
| **Free Edition** | Current, perpetual | **No** | **No** | *"no expiration for personal use"* |
| **14-day trial** | Current | **Yes** | **Yes** | 2 weeks |

**Community Edition is gone.** The announcement (posted 2025-12-15, edited 2025-12-19) reads
*"Community Edition will be retired at the end of 2025. After that, Community Edition accounts will
no longer be accessible"*; a community manager confirmed retirement on 2026-01-09
([community.databricks.com](https://community.databricks.com/t5/announcements/psa-community-edition-retires-on-january-1-2026-move-to-the-free/td-p/141888),
accessed 2026-07-31). **Do not plan against Community Edition. It does not exist.**

**Free Edition terms**, verbatim from the comparison table on
[databricks.com/try-databricks](https://www.databricks.com/try-databricks) (accessed 2026-07-31):
*"No credit card or payment required"*, *"no cloud account required"*, *"with no expiration for
personal use"*, **"One (1) serverless workspace"**, *"Limited compute size and usage"*.

The **14-day trial** is the *opposite* trade: **credit card required, cloud account required**, two
weeks, full platform including classic compute (same source). It is a worse deal for this project
in every dimension — it expires, it needs a card, and your cloud provider bills you for classic
compute. *(The commonly-cited "$400 of trial usage" figure appeared only in search snippets, not in
a page I fetched — **UNVERIFIED**, and irrelevant given Free Edition exists.)*

**Free Edition limits**, verbatim from
[learn.microsoft.com/.../free-edition-limitations](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-edition-limitations)
(page `ms.date` **2026-07-20**, accessed 2026-07-31; the AWS mirror at
[docs.databricks.com](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
carries the same text):

| Resource | Limit |
|---|---|
| **SQL warehouses** | **"One SQL warehouse, limited to a `2X-Small` cluster size."** |
| Serverless notebooks | "Limited compute size and usage." |
| Jobs | "Max of 5 concurrent job tasks per account." |
| Compute model | **Serverless only.** "Custom compute configurations are not supported." |
| Outbound internet | "restricted to a limited set of trusted domains" (lifted by LinkedIn verification) |
| Languages | **R and Scala unsupported** |
| Admin | "One workspace and one metastore per account." "**No access to the account console or account-level APIs.**" |
| Auth | "email OTP, Sign in with Google, and Sign in with Microsoft. No SSO or SCIM support." |
| Quota breach | *"your workspace's compute resources will be shut down and unavailable for the rest of the day (and in extreme cases, the rest of the month)… your data and settings will not be deleted."* |
| Licence | **"Free Edition accounts may not be used for commercial purposes."** |
| Dormancy | "Databricks may delete Free Edition accounts that are inactive for a prolonged period." |
| Support | "not covered by the Databricks support policy or Service Level Agreement (SLA)" |

Two of these deserve to be read carefully rather than skimmed:

- **"No access to the account console or account-level APIs"** is scoped to the *account* plane.
  It does **not** say workspace-level APIs are unavailable, and the workspace UI/SQL editor is the
  advertised product. Do not over-read it into "no API at all."
- **"may not be used for commercial purposes."** legend-lite is an open-source clean-sheet
  implementation with no commercial entity behind it, so this reads as satisfied — but it is a
  *licence term*, and if the project's status ever changes, the Free Edition dependency changes with
  it. Write that down now rather than discover it later.

### 2.2 Does the free option expose a SQL warehouse / JDBC endpoint? — the decisive question

**Yes for the SQL warehouse — stated by Databricks. Yes for JDBC — by inference plus one
third-party demonstration, not by an explicit vendor sentence.** This is the one place in this
document where the evidence is thinner than the conclusion, so here is the chain:

| Link | Strength | Source |
|---|---|---|
| Free Edition includes **one 2X-Small SQL warehouse** | **Vendor, explicit** | [free-edition-limitations](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-edition-limitations) |
| A Databricks SQL warehouse is reached **via a JDBC/ODBC connection** (Server Hostname + HTTP Path + token, on the warehouse's "Connection details" tab) | **Vendor, explicit** (general, not Free-Edition-scoped) | [docs.databricks.com/compute/sql-warehouse](https://docs.databricks.com/aws/en/compute/sql-warehouse/) |
| The limitations page lists **no** restriction on JDBC/ODBC, BI tools, external clients, or personal access tokens | **Vendor, by absence** | same limitations page |
| Someone actually connected **Tableau Desktop to a Free Edition SQL warehouse** using Server Hostname + HTTP Path, noting a personal access token as the alternative auth | **Third-party, dated 2025-12-23** | [bitfern.co.nz](https://www.bitfern.co.nz/blog/tableau-and-databricks-part-1-getting-started/) |

**What I could NOT find — mark this UNVERIFIED:** an official Databricks sentence saying either
"JDBC is supported on Free Edition" *or* "JDBC is not supported on Free Edition." Neither exists on
any page I fetched. The conclusion is an inference from a granted resource plus a documented access
method plus one working third-party report.

**Also UNVERIFIED, and it is the thing most likely to bite:** whether **personal access tokens** can
be *created* in a Free Edition workspace. The generic PAT docs note that a workspace admin can
disable token creation
([docs.databricks.com/dev-tools/auth/pat](https://docs.databricks.com/aws/en/dev-tools/auth/pat)),
and Free Edition's admin surface is deliberately clipped. The Tableau walkthrough above mentions PAT
as an option, which is suggestive but not proof.

> **Therefore: the first action item is not to write code. It is to spend twenty minutes creating a
> Free Edition account, opening the SQL warehouse's Connection details tab, minting a token, and
> running `SELECT 1` from `beeline` or a five-line JVM main.** That single experiment converts the
> weakest link in this entire document into a fact. Everything downstream is cheap once it passes
> and moot if it fails. This is the `H2_BACKEND.md` method: execution beats documentation.

**Sizing sanity check.** A `2X-Small` warehouse and a "max 5 concurrent job tasks" cap are
*irrelevant* to this workload. 200–500 short SELECTs over a few hundred rows is a rounding error on
any warehouse. The binding constraint is the **undisclosed daily fair-use quota** — and note its
failure mode is *"unavailable for the rest of the day"*, which in CI terms is a **red build with a
24-hour cooldown**. Design for that: run the probe sweep on a schedule (nightly), not on every push,
and treat a quota trip as `SKIPPED`, never as `FAIL`. The exact quota numbers are **UNVERIFIED** —
Databricks does not publish them.

### 2.3 JDBC driver — Maven coordinates

**On Maven Central, Apache 2.0, no click-through, no manual download.** Verified from the artifact
itself, not from docs:

```xml
<dependency>
  <groupId>com.databricks</groupId>
  <artifactId>databricks-jdbc</artifactId>
  <version>3.4.2</version>
</dependency>
```

| Fact | Value | Source |
|---|---|---|
| Latest release | **3.4.2**, metadata `lastUpdated` **2026-07-20** | [repo1 maven-metadata.xml](https://repo1.maven.org/maven2/com/databricks/databricks-jdbc/maven-metadata.xml) |
| Licence (from the POM) | **"Apache License, Version 2.0"** | [3.4.2 POM](https://repo1.maven.org/maven2/com/databricks/databricks-jdbc/3.4.2/databricks-jdbc-3.4.2.pom) |
| Artifact name | "Databricks JDBC uber jar" (shaded — watch for classpath conflicts) | same POM |
| Line split | **3.x = open-source driver** (recommended). **2.x = legacy Simba**, latest 2.7.x | [docs.databricks.com/integrations/jdbc-oss](https://docs.databricks.com/aws/en/integrations/jdbc-oss/) |

**The docs are stale.** [docs.databricks.com/integrations/jdbc-oss](https://docs.databricks.com/aws/en/integrations/jdbc-oss/)
says the latest version is **3.3.3**; Maven Central says **3.4.2** as of 2026-07-20. Pin from Maven
Central, not from the docs page.

The legacy **Simba** 2.x driver is the one that historically required a click-through download from
the Databricks site — that objection is dead for the 3.x line, which is plain Apache-2.0 on Central.
*(Whether the 2.x Maven artifact still carries a proprietary Simba EULA in its POM is
**UNVERIFIED** — irrelevant, since 3.x is recommended and Apache-licensed.)*

### 2.4 Local alternative — how close is OSS Spark to Databricks SQL?

**There is no official Databricks SQL local emulator.** Nothing on any Databricks page I fetched
offers a downloadable Databricks Runtime or a local Databricks SQL. *(A secondary SEO page asserts a
"Databricks Local Development Experience (LDE)" that runs a local Databricks runtime; I could not
corroborate it anywhere in official documentation and it does not appear in the docs index —
**treat as UNVERIFIED and probably not a real product name**. Databricks' actual local story is
Databricks Connect, which is a thin client that executes **on a remote workspace** — it is not local
execution and does not remove the cloud dependency.)*

**But local Spark does give you a real JDBC endpoint, for free, hermetically.**

| Component | Coordinates / command | Verified |
|---|---|---|
| Engine | `org.apache.spark:spark-sql_2.13:4.1.2` (Maven, 2026-05-16) | [Maven Central](https://search.maven.org/solrsearch/select?q=g:org.apache.spark+AND+a:spark-sql_2.13) |
| JDBC server | `./sbin/start-thriftserver.sh` — *"By default, the server listens on localhost:10000"*, HiveServer2 protocol | [Spark 4.1.2 docs](https://spark.apache.org/docs/4.1.2/sql-distributed-sql-engine.html) |
| JDBC URL | `jdbc:hive2://localhost:10000` | same |
| Client driver | `org.apache.hive:hive-jdbc:4.2.0` (Maven metadata `lastUpdated` 2025-11-23) | [repo1](https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/maven-metadata.xml) |
| Delta support | `io.delta:delta-spark_2.13:4.3.1` (Maven metadata `lastUpdated` 2026-07-08) | [repo1](https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/maven-metadata.xml) |

#### Which dialect would legend-lite actually be validated against?

**Spark SQL — which is a strict subset of Databricks SQL.** Databricks says so in its own words:
*"The Databricks Runtime, which powers Databricks, includes additional optimizations and proprietary
features that build on and extend Apache Spark"*
([docs.databricks.com/spark/faq](https://docs.databricks.com/aws/en/spark/faq), accessed
2026-07-31). Photon is *"an optimized execution layer"* that *"replaces the JVM-based Spark SQL
execution engine with a native C++ runtime"*
([docs.databricks.com/compute/photon](https://docs.databricks.com/aws/en/compute/photon)).

**Photon is the least of your problems.** It is an *execution* layer under the same Catalyst planner
— its divergence surface is performance and numeric/edge-case behaviour, not syntax. The divergence
that matters to a renderer is the **dialect layer above it**.

**Where local Spark diverges — from the Databricks SQL language reference itself**
([docs.databricks.com/sql/language-manual](https://docs.databricks.com/aws/en/sql/language-manual/),
accessed 2026-07-31):

| Databricks SQL construct family | Available on stock OSS Spark? |
|---|---|
| **Unity Catalog DDL** — principals, privileges, `EXTERNAL LOCATION`, `CREDENTIAL`, `VOLUME` | **No.** Free Edition has one metastore; OSS has none of this vocabulary |
| **Delta statements** — `OPTIMIZE`, `VACUUM`, `RESTORE`, `FSCK REPAIR TABLE` | **Recoverable** — Delta Lake OSS (`io.delta:delta-spark`) supplies these via the Delta SQL extension. Requires wiring it in; not stock Spark |
| **Lakehouse Federation** (federated queries to external DBs) | **No** |
| **SQL scripting** (procedural control flow) | **No** on 4.1.x |
| **Streaming tables / materialized views** | **No** |
| Default table format | Databricks defaults to **Delta**; OSS Spark defaults to **Parquet** |
| **Databricks-only functions** — `measure()`/`agg()` over metric views, `kll_sketch_*` / `theta_sketch_*` / tuple-sketch families, `isearch()`/`search()`, `read_files()`, `ai_query()`, H3 geospatial | **No** — see caveat below |

> **UNVERIFIED, and material:** the Databricks-only function list above was assembled by reading
> [the Databricks built-in function reference](https://docs.databricks.com/aws/en/sql/language-manual/functions/)
> and noting entries with no OSS counterpart I recognise. **Databricks does not mark which functions
> are Databricks-only, and I did not diff the two function catalogues mechanically.** Some entries
> may exist in Spark 4.x under the same name. **Do not build a spelling table from that row.** The
> correct method is the `H2_BACKEND.md` one: enumerate legend-lite's `SqlFn` constants and probe
> each against a real endpoint of each dialect, then diff the two result sets. That is exactly what
> the probe suite is for — this table is a hypothesis for it to test, not a result.

#### The honest characterisation

Local Spark is **an excellent free CI backend and a poor conformance oracle.**

- It will catch real bugs — a Spark-flavoured `Lexicon`/`TypeNames`/`Spellings` triple exercised on
  every push, hermetically, at zero cost, with the same reproducibility as the DuckDB baseline.
- It will **silently pass** SQL that Databricks would reject and **silently reject** SQL Databricks
  would accept, in both directions, across the whole table above.
- So it earns its own scoreboard row (`spark`) and **must not be labelled `databricks`.** Conflating
  them is the `CORRECTNESS_REMEDIATION` §1 failure mode restated at the backend level: a green
  column that means less than it appears to.

**The pairing that actually works:** local Spark as the *continuous* backend, Free Edition as the
*periodic conformance* backend, and a §9-style declared-gap registry recording every construct where
the two disagree. That is the same architecture `H2_BACKEND.md` §9 recommends, applied to a dialect
family rather than a dialect.

---

## 3. Snowflake

### 3.1 The real trial — terms

| Term | Value | Source |
|---|---|---|
| Duration | **30 days** — *"The trial continues for 30 days (from the sign-up date) or until you've depleted your free usage balance, whichever occurs first."* | [docs.snowflake.com/admin-trial-account](https://docs.snowflake.com/en/user-guide/admin-trial-account) |
| Credits | **$400** — *"Unlock the full Snowflake AI Data Cloud with $400 in free credits"*; *"Snowflake trial account credits may be used within 30 days"* | [snowflake.com/snowflake-trial](https://www.snowflake.com/en/snowflake-trial/) |
| Credit card | **Not required to sign up** — *"all you need is a valid email address; no payment information or other qualifying information is required"* | [docs.snowflake.com/admin-trial-account](https://docs.snowflake.com/en/user-guide/admin-trial-account) |
| …but | *"If your credit consumption fully depletes your free usage balance, you must add a credit card to the account to continue using Snowflake."* | same |
| Edition / cloud / region | Chosen at signup, **immutable** — *"You must select a cloud platform (AWS, Azure, or GCP), region and edition during signup; these cannot be changed later."* Enterprise recommended | [snowflake.com/snowflake-trial](https://www.snowflake.com/en/snowflake-trial/) |

All accessed 2026-07-31.

**Trial-account feature restrictions** (from the same docs page): no external network access, no
hybrid tables, no outbound private connectivity, no Snowflake Openflow; Cortex AI functions capped
at roughly ten credits/day without a payment method. **None of these touch JDBC, SQL warehouses, or
ordinary DDL/DML** — the probe workload is entirely unaffected.

### 3.2 What happens when it lapses, and can you start a fresh one

**At expiry:** *"At the end of the trial, the account is suspended. You can still log into a
suspended account, but you cannot use any features."* Reactivation is **not** free —
*"To reactivate a suspended trial account, you must enter a credit card, which converts it to a paid
account"* ([docs.snowflake.com/admin-trial-account](https://docs.snowflake.com/en/user-guide/admin-trial-account),
accessed 2026-07-31). The trial page adds: *"If you don't convert the account before your trial
expires, your account will be suspended and you may lose access to your data."*

**A fresh trial is allowed.** Snowflake University's own FAQ: *"You can sign up for a new trial and
you can often use the same email address as the one you used before"*, and *"You can use the same
email as before, or a different one, it does not matter"*
([learn.snowflake.com](https://learn.snowflake.com/en/pages/university-faqs/), accessed 2026-07-31).

**Read the hedge.** *"often"* is not *"always"*, this is an education-team FAQ rather than the
commercial terms, and **the core Trial Accounts documentation is silent on how many trials one
person may hold** — I found no policy statement either permitting or capping it. Treat re-creation
as **reliable enough to plan a repeat probe run around, and too soft to build CI on**. And note the
consequence that has nothing to do with policy: **a new trial is a new account** — new URL, new
credentials, new empty schema. Every re-creation is a manual credential rotation plus a full
re-seed. That is the actual reason it fails the CI bar.

### 3.3 Is there any perpetual free tier?

**No.** This is a negative finding, so here is exactly how strongly it is held:

- Snowflake's own free-access documentation describes **only** trial accounts
  ([docs.snowflake.com/admin-trial-account](https://docs.snowflake.com/en/user-guide/admin-trial-account)).
- The official trial page frames the entire free path as **30 days of credits**
  ([snowflake.com/snowflake-trial](https://www.snowflake.com/en/snowflake-trial/)).
- Multiple independent 2026 pricing write-ups state flatly that Snowflake has no permanent free
  tier.

**Verified by absence on official pages, corroborated by secondary sources.** I did not find a
vendor sentence saying "there is no free tier" — vendors rarely write that — so this is the
strongest form the claim can take.

**Programmes checked and found not to help:**

| Programme | Finding | Confidence |
|---|---|---|
| **Northstar Education Program** ([snowflake.com/developers/northstar](https://www.snowflake.com/en/developers/northstar/)) | Aimed at students and instructors **within academic institutions**; advertises free courses/badges/curriculum. Whether it grants a **non-expiring compute account** is **UNVERIFIED** — I did not fetch the programme terms. Not obviously applicable to an unaffiliated solo developer. | UNVERIFIED |
| Open-source / OSS-maintainer free compute programme | **None found.** No Snowflake page I fetched or search I ran surfaced one. | Absence of evidence |
| "Powered by Snowflake" | Partner/go-to-market programme, not free compute. | Secondary |

If a genuinely non-expiring Snowflake account exists behind Northstar, it would change the Snowflake
CI verdict from NO to MAYBE. **That is the one Snowflake lead worth twenty more minutes** — read the
Northstar terms directly.

### 3.4 JDBC driver — Maven coordinates

**On Maven Central, Apache 2.0, no click-through.** Verified from the artifact:

```xml
<dependency>
  <groupId>net.snowflake</groupId>
  <artifactId>snowflake-jdbc</artifactId>
  <version>4.3.2</version>
</dependency>
```

| Fact | Value | Source |
|---|---|---|
| Latest release | **4.3.2**, metadata `lastUpdated` **2026-07-16** | [repo1 maven-metadata.xml](https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/maven-metadata.xml) |
| Licence (from the POM) | **"The Apache Software License, Version 2.0"** | [4.3.2 POM](https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/4.3.2/snowflake-jdbc-4.3.2.pom) |
| Variants | `snowflake-jdbc` (fat), `snowflake-jdbc-fips`, `snowflake-jdbc-thin` | [docs.snowflake.com/jdbc-download](https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-download) |
| Major-version note | The 4.x line is recent; 195 versions published in total. If dependency conflicts bite, **`snowflake-jdbc-thin`** is the escape hatch — the default artifact is a shaded fat jar. | repo1 metadata |

### 3.5 Budget arithmetic — will $400 survive a probe suite? (Yes, trivially.)

| Input | Value | Source |
|---|---|---|
| X-Small warehouse burn | **1 credit/hour** (Gen1) | [docs.snowflake.com/warehouses-overview](https://docs.snowflake.com/en/user-guide/warehouses-overview) |
| Billing granularity | *"per-second billing (with a 60-second minimum each time the warehouse starts)"* | same |
| Credit price, AWS US East | ≈ **$2** Standard / **$3** Enterprise / **$4** Business Critical | **Secondary sources only — treat as indicative, not a quote** |

**Derived, not sourced — arithmetic, so check it rather than trust it:**

- $400 ÷ $3 ≈ **133 credit-hours** on Enterprise (≈ 200 on Standard).
- Each **CI run** costs a **minimum of 60 seconds** of warehouse time regardless of how short the
  statements are — that is the resume floor, and it dominates. 60 s = 1/60 credit ≈ **$0.05/run**.
- A 500-statement probe sweep is minutes of wall clock. Call it **2 minutes ⇒ ~$0.10**.
- Even **100 runs/day for the full 30 days** ≈ 100 credit-hours ≈ **$300** — inside budget.

**Conclusion: the $400 does not run out. Day 31 arrives first.** Every Snowflake verdict in this
document turns on the calendar, not the money. Two corollaries worth writing into the harness:

1. **Set `AUTO_SUSPEND` aggressively.** The default idle window, not the queries, is what would burn
   the balance. Idle time is the only realistic way to lose this budget.
2. **Batch the sweep into one warehouse session.** 500 statements in one resume costs one 60-second
   floor; 500 separate CI jobs cost 500 of them — a 500× difference on the dominant cost term.

### 3.6 Local Snowflake emulators — why none of them is acceptable here

Demoted deliberately. Three exist; none is a correctness oracle for this project.

| Emulator | Backend | JDBC | Free? | Verdict |
|---|---|---|---|---|
| **fakesnow** ([tekumara/fakesnow](https://github.com/tekumara/fakesnow)) | **DuckDB** + SQLGlot translation | Yes — issue [#174](https://github.com/tekumara/fakesnow/issues/174) closed, `jdbc:snowflake://127.0.0.1:3141/?…&ssl=false` | Yes, Apache-2.0 | **Disqualified — see below** |
| **LocalStack for Snowflake** ([docs.localstack.cloud/snowflake](https://docs.localstack.cloud/snowflake/)) | **PostgreSQL** + query rewriting ([LocalStack blog](https://blog.localstack.cloud/2024-05-22-introducing-localstack-for-snowflake/): *"At its core, we utilize PostgreSQL as the database engine to store the user data and execute queries"*) | Yes | **$29–35/licence/month**; a [free OSS licence](https://www.localstack.cloud/pricing) exists for non-commercial OSI-licensed publicly-developed projects | Rejected on oracle grounds |
| **snowflake-emulator** ([nnnkkk7/snowflake-emulator](https://github.com/nnnkkk7/snowflake-emulator)) | **DuckDB**, Go | **No** — gosnowflake + REST v2 only | MIT, 44 stars | Rejected: no JDBC, immature |

**fakesnow is disqualified for a structural reason, not a quality one.** It is DuckDB-backed. DuckDB
is already legend-lite's *primary* backend. An oracle that shares an engine with the system under
test cannot detect the class of bug that matters most — **a DuckDB-shaped assumption baked into the
lowering**. Every such assumption would pass on both sides and be invisible in both columns. Two
green columns computed by one engine are one column wearing a costume. The same objection applies to
`nnnkkk7/snowflake-emulator`, also DuckDB.

**LocalStack is genuinely independent** (PostgreSQL-backed) and does hold a free OSS licence
programme, so it fails on different grounds:

- Its own [feature-coverage table](https://docs.localstack.cloud/snowflake/feature-coverage/)
  publishes **no coverage percentage** and marks whole resource families unsupported (e.g. external
  tables); the docs state coverage *"will be updated as additional query features and functions are
  implemented."*
- Its correctness is *"Snowflake syntax rewritten onto PostgreSQL"*, with the blog conceding
  *"there are several more or less subtle differences."* **Subtle differences in the translation
  layer are precisely what a capability probe measures.** An emulator whose failure mode is silently
  answering like Postgres cannot tell you what Snowflake does.

**The general principle, worth writing into the test-matrix contract:** an emulator can serve as a
*smoke* backend — does the SQL parse, does the shape survive a round trip — but **an emulator can
never be the source of truth for a capability map.** `H2_BACKEND.md`'s standard was "executing ~200
probe statements against the real thing." For Snowflake, the real thing is a trial account.

---

## 4. The other backends the same question will be asked about

> **Adopt upstream's gating shape, not a new one.** `BACKEND_PORTABILITY.md` §7 documents how
> legend-engine handles exactly this problem: Snowflake and Databricks are the only two databases
> it puts behind credentials, and both use `<skip>true</skip>` in the module pom plus a
> `pct-cloud-test` profile that re-enables them and reads AWS Secrets Manager. CI activates that
> profile only on same-repo PRs and pushes, so **fork PRs skip silently and nothing is `@Ignore`d**.
> Every backend below should land in that shape: runnable by whoever has an account, invisible to
> everyone else, and never a broken build for a contributor without credentials.
>
> That section also records the tier below this one — **no PCT module and no execution testing**:
> DB2, Sybase, SybaseIQ, Hive, Presto, SparkSQL, Athena, Aurora, BigQuery, Redshift. Be precise
> about the gradations, because they differ: BigQuery, Athena, Aurora and Redshift *do* have
> connection drivers under `connection/driver/vendors/`, so they are wired for execution and simply
> have no PCT suite; DB2 and Sybase have **no driver at all** and cannot execute even in principle.
> Either way, **upstream has never run a compatibility suite against BigQuery**, so §4.1 below is
> greenfield rather than a gap we are behind on.

### 4.1 BigQuery — the best free tier here, with one disqualifying restriction

| Term | Value | Source |
|---|---|---|
| Credit card / billing account | **Not required** — *"experience BigQuery without providing a credit card or creating a billing account for your project"* | [BigQuery sandbox docs](https://docs.cloud.google.com/bigquery/docs/sandbox) |
| Query allowance | **1 TB of processed query data per month** | same |
| Storage | **10 GB of active storage** | same |
| Expiry | Sandbox itself: none stated. **"All tables, views, and partitions automatically expire after 60 days"** | same |
| Unsupported | **"Streaming data"**, **"Data manipulation language (DML) statements"**, "BigQuery Data Transfer Service" | same |

**The historical 1 TB/month figure is confirmed current** (accessed 2026-07-31). 200–500 short
probes over a few hundred rows will not scratch 1 TB.

**But no DML.** `INSERT`, `UPDATE`, `DELETE`, `MERGE` are all blocked in the sandbox. For a probe
harness that needs "some DDL to create small tables; a few hundred rows":

- **Workaround exists:** `CREATE TABLE … AS SELECT` is DDL, and batch **load jobs are free**. Seed
  data is reachable.
- **But the DML probes themselves are unrunnable**, and the 60-day table expiry means seeded
  fixtures silently vanish. Both are manageable, both must be designed for.

**JDBC driver — verified, and Google's docs are stale too:**

```xml
<dependency>
  <groupId>com.google.cloud</groupId>
  <artifactId>google-cloud-bigquery-jdbc</artifactId>
  <version>1.2.0</version>
</dependency>
```

[Google's docs](https://docs.cloud.google.com/bigquery/docs/jdbc-for-bigquery) say **1.1.0**;
[Maven Central](https://repo1.maven.org/maven2/com/google/cloud/google-cloud-bigquery-jdbc/maven-metadata.xml)
says **1.2.0**, `lastUpdated` **2026-07-30** — *published yesterday*. This is the Google-developed
driver and it is on Maven Central, which retires the old "BigQuery JDBC means a Simba click-through
download" objection. *(The Simba/insightsoftware driver still exists as an alternative; Google
recommends its own. Do not confuse `google-cloud-bigquery-jdbc` with `google-cloud-bigquery`, which
is the client library, currently 2.67.0 — not a JDBC driver.)*

### 4.2 Amazon Athena — no free tier

Athena is priced on data scanned with **no free tier for SQL queries**; the widely quoted rate is
**$5.00/TB**. The [official pricing page](https://aws.amazon.com/athena/pricing/) (accessed
2026-07-31) states pricing is based on data scanned and uses $5/TB in its worked examples, but **did
not render an explicit rate line or any free-tier statement** in the fetched content — so the exact
rate is corroborated by secondary 2026 sources rather than quoted from AWS. It also requires an AWS
account (card on file) and an S3 bucket. **Practically free at this workload's scale** — hundreds of
tiny queries is fractions of a cent — **but not *no-cost*, and not card-free.**

### 4.3 Amazon Redshift Serverless — a credit, not a tier

*"$300 credit, which can be used within 90 days of sign-up toward your compute and usage use"*
([aws.amazon.com/redshift/free-trial](https://aws.amazon.com/redshift/free-trial/), accessed
2026-07-31). Explicitly **separate from the AWS Free Tier**, with independent eligibility. In
regions without Serverless, a two-month DC2-large provisioned trial is offered instead. Requires an
AWS account. **90 days is better than Snowflake's 30, and it is still an expiry.** Same verdict
shape: fine for a probe run, unfit for CI.

### 4.4 Summary

| Backend | Card? | Cost at this workload | Expiry | JDBC on Maven Central | CI-viable? |
|---|---|---|---|---|---|
| **Databricks Free Edition** | No | $0 | **None** | Yes — `com.databricks:databricks-jdbc:3.4.2` | **Yes**, within a daily quota |
| **Local Spark** | No | $0 | None | Yes — `org.apache.hive:hive-jdbc:4.2.0` | **Yes** — but it is Spark SQL, not Databricks SQL |
| **BigQuery sandbox** | No | $0 | 60-day table expiry | Yes — `com.google.cloud:google-cloud-bigquery-jdbc:1.2.0` | **Yes, if you can live without DML** |
| **Snowflake trial** | No | ≈$0.10/run | **30 days** | Yes — `net.snowflake:snowflake-jdbc:4.3.2` | **No** |
| **Redshift Serverless** | Yes (AWS acct) | credit-funded | **90 days** | Not investigated | No |
| **Athena** | Yes (AWS acct) | pennies, not zero | None | Not investigated | Only if you accept a card |

---

## 5. Recommended sequencing

1. **Create a Databricks Free Edition account and run `SELECT 1` over JDBC.** Twenty minutes. It
   resolves §2.2 — the single weakest link in this document — and gates everything else Databricks.
   Record whether a personal access token could be minted; that is the specific unknown.
2. **If (1) passes, run the capability probe suite against Free Edition** and produce the
   Databricks column of a §2-style capability map, exactly as `H2_BACKEND.md` did for H2. Execution
   beats every table above.
3. **Stand up local Spark + Thrift server as the continuous backend**, scoreboard row `spark`. Never
   label it `databricks`.
4. **Diff (2) against (3) mechanically** — that diff *is* the Spark-vs-Databricks-SQL divergence
   table this document could only hypothesise (§2.4), and it is the honest version of it.
5. **Burn one Snowflake trial on a single, complete, recorded probe sweep.** Capture the full
   capability map in one pass; assume you get one shot per 30 days. Set `AUTO_SUSPEND` low and run
   the whole sweep in one warehouse session (§3.5).
6. **Read the Northstar programme terms** to settle whether a non-expiring Snowflake account exists
   for an unaffiliated developer. Cheap; changes the Snowflake CI verdict if it does.
7. **Register every capability refusal in the declared-gap registry** (`H2_BACKEND.md` §9). With
   four backends and one of them re-provisioned monthly, a burndown ledger is the only structure that
   keeps honest refusals legible.

---

## 6. What NOT to do

- **Don't plan against Databricks Community Edition.** It was retired 2026-01-01 and accounts are
  inaccessible. Any tutorial, StackOverflow answer, or model recollection that mentions it is
  describing a product that no longer exists.
- **Don't use fakesnow — or any DuckDB-backed Snowflake emulator — as a correctness oracle.**
  legend-lite's primary backend is DuckDB. An oracle sharing the engine under test cannot see the
  bug class that matters: a DuckDB assumption baked into the lowering. It would pass on both sides.
- **Don't let local Spark be scored as "Databricks."** It is a strictly smaller dialect (§2.4). A
  green `databricks` column computed by OSS Spark is a false claim of conformance — the
  `CORRECTNESS_REMEDIATION` §1 failure mode at backend granularity.
- **Don't build CI on a Snowflake trial.** Not because of cost — the credits are ample (§3.5) — but
  because every 30 days it becomes a new account with new credentials and an empty schema. A backend
  that requires a manual credential rotation each month is a manual test, wearing CI's clothes.
- **Don't pin driver versions from vendor documentation.** All three doc pages checked were behind
  Maven Central: Databricks docs said 3.3.3 vs actual 3.4.2; Google's said 1.1.0 vs actual 1.2.0.
  Read `maven-metadata.xml`.
- **Don't treat a Databricks quota trip as a test failure.** Its documented consequence is compute
  *"unavailable for the rest of the day."* That must map to `SKIPPED`, never `FAIL`, or the
  scoreboard will report a capability regression that is really a billing event.
- **Don't take the 14-day Databricks trial** in preference to Free Edition. It requires a card *and*
  a cloud account *and* expires — strictly worse on all three axes for this purpose.
- **Don't assume the BigQuery sandbox can run the seeding you'd write for any other backend.** DML
  is blocked. Seed via load jobs or CTAS, and account for the 60-day table expiry.

---

## 7. What was NOT verified — read this before relying on anything above

Listed in descending order of how much a verdict depends on it.

| # | Claim | Status | Consequence if wrong |
|---|---|---|---|
| 1 | **Databricks Free Edition permits JDBC/ODBC connections to its SQL warehouse** | **Inference + one third-party report (2025-12-23).** No official Databricks sentence permitting *or* prohibiting it exists on any page fetched | **The entire "Databricks cloud free = YES" verdict collapses.** Test it first (§5.1) |
| 2 | **Personal access tokens can be created in a Free Edition workspace** | **UNVERIFIED.** Generic PAT docs note admins can disable token creation; Free Edition's admin surface is clipped | Auth path unknown; may force OAuth U2M, which is awkward in CI |
| 3 | **Free Edition's daily fair-use quota is large enough for a nightly probe sweep** | **UNVERIFIED — Databricks publishes no numbers.** Only the failure mode is documented | CI cadence must be tuned empirically |
| 4 | The Databricks-only function list in §2.4 | **UNVERIFIED — not mechanically diffed** against the Spark 4.x catalogue | Do not derive a spelling table from it; derive it from probe output |
| 5 | **Snowflake trials can be re-created indefinitely** | **PARTIAL** — education-team FAQ says *"often"*; core Trial Accounts docs are silent on any cap | Affects only repeat probe runs; the CI verdict is already NO |
| 6 | Snowflake Northstar grants a non-expiring compute account | **UNVERIFIED — programme terms not fetched** | Would upgrade Snowflake CI from NO to MAYBE |
| 7 | Snowflake credit prices ($2 / $3 / $4) | **Secondary sources only** | Only feeds §3.5's arithmetic, whose conclusion (time, not money, is the constraint) holds at any plausible price |
| 8 | Athena's $5.00/TB rate and absence of a free tier | **Secondary** — the official pricing page did not render an explicit rate line or free-tier statement | Athena is already rejected on the card requirement |
| 9 | Whether the legacy Simba `databricks-jdbc` 2.x artifact carries a proprietary EULA | **UNVERIFIED** | Moot — 3.x is Apache-2.0 and recommended |
| 10 | "Databricks Local Development Experience (LDE)" | **UNVERIFIED, likely not a real product name.** Sourced only to a low-quality SEO page; absent from official docs | Do not search for it; Databricks Connect is the real (remote) local-dev story |

**And the standing caveat, which outranks all ten:** *none of this was executed.* This document is
desk research with a 2026-07-31 timestamp on every source. `H2_BACKEND.md` earned its confidence by
running 200 statements against a real jar and letting execution overrule documentation twice. Until
step 5.1 and 5.2 are done, **this document is a plan for an investigation, not the result of one.**
