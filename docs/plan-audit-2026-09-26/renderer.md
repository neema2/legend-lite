# Research line 6: how DuckDB-centric the ANSI renderer is, and when to fix it

Read-only audit, 2026-09-26. Files: `core/src/main/java/com/legend/sql/dialect/` — AnsiSqlRenderer.java
1377 LOC, DuckDb.java 483, H2.java 722, H2Modern.java 116, Spellings.java 135, TypeNames.java 70,
Lexicon.java 95, CarrierStrategies.java 1474, EngineStyleH2.java 1902; `sql/SqlFn.java` 122 (~209
enum members incl. SqlAgg).

Classes: (a) SQL standard (SQL:1992–2023 incl. T621 trig, T054 GREATEST/LEAST, SQL:2016 JSON_ARRAY);
(b) ≥4 of {PG, MySQL/MariaDB, SQL Server, Snowflake, BigQuery}; (c) PG has it and <4/5; (d) neither
PG nor ≥4/5 (DuckDB-only or DuckDB+1–2 others).

## 1. Inventory
| Surface | (a) | (b) | (c) | (d) | total |
|---|---|---|---|---|---|
| `Spellings.DUCKDB` rows (Spellings.java:49-132) | 22 | 16 | 11 | 28 | 77 |
| Coded arms in base `call()` (AnsiSqlRenderer.java:605-822) | 31 | 7 | 3 | 12 | 53 |
| Base defaults outside `call()` (clauses, literals, agg names, JSON, types, DDL, format codes) | ~22 | 4 | 8 | ~20 | ~54 |
| **Base renderer total** | **75** | **27** | **22** | **60** | **184** |

45% of what the base emits is not standard-or-common; 33% is DuckDB-only. `Spellings.java:13-16`
concedes it ("a DuckDB renderer with an ANSI name"); the base javadoc (AnsiSqlRenderer.java:18-21)
is false as written.

(a) rows: ABS, ACOS, ASIN, ATAN, COALESCE, COS, COSH, EXP, FLOOR_RAW, LN, LOG10, LOWER, POW, SIN,
SINH, SQRT, TAN, TANH, TRIM, UPPER, GREATEST, LEAST. (b) rows: ASCII, ATAN2, COT, DEGREES, LEFT,
LENGTH, LTRIM, MD5, RADIANS, REGEXP_REPLACE, REPEAT, REPLACE, REVERSE, RIGHT, RTRIM, SUBSTRING. Base
(a) arms: the 11 INFIX ops (:68-79), NOT, NEGATE, IS_NULL, IS_NOT_NULL, IN, IS_DISTINCT_FROM,
NULL_SAFE_EQUAL/NOT_EQUAL, MOD, REM, CEILING, FLOOR, ROUND_HALF_UP, XOR, TODAY, DATE_TRUNC_DAY,
CURRENT_USER_FN, PARSE_INT, PARSE_DATE, BOOL_TO_TEXT. Base (b) arms: CONCAT, CONCAT_JOIN (NULL
semantics differ: MySQL/Snowflake/BQ return NULL), PI, SIGN, LPAD, RPAD, UC_FIRST/LC_FIRST.

## 2. (c) items — PostgreSQL-family
CBRT :55; CHR :56; EXTRACT `date_part('p', x)` :72 (H2 undoes H2.java:212-216); FROM_EPOCH_SECONDS
`to_timestamp` :75; JSON_ARRAY_LENGTH :83; MAKE_DATE :95 (PG only); SHA256 :117; SPLIT_PART :121;
STARTS_WITH :123 (H2 undoes :228-231); STRPOS :125 (standard `POSITION(x IN y)`; H2 undoes :234-237);
TIMEZONE :130; NOW `now()` AnsiSqlRenderer.java:745; DATE_TRUNC :755-761; MAKE_TIMESTAMP :763-767;
TIMESTAMPTZ TypeNames.java:53; array cast `T[]` AnsiSqlRenderer.java:1082; JSON type TypeNames.java:67;
agg names verbatim STRING_AGG, BOOL_AND, BOOL_OR (AnsiSqlRenderer.java:992, SqlAgg.java:23-25);
agg-internal ORDER BY :986-991; DROP SCHEMA … CASCADE :1258-1259; AS MATERIALIZED DuckDb.java:52;
to_json DuckDb.java:381; `->`/`->>` DuckDb.java:440, 464; select-list UNNEST DuckDb.java:387;
`'NaN'::DOUBLE`, `SET TimeZone` DuckDb.java:109, 33; chr(31)/chr(30) sentinels AnsiSqlRenderer.java:640-642.

## 2b. (d) items — DuckDB-only
Spellings rows (28): DATE_DIFF :61; DAYNAME :65; ENDS_WITH :67; EPOCH_MS :68; EPOCH_SECONDS `epoch`
:69; ERROR `error()` :70; FORMAT `printf` :74; JARO_WINKLER :81; JSON_TYPE :82; JSON_PRETTY :84;
LEVENSHTEIN :88; LIST_FLATTEN `flatten` :89; LIST_LENGTH `len` :90 (**SQL Server `LEN` = string
length — silent wrong meaning**); MAP_CONCAT :96; MAP_EXTRACT :97; MAP_FROM_ENTRIES :98;
MAP_FROM_LISTS `map` :99; MAP_KEYS :100; MAP_VALUES :101; MATCHES `regexp_matches` :102 (**PG returns
setof text[] — silent wrong meaning**); MONTHNAME :104; REGEXP_EXTRACT :107; REGEXP_EXTRACT_ALL :108;
REGEXP_FULL_MATCH :109; SHA1 :116; SPLIT `string_split` :120 (**SQL Server STRING_SPLIT is a table
function**); STRFTIME :124 (**SQLite reversed arg order**); STRPTIME :126.
Base coded arms (12): ERROR `error(chr(31)||…)` :633-643; JSON_MERGE_PATCH :658; DIVIDE `CAST(x AS
DOUBLE)` :695-696 (bypasses TypeNames whose ANSI row says DOUBLE PRECISION, TypeNames.java:54);
MAP_EMPTY `MAP {}` :726; BIT_NOT `xor(x,-1)` :727; ENCODE_BASE64 :738; GUID `uuid()` :742;
ADD_INTERVAL/ADD_INTERVAL_TEMPORAL `d + to_years(n)` :770-772 (H2 undoes :205-209); TIME_BUCKET
:776-782; FROM_EPOCH_MS :783; INT_DIVIDE `a // b` :784 (**`//` is a line comment on H2** —
H2.java:193-200); DECODE_BASE64 :787 (**PG `decode` is the opposite direction**).
Base defaults (~20): `* EXCLUDE (…)` :459-462, :536-538 (H2 undoes :417-419); `rowid` :541-543;
OrderedListAgg `list(x ORDER BY y)` :492-493; `json_object(k, v, …)` alternating :567-570 (H2 undoes
:619-630); `coalesce(json_group_array(…),'[]')` / `to_json(list(…))` :586-597 (H2 undoes :636-652);
`INTERVAL n DAYS PRECEDING` :975-978 (H2 undoes :349-367); strftime %-codes :1101-1126 (**MySQL `%M`
= month name, DuckDB `%M` = minutes** — H2 undoes :489-521); `MAP(K,V)` cast :1083-1084; `STRUCT(name
T)` cast :1085-1093; HUGEINT TypeNames.java:52; bare DOUBLE TypeNames.java:66; verbatim SqlAgg names
MEDIAN, MODE, LIST, QUANTILE_CONT, QUANTILE_DISC, ARG_MAX, ARG_MIN (H2 undoes at H2.java:532-569);
QUALIFY (DuckDb.java:204, H2.java:178); `ASOF LEFT JOIN` SqlSource.java:170 / DuckDb.java:211;
`TIMESTAMP_NS '…'` DuckDb.java:69; `JSON[]` DuckDb.java:266, 445; DDL types via DdlSpelling.h2Type
(DdlSpelling.java:21-44).
DuckDb.java-held (correctly placed): list_* 30 arms :311-343; lambda `x -> body` :245-249; `[…]` :392;
`{'k': v}` :399-401; struct_extract/insert :423-426, :451-452; list_aggregate/contains :181-192;
list_bool_or/and :287-306; hash/xor/UBIGINT/HUGEINT :352-354; ROUND_EVEN :361-362; PIVOT :217-239;
read_json_objects :266, :279.

## 3. DuckDb.java vs base; H2's undo count
- DuckDb.java: 483 LOC, 34 `@Override`s; holds the structural (d) items plus 4 `call()` if-arms
  (:97-138) and `passes()` switching CarrierStrategies to `Caps.DUCKDB` (:152). Constructor
  `super(Lexicon.DUCKDB, TypeNames.DUCKDB, Spellings.DUCKDB)` (:76). What DuckDb does NOT need to add
  is the measure of the problem: 28 (d) + 11 (c) rows, 12 (d) + 3 (c) arms, ~20 (d) defaults inherited.
- H2 = 722 LOC, 26 overrides + 16 `call()` if-arms (H2.java:191-343) + `Spellings.H2 = DUCKDB + 3
  renames` (Spellings.java:36-44). **26 of 45 override points exist purely to undo a DuckDB spelling.**
  The other 19 are genuine H2 quirks (case-sensitive identifiers :84-165, bool-text coercion :248-271,
  JSON carrier :396-412, normalize :698-713, LateralExplodeToUnion, splitPart UDF :718-721).
- Two more consumers of `Spellings.DUCKDB`: the SQLite renderer (Compiler.java:748-753:
  `AnsiSqlRenderer(Lexicon.SQLITE, TypeNames.ANSI, Spellings.DUCKDB)`) and `EngineStyleH2`
  (EngineStyleH2.java:305). No ANSI spelling row exists anywhere.
- Latent defect: Spellings.java:29-33 claims DuckDB names H2 lacks are "ABSENT so they fail loud",
  but `h2()` starts from `build()` (:37) and only puts 3 rows — all 28 (d) names are PRESENT in
  `Spellings.H2` and fail at H2 parse, not as `DialectCapability`.
- The IR-level half is already portable-first: base `passes()` uses `CarrierStrategies.Caps.H2`
  (AnsiSqlRenderer.java:100-106) and `QualifyToSubselect`; DuckDB opts up. The DuckDB-centricity is
  confined to the spelling layer.

## 4. Lanes and oracles
| Lane | Dialect class | Oracle | Where |
|---|---|---|---|
| `//spec:corpus_duckdb` (gates 4+11) | DuckDb | engine test asserts (rows), host+database judges | spec/BUILD.bazel:86-99; rosters spec/src/test/resources/rcorpus/duckdb-* |
| `//spec:corpus_h2` (gate 5) | H2 (2.1.214) | same, `-Drcorpus.backend=h2` | spec/BUILD.bazel:104-117; rcorpus/h2-* |
| `//pct:pct_duckdb` (gate 6) | DuckDb | PCT manifests `*_manifest.duckdb.json` (5) | pct/src/test/resources/oracle |
| `//pct:pct_h2` (gate 7) | H2Modern (2.4.240) | 27 expected failures pinned | pct/BUILD.bazel:135-140 |
| SQLite | `AnsiSqlRenderer(SQLITE, ANSI, DUCKDB)` | 11 tests, filter/getAll only | core/src/test/java/com/legend/integration/SQLiteIntegrationTest.java |
| CarrierDifferentialTest | portable base vs DuckDb, both executed on DuckDB | row equality (22) | core/src/test/java/com/legend/sql/dialect/CarrierDifferentialTest.java |
| Postgres / MariaDB | none | none | no target in MODULE.bazel / any BUILD |

SQL-text goldens: none for DuckDB. The only text-contract corpus rows are the engine's Postgres text
goldens (`foreign-dialect` ceiling 31, MinimalCorpusTest.java:761-766), all on the fail roster.
Dialect unit tests pin no rendered SQL. **DuckDB byte-identity is guarded by nothing except
execution.** A refactor that re-adds every spelling in DuckDb.java is unobservable by every gate; a
byte change is unobservable until it changes rows. A render-diff harness does not exist.
Testability of a standard base without a new oracle: partly, by the CarrierDifferentialTest pattern
(DuckDB accepts most (a)/(b) spellings), and H2 as a second standard-leaning executor. An embedded
Postgres corpus lane (zonky, ~7s boot, proven on this Mac) needs no new goldens — only a dialect class
and fixture DDL. That lane is what would prove "backend agnostic".

## 5. Recommendation — later phase
**Shape:** `Spellings.ANSI` = the 38 (a)+(b) rows; `Spellings.DUCKDB = ANSI + 39` (pure data,
byte-identical by construction). `TypeNames.ANSI` exists; fix the `DIVIDE` arm to read it (:695). Base
`call()` keeps 31 (a) + 7 (b) arms, spells the 3 (c) temporal arms in standard form (`EXTRACT(P FROM
x)`, `CURRENT_TIMESTAMP`, `INTERVAL 'n' UNIT`), turns the 12 (d) arms + ~10 (d) defaults into
`DialectCapability` hooks or standard spellings. DuckDb.java re-adds each (~250 LOC, to ~730); H2 drops
its 26 undo points (~150 LOC); `Spellings.H2 = ANSI + its own 3`; EngineStyleH2 and the SQLite
constructor get their own rows. ~70 move sites plus `SpellingsTest.everySqlFnClassified` (:133-143)
repointed. 2–4 days including a render-diff harness.
**Why not now:** orthogonal to the untangle (A2/E touch lowering registration and the table, not
`sql/dialect`); with DuckDB and H2 the only executors a standard base is a base nobody runs (every (b)
claim is dialect knowledge, not a probe; the project's rule is a spelling exists only once probed);
not observable by any of the eleven gates; a data-only half leaves two truths.
**When:** its own phase after step A lands, triggered by wiring a third executing backend (embedded
Postgres as `//spec:corpus_postgres`; MariaDB second). Order: render-diff harness → Spellings/TypeNames
data split (byte-identical) → coded arms → H2 shrink → Postgres lane green.
**Risks to carry:** the silent-wrong-meaning collisions (`len`, `regexp_matches`, `%M`, `decode`,
`//`, `strftime` order) — (d) rows must leave the base as throws, not defaults (invariant 4);
EngineStyleH2.java:305 and Compiler.java:753 must be repointed in the same commit; the `Spellings.H2`
"ABSENT" comment is already false; gate budget +40–60s per new lane against the 12-minute ceiling.
