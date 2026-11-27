> **Research for step 6B, 2026-11-27.** Read from the code at `38a12c97a` by a read-only research pass; nothing was
> run, and what it could not verify by reading it says so. Input to step 6B's design, not a ruling and not a design.

# Carriers: Pure values held in a form where the database's `=` / `<` / DISTINCT / IN is not Pure equality

Source: worktree /home/user/ll-num at 38a12c97a, read-only. All paths are relative to
`core/src/main/java/com/legend/`, so `lowering/X.java:N` means `core/src/main/java/com/legend/lowering/X.java:N`.
"Verified" means read in the code. "Not verified" means the claim is about a database's behaviour that I did not see
measured in the code or a comment.

## The table

### C1. TEMPORAL_TEXT: a partial date, or a date-time whose written precision a TIMESTAMP cannot hold

1. **What it holds.** A `Date` year (`%1973`) or year-month (`%1973-11`). Also `date(y)`, `date(y,m)`, `date(y,m,d,h)`
   and `date(y,m,d,h,mi)`, which are padded text such as `'2015-04-16T14'`. Also `date(...)` with a Float or Decimal
   second. Also the `%latest` sentinel. Also a date-time literal of hour, minute or subsecond precision at a scalar
   root. Also `adjust` of a partial by YEARS or MONTHS, and `adjust` of a date-time with more than 6 subsecond digits.
   Also `timeBucket` of a subsecond literal. Also lite's `dateTimeOfText` and `timeOfText`. It is VARCHAR on every
   backend, and the label exists only in the plan: `sql/SqlType.java:24-34`. Every dialect renders the cast as nothing,
   for example `sql/dialect/SQLite.java:1369`.
2. **Where it is made, and why.**
   - `lowering/MatchFold.java:94-102` (`dateLit`): Year and YearMonth, "the TEMPORAL_TEXT-stamped print form".
   - `lowering/DateCtorRule.java:105-125`: `date()` with fractional seconds. The comment says "real pure prints the
     ISO form with the fraction trimmed", and the value is `STRFTIME`, then `RTRIM '0'`.
   - `lowering/DateCtorRule.java:131-145`: `date()` with 1 to 5 parts, built as LPAD/CONCAT text. The comment says
     "the partial print".
   - `lowering/DateShifts.java:166-180`: `adjust` of a partial, `STRFTIME` to `YEAR4` or `YEAR_MONTH`. The comment says
     "real pure GROWS precision".
   - `lowering/DateShifts.java:190-197`: more than 6 subsecond digits. The comment says "real pure preserves subsecond
     print precision".
   - `lowering/Scalars.java:678-689`: `timeBucket` of a subsecond literal. The comment says "keeps the INPUT LITERAL's
     print precision" (`Reason.PRINT`).
   - `lowering/Scalars.java:2235-2241`: `dateTimeOfText` and `timeOfText`. These carry the database's text as a
     decode label, for the Cypher island's nanoseconds.
   - `lowering/RootLiterals.java:36-53`: an hour, minute or subsecond literal at a scalar root (`Lowerer.java:1047`).
     The comment says "a SQL TIMESTAMP round-trip cannot reproduce" the written precision.
   - `lowering/Lowerer.java:3923-3926`: `%latest` as `'9999-12-31T00:00:00.0000'`, "the engine's STRING sentinel".
   - `lowering/LiteralSpelling.java:220-253` (`wireValueEgress`, reached from `Fold.java:1522` and
     `Lowerer.java:1033`): at a value root, a TIMESTAMP cell becomes `STRFTIME ISO_NANO` text. The comment says this
     is "the engine's own decode … nine subsecond digits". Written temporal literals inside collections become static
     text.
   - `lowering/MixedEncoding.java:214-217`, `:594`, `:611-612` and `:754`: the `Date` channel of a tagged list. The
     comment says "PARTIAL dates travel as STRINGS … the string IS the print form".
3. **Databases.** All of them. The marker is a no-op cast, so the database sees VARCHAR. MongoDB sees a `string`
   (`MongoDb.java:2589`).
4. **What plain SQL gets wrong.**
   - `=` between two texts is right only at the same precision and the same spelling. `%1973 == date(1973)` works only
     because both are spelled `'1973'`.
   - `=` between text and a TIMESTAMP is an error ("invalid timestamp field format", `Equalities.java:175-179`) or a
     text comparison. Example: `date(2015,4,16,14) == %2015-04-16T14:00:00`.
   - `<` compares lexically. Text against a DATE is a conversion error (`Scalars.java:110-112`). Text against text is
     wrong for years with other than 4 digits: `date(10000) < date(9999)` is true lexically, because DateCtorRule
     LPADs to width 4 only (`DateCtorRule.java:132-140`).
   - DISTINCT, GROUP BY and IN see texts. `[date(2015,4,16,14), %2015-04-16T14:00]->removeDuplicates()` keeps both
     values, where Pure dedups them by instant (S399).
5. **Compensations today.**
   - `=` of two partials: `Equalities.java:145-171`. The same precision gives text `=`. A computed partial against a
     literal partial (`:160`) gives text `=`. A full date against a partial folds to false (`:166-168`).
   - `=` of text against a TIMESTAMP: `Equalities.java:173-184`. Both sides go through
     `SideEqualities.instantOfText` (`SideEqualities.java:1122-1132`, which pads an hour with `:00` and JUDGE-casts to
     TIMESTAMP) and `instantOfHeld` (`:1115`).
   - At the compatibility doors: `Equalities.upstreamsPartialDateTime` (`Equalities.java:284-301`). It compares by
     written precision.
   - Whole-side comparisons (asserts and RootComparison): `SideEqualities.instants` (`:1072-1112`) and
     `writtenDate` / `DATE_KINDS` / `writtenDateValue` (`:751`, `:783-861`). These turn a year into a BIGINT, a
     year-month into year*12+month, a day into a DATE and a date-time into a TIMESTAMP.
   - Ordering: `Scalars.dateArg` (`Scalars.java:2945-2958`) pads a partial **literal** only.
   - In tagged lists, the `Date` channel compares text `=` within its own family (`TaggedLists.java:618-643`).
6. **Gaps (by code reading).**
   - Ordering of a computed partial or text date-time. `dateArg` only matches `TypedCDate`, so
     `date(2015,4,16,14) < %2015-04-17T00:00:00` gets no pad and no instant reading. No `TEMPORAL_TEXT` appears in
     `Sorts.java` or `Comparators.java` (grep).
   - `in` and `contains`. `Scalars.inRule` (`Scalars.java:~3780-3840`) applies only `comparisonWireOperand`, tagged
     handling and variant wrapping, with no instant arm.
   - `removeDuplicates` and DISTINCT over these values. `ListEncodings.orderedDedup` is a plain database dedup.
   - A wide or negative year in text ordering.
7. **Meaning or print?** Mostly **PRINT**, with one exception: a year or a year-month has no native SQL type, so for
   those two the carrier is **MEANING** (it could be a typed (year, month) pair rather than text). Every date-time
   variant here (hour, minute, subsecond digit count, `%latest`, the value-root nine-digit egress) exists only to keep
   how it prints. The register already compares date-times by instant (S399). So the typed TIMESTAMP could be carried
   and the text made at the boundary, with these exceptions: nanoseconds on a microsecond database (S181 refuses
   these), and `%latest`.

### C2. DECIMAL_TEXT: a store-read Decimal as scale-stripped `'1.5D'` text

1. **What it holds.** A Decimal cell rooted in a store column, at a value root (scalar or value collection). It is
   the canonical text: the digits with fractional trailing zeros and any bare point removed, then `D`. Its contract is
   at `sql/SqlType.java:35-47`.
2. **Where it is made, and why.** `lowering/LiteralSpelling.java:256-299`: `wireValueEgress` calls `decimalText`
   for column-rooted cells. The stated reason: "the engine's own decode for that lane erases wire scale" (R3/R8). It is
   applied by `Fold.conformValueEgress` (`Fold.java:1504-1535`) from `Lowerer.java:1033`. It is also made on purpose
   for comparison: `SideEqualities.besideDecimals` (`SideEqualities.java:1196-1229`, at `:1219-1222`), at `:1568`
   (grid cells), and in `besideExact` (`:1673-1682`, Decimals too wide for every Integer).
3. **Databases.** All of them except MongoDB, which refuses it (`MongoDb.java:2600`). SQLite renders it as nothing
   (`SQLite.java:1369`).
4. **What plain SQL gets wrong.** `'1.5D'` compared with a DECIMAL value is a type error or a wrong text compare.
   `'10D' < '9D'` is true lexically. Ordering of this carrier is meaningless.
5. **Compensations today.**
   - `SideEqualities.oneCarrier` (`:1145-1148`) refuses the pair unless both sides are DECIMAL_TEXT.
   - `besideDecimals` writes the other side as the same canonical text.
   - `kindsOf` (`:696-700`) gives a DECIMAL_TEXT side no tagged reading.
   - `wireValueEgress` skips cells that are already text (`LiteralSpelling.java:210-216`).
   - Gap: ordering, which happens only at side level, where text is the wrong key. I found no `<` member over
     DECIMAL_TEXT (not verified that one is never reached).
6. **Meaning or print?** **PRINT only.** Under S139 lite compares Decimals by value, so the scale erasure is a
   legend-engine decode emulation. The typed DECIMAL could be carried, and the text written at the boundary.

### C3. A list (and a struct, a pair, a map) as JSON text

1. **What it holds.** Any `T[*]` value in a SQL expression (an `ArrayLit` or a `LIST` aggregate), any struct
   (Pair, instance) and any map.
2. **Where it is made.**
   - SQLite: `json_array(...)` (`SQLite.java:822`) and json objects (`:1106`). Each element goes through `json(x)`
     (`:884-904`). A Decimal element is `json(printf('%.<scale>f', x))` (`:896-901`, the reason being "read back as
     its cells"). A Float is `printf('%!.17g')` (`:917-923`). A Boolean is JSON true or false (`:910-912`).
   - MariaDB and MySQL: `JSON_ARRAY` (`MariaDb.java:1337-1338`) and `JSON_OBJECT` (`:1342-1345`). Elements go through
     `json(x)` (`:1113-1137`).
   - H2: `JSON_ARRAY(... NULL ON NULL)` (`H2.java:994-1005`). Structs are `JSON_OBJECT` (`:1495-1502`). The comment
     says "dates serialize as ISO strings".
   - BigQuery: native ARRAY with no NULL elements and no nested lists (`BigQuery.java:1191-1205`). Composite equality
     is `TO_JSON_STRING(a) = TO_JSON_STRING(b)` (`BigQuery.java:1871-1877`, `:1984-1988`).
   - Snowflake: `ARRAY_CONSTRUCT` (`Snowflake.java:825`). This is a VARIANT array.
   - DuckDB, Postgres, Trino, ClickHouse and Databricks: native arrays. MongoDB: BSON arrays.
   - Why: these databases have no array type (SQLite and MariaDB header comments: `SQLite.java:47-51`,
     `MariaDb.java:46-49`). This is **meaning**, not print.
3. **Databases.** SQLite, MariaDB, MySQL and H2 hold a list as JSON text. BigQuery compares a composite as JSON text.
   Snowflake holds a VARIANT.
4. **What plain SQL gets wrong.**
   - `=` is a text or byte compare: SQLite `json(a) = json(b)` (`SQLite.java:1916-1923`), MariaDB
     `JSON_COMPACT = JSON_COMPACT` (`MariaDb.java:2138-2146`).
   - Text compare breaks on Decimal scale: `[1D, 2.00D] == [1D, 2D]` is false as text and true in lite (S139).
   - Text compare breaks on SQLite date-time spelling inside lists, and on Integer against Decimal inside lists:
     `[1, 2] == [1D, 2D]` is true under the 2026-11-27 ruling. Text compare would be correct for Float against Integer
     (`[1.0]` against `[1]` differs, which is right).
   - Text compare breaks on key order for maps. This is order-sensitive on every DB for maps, including DuckDB's
     native MAP (not verified).
   - `<` on JSON text is lexical. DISTINCT and GROUP BY are by text.
   - Native arrays have the opposite problem: list equality coerces Integer to Float (register S127, cited at
     `sql/SqlFn.java:154-155`). So `[1] == [1.0]` may be true natively (not verified per DB).
5. **Compensations today.**
   - Whole sides: `SideEqualities.collections` (`:214-248`) compares lists element by element through rows
     (`SAME_IN_ORDER` / `SAME_IN_ANY_ORDER`, rendered by `AnsiSqlRenderer.java:1383-1410`, `SameValuesForm`;
     MariaDB's `sameValues`, `MariaDb.java:1950`).
   - Maps: compared by entries sorted by key (`SideEqualities.entryRows`, `:254-275`).
   - SQLite: a Decimal element is written at its scale so that it reads back as its cell
     (`SQLite.java:896-901`).
   - Gap: `==` of two list or struct values inside an expression (a filter, an `if`, a map lambda). That path is
     `Equalities.equal`, then `nullArms`, then the dialect's `equalArm`, which is text equality on SQLite, MariaDB,
     MySQL and H2, and `TO_JSON_STRING` on BigQuery. No element-wise reading is done there (`Equalities.java:202-214`).
   - Gap: dedup or GROUP BY of list-valued values.
6. **Meaning or print?** **MEANING** for the container: the database has no array, struct or map. **PRINT** for the
   element spellings inside it: a Decimal's scale digits on SQLite are there "so its cells read back"
   (`SQLite.java:896-898`). A canonical element form (scale-stripped Decimal, one date-time form) would make text `=`
   correct for one kind.

### C4. SQLite: a date-time (and a day, a time) as the text it was written in

1. **What it holds.** Every DATE, TIMESTAMP, TIMESTAMPTZ and TIME value on SQLite (`SQLite.java:42-44`,
   `scalarTypeName` `:200`). A literal is `'<iso with T→space>'` (`SQLite.java:765-767`). A loaded cell is "the
   database's to read" as written (`setup/RowLoad.java:208-214`); SQLite keeps 9 digits (`SQLite.java:179-181`).
2. **Where it is made.** The dialect itself; SQLite has no temporal type. This is **meaning**.
3. **Databases.** SQLite.
4. **What plain SQL gets wrong.**
   - `$x.ts == %2020-01-01T10:00:00.5` against a stored `'2020-01-01 10:00:00.50'` is false.
   - A `T` against a space separator breaks both `=` and `<`.
   - A day against midnight: `'2020-01-02'` is not `'2020-01-02 00:00:00'`. S399 says this is "false on SQLite, true
     on DuckDB and H2".
   - DISTINCT and GROUP BY are by text.
5. **Compensations today.**
   - Only JUDGE-reason casts normalize: `SQLite.variantAwareCast` (`SQLite.java:1341-1366`) writes a TIMESTAMP as
     `'YYYY-MM-DD HH:MM:SS.fffffffff'` (the dates audit's HIGH 3 is cited there), and `judgesAsHeld` returns false
     (`:1334-1337`). These are reached from `SideEqualities.instants`, `instantOfHeld` and `instantOfText`, and from
     `Equalities.java:181-185` only when one side is TEMPORAL_TEXT.
   - Gap: two TIMESTAMP sides in a product expression (a column against a literal, a column against a column) get
     plain text `=` and `<`. Nothing in `Equalities.equal` reads two TIMESTAMP sides as instants.
   - SQLite window RANGE frames use `julianday` (`SQLite.java:3243-3256`). Ordering keys elsewhere use the text.
6. **Meaning or print?** **MEANING** (no type). The fix is a one-form canon on every value, at write time or at
   compare time.

### C5. SQLite: a Decimal as a REAL

1. **What it holds.** Every Decimal on SQLite. "There is no exact decimal: a decimal is a REAL"
   (`SQLite.java:45-46`). A root column is printf text at its scale (`deliveredValue`, `:466-480`).
2. **Where it is made.** The dialect; SQLite has no exact decimal. This is **meaning**.
3. **Databases.** SQLite.
4. **What plain SQL gets wrong.** `=` and `<` between Decimals that differ past about 15 to 17 significant digits
   (for example two `DECIMAL(38,20)` values) compare as equal doubles. This is not verified by a probe. A Decimal is
   also indistinguishable from a Float in SQLite storage.
5. **Compensations today.** Arithmetic is refused (`SQLite.java:1645-1680`, D2a). Comparison and dedup are not
   compensated (gap, by code reading).
6. **Meaning or print?** **MEANING.**

### C6. Variant / Any as JSON (`TO_VARIANT`)

1. **What it holds.** An `Any`-typed value or root, the branches of an `if` with mixed kinds, a Pair field widened to
   Any, a Number-LUB literal list such as `[1, 2.5]` outside a root, and an instance in a layout-less slot.
2. **Where it is made.**
   - `Lowerer.java:1076-1081`: an Any root is boxed.
   - `MixedEncoding.java:851-865`: `lubCase` and `variantBranch`.
   - `MixedEncoding.java:921`, `:1043-1052` and `:1081-1097`: `variantElement`, `variantSlot` and `slotCarrier`.
   - `VariantShapes.java:100-117`: `pairToLub`.
   - `Lowerer.java:3981-3995`: a Number-LUB literal mix, "the variant carrier keeps each element's own kind".
   - `Lowerer.java:3966-3979`: a non-tagged Any collection.
   - `VariantShapes.java:31-41`: a literal is wrapped to meet a JSON side.
   - Why: there is no SQL union type. This is **meaning**.
3. **Databases.** All of them. The JSON type differs: DuckDB JSON is text, compared as text or cast
   (`DuckDb.java:1196-1223`); Postgres uses jsonb; H2 uses JSON; SQLite and MariaDB use text; Snowflake uses VARIANT.
4. **What plain SQL gets wrong.**
   - JSON erases kinds. `to_json` of a date and of its string are "the SAME bytes" (`Lowerer.java:3945-3948`). So
     `%2020-01-01` and `'2020-01-01'` are equal as Any.
   - An enumeration's value is its name string, so it is equal to the String.
   - A Decimal against a Float: `1.0D` and `1.0` are one JSON number.
   - Text JSON on DuckDB: `1` is not `1.0`. jsonb on Postgres compares numbers by value, so `1 = 1.0` (not verified).
     The answer therefore differs per database.
   - `<` on DuckDB JSON text is lexical. DISTINCT is by text.
5. **Compensations today.**
   - Side level: `SideEqualities.JSON_KINDS` (`:755-781`) reads a JSON value as String, Integer, Float or Boolean
     only. A date in JSON gets no typed member (`instants`, `:1081-1087`, the dates audit's LOW 6).
   - The identity-only equality of a Variant is refused (S401, `SideEqualities.refuseIdentityOnly`, `:2124-2127`;
     `Scalars.java:3586-3590` for dedup).
   - Literal alignment: `VariantShapes.alignLiteralToJson`.
   - The canon of an Any is computed by JSON type (`CanonicalRenderSql.anyJsonCanon`, `:1079-1100`). The comment
     there admits "erased kinds — temporals and Decimals travel as their JSON forms".
   - Gap: Date, Decimal and enum inside a variant (by code reading of `JSON_KINDS`).
6. **Meaning or print?** **MEANING.** The cure in the code's own direction is the tagged carrier (C7), which keeps
   the kind as a type.

### C7. A tagged value or list (`SqlExpr.Tagged`, `SqlExpr.TaggedList`)

1. **What it holds.** A collection of mixed kinds, a mixed selection's winner, a Number-LUB literal mix at a root or
   under list operations. Each element is in a channel per (kind, SQL slot), with a tag and a rank comparable.
2. **Where it is made.** `MixedEncoding.taggedList` (`MixedEncoding.java:164-240`), `mixedNumericLiteral`
   (`:297-313`), `Lowerer.java:3952-3965`, `Scalars.java:3447` and `Equalities.java:349`. The kinds are carried as
   types. This is **meaning**.
3. **Databases.** All of them. Channels are arrays, which are JSON on C3's databases.
4. **What plain SQL gets wrong.**
   - Comparing two tagged lists positionally with `=` mixes channels.
   - Channels hold the same family at different SQL types: a Decimal per scale (`:200-207`), and `DateTime(n)` per
     written digits (`:224-227`, `:597-600`). So a channel-wise `=` misses `1.5D` in `DECIMAL(2,1)` against
     `1.50D` in `DECIMAL(3,2)`, and misses date-times of different written precision.
   - The rank comparable makes every number a DOUBLE (`MixedEncoding.java:33-37`), so it is lossy for big integers
     and Decimals.
5. **Compensations today.**
   - `TaggedLists.sameElement` for a list at positions p and q (`TaggedLists.java:618-643`), a tagged value against
     a static kind (`:649-666`), and two tagged values (`:719-741`).
   - `sameList` (`:669-683`) and `sameListOfKind` (`:686-703`).
   - `contains` and `containsElement` (`:747-812`).
   - `dedup` (`:611-613`).
   - All of these go through `NumberFamilies.canEqual` (`NumberFamilies.java:26-29`) and `MixedEncoding.family`
     (`:443-445`, which folds `DateTime(n)` to DateTime).
   - Equality is dispatched from `Equalities.taggedEquality` (`Equalities.java:330ff`).
   - Inexact ranks are refused (`TaggedLists.refuseInexactRanks`, `:487`).
   - The side level reads kinds with `kindsOf` (`SideEqualities.java:701-716`).
   - A value of a type known only at run time against a tagged value is refused (`TaggedLists.java:650-653`).
6. **Meaning or print?** **MEANING**: the kind is kept as a type. The one print fact is `DateTime(n)`. Written
   subsecond digits are put into the kind (`MixedEncoding.java:224-227`) so that the canon can print them
   (`CanonicalRenderSql.java:1043-1046`). Equality has to undo this with `family()`. This is print-in-meaning.

### C8. A list of days and date-times as one kind

1. **What it holds.** `[%2020-01-01T00:00, %2020-01-02]` read through `map`, `filter`, `if` or a let. A literal
   collection would be tagged, but these become a plain array of one SQL type, so a day becomes a midnight TIMESTAMP.
2. **Where it is made.** The plain-array lowering of a non-tagged Date list (DuckDB and H2 arrays hold one type,
   `SideEqualities.java:2133-2137`).
3. **Databases.** All. Measured on DuckDB: `->map(x|$x)->contains(%2020-01-02T00:00)` is true
   (`HeldKinds.java:98-102`).
4. **What plain SQL gets wrong.** `=`, IN, DISTINCT and `<` see a day as midnight. S399 says a day is never a
   date-time. For example `%2020-01-02T00:00 == [%2020-01-02, %2020-01-01T00:00]->at(0)` is true (S399's known gap).
5. **Compensations today.** Refused by name: `SideEqualities.refuseMixedDays` (`:2138-2146`), from
   `HeldKinds.refuseAcrossKinds` (`HeldKinds.java:97-108`) and `refuseIdentityOnly`. Gap: a list bound by a let is
   "unread: a recorded gap" (`SideEqualities.java:2137`). The `at(0)` form is also still open (S399).
6. **Meaning or print?** **MEANING.** The kind is lost in the array.

### C9. A computed Number list as one numeric SQL type

1. **What it holds.** A `Number[*]` whose elements are computed and of different kinds, for example
   `[$x.i, $x.f]`.
2. **Where it is made.** `Lowerer.java:3981-3985`: "computed elements keep the plain array so downstream aggregates
   still type". The database coerces 1 to 1.0.
3. **Databases.** All of them that have arrays. On JSON databases the numbers keep their JSON number spelling.
4. **What plain SQL gets wrong.** `contains(1.0)` and `==` against `[1.0, …]` are true where Pure says false (exact
   against approximate). DISTINCT merges `1` and `1.0`.
5. **Compensations today.** None found at expression level (gap, by code reading). Register S127 records the list
   coercion. At side level a mixed side never reaches `SAME_IN_ORDER` (`SqlFn.java:154-155`). Not verified whether
   the typer refuses the cross-family needle first.
6. **Meaning or print?** **MEANING.**

### C10. A Float literal as a DECIMAL (target kind DECIMAL)

1. **What it holds.** Every Float literal on ClickHouse, BigQuery and Snowflake (`floatLiteralKind`:
   `ClickHouse.java:900`, `BigQuery.java:774`, `Snowflake.java:340`; default DOUBLE at `AnsiSqlRenderer.java:3021`;
   the reason is at `sql/SqlExpr.java:684-691`: "lanes have not measured yet"). Also every Float literal whose digits
   do not round-trip as a double, on all databases (`TypedCFloat.exact`, `Lowerer.java:3908-3911`). MariaDB reads
   `1.5` as DECIMAL (`MariaDb.java:51`).
2. **Where it is made.** Above. Register S20.
3. **Databases.** ClickHouse, BigQuery and Snowflake, plus the `exact` case everywhere.
4. **What plain SQL gets wrong.** `0.1 + 0.2 == 0.3` is true there, and false in Pure (IEEE). A Float literal
   against a Float column is compared after the database's own coercion.
5. **Compensations today.** None: it is a recorded unmeasured state (S20).
6. **Meaning or print?** Neither. It is an **unmeasured interim** that changes meaning.

### C11. An enumeration as its name text, or as a mapping's DECODE chain over source values

1. **What it holds.** An enum value is its NAME as VARCHAR (`MixedEncoding.java:229-233`, `:601-604`). A mapped enum
   property is a `CASE` chain from source value to name.
2. **Where it is made.** The general lowering, and the mapping enum decode. The canon spells `Enumeration.NAME`.
3. **Databases.** All.
4. **What plain SQL gets wrong.**
   - `=` between enums of two enumerations with the same name is true at the database.
   - `=` between an enum and a String of the same text is true at the database. The typer refuses this when the
     types are known; inside JSON or Any they are indistinguishable (C6).
   - `<` by name is correct: Pure orders enums by name (`MixedEncoding.java:381-383`).
5. **Compensations today.**
   - Tagged channels key the kind by enumeration path.
   - `EnumSourceValues.decodeInvert` (`EnumSourceValues.java:28-89`) is a SQL-shape rewrite (source-space
     comparison), not a correctness fix. Its plain-String-literal branch compares raw store space, for legend-engine
     parity (`:34-48`).
   - Side level gives an enum no tagged reading (`SideEqualities.java:703-705`, refused).
6. **Meaning or print?** **MEANING** for the name. The source-value space is a mapping artifact.

### C12. A mapping wire coercion (`TypedCast.wire`)

1. **What it holds.** A store column whose physical type differs from the property's Pure type (`castAsDeclared`;
   `TypedCast.java:18-19`).
2. **Where it is made.** The mapping read. In a comparison it is unwrapped: `CastPolicy.comparisonWireOperand`
   (`CastPolicy.java:268-288`) compares the RAW column with a literal of the source's type.
3. **Databases.** All.
4. **What plain SQL gets wrong.** The comparison happens in the store type, not the Pure type. A VARCHAR id `'04'`
   against 4 would be compared as text (inferred from code; not verified).
5. **Compensation.** This is legend-engine parity by design.
6. **Meaning or print?** Mapping meaning.

### C13. MariaDB and MySQL: VARCHAR under `utf8mb4_bin` (PAD SPACE)

1. **What it holds.** Every String on MariaDB and MySQL.
2. **Where it is made.** The session sets `SET NAMES utf8mb4 COLLATE utf8mb4_bin` (`MariaDb.java:277`,
   `MySql.java:171`). The column type is at `MariaDb.java:1157`. NO PAD would clash with the JSON functions
   (`MariaDb.java:43-45`).
3. **Databases.** MariaDB and MySQL.
4. **What plain SQL gets wrong.** `'a' == 'a '` is true there, and false in Pure. `' ' == ''` is true. GROUP BY,
   DISTINCT and IN merge values that differ by trailing spaces.
5. **Compensations today.** Only emptiness is tested by length (`MariaDb.java:44-45`). General `=` is not compensated
   (gap: `equalArm`, `MariaDb.java:2138-2146`, handles composites only).
6. **Meaning or print?** **MEANING** of the database's collation.

### C14. Text ordering by the database's collation

1. **What it holds.** String `<`, sort, min and max.
2. **Where it is made.** The database's default collation. `BINARY_COLLATE` (`sql/SqlFn.java:102-108`) is used only
   by the test-data generator. The test Postgres runs `initdb --no-locale` (`testing/.../EmbeddedPostgres.java:109-110`),
   so the lanes hide this.
3. **Databases.** Postgres (any locale cluster), Snowflake and BigQuery (not verified), and others.
4. **What plain SQL gets wrong.** `'B' < 'a'` is true in Pure (code units). Under a locale collation it is false.
5. **Compensations today.** None in product ordering (gap).
6. **Meaning or print?** **MEANING**, from the environment.

### C15. MongoDB: a day and a date-time both as BSON `date`, at milliseconds

1. **What it holds.** DATE and TIMESTAMP, which are both `"date"` (`MongoDb.java:2590`). The precision is
   milliseconds (`MongoDb.java:829-830`).
2. **Where it is made.** The dialect. This is **meaning**.
3. **Databases.** MongoDB.
4. **What plain SQL gets wrong.** A day equals midnight. Sub-millisecond date-times are lost. MongoDB's `$eq`
   compares long and double by value (not verified in code).
5. **Compensations.** None found.
6. **Meaning or print?** **MEANING.**

### Not carriers, but checked

- Booleans as 1/0 on SQLite and MariaDB: the tagged lists give them their own channel, and JSON writes true or false
  (`SQLite.java:910-912`, `MariaDb.java:1129-1131`).
- An Integer as BIGINT or HUGEINT: `besideDecimals` passes a HUGEINT through (`SideEqualities.java:1203-1209`).
- Instances: the keyed `==` compares canonical render TEXTS (`InstanceEquality.java:1-30`,
  `CanonicalRenderSql`). The canon puts the Decimal scale in normalized form. Whether a DateTime key canons by instant
  or by written digits is not verified. `CanonicalRenderSql.java:1043-1046` keeps written digits for tagged slots,
  which conflicts with S399 if it is reached by product equality.

## What a single comparison function would need to know about a side

These are the minimal facts, each fixed at planning from types and SQL types. None is read from a row.

1. **Pure kind.** The static Pure type, reduced to the equality families: exact number, Float, String, Boolean, each
   enumeration path, Year, YearMonth, StrictDate, DateTime, StrictTime, class or keys, list, map, table, Variant.
   Or "known only at run time" (Any, Number, abstract Date, a type variable), which needs a tag. (C1, C6, C7, C11,
   C15.)
2. **SQL carrier.** How the side is physically held:
   - typed native (DATE, TIMESTAMP, DECIMAL(p,s), BIGINT, HUGEINT, DOUBLE, VARCHAR, BOOLEAN);
   - TEMPORAL_TEXT, with its written precision if known (year, year-month, hour, minute, digits);
   - DECIMAL_TEXT;
   - JSON or variant;
   - tagged, with its channel list (kind and slot);
   - a JSON-text container against a native container;
   - a mapping DECODE chain or a wire cast;
   - a database whose "native" form is itself text or approximate: SQLite date-time and REAL decimal, MongoDB date,
     MariaDB PAD SPACE text.
   (All rows.)
3. **Shape.** One value, a list (ordered, or a multiset), a map, a struct or a table. For a list: are its elements
   of one kind, or tagged? (C3, C7, C8, C9.)
4. **Whether it is written (a literal) or computed,** and for a written date its literal precision. This drives the
   fold rule and the compatibility doors' precision equality (`Equalities.upstreamsPartialDateTime`). (C1.)
5. **The target database's facts for that carrier:**
   - does its `=` on the carrier mean value equality (native array coercion, S127; JSON as text against jsonb by
     value; PAD SPACE; locale collation);
   - does it have a native type for the kind (no exact decimal, no temporal, no array);
   - what digits it keeps (S181).
   (C3, C4, C5, C10, C13, C14, C15.)
6. **Which operation it is:** equality, ordering, membership or dedup or GROUP BY, as a condition or as a value
   (empty side, `Lowerer.asCondition`). The canonical key needed for dedup and GROUP BY (one form per value) is
   stronger than what `=` needs. The ordering key (instant, number) differs from the equality key for partial dates
   and DECIMAL_TEXT.

From these, a single function can choose one of five ways to compare:

- **(a)** native `=` or `<`, where both sides are native typed and the database's operator is value-exact for the
  kind;
- **(b)** a normalizing read on both sides into one typed compare form: an instant TIMESTAMP via the JUDGE cast; a
  Decimal or canonical text via `besideDecimals`; a year as a BIGINT and a year-month as y*12+m via `DATE_KINDS`;
  sorted map entries;
- **(c)** element-wise over rows (lists or maps on JSON databases);
- **(d)** by tag, then family, then channel (tagged);
- **(e)** a planning-time refusal or a fold to false (different families; day against date-time; a kind only the run
  time names inside JSON).

The code today makes these choices in at least five places: `Equalities.equal`, `Scalars.inRule` and the ordering
rule, `TaggedLists`, `SideEqualities` and the dialects' `equalArm`. The gaps above are the places where the five
disagree.
