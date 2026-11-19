# firstDayOfWeek and the Java locale: the probe (A3 piece 4, 2026-11-19)

Read for S387 and the differences page's item 13. Each database was asked directly over JDBC from a single-file Java
program, under three JVM locales (`-Duser.language` and `-Duser.country`: `en_US`, `en_GB`, `fr_FR`). The jars are the
ones lite's build resolves: legend-engine's H2 2.1.214 (`+product_jars+h2_engine`, the corpus referee's), lite's H2
2.5.252 (`+product_jars+h2`) and DuckDB JDBC 1.5.6.0 (`+product_jars+duckdb_jdbc`). With no `LANG` set (this container)
Java's default locale is `en_US`. The corpus referee's answer, `2014-11-30`, is the `en_US` answer.

## What each statement is

- `Week.java` (H2, `MODE=LEGACY`):
  - legend-engine's H2 SELECT text, from its dialect translation (`relationalMappingExecution.pure:440-443` sends every
    H2 SELECT there; `sqlDialectTranslation/toPostgresModel.pure:356`, `dateTruncCall` at :612-619; the corpus pins it
    at `testToSQLString.pure:650`): `cast(cast(date_trunc('week', d) as timestamp) as date)`, for Thursday 2014-12-04
    and Monday 2014-12-01;
  - `extension`: legend-engine's H2 extension text, which no H2 SELECT reaches
    (`h2Extension2_1_214.pure:223`, `h2Extension1_4_200.pure:118`): `dateadd(DAY, -(mod(dayofweek(d)+5, 7)), d)`.
- `IsoWeek.java`: the same with `date_trunc('iso_week', ...)`, lite's H2 spelling since A5 (`7cd59a571`). Its third
  column is the extension text again.
- `ExtractWeek.java` (lite's H2 2.5.252): `extract(WEEK from d)` and `DATEDIFF(WEEK, a, b)`, the spellings lite's H2
  dialect still writes for a week part (`H2.datePart`, `case WEEK -> "WEEK"`, at `7cd59a571`).
- `DuckWeek.java`: legend-engine's DuckDB text, `date_add(d, to_days(cast(-(isodow(d)-1) as integer)))`
  (`duckdbExtension.pure:228`).

Pure's answer is the Monday: `firstDayOfWeek` is `mostRecentDayOfWeek(DayOfWeek.Monday)` (legend-engine's
`core/pure/corefunctions/dateExtension.pure:202`), so 2014-12-01 for both dates.

## Read

- legend-engine on H2 (its SELECT route): the locale's first day of the week. The Sunday 2014-11-30 under `en_US`; the
  Monday 2014-12-01 under `en_GB` and `fr_FR`. The same on H2 2.1.214 and 2.5.252.
- legend-engine's H2 extension text (not on the SELECT route): the Monday under `en_US`, the Tuesday 2014-12-02 under
  `en_GB` and `fr_FR` (`dayofweek` follows the locale too).
- lite's H2 since A5 (`iso_week`): the Monday under every locale, on both H2 versions.
- legend-engine on DuckDB: the Monday under every locale.
- Found on the way, for the product track (not firstDayOfWeek): lite's H2 `extract(WEEK ...)` and `DATEDIFF(WEEK, ...)`
  follow the locale too. Sunday 2014-11-30 is week 49 under `en_US` and week 48 under `en_GB`. `DATEDIFF(WEEK)` from
  Saturday 11-29 to Sunday 11-30 is 1 under `en_US` and 0 under `en_GB`.

## The outputs, verbatim (the JVM's option banners removed)

```
h2-2.1.214 Week.java: en_US trunc(Thu 12-04)=2014-11-30 trunc(Mon 12-01)=2014-11-30 extension(Thu 12-04)=2014-12-01
h2-2.1.214 IsoWeek.java: en_US trunc(Thu 12-04)=2014-12-01 trunc(Mon 12-01)=2014-12-01 extension(Thu 12-04)=2014-12-01
h2-2.5.252 Week.java: en_US trunc(Thu 12-04)=2014-11-30 trunc(Mon 12-01)=2014-11-30 extension(Thu 12-04)=2014-12-01
h2-2.5.252 IsoWeek.java: en_US trunc(Thu 12-04)=2014-12-01 trunc(Mon 12-01)=2014-12-01 extension(Thu 12-04)=2014-12-01
h2-2.5.252 ExtractWeek.java: en_US extract(WEEK, Sun 11-30)=49 extract(WEEK, Mon 12-01)=49 DATEDIFF(WEEK, Sat 11-29, Sun 11-30)=1 DATEDIFF(WEEK, Sun 11-30, Mon 12-01)=0
duckdb-1.5.6.0 DuckWeek.java: en_US engineDuckDb(Thu 12-04)=2014-12-01 00:00:00.0 (Mon 12-01)=2014-12-01 00:00:00.0 (Sun 11-30)=2014-11-24 00:00:00.0
h2-2.1.214 Week.java: en_GB trunc(Thu 12-04)=2014-12-01 trunc(Mon 12-01)=2014-12-01 extension(Thu 12-04)=2014-12-02
h2-2.1.214 IsoWeek.java: en_GB trunc(Thu 12-04)=2014-12-01 trunc(Mon 12-01)=2014-12-01 extension(Thu 12-04)=2014-12-02
h2-2.5.252 Week.java: en_GB trunc(Thu 12-04)=2014-12-01 trunc(Mon 12-01)=2014-12-01 extension(Thu 12-04)=2014-12-02
h2-2.5.252 IsoWeek.java: en_GB trunc(Thu 12-04)=2014-12-01 trunc(Mon 12-01)=2014-12-01 extension(Thu 12-04)=2014-12-02
h2-2.5.252 ExtractWeek.java: en_GB extract(WEEK, Sun 11-30)=48 extract(WEEK, Mon 12-01)=49 DATEDIFF(WEEK, Sat 11-29, Sun 11-30)=0 DATEDIFF(WEEK, Sun 11-30, Mon 12-01)=1
duckdb-1.5.6.0 DuckWeek.java: en_GB engineDuckDb(Thu 12-04)=2014-12-01 00:00:00.0 (Mon 12-01)=2014-12-01 00:00:00.0 (Sun 11-30)=2014-11-24 00:00:00.0
h2-2.1.214 Week.java: fr_FR trunc(Thu 12-04)=2014-12-01 trunc(Mon 12-01)=2014-12-01 extension(Thu 12-04)=2014-12-02
h2-2.1.214 IsoWeek.java: fr_FR trunc(Thu 12-04)=2014-12-01 trunc(Mon 12-01)=2014-12-01 extension(Thu 12-04)=2014-12-02
h2-2.5.252 Week.java: fr_FR trunc(Thu 12-04)=2014-12-01 trunc(Mon 12-01)=2014-12-01 extension(Thu 12-04)=2014-12-02
h2-2.5.252 IsoWeek.java: fr_FR trunc(Thu 12-04)=2014-12-01 trunc(Mon 12-01)=2014-12-01 extension(Thu 12-04)=2014-12-02
h2-2.5.252 ExtractWeek.java: fr_FR extract(WEEK, Sun 11-30)=48 extract(WEEK, Mon 12-01)=49 DATEDIFF(WEEK, Sat 11-29, Sun 11-30)=0 DATEDIFF(WEEK, Sun 11-30, Mon 12-01)=1
duckdb-1.5.6.0 DuckWeek.java: fr_FR engineDuckDb(Thu 12-04)=2014-12-01 00:00:00.0 (Mon 12-01)=2014-12-01 00:00:00.0 (Sun 11-30)=2014-11-24 00:00:00.0
```

## The sources

### Week.java

```java
import java.sql.*;
public class Week {
  public static void main(String[] a) throws Exception {
    try (Connection c = DriverManager.getConnection("jdbc:h2:mem:w;MODE=LEGACY");
         Statement s = c.createStatement();
         ResultSet r = s.executeQuery("select cast(cast(date_trunc('week', DATE '2014-12-04') as timestamp) as date), cast(cast(date_trunc('week', DATE '2014-12-01') as timestamp) as date), dateadd(DAY, -(mod(dayofweek(DATE '2014-12-04')+5, 7)), DATE '2014-12-04')")) {
      r.next();
      System.out.println(java.util.Locale.getDefault() + " trunc(Thu 12-04)=" + r.getString(1) + " trunc(Mon 12-01)=" + r.getString(2) + " extension(Thu 12-04)=" + r.getString(3));
    }
  }
}
```

### IsoWeek.java

```java
import java.sql.*;
public class IsoWeek {
  public static void main(String[] a) throws Exception {
    try (Connection c = DriverManager.getConnection("jdbc:h2:mem:w;MODE=LEGACY");
         Statement s = c.createStatement();
         ResultSet r = s.executeQuery("select cast(cast(date_trunc('iso_week', DATE '2014-12-04') as timestamp) as date), cast(cast(date_trunc('iso_week', DATE '2014-12-01') as timestamp) as date), dateadd(DAY, -(mod(dayofweek(DATE '2014-12-04')+5, 7)), DATE '2014-12-04')")) {
      r.next();
      System.out.println(java.util.Locale.getDefault() + " trunc(Thu 12-04)=" + r.getString(1) + " trunc(Mon 12-01)=" + r.getString(2) + " extension(Thu 12-04)=" + r.getString(3));
    }
  }
}
```

### ExtractWeek.java

```java
import java.sql.*;
public class ExtractWeek {
  public static void main(String[] a) throws Exception {
    try (Connection c = DriverManager.getConnection("jdbc:h2:mem:w");
         Statement s = c.createStatement();
         ResultSet r = s.executeQuery("select extract(WEEK from DATE '2014-11-30'), extract(WEEK from DATE '2014-12-01'), DATEDIFF(WEEK, DATE '2014-11-29', DATE '2014-11-30'), DATEDIFF(WEEK, DATE '2014-11-30', DATE '2014-12-01')")) {
      r.next();
      System.out.println(java.util.Locale.getDefault() + " extract(WEEK, Sun 11-30)=" + r.getString(1) + " extract(WEEK, Mon 12-01)=" + r.getString(2) + " DATEDIFF(WEEK, Sat 11-29, Sun 11-30)=" + r.getString(3) + " DATEDIFF(WEEK, Sun 11-30, Mon 12-01)=" + r.getString(4));
    }
  }
}
```

### DuckWeek.java

```java
import java.sql.*;
public class DuckWeek {
  public static void main(String[] a) throws Exception {
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
         Statement s = c.createStatement();
         ResultSet r = s.executeQuery("select date_add(DATE '2014-12-04', to_days(cast(-(isodow(DATE '2014-12-04')-1) as integer))), date_add(DATE '2014-12-01', to_days(cast(-(isodow(DATE '2014-12-01')-1) as integer))), date_add(DATE '2014-11-30', to_days(cast(-(isodow(DATE '2014-11-30')-1) as integer)))")) {
      r.next();
      System.out.println(java.util.Locale.getDefault() + " engineDuckDb(Thu 12-04)=" + r.getString(1) + " (Mon 12-01)=" + r.getString(2) + " (Sun 11-30)=" + r.getString(3));
    }
  }
}
```
