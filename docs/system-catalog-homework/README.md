# The system catalog's homework: the probes (2026-11-08)

The evidence for `docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md` §1. Each probe is a plain Java program, kept as `.java.txt`
so no build compiles it. Each runs against core's main classes compiled outside Bazel, which is a fast loop that needs no
build server.

| Probe | Measures | Result (2026-11-08, at `9de03396`) |
|---|---|---|
| `SeedSplit` | a model file's system-store rows and characters, per table | one class: 2,582 rows, 327,148 characters; `two-classes.pure.txt`: 327,224; `mapping-model.pure.txt`: 362,066 |
| `StressSeed` | the same for the stress corpus's model, assembled as `StressCorpus` assembles it (linked projects, then `core/src/test/resources/stress`, minus `StressExclusions`) | 249 sources, 9,106 elements, 117,113 rows, 17,980,176 characters |
| `SplitCheck` | whether every platform row appears unchanged in the stress model's catalog | 0 missing or altered; 114,533 rows added |
| `CatalogCheck` | registering the stress model's rows in one transaction on DuckDB and H2, timed; whether another session sees them; what a rollback leaves | §1.4's table |

## How to run them

```bash
S=<a scratch directory>
EXT=$(bazel info output_base)/external
JDK=$EXT/rules_java++toolchains+remotejdk25_linux
cd <the repository root>
# core's main classes, outside Bazel (about 25 s)
find core/src/main/java base/src/main/java json/src/main/java arrow_ipc/src -name "*.java" > $S/srcs.txt
$JDK/bin/javac -nowarn -proc:none -d $S/real @$S/srcs.txt
cp -r core/src/main/resources/. $S/real/
# the probes (rename .java.txt to .java)
mkdir -p $S/src $S/cls
for f in SeedSplit StressSeed SplitCheck CatalogCheck; do
  cp docs/system-catalog-homework/$f.java.txt $S/src/$f.java
done
CP=$S/real:$S/cls:$EXT/+product_jars+duckdb_jdbc/jar/duckdb_jdbc-1.5.6.0.jar:$EXT/+product_jars+h2/jar/h2-2.5.252.jar
$JDK/bin/javac -nowarn -cp $CP -d $S/cls $S/src/*.java
$JDK/bin/java -cp $CP SeedSplit docs/system-catalog-homework/two-classes.pure.txt
$JDK/bin/java -Xmx8g -cp $CP StressSeed
$JDK/bin/java -Xmx8g -cp $CP SplitCheck
$JDK/bin/java -Xmx8g -cp $CP CatalogCheck
```

`StressSeed` reads the repository's files at absolute paths under `/home/user/legend-lite`; change them for another
checkout. `CatalogCheck` writes the DuckDB rows through a TEMP staging table and an `INSERT ... SELECT`, the shape that
keeps them in the transaction. Its two other DuckDB measurements were made by editing `append` and the call sites:
- batched prepared inserts: 41,274 ms, isolated;
- the appender straight into the catalog table: 240 ms, not isolated.

## Other evidence used

- **Who reads the metamodel in the relational corpus:** the `[corpus2] statement-origins` line of a DuckDB corpus run
  (the step check's corpus log). It reads `system=120`, in one test, `testRelationalDatabaseMapper`.
- **Upstream:** the pinned archives `@legend_pure_src` (5.99.0) and `@legend_engine_src` (4.145.0) under Bazel's
  `external/`. The files are named in the design's §2.
