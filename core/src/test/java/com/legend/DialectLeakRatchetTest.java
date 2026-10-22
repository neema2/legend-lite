// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE DIALECT-LEAK RATCHET (the dialect program's Phase 0, 2026-10-13; docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md):
 * the census's mechanical part, kept. Every database decision belongs to its dialect (or, for execution, its
 * connector); a line that names a database anywhere else is a leak the program moves home. Counted per file and per
 * kind, by line, in code (a comment never counts; a string only for a JDBC URL):
 * <ul>
 *   <li>{@code database-type}: {@code DatabaseType.X};</li>
 *   <li>{@code dialect-class}: a dialect class constructed, tested, cast to or named by its package;</li>
 *   <li>{@code driver}: a JDBC driver's own package;</li>
 *   <li>{@code product-name}: a database's name in an identifier ({@code isPostgres}, {@code DuckDbCells});</li>
 *   <li>{@code jdbc-url}: a {@code jdbc:} URL written inline;</li>
 *   <li>{@code product-call}: {@code getDatabaseProductName}, a decision by what the driver says it is.</li>
 * </ul>
 * The owners are not counted ({@link DatabaseOwners}): the dialect package and its tests, the DuckDB-only sources,
 * {@code com.legend.database.Databases}, which maps a declared type to its dialect, and since 2026-10-15 (the user's
 * decisions D9 and D11) the model language's vocabulary (the lexer, the parser, the protocol, the model) and the SQL
 * input languages; {@link DialectOwnerExclusivityTest} holds each to its own database. Since 2026-10-18 (the program's
 * close: D11 moved the last counted lines, the SQL translator's Postgres rules, into Postgres's own files) this is a
 * BAN: no file outside the owners names a database, and its ledger of pins ({@code dialect-leaks/ledger.tsv}, shrink
 * only since Phase 0) is deleted. A new leak fails here; its fix is to move the decision into its owner, never a pin.
 * The run writes what it counted to its outputs, {@code dialect-leaks.tsv} (empty while the ban holds).
 */
@Tag("census")
class DialectLeakRatchetTest {

    /** The Java sources the census target declares: core, the modules that test it, and since 2026-10-18 (the program's
     *  Phase 7) every other module that plans, renders or runs SQL on a database: the warehouse (a host owner:
     *  DatabaseOwners), the tab's and Python's planner boundary (wasm's planner, native) and the test databases'
     *  launchers (testing). Not read: the build's tools, wasm's conformance harness (it tests the JDK's classes) and
     *  the other products (sdlc-server, depot-server, query-store, json, base, site, datacube's tools), whose hits,
     *  measured that day, were three false positives ({@code Sha1}'s {@code h2}, {@code Bump}'s {@code engineArchive},
     *  a {@code jdbc:postgresql:} string the conformance harness parses as a URI), the census renderer (a census tool,
     *  naming every dialect by design) and a tool named for Postgres ({@code AppPostgres}). */
    private static final List<String> ROOTS = List.of("core/src", "pct/src", "spec/src", "parser-equivalence/src",
            "warehouse/src/main/java", "warehouse/src/test/java", "wasm/src/main/java/planner", "native/src/main/java",
            "testing/src/main/java");

    private static final String DIALECTS = "AnsiSqlRenderer|DuckDb|H2Modern|H2|Postgres|Snowflake|Databricks|BigQuery"
            + "|SQLite|MariaDb|MySql|Trino|ClickHouse|MongoDb|EngineStyleH2|EngineStyleDB2|EngineStyleComposite";

    /** H2 and DB2 as a word or an identifier's word ({@code h2}, {@code isH2}, {@code LocalH2}, {@code ENGINE_H2},
     *  {@code h2Mode}, {@code isDb2}): after a lower-case letter or digit only in upper case, a camel-case word's start,
     *  so {@code width2} is no H2; never followed by a digit ({@code H264}). Until the PR #39 audit (2026-10-14) both
     *  needed a word boundary on each side, and an identifier naming either was not counted. */
    private static final String H2_OR_DB2 = "(?-i:(?<=[a-z0-9])H2|(?<![A-Za-z0-9])[Hh]2"
            + "|(?<=[a-z0-9])D[Bb]2|(?<![A-Za-z0-9])[Dd][Bb]2)(?![0-9])";

    private static final String PRODUCTS = "duck\\s?db|postgres(?:ql)?|snowflake|databricks|big\\s?query|sqlite"
            + "|maria\\s?db|mysql|trino|presto|click\\s?house|" + H2_OR_DB2 + "|sybase|oracle|sql\\s?server|redshift"
            + "|athena|spanner|hive\\b|memsql|singlestore|mongo|elastic";

    /** The kinds counted in code, each by the census's own pattern (scanner/scan.py). */
    static final Map<String, Pattern> IN_CODE = Map.of(
            // 2026-10-16, the dialect program's Phase 3 (PARK-16's acceptance test): DatabaseType.values()
            // enumerates every database and names none; until then the pattern counted it as DatabaseType.X.
            // valueOf and .class are still counted (the PR audit of 2026-10-16: Enum.valueOf(DatabaseType.class,
            // "H2") picks one database by a literal)
            "database-type", Pattern.compile("\\bDatabaseType\\s*\\.\\s*(?!values\\s*\\()\\w+"),
            "dialect-class", Pattern.compile("\\b(new\\s+|instanceof\\s+|\\(\\s*)(?:com\\.legend\\.sql\\.dialect\\.)?("
                    + DIALECTS + ")\\b(\\s*\\(|\\s+\\w|\\s*\\)|\\.class)|\\bcom\\.legend\\.sql\\.dialect\\.(" + DIALECTS
                    + ")\\b"),
            "driver", Pattern.compile("\\b(org\\.duckdb|org\\.h2\\.|org\\.postgresql|net\\.snowflake|com\\.databricks"
                    + "|com\\.simba|com\\.google\\.cloud\\.bigquery|org\\.sqlite|org\\.mariadb|com\\.mysql|io\\.trino"
                    + "|com\\.clickhouse|com\\.mongodb|org\\.bson)"),
            "product-name", Pattern.compile("(?i)\\b\\w*(" + PRODUCTS + ")\\w*"),
            "product-call", Pattern.compile("\\bgetDatabaseProductName\\b"));

    static final Pattern JDBC_URL = Pattern.compile("jdbc:\\w+");

    /** The type pattern counts a database the code names, and not the enumeration of all of them (2026-10-16). */
    @Test
    void theTypePatternSeesANamedTypeOnly() {
        Pattern types = IN_CODE.get("database-type");
        for (String named : List.of("DatabaseType.H2", "DatabaseType . DuckDB", "DatabaseType.valueOf(n)",
                "DatabaseType.valuesOf", "Enum.valueOf(DatabaseType.class, \"H2\")", "EnumSet.allOf(DatabaseType.class)")) {
            assertTrue(types.matcher("x = " + named + ";").find(), named);
        }
        for (String not : List.of("DatabaseType.values()", "DatabaseType . values ()")) {
            assertTrue(!types.matcher("x = " + not + ";").find(), not);
        }
    }

    /** The product pattern counts H2 and DB2 as an identifier's word, as it counts {@code isPostgres}, and not as a
     *  word's letters (the PR #39 audit's planted identifiers, 2026-10-14). */
    @Test
    void theProductPatternSeesH2AndDb2InIdentifiers() {
        Pattern products = IN_CODE.get("product-name");
        for (String named : List.of("h2", "H2", "isH2", "LocalH2", "EmbeddedH2", "H2Settings", "rawH2IsNative",
                "ENGINE_H2", "h2Mode", "isDb2", "DB2", "EngineStyleDB2", "db2Text", "isPostgres", "DuckDbCells")) {
            assertTrue(products.matcher("x = " + named + ";").find(), named);
        }
        for (String not : List.of("width2", "sha2", "SHA256", "H264", "cache2", "IDB2X", "depth2")) {
            assertTrue(!products.matcher("x = " + not + ";").find(), not);
        }
    }

    @Test
    void noDatabaseIsNamedOutsideItsOwners() throws IOException {
        // the ban is only as wide as what it reads (the review of D11, 2026-10-18): every root must yield Java sources,
        // so a root the census target stops declaring (core/BUILD.bazel's _CENSUS_READS) fails here, not silently
        for (String root : ROOTS) {
            assertTrue(SourceFiles.under(root).stream().anyMatch(f -> f.toString().endsWith(".java")),
                    "the ban read no Java source under " + root + ": declare it in the census target's files");
        }
        TreeMap<String, Integer> counts = new TreeMap<>();
        for (Path f : SourceFiles.under(ROOTS.get(0), ROOTS.subList(1, ROOTS.size()).toArray(String[]::new))) {
            String rel = SourceFiles.path(f);
            if (!rel.endsWith(".java")) {
                continue;
            }
            String src = Files.readString(f);
            if (DatabaseOwners.of(rel, src) != null) {
                continue;
            }
            Lines lines = lex(src);
            for (var kind : IN_CODE.entrySet()) {
                int n = (int) lines.code().stream().filter(l -> kind.getValue().matcher(l).find()).count();
                if (n > 0) {
                    counts.put(rel + "\t" + kind.getKey(), n);
                }
            }
            int urls = (int) lines.strings().stream().filter(l -> JDBC_URL.matcher(l).find()).count();
            if (urls > 0) {
                counts.put(rel + "\tjdbc-url", urls);
            }
        }
        List<String> table = new ArrayList<>();
        counts.forEach((k, v) -> table.add(k + "\t" + v));
        Files.createDirectories(com.legend.testing.TestOutputs.dir());
        Files.write(com.legend.testing.TestOutputs.file("dialect-leaks.tsv"), table);

        assertTrue(counts.isEmpty(), "a database named outside its owners (DatabaseOwners: a dialect, a connector, a test"
                + " backend, an input language, the registries) -- move the decision into its owner: " + table
                + " (the whole table is the run's dialect-leaks.tsv)");
    }

    /** A source's lines as code (comments and string literals blanked) and as its string literals, line by line. */
    record Lines(List<String> code, List<String> strings) {
    }

    static Lines lex(String src) {
        StringBuilder code = new StringBuilder(src.length());
        StringBuilder strings = new StringBuilder();
        int i = 0;
        int n = src.length();
        while (i < n) {
            char c = src.charAt(i);
            if (src.startsWith("//", i)) {
                while (i < n && src.charAt(i) != '\n') {
                    i++;
                }
            } else if (src.startsWith("/*", i)) {
                int end = src.indexOf("*/", i + 2);
                end = end < 0 ? n : end + 2;
                blank(src, i, end, code, strings);
                i = end;
            } else if (src.startsWith("\"\"\"", i)) {
                int end = src.indexOf("\"\"\"", i + 3);
                end = end < 0 ? n : end + 3;
                literal(src, i, end, code, strings);
                i = end;
            } else if (c == '"' || c == '\'') {
                int j = i + 1;
                while (j < n && src.charAt(j) != c && src.charAt(j) != '\n') {
                    j += src.charAt(j) == '\\' ? 2 : 1;
                }
                int end = Math.min(j + 1, n);
                if (c == '"') {
                    literal(src, i, end, code, strings);
                } else {
                    blank(src, i, end, code, strings);
                }
                i = end;
            } else {
                code.append(c);
                strings.append(c == '\n' ? '\n' : ' ');
                i++;
            }
        }
        return new Lines(List.of(code.toString().split("\n", -1)), List.of(strings.toString().split("\n", -1)));
    }

    /** {@code src[from, to)} as neither code nor string: newlines kept, so both stay line for line. */
    private static void blank(String src, int from, int to, StringBuilder code, StringBuilder strings) {
        for (int k = from; k < to; k++) {
            char ch = src.charAt(k) == '\n' ? '\n' : ' ';
            code.append(ch);
            strings.append(ch);
        }
    }

    /** {@code src[from, to)}, a string literal: in the strings, blanked in the code. */
    private static void literal(String src, int from, int to, StringBuilder code, StringBuilder strings) {
        for (int k = from; k < to; k++) {
            char ch = src.charAt(k);
            code.append(ch == '\n' ? '\n' : ' ');
            strings.append(ch);
        }
    }
}
