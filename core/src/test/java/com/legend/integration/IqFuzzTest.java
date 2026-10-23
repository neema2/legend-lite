// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.server.QueryService;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.IqFuzz;
import com.legend.testcases.IqParity;
import com.legend.testcases.SqlParity;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The oracle-free fuzzer (the IQ scale program, docs/IQ_SCALE_PROGRAM_2026_10_22.md P2): random IQ queries over the
 * parity schema ({@link IqFuzz}), each run as an {@code #IQ{...}#} island on DuckDB and planned for Snowflake. No IQ
 * answer is known, so each is judged by what must hold whatever IQ answers:
 *
 * <ul>
 *   <li><b>no crash</b>: the island answers, names IQ's error, or refuses by name; any other exception, in DuckDB's run
 *       or Snowflake's plan, is a finding;</li>
 *   <li><b>no database error</b>: SQL the island sends DuckDB runs, unless the island raises IQ's error there by name
 *       (an overflow past a type, D-N3's known divergence, is counted apart);</li>
 *   <li><b>ternary logic partitioning</b> (SQLancer's TLP): a query's rows are, as a multiset, its rows where a
 *       predicate holds, where it fails, and where it is unknown;</li>
 *   <li><b>the set identities</b>: DISTINCT, UNION, INTERSECT and EXCEPT [ALL] against the shape's rows;</li>
 *   <li><b>NoREC</b> (SQLancer's): the rows where a predicate holds, counted by a WHERE and by a CASE in the select
 *       list, are as many.</li>
 * </ul>
 *
 * A finding replays from its seed. Manual ({@code //core:iq_fuzz_test}): {@code -Diq.fuzz.seed} (default 1),
 * {@code -Diq.fuzz.count} (default 2,000), {@code -Diq.fuzz.depth} (default 3), {@code -Diq.fuzz.profile} (default
 * {@code iq17-default}). The findings and the refusals' reasons, ranked, go to the test's outputs.
 */
class IqFuzzTest {

    private static final TestBackend PLATFORM = TestBackends.platform();

    private static final String SNOWFLAKE = "type: Snowflake;\n    specification: Snowflake { name: 'IQ_FUZZ';"
            + " account: 'acct'; warehouse: 'wh'; region: 'us-east-1'; };\n    auth: SnowflakePublic { publicUserName:"
            + " 'u'; privateKeyVaultReference: 'vault/k'; passPhraseVaultReference: 'vault/pp'; };";

    /** A Postgres connection the lens plans for (render only: no server is reached). */
    private static final String POSTGRES = "type: Postgres;\n    specification: Static { host: '127.0.0.1'; port: 5432;"
            + " name: 'postgres'; };\n    auth: Test;";

    /** One query's outcome: its kind, and its rows (ROWS) or its reason. */
    record Outcome(String kind, List<String> rows, String reason) {
    }

    @Test
    void randomIslandsNeverCrashAndPartitionTheirRows() throws Exception {
        long seed = Long.getLong("iq.fuzz.seed", 1L);
        int count = Integer.getInteger("iq.fuzz.count", 2000);
        int depth = Integer.getInteger("iq.fuzz.depth", 3);
        String profile = System.getProperty("iq.fuzz.profile", "iq17-default");
        List<SqlParity.Table> tables = IqParity.schema(Runfile.property("iq.parity.schema"));
        Set<String> names = IqParity.tableNames(tables);
        String model = IqParity.model(tables, PLATFORM.connectionBody());
        var snowflake = com.legend.Compiler.compileModel(IqParity.model(tables, SNOWFLAKE));
        Map<String, Integer> kinds = new TreeMap<>();
        Map<String, Integer> reasons = new TreeMap<>();
        Map<String, String> examples = new TreeMap<>();
        StringBuilder findings = new StringBuilder("seed\tcheck\tsql\twhat\n");
        int found = 0;
        int partitioned = 0;
        try (Connection session = PLATFORM.open()) {
            IqParity.seed(PLATFORM, session, tables);
            QueryService service = new QueryService();
            for (int i = 0; i < count; i++) {
                long s = seed * 1_000_003L + i;
                IqFuzz gen = new IqFuzz(s, tables, depth);
                IqFuzz.Shape shape = gen.shape();
                String p = gen.predicate(shape.scope());
                // before complex(), whose own shape numbers its correlation names afresh: a predicate drawn after it
                // could name a subquery's table as one the shape already names (IQ's -143, rightly)
                String p2 = gen.predicate(shape.scope());
                String complex = gen.complex();
                List<String> sqls = new ArrayList<>(List.of(shape.sql(), shape.where("(" + p + ")"),
                        shape.where("NOT (" + p + ")"), shape.where("(" + p + ") IS UNKNOWN"), complex,
                        shape.partitions(p)));
                sqls.addAll(setQueries(shape, p, p2));
                // NoREC (13, 14): the predicate counted where the island filters and where it computes a CASE
                sqls.add(shape.countWhere(p));
                sqls.add(shape.countCase(p));
                List<Outcome> outcomes = new ArrayList<>();
                for (int q = 0; q < sqls.size(); q++) {
                    String sql = sqls.get(q);
                    Outcome o = run(service, session, model, sql, profile, tables, names, s, q);
                    outcomes.add(o);
                    kinds.merge(o.kind(), 1, Integer::sum);
                    if (o.kind().equals("TRANSLATED") || o.kind().equals("REFUSED")) {
                        reasons.merge(o.kind() + " " + reason(o.reason()), 1, Integer::sum);
                        examples.putIfAbsent(o.kind() + " " + reason(o.reason()), s + "\t" + sql);
                    }
                    if (o.kind().equals("CRASH") || o.kind().equals("DBERROR")) {
                        found++;
                        findings.append(s).append('\t').append(o.kind()).append('\t').append(sql).append('\t')
                                .append(o.reason()).append('\n');
                    }
                    if (o.kind().equals("ROWS")) {
                        String crash = snowflakeCrash(snowflake, sql, profile, tables, names, s, q);
                        if (crash != null) {
                            found++;
                            findings.append(s).append("\tSNOWFLAKE CRASH\t").append(sql).append('\t').append(crash)
                                    .append('\n');
                        }
                    }
                }
                found += setIdentities(outcomes, sqls, s, profile.startsWith("sqla17"), findings);
                if (outcomes.get(13).kind().equals("ROWS") && outcomes.get(14).kind().equals("ROWS")
                        && !outcomes.get(13).rows().equals(outcomes.get(14).rows())) {
                    found++;
                    findings.append(s).append("\tNOREC\t").append(sqls.get(14)).append('\t').append("WHERE counts ")
                            .append(outcomes.get(13).rows()).append(", the CASE ").append(outcomes.get(14).rows())
                            .append('\n');
                }
                if (outcomes.subList(0, 4).stream().allMatch(o -> o.kind().equals("ROWS"))) {
                    partitioned++;
                    List<String> whole = sorted(outcomes.get(0).rows());
                    List<String> parts = new ArrayList<>();
                    for (int q = 1; q < 4; q++) {
                        parts.addAll(outcomes.get(q).rows());
                    }
                    parts = sorted(parts);
                    if (!whole.equals(parts)) {
                        found++;
                        findings.append(s).append("\tTLP\t").append(shape.sql()).append('\t').append("predicate ")
                                .append(p).append(": ").append(whole.size()).append(" rows, the partitions ")
                                .append(outcomes.get(1).rows().size()).append(" + ").append(outcomes.get(2).rows().size())
                                .append(" + ").append(outcomes.get(3).rows().size()).append('\n');
                    } else if (outcomes.get(5).kind().equals("ROWS") && !whole.equals(sorted(outcomes.get(5).rows()))) {
                        // the same partitions as one query of nested UNION ALLs (Q19)
                        found++;
                        findings.append(s).append("\tTLP-UNION\t").append(shape.partitions(p)).append('\t')
                                .append(whole.size()).append(" rows, the union of the partitions ")
                                .append(outcomes.get(5).rows().size()).append('\n');
                    }
                }
            }
        }
        // each reason with its first query (its seed, then its SQL), as the loop's next look
        StringBuilder ranked = new StringBuilder("count\treason\tseed\tsql\n");
        reasons.entrySet().stream().sorted((a, b) -> b.getValue() - a.getValue())
                .forEach(e -> ranked.append(e.getValue()).append('\t').append(e.getKey()).append('\t')
                        .append(examples.get(e.getKey())).append('\n'));
        System.out.println("iq-fuzz seed " + seed + ", " + count + " shapes, depth " + depth + ", " + profile + ": "
                + kinds + "; " + partitioned + " partitioned; " + found + " findings");
        String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (outputs != null) {
            Files.writeString(Path.of(outputs, "iq-fuzz-findings.tsv"), findings.toString(), StandardCharsets.UTF_8);
            Files.writeString(Path.of(outputs, "iq-fuzz-reasons.tsv"), ranked.toString(), StandardCharsets.UTF_8);
        }
        assertTrue(found == 0, found + " findings (the test's outputs, iq-fuzz-findings.tsv):\n"
                + findings.substring(0, Math.min(findings.length(), 20_000)));
    }

    /** The set identities' queries, after the six above (6..12): A's distinct rows, then A INTERSECT, EXCEPT, UNION,
     *  UNION ALL, INTERSECT ALL and EXCEPT ALL B, A the shape where {@code p} (query 1) and B the shape where
     *  {@code p2} (the IQ scale program's Q20). */
    private static List<String> setQueries(IqFuzz.Shape shape, String p, String p2) {
        List<String> out = new ArrayList<>(List.of(shape.distinct(p)));
        for (String op : SET_OPS) {
            out.add(shape.set(p, op, p2));
        }
        return out;
    }

    private static final List<String> SET_OPS = List.of("INTERSECT", "EXCEPT", "UNION", "UNION ALL", "INTERSECT ALL",
            "EXCEPT ALL");

    /** The identities over A's rows (query 1) and its set operations with B (queries 6..12): DISTINCT A is A's rows
     *  each once; A INTERSECT B and A EXCEPT B split DISTINCT A; A UNION B is A UNION ALL B each once; A INTERSECT ALL
     *  B and A EXCEPT ALL B split A's rows. Rows compared by their collation keys ({@link #collationKey}): where equal
     *  keys differ in their text, IQ shows one of them, the plan's choice. Each a finding appended to
     *  {@code findings}; the number found. */
    private static int setIdentities(List<Outcome> outcomes, List<String> sqls, long seed, boolean caseFolded,
            StringBuilder findings) {
        int found = 0;
        // {check, the whole, its parts...}, each an index into the outcomes; the whole each once where the check says
        int[][] checks = {{1, 6}, {6, 7, 8}, {10, 9}, {1, 11, 12}};
        String[] names = {"DISTINCT", "SET INTERSECT+EXCEPT", "SET UNION", "SET INTERSECT ALL+EXCEPT ALL"};
        for (int c = 0; c < checks.length; c++) {
            int[] at = checks[c];
            if (!java.util.Arrays.stream(at).allMatch(i -> outcomes.get(i).kind().equals("ROWS"))) {
                continue;
            }
            List<String> whole = sorted(collationKeys(outcomes.get(at[0]).rows(), caseFolded));
            List<String> parts = new ArrayList<>();
            for (int i = 1; i < at.length; i++) {
                parts.addAll(collationKeys(outcomes.get(at[i]).rows(), caseFolded));
            }
            // DISTINCT and UNION: the part, each once, is the whole's rows each once
            if (c == 0 || c == 2) {
                whole = sorted(new ArrayList<>(new java.util.LinkedHashSet<>(whole)));
            }
            parts = sorted(parts);
            if (!whole.equals(parts)) {
                found++;
                findings.append(seed).append('\t').append(names[c]).append('\t').append(sqls.get(at[at.length - 1]))
                        .append('\t').append(whole.size()).append(" rows, the parts ").append(parts.size())
                        .append('\n');
            }
        }
        return found;
    }

    private static List<String> collationKeys(List<String> rows, boolean caseFolded) {
        return rows.stream().map(r -> collationKey(r, caseFolded)).toList();
    }

    /** A row's text as the profile's database tells rows apart: an IQ database's ignores a cell's trailing blanks ('a'
     *  and 'a ' are equal), a sqla17 profile's database folds the ASCII letters and keeps the blanks (fuzz round 5,
     *  2026-10-23: its DISTINCT shows 'A' and 'A ' apart). */
    static String collationKey(String row, boolean caseFolded) {
        return caseFolded ? row.toLowerCase(java.util.Locale.ROOT) : row.replaceAll(" +(?=, |]$)", "");
    }

    /**
     * The scale probe (the IQ scale program, P1.4 and P4): the census's shapes at and past its budgets, each translated,
     * planned for DuckDB and for Snowflake (render only) and run on DuckDB, its time, its SQL's size and its outcome
     * reported ({@code iq-scale.tsv} in the test's outputs). A crash or a database error fails it; a refusal by name is
     * reported, as the limit it is. {@code -Diq.scale.factor} (default 1) multiplies every size.
     */
    @Test
    void theCensusShapesPlanAndRunAtScale() throws Throwable {
        // on the server's request stack (LegendHttpServer.REQUEST_STACK_BYTES), as a query the product runs
        Throwable[] failed = new Throwable[1];
        Thread t = new Thread(null, () -> {
            try {
                censusShapes();
            } catch (Throwable x) {
                failed[0] = x;
            }
        }, "iq-scale-probe", com.legend.server.LegendHttpServer.REQUEST_STACK_BYTES);
        t.start();
        t.join();
        if (failed[0] != null) {
            throw failed[0];
        }
    }

    /** A scale shape's family: its SQL at a size, and the largest size the reach probe tries. */
    private record Family(java.util.function.IntFunction<String> sql, int max) {
    }

    private static final Map<String, Family> FAMILIES = new java.util.LinkedHashMap<>();

    static {
        FAMILIES.put("union all", new Family(IqFuzzTest::unionAll, 4_096));
        FAMILIES.put("joins", new Family(IqFuzzTest::joins, 2_048));
        FAMILIES.put("parentheses", new Family(n -> "SELECT " + "(".repeat(n) + "id" + " + 1)".repeat(n) + " AS v"
                + " FROM t WHERE id = 1", 4_096));
        FAMILIES.put("nested functions", new Family(n -> "SELECT " + "ABS(".repeat(n) + "id" + ")".repeat(n) + " AS v"
                + " FROM t WHERE id = 1", 4_096));
        FAMILIES.put("nested CASE", new Family(IqFuzzTest::nestedCase, 4_096));
        FAMILIES.put("CASE of whens", new Family(IqFuzzTest::flatCase, 16_384));
        FAMILIES.put("IN list", new Family(n -> "SELECT id FROM t WHERE id IN (" + numbers(n) + ") ORDER BY id",
                131_072));
        FAMILIES.put("select items", new Family(IqFuzzTest::selectItems, 32_768));
        FAMILIES.put("AND", new Family(n -> "SELECT id FROM t WHERE " + andChain(n) + " ORDER BY id", 32_768));
        FAMILIES.put("OR", new Family(n -> "SELECT id FROM t WHERE " + andChain(n).replace(" AND ", " OR ")
                .replace("<>", "=") + " ORDER BY id", 32_768));
        FAMILIES.put("string concatenation", new Family(n -> "SELECT " + "label || ".repeat(n) + "'.' AS v FROM u",
                8_192));
        FAMILIES.put("derived tables nested", new Family(IqFuzzTest::nestedDerived, 1_024));
        FAMILIES.put("CTE chain", new Family(IqFuzzTest::cteChain, 1_024));
        FAMILIES.put("BIGINT sum", new Family(n -> "SELECT id, " + "bg + ".repeat(n - 1) + "bg AS v FROM t WHERE"
                + " id <> 1", 4_096));
    }

    /**
     * The default stack's reach (the IQ scale program's Q18): each scale shape's family at doubling sizes, translated and
     * planned for DuckDB on a thread of {@code -Diq.stack.kb} (default 1,024, the JDK's default on 64-bit Linux), until
     * one overflows, fails or takes past 30 s. Reported, not asserted ({@code iq-stack.tsv}): the largest size planned,
     * and the frames that overflowed past it, which name the layer to make iterative. {@code -Diq.scale.only}
     * narrows the families.
     */
    @Test
    void theDefaultStacksReach() throws Exception {
        long stack = Long.getLong("iq.stack.kb", 1_024L) << 10;
        List<SqlParity.Table> tables = IqParity.schema(Runfile.property("iq.parity.schema"));
        Set<String> names = IqParity.tableNames(tables);
        var duckdb = com.legend.Compiler.compileModel(IqParity.model(tables, PLATFORM.connectionBody()));
        String only = System.getProperty("iq.scale.only");
        StringBuilder report = new StringBuilder("family\tlargest planned\tms\tpast it\n");
        for (var e : FAMILIES.entrySet()) {
            if (only != null && !e.getKey().matches(only)) {
                continue;
            }
            int planned = 0;
            long plannedMs = 0;
            String past = "none up to " + e.getValue().max();
            for (int n = 8; n <= e.getValue().max(); n *= 2) {
                String island = island(e.getValue().sql().apply(n), "iq17-default", tables, names, 0, n);
                Object[] out = new Object[1];
                long t0 = System.nanoTime();
                Thread t = new Thread(null, () -> {
                    try {
                        out[0] = com.legend.Compiler.query(duckdb, island).plan(IqParity.RUNTIME).sql();
                    } catch (Throwable x) {
                        out[0] = x;
                    }
                }, "iq-stack-probe", stack);
                t.start();
                t.join();
                long ms = (System.nanoTime() - t0) / 1_000_000;
                if (out[0] instanceof Throwable x) {
                    past = n + ": " + (x instanceof StackOverflowError ? "StackOverflowError " + frames(x)
                            : classify(x).kind() + " " + classify(x).reason());
                    break;
                }
                planned = n;
                plannedMs = ms;
                if (ms > 30_000) {
                    past = "stopped: " + n + " planned in " + ms + " ms";
                    break;
                }
            }
            report.append(e.getKey()).append('\t').append(planned).append('\t').append(plannedMs).append('\t')
                    .append(past.length() > 400 ? past.substring(0, 400) : past).append('\n');
        }
        System.out.println("iq-stack, " + (stack >> 10) + " KB:\n" + report);
        String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (outputs != null) {
            Files.writeString(Path.of(outputs, "iq-stack.tsv"), report.toString(), StandardCharsets.UTF_8);
        }
    }

    private void censusShapes() throws Exception {
        int f = Integer.getInteger("iq.scale.factor", 1);
        List<SqlParity.Table> tables = IqParity.schema(Runfile.property("iq.parity.schema"));
        Set<String> names = IqParity.tableNames(tables);
        String model = IqParity.model(tables, PLATFORM.connectionBody());
        var duckdb = com.legend.Compiler.compileModel(model);
        var snowflake = com.legend.Compiler.compileModel(IqParity.model(tables, SNOWFLAKE));
        Map<String, String> shapes = new java.util.LinkedHashMap<>();
        for (int n : new int[] {500 * f, 1_000 * f, 2_000 * f}) {
            shapes.put("union all of " + n, unionAll(n));
        }
        for (int n : new int[] {350 * f, 700 * f, 1_200 * f}) {
            shapes.put("joins " + n, joins(n));
        }
        for (int n : new int[] {500 * f, 1_000 * f}) {
            shapes.put("parentheses " + n, "SELECT " + "(".repeat(n) + "id" + " + 1)".repeat(n) + " AS v FROM t"
                    + " WHERE id = 1");
            shapes.put("nested functions " + n, "SELECT " + "ABS(".repeat(n) + "id" + ")".repeat(n) + " AS v FROM t"
                    + " WHERE id = 1");
            shapes.put("nested CASE " + n, nestedCase(n));
            shapes.put("CASE of whens " + n, flatCase(n));
        }
        for (int n : new int[] {20_000 * f, 60_000 * f}) {
            shapes.put("IN list of " + n, "SELECT id FROM t WHERE id IN (" + numbers(n) + ") ORDER BY id");
        }
        for (int n : new int[] {5_000 * f, 16_000 * f}) {
            shapes.put("select items " + n, selectItems(n));
            shapes.put("AND of " + n, "SELECT id FROM t WHERE " + andChain(n) + " ORDER BY id");
        }
        for (int n : new int[] {50 * f, 200 * f}) {
            shapes.put("derived tables nested " + n, nestedDerived(n));
            shapes.put("CTE chain " + n, cteChain(n));
        }
        // the IQ scale program, 2026-10-23: a range check at each level of a chain copied its operand three times a
        // level, a 10-term BIGINT sum past Snowflake's statement timeout (3^9); the size check below holds such growth
        for (int n : new int[] {10 * f, 50 * f, 200 * f}) {
            shapes.put("integer sum of " + n, "SELECT id, " + "it + ".repeat(n - 1) + "it AS v FROM t ORDER BY id");
            shapes.put("BIGINT sum of " + n, "SELECT id, " + "bg + ".repeat(n - 1) + "bg AS v FROM t WHERE id <> 1"
                    + " ORDER BY id");
        }
        // -Diq.scale.only=<regex>: the shapes whose names match
        String only = System.getProperty("iq.scale.only");
        if (only != null) {
            shapes.keySet().removeIf(k -> !k.matches(only));
        }
        StringBuilder report = new StringBuilder("shape\toutcome\tms\tduckdb sql chars\tsnowflake\tdetail\n");
        List<String> problems = new ArrayList<>();
        try (Connection session = PLATFORM.open()) {
            IqParity.seed(PLATFORM, session, tables);
            QueryService service = new QueryService();
            int q = 0;
            for (var e : shapes.entrySet()) {
                String island = island(e.getValue(), "iq17-default", tables, names, 0, q++);
                long t0 = System.nanoTime();
                String sql = "";
                String snow;
                Outcome o;
                long planMs = -1;
                try {
                    sql = com.legend.Compiler.query(duckdb, island).plan(IqParity.RUNTIME).sql();
                    planMs = (System.nanoTime() - t0) / 1_000_000;
                    o = run(service, session, model, e.getValue(), "iq17-default", tables, names, 0, q);
                    o = new Outcome(o.kind(), o.rows(), o.reason() + " (planned in " + planMs + " ms)");
                } catch (Exception | StackOverflowError x) {
                    o = classify(x);
                    if (x instanceof StackOverflowError) {
                        o = new Outcome(o.kind(), o.rows(), o.reason() + " " + frames(x));
                    }
                }
                long ms = (System.nanoTime() - t0) / 1_000_000;
                try {
                    snow = "PLANNED " + com.legend.Compiler.query(snowflake, island).plan(IqParity.RUNTIME).sql()
                            .length();
                } catch (Exception | StackOverflowError x) {
                    snow = classify(x).kind() + ": " + classify(x).reason();
                }
                String detail = o.kind().equals("ROWS") ? o.rows().size() + " rows" + o.reason() : o.reason();
                report.append(e.getKey()).append('\t').append(o.kind()).append('\t').append(ms).append('\t')
                        .append(sql.length()).append('\t').append(snow.length() > 200 ? snow.substring(0, 200) : snow)
                        .append('\t').append(detail.length() > 300 ? detail.substring(0, 300) : detail).append('\n');
                if (o.kind().equals("CRASH") || o.kind().equals("DBERROR") || snow.startsWith("CRASH")) {
                    problems.add(e.getKey() + ": " + o.kind() + " " + detail + " / snowflake " + snow);
                }
                // the SQL grows with its island linearly: a hundredfold is a check or a guard copied per level
                if (sql.length() > 100L * e.getValue().length()) {
                    problems.add(e.getKey() + ": the DuckDB SQL is " + sql.length() + " characters for an island of "
                            + e.getValue().length());
                }
            }
        }
        System.out.println(report);
        String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (outputs != null) {
            Files.writeString(Path.of(outputs, "iq-scale.tsv"), report.toString(), StandardCharsets.UTF_8);
        }
        assertTrue(problems.isEmpty(), problems.size() + " problems:\n" + String.join("\n", problems));
    }

    /** The loop's lens: each IQ query of {@code -Diq.show.sql} (several split by {@code ;;}; default a three-way
     *  join) with its island, its DuckDB and Snowflake SQL ({@code -Diq.show.plans=false} leaves them out) and its
     *  outcome on DuckDB, under {@code -Diq.fuzz.profile}. */
    @Test
    void showsAQuerysPlans() throws Exception {
        // -Diq.show.file=<a file of them>, as a launcher quotes no SQL (run it with --strategy=TestRunner=standalone)
        String file = System.getProperty("iq.show.file");
        String sqls = file != null ? Files.readString(Path.of(file), StandardCharsets.UTF_8) : System.getProperty(
                "iq.show.sql", "SELECT x0.id, x2.it FROM t x0 JOIN t x1 ON x1.id = x0.id JOIN t x2 ON x2.id = x1.id"
                + " WHERE x0.id < 3 ORDER BY x0.id");
        boolean plans = !"false".equals(System.getProperty("iq.show.plans"));
        String profile = System.getProperty("iq.fuzz.profile", "iq17-default");
        List<SqlParity.Table> tables = IqParity.schema(Runfile.property("iq.parity.schema"));
        Set<String> names = IqParity.tableNames(tables);
        String model = IqParity.model(tables, PLATFORM.connectionBody());
        try (Connection session = PLATFORM.open()) {
            IqParity.seed(PLATFORM, session, tables);
            QueryService service = new QueryService();
            int q = 0;
            for (String sql : sqls.split(";;")) {
                String island = island(sql.strip(), profile, tables, names, 0, q);
                System.out.println("== " + sql.strip() + "\n-- island\n" + island);
                if (plans) {
                    // the island's Pure: a case with a setting is translated by the harness (ansinull's own On)
                    try {
                        IqParity.Case c = new IqParity.Case("lite/fuzz.yaml", "show" + q, sql.strip(),
                                List.of(profile), null, null, null, null, null, "query.ansinull=On", null);
                        System.out.println("-- pure\n" + IqParity.query(IqParity.runs(List.of(c)).get(0), tables,
                                names));
                    } catch (Exception | StackOverflowError x) {
                        System.out.println("-- pure: " + oneLine(x));
                    }
                }
                for (String[] lane : plans ? new String[][] {{"duckdb", PLATFORM.connectionBody()},
                        {"snowflake", SNOWFLAKE}, {"postgres", POSTGRES}} : new String[0][]) {
                    try {
                        var ctx = com.legend.Compiler.compileModel(IqParity.model(tables, lane[1]));
                        System.out.println("-- " + lane[0] + "\n" + com.legend.Compiler.query(ctx, island)
                                .plan(IqParity.RUNTIME).sql());
                    } catch (Exception | StackOverflowError x) {
                        System.out.println("-- " + lane[0] + ": " + oneLine(x));
                    }
                }
                Outcome o = run(service, session, model, sql.strip(), profile, tables, names, 0, q++);
                System.out.println("-- outcome: " + o.kind() + " " + (o.kind().equals("ROWS") ? o.rows() : o.reason()));
            }
        }
    }

    /** The methods a stack overflow repeats, the most frequent first: which layer recursed. */
    private static String frames(Throwable x) {
        Map<String, Integer> counts = new java.util.HashMap<>();
        for (StackTraceElement e : x.getStackTrace()) {
            String c = e.getClassName();
            counts.merge(c.substring(c.lastIndexOf('.') + 1) + "." + e.getMethodName(), 1, Integer::sum);
        }
        StringBuilder sb = new StringBuilder("[");
        counts.entrySet().stream().sorted((a, b) -> b.getValue() - a.getValue()).limit(8)
                .forEach(e -> sb.append(e.getKey()).append('x').append(e.getValue()).append(' '));
        return sb.append(']').toString();
    }

    private static String unionAll(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append(i == 0 ? "" : " UNION ALL ").append("SELECT id, ").append(i).append(" AS k FROM t WHERE id = ")
                    .append(1 + i % 11);
        }
        return sb.toString();
    }

    private static String joins(int n) {
        StringBuilder sb = new StringBuilder("SELECT x0.id, x" + (n - 1) + ".it FROM t x0");
        for (int i = 1; i < n; i++) {
            sb.append(" JOIN t x").append(i).append(" ON x").append(i).append(".id = x").append(i - 1).append(".id");
        }
        return sb.append(" WHERE x0.id < 3 ORDER BY x0.id").toString();
    }

    private static String nestedCase(int n) {
        String e = "id";
        for (int i = 0; i < n; i++) {
            e = "CASE WHEN id > " + (i % 7) + " THEN " + e + " ELSE " + i + " END";
        }
        return "SELECT " + e + " AS v FROM t WHERE id = 1";
    }

    private static String flatCase(int n) {
        StringBuilder sb = new StringBuilder("SELECT CASE");
        for (int i = 0; i < n; i++) {
            sb.append(" WHEN id = ").append(i).append(" THEN ").append(i * 2);
        }
        return sb.append(" ELSE -1 END AS v FROM t ORDER BY id").toString();
    }

    private static String numbers(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append(i == 0 ? "" : ", ").append(i * 3);
        }
        return sb.toString();
    }

    private static String selectItems(int n) {
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < n; i++) {
            sb.append(i == 0 ? "" : ", ").append("id + ").append(i).append(" AS c").append(i);
        }
        return sb.append(" FROM t WHERE id = 1").toString();
    }

    private static String andChain(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append(i == 0 ? "" : " AND ").append("id <> ").append(1000 + i);
        }
        return sb.toString();
    }

    private static String nestedDerived(int n) {
        String q = "SELECT id, it FROM t";
        for (int i = 0; i < n; i++) {
            q = "SELECT d" + i + ".id, d" + i + ".it + 1 AS it FROM (" + q + ") d" + i;
        }
        return q + " ORDER BY 1";
    }

    private static String cteChain(int n) {
        StringBuilder sb = new StringBuilder("WITH c0 AS (SELECT id, it FROM t)");
        for (int i = 1; i < n; i++) {
            sb.append(", c").append(i).append(" AS (SELECT id, it + 1 AS it FROM c").append(i - 1).append(")");
        }
        return sb.append(" SELECT id, it FROM c").append(n - 1).append(" ORDER BY id").toString();
    }

    /** The island of {@code sql}, under {@code profile}, as the parity lanes write it. */
    static String island(String sql, String profile, List<SqlParity.Table> tables, Set<String> names,
            long seed, int q) {
        IqParity.Case c = new IqParity.Case("lite/fuzz.yaml", "s" + seed + "_" + q, sql, List.of(profile), null, null,
                null, null);
        return IqParity.query(IqParity.runs(List.of(c)).get(0), tables, names);
    }

    private static Outcome run(QueryService service, Connection session, String model, String sql, String profile,
            List<SqlParity.Table> tables, Set<String> names, long seed, int q) {
        try {
            var result = service.execute(model, island(sql, profile, tables, names, seed, q), IqParity.RUNTIME,
                    session);
            List<String> rows = new ArrayList<>();
            for (List<Object> row : SqlParity.grid(result).rows()) {
                rows.add(row.toString());
            }
            return new Outcome("ROWS", rows, "");
        } catch (Exception | StackOverflowError e) {
            return classify(e);
        }
    }

    static Outcome classify(Throwable e) {
        boolean sql = false;
        for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof com.legend.error.NotImplementedException
                    || t instanceof com.legend.sql.dialect.DialectCapability) {
                return new Outcome("REFUSED", List.of(), oneLine(t));
            }
            if (t instanceof com.legend.error.DataError && t.getMessage() != null
                    && t.getMessage().matches("(?s)(Binder|Parser|Catalog|Not implemented|INTERNAL|Invalid Input)"
                    + " Error.*")) {
                // the database refused the SQL the island sent: lite's bug, or a limit of the database's own
                return new Outcome(t.getMessage().contains("depth") ? "DBLIMIT" : "DBERROR", List.of(), oneLine(t));
            }
            if (t instanceof com.legend.protocol.sql.SqlTranslationException
                    || t instanceof com.legend.error.IslandSyntaxException || t instanceof com.legend.error.DataError
                    || t.getClass().getSimpleName().equals("TypeInferenceException") && t.getMessage() != null
                    && t.getMessage().startsWith("#IQ")) {
                return new Outcome("TRANSLATED", List.of(), oneLine(t));
            }
            sql |= t instanceof java.sql.SQLException;
        }
        if (sql) {
            String m = oneLine(e).toLowerCase(java.util.Locale.ROOT);
            return new Outcome(m.contains("overflow") || m.contains("out of range") ? "OVERFLOW" : "DBERROR",
                    List.of(), oneLine(e));
        }
        return new Outcome("CRASH", List.of(), oneLine(e));
    }

    /** Snowflake's plan of an island DuckDB answered: null, or the crash (a refusal by name is no crash). */
    private static String snowflakeCrash(com.legend.compiler.element.ModelContext snowflake, String sql, String profile,
            List<SqlParity.Table> tables, Set<String> names, long seed, int q) {
        try {
            com.legend.Compiler.query(snowflake, island(sql, profile, tables, names, seed, q)).plan(IqParity.RUNTIME)
                    .sql();
            return null;
        } catch (Exception | StackOverflowError e) {
            Outcome o = classify(e);
            return o.kind().equals("CRASH") || o.kind().equals("DBERROR") ? o.reason() : null;
        }
    }

    /** A reason with its numbers and quoted text blanked, so that one rule's refusals count together. */
    private static String reason(String message) {
        String m = message.replaceAll("'[^']*'", "'_'").replaceAll("\"[^\"]*\"", "\"_\"").replaceAll("\\d+", "N");
        return m.length() > 160 ? m.substring(0, 160) : m;
    }

    private static List<String> sorted(List<String> rows) {
        List<String> out = new ArrayList<>(rows);
        java.util.Collections.sort(out);
        return out;
    }

    private static String oneLine(Throwable e) {
        String m = (e.getClass().getSimpleName() + ": " + e.getMessage()).replace('\n', ' ').replace('\t', ' ');
        return m.length() > 400 ? m.substring(0, 400) + "..." : m;
    }
}
