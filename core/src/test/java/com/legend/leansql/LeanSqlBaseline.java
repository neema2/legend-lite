// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.leansql;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.json.Json;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.StorelessRuntime;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * THE PRODUCT-SQL SIZE BASELINE (G3's guard, "a product-SQL size baseline ... shrink-only"; E3 of the total-wall
 * program, ruled by the orchestrator 2026-11-12, docs/LEAN_PRODUCT_SQL_DESIGN_2026_11_12.md §6): a FIXED set of
 * queries planned by the product's one planning entry ({@code TypedQuery.executionPlan}) on every database the
 * platform implements -- planning only, no database -- and measured per SET, DATABASE and FAMILY: the statements the
 * plans send, their characters, and the plans' own JSON characters.
 *
 * <p>Two measures, so a change cannot pass by moving bytes: SQL text moved into a bound value, plan data or a second
 * statement still grows the plan's JSON or the statement count. Pinned per FAMILY (the query's outermost function,
 * "let" for a query with a {@code let}, or the root's kind), so a gain in one family cannot pay for a loss in another.
 * The pins are {@code lean-sql/baseline.tsv}; {@link #drift} is the rule: growth fails, a shrink of more than
 * {@link #RESHRINK} asks for a re-pin (by {@code bazel run //core:update_lean_sql_baseline}, dated in the commit).
 */
public final class LeanSqlBaseline {

    private LeanSqlBaseline() {
    }

    /** A shrink smaller than this fraction of a pin stands without a re-pin; growth has no slack. */
    public static final double RESHRINK = 0.01;

    /** One pin's key and its three measures. */
    public record Key(String set, String database, String family) implements Comparable<Key> {
        @Override
        public int compareTo(Key o) {
            int c = set.compareTo(o.set);
            c = c != 0 ? c : database.compareTo(o.database);
            return c != 0 ? c : family.compareTo(o.family);
        }
    }

    /** {@code statements} sent, their {@code chars}, the plans' JSON characters ({@code plan}), their {@code
     *  subselects} and their {@code repeated} characters ({@link #shape}; the lean program, the user's definition of
     *  done of 2026-11-15: "subselects only when need, repetition only when need"; -1 where a pin predates them); the
     *  worst case. */
    public record Measure(long statements, long chars, long plan, long subselects, long repeated, long refused,
            String worst) {
        public Measure(long statements, long chars, long plan, String worst) {
            this(statements, chars, plan, 0, 0, 0, worst);
        }

        public Measure(long statements, long chars, long plan, long subselects, long repeated, String worst) {
            this(statements, chars, plan, subselects, repeated, 0, worst);
        }

        Measure plus(long s, long c, long p, long[] shape, String caseText, long caseChars, long worstChars) {
            return new Measure(statements + s, chars + c, plan + p, subselects + shape[0], repeated + shape[1],
                    refused, caseChars > worstChars ? caseText : worst);
        }

        /** This measure with one more query refused at planning (the lean program, 2026-11-15: a refused query sends
         *  nothing, so without its count it would read as a family's SQL shrinking). */
        public Measure refusedOne(String caseText) {
            return new Measure(statements, chars, plan, subselects, repeated, refused + 1,
                    worst.isEmpty() ? caseText : worst);
        }

        /** This measure under another worst case (a read pin carries none). */
        public Measure withWorst(String w) {
            return new Measure(statements, chars, plan, subselects, repeated, refused, w);
        }

        /** This measure with {@code statements}' own added (the corpus's: no plan JSON of its own). */
        public Measure plus(List<String> statements, long plan, String test) {
            long c = 0;
            long[] sh = new long[2];
            for (String st : statements) {
                c += st.length();
                long[] one = shape(st);
                sh[0] += one[0];
                sh[1] += one[1];
            }
            return new Measure(this.statements + statements.size(), chars + c, this.plan + plan,
                    subselects + sh[0], repeated + sh[1], refused, worst.isEmpty() ? test : worst);
        }
    }

    /** The smallest group counted a repeat (characters, the parentheses included): an operand under this costs less
     *  written again than bound (the design's §5.1, "trivial operands"). */
    static final int REPEAT_MIN = 16;

    /**
     * {@code statement}'s shape, as the database reads it: [0] its SUBSELECTS -- each parenthesised {@code SELECT} or
     * {@code WITH} (a derived table, a scalar or EXISTS subquery, a CTE's body) -- and [1] its REPEATED characters:
     * every parenthesised group of at least {@link #REPEAT_MIN} characters whose exact text is written more than once
     * in the statement counts each copy past the first, and a group inside a repeated group is not counted again (the
     * outermost repeat is the one a binding removes). Read from the text the plan sends, so a repeat moved into plan
     * data or a second statement still counts there.
     */
    public static long[] shape(String statement) {
        long subselects = 0;
        java.util.regex.Matcher m = SUBSELECT.matcher(statement);
        while (m.find()) {
            subselects++;
        }
        // every balanced group, by its text; quotes skipped so a parenthesis in a literal is no group
        List<int[]> groups = new ArrayList<>();
        java.util.ArrayDeque<Integer> open = new java.util.ArrayDeque<>();
        char quote = 0;
        for (int i = 0; i < statement.length(); i++) {
            char ch = statement.charAt(i);
            if (quote != 0) {
                if (ch == quote) {
                    quote = 0;
                }
            } else if (ch == '\'' || ch == '"') {
                quote = ch;
            } else if (ch == '(') {
                open.push(i);
            } else if (ch == ')' && !open.isEmpty()) {
                int start = open.pop();
                if (i + 1 - start >= REPEAT_MIN) {
                    groups.add(new int[] {start, i + 1});
                }
            }
        }
        Map<String, Integer> copies = new java.util.HashMap<>();
        for (int[] g : groups) {
            copies.merge(statement.substring(g[0], g[1]), 1, Integer::sum);
        }
        // outermost first: a repeated group's own groups are not counted again
        groups.sort((a, b) -> a[0] != b[0] ? Integer.compare(a[0], b[0]) : Integer.compare(b[1], a[1]));
        long repeated = 0;
        java.util.Set<String> seen = new java.util.HashSet<>();
        int coveredTo = -1;
        for (int[] g : groups) {
            if (g[1] <= coveredTo) {
                continue;
            }
            String text = statement.substring(g[0], g[1]);
            if (copies.get(text) > 1) {
                if (!seen.add(text)) {
                    repeated += text.length();
                }
                coveredTo = g[1];
            }
        }
        return new long[] {subselects, repeated};
    }

    private static final java.util.regex.Pattern SUBSELECT = java.util.regex.Pattern.compile(
            "\\(\\s*(?:SELECT|WITH)\\b", java.util.regex.Pattern.CASE_INSENSITIVE);

    /** Every set planned on every database the platform implements whose storeless runtime is relational; a case a
     *  database refuses at planning counts as such ({@code refused}, its own family row), never as nothing. */
    public static Map<Key, Measure> measure(Map<String, Path> sets) throws IOException {
        return measure(sets, type -> true);
    }

    /** {@link #measure(Map)} on the databases {@code which} admits. */
    public static Map<Key, Measure> measure(Map<String, Path> sets,
            java.util.function.Predicate<DatabaseType> which) throws IOException {
        Map<Key, Measure> out = new TreeMap<>();
        Map<Key, Long> worst = new TreeMap<>();
        for (var set : sets.entrySet()) {
            List<String> lines = Files.readAllLines(set.getValue(), StandardCharsets.UTF_8);
            for (DatabaseType type : com.legend.database.Databases.implemented()) {
                if (!relational(type) || !which.test(type)) {
                    continue;
                }
                // each model compiled once per database, as measureOwn holds one per model (PR #118 fix round,
                // 2026-11-18: the set's 2,451 queries share 43 models, and compiling the model for every query on every
                // database was most of this test's time -- 300 s on CI's Windows runner); a model that does not compile is
                // compiled, and refused, again for each of its queries
                Map<String, com.legend.compiler.element.ModelContext> compiled = new java.util.HashMap<>();
                for (String line : lines) {
                    if (line.isBlank()) {
                        continue;
                    }
                    Json.Obj c = Json.parseObject(line);
                    String model = ((Json.Str) c.get("model")).value();
                    String expression = ((Json.Str) c.get("expression")).value();
                    String family = "refused";
                    long s = 0;
                    long ch = 0;
                    long p = 0;
                    long[] sh = new long[2];
                    boolean refused = false;
                    try {
                        TypedQuery q = Compiler.query(compiled.computeIfAbsent(model,
                                m -> Compiler.compileModel(withRuntime(m, type))), expression);
                        family = family(q);
                        ExecutionPlan plan = q.executionPlan(StorelessRuntime.RUNTIME,
                                Set.of(com.legend.platform.Feature.CORRECT_SQL_SUBSTRING_INDEXING));
                        List<String> statements = new ArrayList<>();
                        collect(plan.root(), statements);
                        s = statements.size();
                        for (String st : statements) {
                            ch += st.length();
                            add(sh, shape(st));
                        }
                        p = PlanJson.write(plan).length();
                    } catch (RuntimeException e) {
                        if (!isRefusal(e)) {
                            throw e;
                        }
                        // refused at planning: counted under its family where it has one (a query refused by the
                        // compiler itself has none: "refused")
                        refused = true;
                    }
                    Key k = new Key(set.getKey(), type.name(), family);
                    long prior = worst.getOrDefault(k, -1L);
                    Measure was = out.getOrDefault(k, new Measure(0, 0, 0, ""));
                    out.put(k, refused ? was.refusedOne(expression) : was.plus(s, ch, p, sh, expression, ch, prior));
                    if (ch > prior) {
                        worst.put(k, ch);
                    }
                }
            }
        }
        return out;
    }

    /** A set of product queries each planned on its OWN runtime ({@code {"model", "query", "runtime"}}, captured by
     *  the test harness's {@code QueryCaptureListener}, {@code -Dlegend.capture.queries=FILE}: core's): measured on the database each plan targets, under
     *  the set's name. */
    public static Map<Key, Measure> measureOwn(String set, Path file) throws IOException {
        Map<Key, Measure> out = new TreeMap<>();
        Map<Key, Long> worst = new TreeMap<>();
        // each model once ({"model_id", "model"}), then the queries over it ({"model_id", "query", "runtime"}); each
        // model compiled once, as the run that captured them compiled it
        Map<Long, String> models = new java.util.HashMap<>();
        Map<Long, com.legend.compiler.element.ModelContext> compiled = new java.util.HashMap<>();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            Json.Obj c = Json.parseObject(line);
            long id = ((Json.Num) c.get("model_id")).longValue();
            if (c.has("model")) {
                models.put(id, ((Json.Str) c.get("model")).value());
                continue;
            }
            String query = ((Json.Str) c.get("query")).value();
            String runtime = ((Json.Str) c.get("runtime")).value();
            String family;
            String database;
            long s = 0;
            long ch = 0;
            long p = 0;
            long[] sh = new long[2];
            try {
                com.legend.compiler.element.ModelContext ctx = compiled.get(id);
                if (ctx == null) {
                    ctx = Compiler.compileModel(models.get(id));
                    compiled.put(id, ctx);
                }
                TypedQuery q = Compiler.query(ctx, query);
                family = family(q);
                ExecutionPlan plan = q.executionPlan(runtime.isEmpty() ? null : runtime);
                List<String> statements = new ArrayList<>();
                collect(plan.root(), statements);
                s = statements.size();
                for (String st : statements) {
                    ch += st.length();
                    add(sh, shape(st));
                }
                p = PlanJson.write(plan).length();
                database = plan.answers() && plan.sendsTheAnswer()
                        ? plan.answerSql().target().database().type().name() : "constant";
            } catch (RuntimeException e) {
                        if (!isRefusal(e)) {
                            throw e;
                        }
                // the database a plan targets is the plan's: a refused query has none, so it counts under "refused"
                family = "refused";
                database = "refused";
            }
            Key k = new Key(set, database, family);
            long prior = worst.getOrDefault(k, -1L);
            Measure was = out.getOrDefault(k, new Measure(0, 0, 0, ""));
            out.put(k, family.equals("refused") ? was.refusedOne(query) : was.plus(s, ch, p, sh, query, ch, prior));
            if (ch > prior) {
                worst.put(k, ch);
            }
        }
        return out;
    }

    private static void add(long[] into, long[] shape) {
        into[0] += shape[0];
        into[1] += shape[1];
    }

    /** Whether {@code type} is a relational database: MongoDB's sessions are its connector's own and its statement a
     *  pipeline, measured separately (the orchestrator, 2026-11-12). */
    static boolean relational(DatabaseType type) {
        return !com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(type)).ownSessions();
    }

    /**
     * Whether {@code e} is a PLANNING REFUSAL, the named kinds the planner refuses a query by: a compile error
     * ({@link com.legend.error.LegendCompileException}), a construct not planned
     * ({@link com.legend.error.NotImplementedException}), a dialect's capability gap
     * ({@link com.legend.sql.DialectCapability}) or a render arm's unsupported variant
     * ({@link UnsupportedOperationException}). A measure counts only these as refused; any other throwable is a
     * defect and fails the measure with its stack (G7, round 55f, 2026-11-15: a measurement bug's
     * IllegalArgumentException was counted as two corpus bodies' refusals).
     */
    public static boolean isRefusal(Throwable e) {
        return e instanceof com.legend.error.LegendCompileException
                || e instanceof com.legend.error.NotImplementedException
                || e instanceof com.legend.sql.DialectCapability || isRenderArmRefusal(e);
    }

    /** A dialect's render arm that cannot express a variant throws {@code UnsupportedOperationException} (AGENTS.md
     *  invariant 3): that, thrown FROM a dialect, is a refusal; the same class thrown anywhere else (an immutable
     *  collection mutated while planning) is a defect, never counted as refused (the PR1 re-review's B7, 2026-11-16). */
    public static boolean isRenderArmRefusal(Throwable e) {
        if (e.getClass() != UnsupportedOperationException.class) {
            return false;
        }
        StackTraceElement[] at = e.getStackTrace();
        return at.length > 0 && at[0].getClassName().startsWith("com.legend.sql.dialect.");
    }

    /** {@code model} with the storeless runtime on {@code type}: its in-process declaration where the connector has
     *  one, else a server's coordinates (planning opens nothing). */
    static String withRuntime(String model, DatabaseType type) {
        try {
            return StorelessRuntime.with(model, type);
        } catch (RuntimeException e) {
            return StorelessRuntime.onServer(model, type, "localhost", 1, "legend");
        }
    }

    /** The query's family: "let" where its body binds one; else its root's function (a native's or a user
     *  function's id), or the root's kind. */
    static String family(TypedQuery q) {
        List<TypedSpec> body = q.body();
        for (TypedSpec s : body) {
            if (s instanceof TypedLet) {
                return "let";
            }
        }
        TypedSpec root = body.get(body.size() - 1);
        return switch (root) {
            case TypedNativeCall nc -> nc.callee().id().qualified();
            case TypedUserCall uc -> "user";
            default -> root.getClass().getSimpleName();
        };
    }

    /** Every statement {@code node} sends: the query's own, never a target's loads or session statements. */
    public static void statements(ExecutionPlan.Node node, List<String> out) {
        collect(node, out);
    }

    /** Every statement the plan's nodes send: the query's own, never a target's loads or session statements. */
    static void collect(ExecutionPlan.Node node, List<String> out) {
        switch (node) {
            case ExecutionPlan.Sequence s -> s.steps().forEach(step -> collect(step, out));
            case ExecutionPlan.TdsResult t -> out.add(t.sql().statement());
            case ExecutionPlan.Keyed k -> {
                out.add(k.keys().statement());
                out.add(k.body().statement());
            }
            case ExecutionPlan.Update u -> out.add(u.sql().statement());
            case ExecutionPlan.Effect e -> out.add(e.sql().statement());
            case ExecutionPlan.Check c -> out.add(c.sql().statement());
            case ExecutionPlan.Load l -> {
            }
            case ExecutionPlan.Constant c -> {
            }
        }
    }

    /** The drift of {@code measured} from {@code pinned}, one line each: a measure that grew (any amount), a pin that
     *  shrank by more than {@link #RESHRINK} (re-pin), a key new or gone. Empty when the baseline holds. */
    public static List<String> drift(Map<Key, Measure> pinned, Map<Key, Measure> measured) {
        List<String> out = new ArrayList<>();
        for (var e : measured.entrySet()) {
            Measure pin = pinned.get(e.getKey());
            Measure now = e.getValue();
            if (pin == null) {
                out.add(e.getKey() + ": new (" + now + ") -- re-pin");
                continue;
            }
            check(out, e.getKey(), "statements", pin.statements(), now.statements(), now.worst());
            check(out, e.getKey(), "chars", pin.chars(), now.chars(), now.worst());
            check(out, e.getKey(), "plan JSON chars", pin.plan(), now.plan(), now.worst());
            // a pin that predates the shape measures (-1) holds none of them
            if (pin.subselects() >= 0) {
                check(out, e.getKey(), "subselects", pin.subselects(), now.subselects(), now.worst());
            }
            if (pin.repeated() >= 0) {
                check(out, e.getKey(), "repeated chars", pin.repeated(), now.repeated(), now.worst());
            }
            if (pin.refused() >= 0) {
                check(out, e.getKey(), "queries refused", pin.refused(), now.refused(), now.worst());
            }
        }
        for (Key k : pinned.keySet()) {
            if (!measured.containsKey(k)) {
                out.add(k + ": pinned and gone -- re-pin");
            }
        }
        return out;
    }

    private static void check(List<String> out, Key k, String what, long pin, long now, String worst) {
        if (now > pin) {
            out.add(k + ": " + what + " GREW " + pin + " -> " + now + " (lean SQL is shrink-only, G3); its largest"
                    + " case: " + (worst.length() > 160 ? worst.substring(0, 160) + "..." : worst));
        } else if (now < pin * (1 - RESHRINK)) {
            out.add(k + ": " + what + " shrank " + pin + " -> " + now + " -- re-pin (bazel run"
                    + " //core:update_lean_sql_baseline), dated in the commit");
        }
    }

    private static final String HEADER = "set\tdatabase\tfamily\tstatements\tchars\tplan\tsubselects\trepeated\trefused";

    /** The pins as written: {@code set, database, family, statements, chars, plan}, sorted. */
    public static String write(Map<Key, Measure> pins) {
        StringBuilder b = new StringBuilder("# THE PRODUCT-SQL SIZE BASELINE (LeanSqlBaseline): generated by bazel run"
                + " //core:update_lean_sql_baseline; a change is dated in its commit\n"
                + HEADER + "\n");
        for (var e : new TreeMap<>(pins).entrySet()) {
            Key k = e.getKey();
            Measure m = e.getValue();
            b.append(k.set()).append('\t').append(k.database()).append('\t').append(k.family()).append('\t')
                    .append(m.statements()).append('\t').append(m.chars()).append('\t').append(m.plan()).append('\t')
                    .append(m.subselects()).append('\t').append(m.repeated()).append('\t').append(m.refused())
                    .append('\n');
        }
        return b.toString();
    }

    /** The pins {@link #write} wrote. */
    public static Map<Key, Measure> read(String tsv) {
        Map<Key, Measure> out = new TreeMap<>();
        for (String line : tsv.split("\n")) {
            // the header, as written since the shape measures (2026-11-15) or before them
            if (line.isBlank() || line.startsWith("#") || line.startsWith("set\tdatabase\tfamily\t")) {
                continue;
            }
            String[] f = line.split("\t", -1);
            if (f.length != 6 && f.length != 8 && f.length != 9) {
                throw new IllegalArgumentException("lean-sql/baseline.tsv: a line is set, database, family,"
                        + " statements, chars, plan[, subselects, repeated[, refused]]: '" + line + "'");
            }
            out.put(new Key(f[0], f[1], f[2]),
                    new Measure(Long.parseLong(f[3]), Long.parseLong(f[4]), Long.parseLong(f[5]),
                            f.length > 6 ? Long.parseLong(f[6]) : -1, f.length > 7 ? Long.parseLong(f[7]) : -1,
                            f.length > 8 ? Long.parseLong(f[8]) : -1, ""));
        }
        return out;
    }

    /** {@code bazel run //core:update_lean_sql_baseline}: the pins measured again and written over the source tree's
     *  {@code core/src/test/resources/lean-sql/baseline.tsv}; the totals printed per database. */
    public static void main(String[] args) throws IOException {
        String workspace = System.getenv("BUILD_WORKSPACE_DIRECTORY");
        if (workspace == null) {
            throw new IllegalStateException("run it with bazel run //core:update_lean_sql_baseline");
        }
        Path dir = Path.of(workspace, "core/src/test/resources/lean-sql");
        Map<Key, Measure> pins = new TreeMap<>(measure(Map.of("channel_a", dir.resolve("channel_a.jsonl"))));
        pins.putAll(measureOwn("core", dir.resolve("core.jsonl")));
        Files.writeString(dir.resolve("baseline.tsv"), write(pins), StandardCharsets.UTF_8);
        Map<String, long[]> totals = new TreeMap<>();
        pins.forEach((k, m) -> {
            long[] t = totals.computeIfAbsent(k.set() + " " + k.database(), x -> new long[3]);
            t[0] += m.statements();
            t[1] += m.chars();
            t[2] += m.plan();
        });
        totals.forEach((k, t) -> System.out.println(k + ": statements " + t[0] + ", chars " + t[1] + ", plan JSON "
                + t[2]));
    }
}
