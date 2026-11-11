// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.base.Nullable;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.Literals;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPlannedValue;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTestDataGen;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.NotImplementedException;
import com.legend.sql.SqlQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE TEST-DATA GENERATOR'S READS SPLICED (B2d.2 of the total-wall program, 2026-11-11; the design the user approved on
 * 2026-11-10, docs/data-delivery-homework/TEST_DATA_GENERATOR_DESIGN_2026_11_10.md): a body's
 * {@code generateTestData(...)}, planned by the test road's {@link Generator} (the generator's planning library, which
 * the compiler does not reach -- the {@link ResultEnvelopeSplice.Frames} pattern: the compiler owns what a splice means,
 * the planner what the value is), and its result's reads made the values they are:
 * <ul>
 *   <li>{@code $r.sqls}: the fetches' texts, a literal list (their text differs from upstream's, which names temporary
 *       tables: the user's ruling of 2026-11-10, a register row);</li>
 *   <li>{@code $r.dataCsvString}: the planned statement's one cell ({@link TypedPlannedValue}), read where the value
 *       stands -- a statement the run sends, the database's work.</li>
 * </ul>
 * The let that bound the generator is spliced away: a read let is evaluated where it is read (lite's let rule), so the
 * generator's statement is sent where its text is read, and a body reading only its {@code sqls} sends none. A read of
 * the result other than those two is refused by name; a carrier of another flavour stays, and the body's class rule
 * refuses it by name. The loads of a generated text compared with a constant text's are the database's comparison of
 * their data ({@link #compared}). Typed nodes are minted here, in the compiler's layer (AGENTS invariant 7).
 */
public final class GeneratorSplice {

    private GeneratorSplice() {
    }

    /** The properties of upstream's {@code TestDataGenResult} this splice answers. */
    private static final String SQLS = "sqls";
    private static final String CSV = "dataCsvString";

    /** The generator's planning, the test road's to supply. */
    public interface Generator {
        /** {@code g} planned: its fetches' texts and the statement answering its test-data text. */
        Generated plan(TypedTestDataGen g);

        /** Whether {@code generated}'s data is the data of the test-data text {@code expected}, on the tables of the
         *  database {@code dbFqn}: each table compared as a set of rows, the database's work. */
        Same same(Generated generated, String expected, String dbFqn);

        /** Every table of the database {@code dbFqn} as {@code generated}'s slice reloaded holds it, by physical name:
         *  the slice's rows, or none. */
        Map<String, SqlQuery> slice(Generated generated, String dbFqn);
    }

    /** Whether a generated slice holds an expected text's data: decided at planning ({@link Holds}, {@link Fails}),
     *  or a statement the database answers in one Boolean cell ({@link Compared}). */
    public sealed interface Same {
        record Holds() implements Same {
        }

        record Fails(String why) implements Same {
        }

        record Compared(SqlQuery statement) implements Same {
        }
    }

    /** One generator planned: its fetches' texts, written in the body's dialect, and the read-only statement whose one
     *  cell is its test-data text. */
    public interface Generated {
        List<String> sqls();

        SqlQuery csv();
    }

    /** Whether {@code body} holds a generator this splice plans ({@code generateTestData}). */
    public static boolean holdsAGenerator(List<TypedSpec> body) {
        for (TypedSpec s : body) {
            if (holds(s)) {
                return true;
            }
        }
        return false;
    }

    private static boolean holds(TypedSpec n) {
        if (n instanceof TypedTestDataGen g && "generate".equals(g.flavor())) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (holds(c)) {
                return true;
            }
        }
        return false;
    }

    /** {@code body} with each generator it binds planned by {@code generator}, its reads spliced, and the loads of its
     *  text compared with another text's made the database's comparison ({@link #compared}). */
    public static List<TypedSpec> spliced(List<TypedSpec> body, ModelContext ctx, Generator generator) {
        Map<String, Generated> bound = new HashMap<>();
        // each generated text read, by its node: the comparisons below know the generator's text by it
        Map<TypedPlannedValue, Generated> texts = new IdentityHashMap<>();
        List<TypedSpec> out = new ArrayList<>(body.size());
        for (TypedSpec statement : body) {
            TypedSpec s = substituted(statement, bound, generator, texts);
            if (s instanceof TypedLet let) {
                // a rebinding ends the generator's binding (its own value read the earlier one above)
                bound.remove(let.name());
                TypedTestDataGen g = generator(let.value());
                if (g != null) {
                    bound.put(let.name(), generator.plan(g));
                    continue;
                }
            }
            out.add(s);
        }
        return compared(out, ctx, generator, texts);
    }

    // ===== the loads of a generated text, compared (B2d.2: assertTestData) =====

    /** {@code setUpDataSQLs(data: String[1], db: Database[*])}: a test-data text's load statements on a database
     *  (upstream's {@code toDDL.pure}). */
    private static final com.legend.model.FunctionId SET_UP_DATA_SQLS = com.legend.model.FunctionId.of(
            com.legend.builtin.Pure.SET_UP_DATA_SQLS__STRING_1__DATABASE_MANY);
    /** {@code assert(condition: Boolean[1], message: String[1])}. */
    private static final com.legend.model.FunctionId ASSERT = com.legend.model.FunctionId.of(
            com.legend.builtin.Pure.ASSERT__BOOLEAN_1__STRING_1);

    /** The loads {@code setUpDataSQLs(text, db)} of a text: the generator's ({@code generated}) or a constant
     *  ({@code constant}), on the database {@code db}. */
    private record Loads(@Nullable Generated generated, @Nullable String constant, String db) {
    }

    /**
     * {@code body} with each {@code assertSameElements} over the loads of two test-data texts, one the generator's,
     * made the database's comparison of the data they denote: {@code assert(<the comparison's cell>, message)}. The
     * general rule, never a helper's name: upstream's {@code assertTestData} compares the two load-statement lists
     * {@code setUpDataSQLs} writes, and a generated text's statements are a run's values the plan cannot hold (the
     * texts would be SQL built from rows read, S133's hole), so the comparison is of the data, each table as a set of
     * rows (the user's ruling, 2026-11-10: "upstream's comparison by other means"). The loads' lets the comparison
     * consumed are spliced away; a generated text's loads read any other way are refused by name.
     */
    private static List<TypedSpec> compared(List<TypedSpec> body, ModelContext ctx, Generator generator,
            Map<TypedPlannedValue, Generated> texts) {
        if (texts.isEmpty()) {
            return List.copyOf(body);
        }
        List<TypedSpec> out = new ArrayList<>(body.size());
        java.util.Set<String> consumed = new java.util.HashSet<>();
        // the slice a reload made the database's tables (a read of them after it reads it), until a write
        Map<String, SqlQuery> reloaded = null;
        boolean ran = false;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            List<TypedSpec> before = out;
            Loads reload = reload(statement, before, texts);
            if (reload != null) {
                // the slice reloaded (B2d.2, the user's ruling of 2026-11-11: "Read only reload is fine"): nothing is
                // written; the query run after it is planned again over the slice's rows
                reloaded = generator.slice(java.util.Objects.requireNonNull(reload.generated()), reload.db());
                ran = false;
                TypedSpec source = ((com.legend.compiler.spec.typed.TypedMap) statement).source();
                if (source instanceof TypedVariable v) {
                    consumed.add(v.name());
                }
                continue;
            }
            if (reloaded != null) {
                TypedSpec swapped = !ran && statement instanceof TypedLet let ? overSlice(let, before, reloaded,
                        consumed) : null;
                if (swapped != null) {
                    ran = true;
                    out.add(swapped);
                    continue;
                }
                if (writes(statement)) {
                    reloaded = null;
                } else if (!planBuild(statement) && readsTheDatabase(statement)) {
                    throw new NotImplementedException("a read of the database after the test-data generator's slice"
                            + " was reloaded, other than the query run over it (" + (ran ? "after" : "before")
                            + " that query): not planned (B2d's read-only reload swaps the tables of that query alone)");
                }
            }
            if (statement instanceof TypedNativeCall call && com.legend.builtin.NativeFn.Verdict.of(call.callee().id())
                    .orElse(null) == com.legend.builtin.NativeFn.Verdict.ASSERT_SAME_ELEMENTS
                    && (call.args().size() == 2 || call.args().size() == 3)) {
                Loads a = loads(call.args().get(0), before, texts);
                Loads b = loads(call.args().get(1), before, texts);
                if (a != null && b != null && (a.generated() != null || b.generated() != null)) {
                    String message = call.args().size() == 3 && Literals.fold(call.args().get(2), before)
                            instanceof String m ? m : null;
                    out.add(comparison(a, b, message, ctx, generator));
                    for (TypedSpec arg : call.args().subList(0, 2)) {
                        if (arg instanceof TypedVariable v) {
                            consumed.add(v.name());
                        }
                    }
                    continue;
                }
            }
            out.add(statement);
        }
        // the loads' lets the comparisons consumed, read no more: spliced away (a text's loads are values of the text,
        // nothing sent); a generated text's loads left anywhere are refused by name
        List<TypedSpec> kept = new ArrayList<>(out.size());
        for (int i = 0; i < out.size(); i++) {
            TypedSpec statement = out.get(i);
            if (statement instanceof TypedLet let && consumed.contains(let.name())
                    && !com.legend.compiler.spec.typed.FreeVars.ofBodyReadingTrees(out.subList(i + 1, out.size()))
                            .contains(let.name())) {
                continue;
            }
            kept.add(statement);
        }
        for (int i = 0; i < kept.size(); i++) {
            refuseGeneratedLoads(kept.get(i), kept.subList(0, i), texts);
        }
        return List.copyOf(kept);
    }

    // ===== the slice reloaded, read only (B2d.2: loadAndTestExecution) =====

    /** {@code statement}'s generated loads if it RELOADS them -- each of the generator's load statements executed on the
     *  database ({@code loads->map(sql | executeInDb($sql, connection))}, upstream's {@code loadAndTestExecution}) --
     *  or null. */
    private static @Nullable Loads reload(TypedSpec statement, List<TypedSpec> before,
            Map<TypedPlannedValue, Generated> texts) {
        if (!(statement instanceof com.legend.compiler.spec.typed.TypedMap map)
                || map.mapper().parameters().size() != 1 || map.mapper().body().size() != 1
                || !(map.mapper().body().get(0) instanceof TypedNativeCall exec)
                || com.legend.builtin.NativeFn.Effect.of(exec.callee().id()).orElse(null)
                        != com.legend.builtin.NativeFn.Effect.EXECUTE_IN_DB
                || exec.args().isEmpty() || !(exec.args().get(0) instanceof TypedVariable sql)
                || !sql.name().equals(map.mapper().parameters().get(0))) {
            return null;
        }
        Loads l = loads(map.source(), before, texts);
        return l != null && l.generated() != null ? l : null;
    }

    /**
     * {@code let} -- a frame, {@code execute(query, mapping, runtime, ...)} or an execution plan's
     * {@code execute(executionPlan(query, ...), ...)} -- with its query read over {@code slice}: the query lambda's
     * last statement wrapped in a {@link com.legend.compiler.spec.typed.TypedTableSwap} (the query planned again over
     * the slice's rows, read only). The plan's build, where a let binds it, read in place (its let consumed). Null
     * where {@code let} is no frame.
     */
    private static @Nullable TypedSpec overSlice(TypedLet let, List<TypedSpec> before, Map<String, SqlQuery> slice,
            java.util.Set<String> consumed) {
        TypedSpec rhs = let.value();
        if (!(rhs instanceof TypedNativeCall frame
                && com.legend.builtin.NativeFn.Handle.runsAFrame(frame.callee().id()))) {
            return null;
        }
        com.legend.builtin.NativeFn.Handle h = com.legend.builtin.NativeFn.Handle.of(frame.callee().id())
                .orElseThrow();
        List<TypedSpec> args = new ArrayList<>(frame.args());
        switch (h) {
            case EXECUTE -> args.set(0, swappedQuery(args.get(0), before, slice));
            case EXECUTION_PLAN_EXECUTE -> {
                TypedSpec build = com.legend.compiler.spec.typed.Lets.bound(args.get(0), before);
                if (!(build instanceof TypedNativeCall b && com.legend.builtin.NativeFn.Handle.of(b.callee().id())
                        .orElse(null) == com.legend.builtin.NativeFn.Handle.EXECUTION_PLAN)) {
                    throw new NotImplementedException("the query run over the test-data generator's reloaded slice: an"
                            + " execution plan that does not trace to its executionPlan(...) build: not planned (B2d)");
                }
                List<TypedSpec> bargs = new ArrayList<>(b.args());
                bargs.set(0, swappedQuery(bargs.get(0), before, slice));
                if (args.get(0) instanceof TypedVariable v) {
                    consumed.add(v.name());
                }
                args.set(0, b.withChildren(bargs));
            }
            default -> throw new NotImplementedException("the query run over the test-data generator's reloaded slice"
                    + " by " + frame.callee().qualifiedName() + ": not planned (B2d plans execute and an execution"
                    + " plan's execute)");
        }
        return new TypedLet(let.name(), frame.withChildren(args), let.info());
    }

    /** The query {@code q} (a lambda, or a let's) with its last statement read over {@code slice}. */
    private static TypedSpec swappedQuery(TypedSpec q, List<TypedSpec> before, Map<String, SqlQuery> slice) {
        if (!(com.legend.compiler.spec.typed.Lets.bound(q, before) instanceof TypedLambda lam)
                || !lam.parameters().isEmpty() || lam.body().isEmpty()) {
            throw new NotImplementedException("the query run over the test-data generator's reloaded slice: no"
                    + " parameterless query lambda: not planned (B2d)");
        }
        TypedSpec last = lam.body().get(lam.body().size() - 1);
        if (!Type.isRelation(last.info().type())
                && !com.legend.compiler.element.type.PlatformTypes.isTdsType(last.info().type())) {
            throw new NotImplementedException("the query run over the test-data generator's reloaded slice answers "
                    + last.info().type().typeName() + ", not a relation: not planned (B2d swaps a relation's tables)");
        }
        List<TypedSpec> body = new ArrayList<>(lam.body());
        body.set(body.size() - 1, new com.legend.compiler.spec.typed.TypedTableSwap(last, slice, last.info()));
        return lam.withChildren(body);
    }

    /** Whether {@code statement} writes the database (a database effect at its root, or a user call, whose effects
     *  the splice does not see): after it the reloaded slice is no longer what its tables hold. */
    private static boolean writes(TypedSpec statement) {
        TypedSpec v = statement instanceof TypedLet let ? let.value() : statement;
        return v instanceof com.legend.compiler.spec.typed.TypedUserCall
                || v instanceof TypedNativeCall call && com.legend.builtin.NativeFn.Effect.isDbEffect(call.callee().id())
                || v instanceof com.legend.compiler.spec.typed.TypedMap m && m.mapper().body().size() == 1
                        && m.mapper().body().get(0) instanceof TypedNativeCall c
                        && com.legend.builtin.NativeFn.Effect.isDbEffect(c.callee().id());
    }

    /** Whether {@code statement} binds an execution plan's build ({@code let p = executionPlan(query, ...)}): it reads
     *  nothing at its turn (the frame that executes it does). */
    private static boolean planBuild(TypedSpec statement) {
        return statement instanceof TypedLet let && let.value() instanceof TypedNativeCall b
                && com.legend.builtin.NativeFn.Handle.of(b.callee().id()).orElse(null)
                        == com.legend.builtin.NativeFn.Handle.EXECUTION_PLAN;
    }

    /** Whether {@code n} reads the database: a class's extent, a table, a from(), a frame, a raw read, a generator. */
    private static boolean readsTheDatabase(TypedSpec n) {
        if (n instanceof com.legend.compiler.spec.typed.TypedGetAll
                || n instanceof com.legend.compiler.spec.typed.TypedTableReference
                || n instanceof com.legend.compiler.spec.typed.TypedFrom
                || n instanceof com.legend.compiler.spec.typed.TypedRawSqlRelation
                || n instanceof TypedTestDataGen || n instanceof TypedPlannedValue
                || n instanceof TypedNativeCall call && com.legend.builtin.NativeFn.Handle.runsAFrame(call.callee().id())) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (readsTheDatabase(c)) {
                return true;
            }
        }
        return false;
    }

    /** {@code e}'s loads ({@code setUpDataSQLs(text, db)}, in place or a let's) of the generator's text or a constant
     *  one on a database the statement names, or null. */
    private static @Nullable Loads loads(TypedSpec e, List<TypedSpec> before, Map<TypedPlannedValue, Generated> texts) {
        TypedSpec v = com.legend.compiler.spec.typed.Lets.bound(e, before);
        if (!(v instanceof TypedNativeCall call && SET_UP_DATA_SQLS.equals(call.callee().id()))) {
            return null;
        }
        TypedSpec db = com.legend.compiler.spec.typed.Lets.bound(call.args().get(1), before);
        if (!(db instanceof com.legend.compiler.spec.typed.TypedPackageableRef ref)) {
            return null;
        }
        TypedSpec text = com.legend.compiler.spec.typed.Lets.bound(call.args().get(0), before);
        if (text instanceof TypedPlannedValue pv && texts.containsKey(pv)) {
            return new Loads(texts.get(pv), null, ref.fullPath());
        }
        return Literals.fold(call.args().get(0), before) instanceof String constant
                ? new Loads(null, constant, ref.fullPath()) : null;
    }

    /** {@code assert(<the data a and b denote are the same>, message)}. */
    private static TypedSpec comparison(Loads a, Loads b, @Nullable String message, ModelContext ctx,
            Generator generator) {
        if (a.generated() != null && b.generated() != null) {
            throw new NotImplementedException("assertSameElements over the loads of two generated test-data texts: not"
                    + " planned (B2d compares a generated text's with a constant text's)");
        }
        Loads generated = a.generated() != null ? a : b;
        Loads constant = a.generated() != null ? b : a;
        if (!generated.db().equals(constant.db())) {
            throw new NotImplementedException("assertSameElements over the loads of test-data texts on two databases,"
                    + " " + generated.db() + " and " + constant.db() + ": not planned (B2d)");
        }
        Same same = generator.same(java.util.Objects.requireNonNull(generated.generated()),
                java.util.Objects.requireNonNull(constant.constant()), generated.db());
        ExprType bool = new ExprType(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE);
        ExprType string = new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE);
        String words = (message != null ? message + ": " : "")
                + "the generated test data is not the expected test data (each table compared as a set of rows)";
        TypedSpec condition = switch (same) {
            case Same.Holds h -> new TypedCBoolean(true, bool);
            case Same.Fails f -> {
                words = (message != null ? message + ": " : "") + f.why();
                yield new TypedCBoolean(false, bool);
            }
            case Same.Compared c -> new TypedPlannedValue(c.statement(), bool);
        };
        com.legend.compiler.element.TypedFunction assertFn = ctx.findFunction(
                com.legend.builtin.NativeFn.Verdict.ASSERT.fqn()).stream().filter(f -> ASSERT.equals(f.id()))
                .findFirst().orElseThrow(() -> new IllegalStateException("no assert(Boolean[1], String[1]) native"));
        return new TypedNativeCall(assertFn, List.of(condition, new TypedCString(words, string)), bool);
    }

    /** A generated text's loads read in {@code n} -- anything but the comparison planned above: refused by name. */
    private static void refuseGeneratedLoads(TypedSpec n, List<TypedSpec> before,
            Map<TypedPlannedValue, Generated> texts) {
        TypedSpec v = n instanceof TypedLet let ? let.value() : n;
        Loads l = loads(v, before, texts);
        if (l != null && l.generated() != null) {
            throw new NotImplementedException("the load statements of the test-data generator's text (setUpDataSQLs),"
                    + " read other than compared as a set with a constant text's (assertSameElements): not planned"
                    + " (B2d)");
        }
        for (TypedSpec c : v.children()) {
            refuseGeneratedLoads(c, before, texts);
        }
    }

    /** The generator {@code value} is ({@code generateTestData(...)}, under a {@code toOne()}), or null. */
    private static @Nullable TypedTestDataGen generator(TypedSpec value) {
        TypedSpec v = value;
        while (v instanceof TypedNativeCall c && c.args().size() == 1
                && ResultEnvelopeSplice.TO_ONE_FQN.equals(c.callee().qualifiedName())) {
            v = c.args().get(0);
        }
        return v instanceof TypedTestDataGen g && "generate".equals(g.flavor()) ? g : null;
    }

    /** {@code n} with each read of a bound generator's result -- or of a generator in place -- the value it is; each
     *  text read recorded in {@code texts} (the comparisons read them). */
    private static TypedSpec substituted(TypedSpec n, Map<String, Generated> bound, Generator generator,
            Map<TypedPlannedValue, Generated> texts) {
        if (n instanceof TypedPropertyAccess pa) {
            Generated g = null;
            if (pa.source() instanceof TypedVariable v && bound.containsKey(v.name())) {
                g = bound.get(v.name());
            } else {
                TypedTestDataGen inPlace = generator(pa.source());
                if (inPlace != null) {
                    g = generator.plan(inPlace);
                }
            }
            if (g != null) {
                TypedSpec value = read(g, pa);
                if (value instanceof TypedPlannedValue pv) {
                    texts.put(pv, g);
                }
                return value;
            }
        }
        if (n instanceof TypedVariable v && bound.containsKey(v.name())) {
            throw new NotImplementedException("the test-data generator's result $" + v.name() + " read other than by"
                    + " its " + SQLS + " or its " + CSV + ": not planned (B2d)");
        }
        Map<String, Generated> scope = bound;
        if (n instanceof TypedLambda lam && lam.parameters().stream().anyMatch(bound::containsKey)) {
            // a lambda parameter of the same name shadows the binding inside the lambda
            scope = new HashMap<>(bound);
            lam.parameters().forEach(scope::remove);
        }
        List<TypedSpec> kids = n.children();
        if (kids.isEmpty()) {
            return n;
        }
        List<TypedSpec> next = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = substituted(k, scope, generator, texts);
            changed |= r != k;
            next.add(r);
        }
        return changed ? n.withChildren(next) : n;
    }

    /** The value of {@code pa}, a read of {@code g}'s result. */
    private static TypedSpec read(Generated g, TypedPropertyAccess pa) {
        return switch (pa.property()) {
            case SQLS -> {
                ExprType one = new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE);
                List<TypedSpec> texts = new ArrayList<>(g.sqls().size());
                for (String t : g.sqls()) {
                    texts.add(new TypedCString(t, one));
                }
                yield new TypedCollection(texts, new ExprType(Type.Primitive.STRING,
                        new Multiplicity.Bounded(texts.size(), texts.size())));
            }
            case CSV -> new TypedPlannedValue(g.csv(), new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE));
            default -> throw new NotImplementedException("the test-data generator's result's " + pa.property()
                    + ": not planned (B2d answers its " + SQLS + " and its " + CSV + ")");
        };
    }
}
