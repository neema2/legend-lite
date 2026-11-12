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
 * their rows, decided as upstream decides it ({@link #compared}). A database effect between the generator's let and a
 * read of its result is refused by name: here the generator's statement runs where its result is read, upstream's at
 * its let (the user's ruling of 2026-11-11, "read where read", a register row). Typed nodes are minted here, in the
 * compiler's layer (AGENTS invariant 7).
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

        /** Whether the load statements of {@code generated}'s text and of the test-data text {@code expected} on the
         *  database {@code dbFqn} are the same, as upstream's {@code assertTestData} decides it (the user's ruling of
         *  2026-11-11, "Agreed"): the database's work; a failure's words begin with {@code lead}. */
        Same same(Generated generated, String expected, String dbFqn, String lead);

        /** Every table of the database {@code dbFqn} (and of each database it includes) as {@code generated}'s slice
         *  reloaded holds it: the slice's rows, or none. */
        Slice slice(Generated generated, String dbFqn);
    }

    /** Whether a generated text's loads are an expected text's: decided at planning ({@link Fails}), or two statements
     *  the database answers ({@link Compared}): {@code holds}' one Boolean cell, and {@code differences}' one text
     *  naming the rows that differ (the failure's words). */
    public sealed interface Same {
        record Fails(String why) implements Same {
        }

        record Compared(SqlQuery holds, SqlQuery differences) implements Same {
        }
    }

    /** A reloaded slice: the table references the reload replaces ({@code reads}, each {@code store|physical name}: a
     *  reference whose table is the reloaded database's or an included one's, whatever database names it) and each
     *  table's rows by physical name ({@code TABLE} in the default schema, else {@code SCHEMA.TABLE}). */
    public record Slice(java.util.Set<String> reads, Map<String, SqlQuery> tables) {
        public Slice {
            reads = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(reads));
            tables = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(tables));
        }
    }

    /** One generator planned: its fetches' texts, written in the body's dialect, and the read-only statement whose one
     *  cell is its test-data text. */
    public interface Generated {
        List<String> sqls();

        SqlQuery csv();
    }

    /** {@code source} read over a slice: each table it reads by a typed reference whose identity is one of
     *  {@code reads} ({@code store|physical name}) read from {@code tables} instead (the generator's view fetch, B2d.3:
     *  a view's relation over the clauses that fetched its tables, as the reload reads a slice). */
    public static TypedSpec overSlice(TypedSpec source, java.util.Set<String> reads, Map<String, SqlQuery> tables) {
        return new com.legend.compiler.spec.typed.TypedTableSwap(source, reads, tables, source.info());
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
     *  text compared with another text's made the database's comparison ({@link #compared}); {@code specs} compiles
     *  the bodies of the user functions a statement calls (what a statement reaches, {@link StatementEffects}). */
    public static List<TypedSpec> spliced(List<TypedSpec> body, ModelContext ctx, SpecCompiler specs,
            Generator generator) {
        Map<String, Generated> bound = new HashMap<>();
        // each generated text read, by its node: the comparisons below know the generator's text by it
        Map<TypedPlannedValue, Generated> texts = new IdentityHashMap<>();
        List<TypedSpec> out = new ArrayList<>(body.size());
        // each statement's reads of a generator's result, by its place; each generator's let, by the place after it
        List<java.util.Set<Generated>> uses = new ArrayList<>(body.size());
        Map<Generated, Integer> letAt = new IdentityHashMap<>();
        for (TypedSpec statement : body) {
            java.util.Set<Generated> used = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
            TypedSpec s = substituted(statement, bound, generator, texts, used);
            if (s instanceof TypedLet let) {
                // a rebinding ends the generator's binding (its own value read the earlier one above)
                bound.remove(let.name());
                TypedTestDataGen g = generator(let.value());
                if (g != null) {
                    Generated planned = generator.plan(g);
                    bound.put(let.name(), planned);
                    letAt.put(planned, out.size());
                    continue;
                }
            }
            out.add(s);
            uses.add(used);
        }
        refuseEffectsBeforeARead(out, uses, letAt, texts, specs);
        return compared(out, ctx, specs, generator, texts);
    }

    /**
     * A database effect -- a statement reaching one, through the user functions it calls; a reload of a generated text
     * is none, it writes nothing -- standing between a generator's let and a read of its result (its {@code sqls},
     * its text, a let carrying either, the comparison or the reload of its loads): refused by name (B2d.2's audit, M2).
     * Here the generator's statement runs where its result is read (the user's ruling of 2026-11-11, "read where read",
     * a register row), upstream's at its let: across an effect the two read different data.
     */
    private static void refuseEffectsBeforeARead(List<TypedSpec> body, List<java.util.Set<Generated>> uses,
            Map<Generated, Integer> letAt, Map<TypedPlannedValue, Generated> texts, SpecCompiler specs) {
        if (letAt.isEmpty()) {
            return;
        }
        Map<String, java.util.Set<Generated>> carriers = new HashMap<>();
        Map<com.legend.model.FunctionId, Boolean> memo = new HashMap<>();
        List<Integer> effects = new ArrayList<>();
        for (int i = 0; i < body.size(); i++) {
            TypedSpec s = body.get(i);
            java.util.Set<Generated> used = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
            used.addAll(uses.get(i));
            for (String v : com.legend.compiler.spec.typed.FreeVars.of(s)) {
                used.addAll(carriers.getOrDefault(v, java.util.Set.of()));
            }
            for (Generated g : used) {
                Integer at = letAt.get(g);
                for (int e : effects) {
                    if (at != null && e >= at) {
                        throw new NotImplementedException("a database effect between the test-data generator's let and"
                                + " a read of its result: not planned (B2d: the generator's statement runs where its"
                                + " result is read, upstream's at its let, and across the effect the two read different"
                                + " data)");
                    }
                }
            }
            if (s instanceof TypedLet let) {
                if (used.isEmpty()) {
                    carriers.remove(let.name());
                } else {
                    carriers.put(let.name(), used);
                }
            }
            if (reload(s, body.subList(0, i), texts) == null
                    && StatementEffects.reaches(s, GeneratorSplice::writesTheDatabase, specs, memo)) {
                effects.add(i);
            }
        }
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
     * made the database's comparison of the rows they load: {@code assert(<the comparison's cell>, <the rows that
     * differ>)}. The general rule, never a helper's name: upstream's {@code assertTestData} compares the two
     * load-statement lists {@code setUpDataSQLs} writes, and a generated text's statements are a run's values the plan
     * cannot hold (the texts would be SQL built from rows read, S133's hole), so the database compares what decides
     * them -- each table's rows, each cell as upstream's insert writes it, counted -- passing or failing exactly as
     * upstream does (the user's rulings: 2026-11-10, "upstream's comparison by other means"; 2026-11-11, "Agreed"), the
     * failure's words lite's own (a register row). After a reload, the slice is what upstream's database holds to the
     * body's end: the query run over it is planned over the slice's rows, and any other statement that reaches the
     * database is refused by name. The loads' lets the comparison consumed are spliced away; a generated text's loads
     * read any other way are refused by name.
     */
    private static List<TypedSpec> compared(List<TypedSpec> body, ModelContext ctx, SpecCompiler specs,
            Generator generator, Map<TypedPlannedValue, Generated> texts) {
        if (texts.isEmpty()) {
            return List.copyOf(body);
        }
        List<TypedSpec> out = new ArrayList<>(body.size());
        java.util.Set<String> consumed = new java.util.HashSet<>();
        Map<com.legend.model.FunctionId, Boolean> memo = new HashMap<>();
        // the slice a reload made the database's tables, and the runtime whose connection it was reloaded on: from the
        // reload to the body's end (upstream's database holds the slice from then on)
        Slice reloaded = null;
        TypedSpec reloadedOn = null;
        boolean ran = false;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            List<TypedSpec> before = out;
            Reload reload = reload(statement, before, texts);
            if (reload != null) {
                // the slice reloaded (B2d.2, the user's ruling of 2026-11-11: "Read only reload is fine"): nothing is
                // written; the query run after it is planned again over the slice's rows
                reloaded = generator.slice(java.util.Objects.requireNonNull(reload.loads().generated()),
                        reload.loads().db());
                reloadedOn = runtimeOf(reload.connection(), before);
                ran = false;
                TypedSpec source = ((com.legend.compiler.spec.typed.TypedMap) statement).source();
                if (source instanceof TypedVariable v) {
                    consumed.add(v.name());
                }
                continue;
            }
            if (reloaded != null) {
                TypedSpec swapped = !ran && statement instanceof TypedLet let ? overSlice(let, before, reloaded,
                        java.util.Objects.requireNonNull(reloadedOn), consumed, specs, memo) : null;
                if (swapped != null) {
                    ran = true;
                    out.add(swapped);
                    continue;
                }
                // every other statement after the reload that reaches the database -- a read, a write, a user
                // function or an executeInDb whose body does, a generator's statement -- reads or changes what
                // upstream's database holds then, the slice: refused by name, never assumed (B2d.2's audit, H1)
                if (!planBuild(statement)
                        && StatementEffects.reaches(statement, GeneratorSplice::touchesTheDatabase, specs, memo)) {
                    throw new NotImplementedException("a statement that reads or writes the database after the"
                            + " test-data generator's slice was reloaded, other than the query run over it ("
                            + (ran ? "after" : "before") + " that query): not planned (B2d's read-only reload swaps the"
                            + " tables of that query alone; upstream's database holds the slice from the reload on)");
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

    /** A reload: the generated loads, executed on {@code connection}. */
    private record Reload(Loads loads, TypedSpec connection) {
    }

    /** {@code statement}'s generated loads if it RELOADS them -- each of the generator's load statements executed on the
     *  database ({@code loads->map(sql | executeInDb($sql, connection))}, upstream's {@code loadAndTestExecution}) --
     *  or null. */
    private static @Nullable Reload reload(TypedSpec statement, List<TypedSpec> before,
            Map<TypedPlannedValue, Generated> texts) {
        if (!(statement instanceof com.legend.compiler.spec.typed.TypedMap map)
                || map.mapper().parameters().size() != 1 || map.mapper().body().size() != 1
                || !(map.mapper().body().get(0) instanceof TypedNativeCall exec)
                || com.legend.builtin.NativeFn.Effect.of(exec.callee().id()).orElse(null)
                        != com.legend.builtin.NativeFn.Effect.EXECUTE_IN_DB
                || exec.args().size() != 2 || !(exec.args().get(0) instanceof TypedVariable sql)
                || !sql.name().equals(map.mapper().parameters().get(0))) {
            return null;
        }
        Loads l = loads(map.source(), before, texts);
        return l != null && l.generated() != null ? new Reload(l, exec.args().get(1)) : null;
    }

    /** The runtime whose connection {@code connection} is -- {@code $runtime.connectionStores.connection}, under a
     *  {@code toOne()} or a {@code cast(...)}, each traced through the lets before it (upstream's
     *  {@code loadAndTestExecution}) -- traced to its value. Any other connection is refused by name: the query run
     *  after the reload must read the connection it was reloaded on (B2d.2's audit, M1). */
    private static TypedSpec runtimeOf(TypedSpec connection, List<TypedSpec> before) {
        TypedSpec c = unwrapped(connection, before);
        if (c instanceof TypedPropertyAccess conn && "connection".equals(conn.property())
                && unwrapped(conn.source(), before) instanceof TypedPropertyAccess stores
                && "connectionStores".equals(stores.property())) {
            return com.legend.compiler.spec.typed.Lets.bound(stores.source(), before);
        }
        throw new NotImplementedException("the test-data generator's slice reloaded on a connection that is not a"
                + " runtime's ($runtime.connectionStores.connection): not planned (B2d plans the query run over the"
                + " slice on the connection it was reloaded on)");
    }

    /** {@code e} traced through the lets before it, a {@code toOne()} or a {@code cast(...)} read through. */
    private static TypedSpec unwrapped(TypedSpec e, List<TypedSpec> before) {
        TypedSpec c = com.legend.compiler.spec.typed.Lets.bound(e, before);
        while (true) {
            if (c instanceof com.legend.compiler.spec.typed.TypedCast cast) {
                c = com.legend.compiler.spec.typed.Lets.bound(cast.source(), before);
            } else if (c instanceof TypedNativeCall call && call.args().size() == 1
                    && ResultEnvelopeSplice.TO_ONE_FQN.equals(call.callee().qualifiedName())) {
                c = com.legend.compiler.spec.typed.Lets.bound(call.args().get(0), before);
            } else {
                return c;
            }
        }
    }

    /**
     * {@code let} -- a frame, {@code execute(query, mapping, runtime, ...)} or an execution plan's
     * {@code execute(executionPlan(query, ...), ...)} -- with its query read over {@code slice}: the query lambda's
     * last statement wrapped in a {@link com.legend.compiler.spec.typed.TypedTableSwap} (the query planned again over
     * the slice's rows, read only). The plan's build, where a let binds it, read in place (its let consumed). Null
     * where {@code let} is no frame.
     */
    private static @Nullable TypedSpec overSlice(TypedLet let, List<TypedSpec> before, Slice slice, TypedSpec on,
            java.util.Set<String> consumed, SpecCompiler specs, Map<com.legend.model.FunctionId, Boolean> memo) {
        TypedSpec rhs = let.value();
        if (!(rhs instanceof TypedNativeCall frame
                && com.legend.builtin.NativeFn.Handle.runsAFrame(frame.callee().id()))) {
            return null;
        }
        com.legend.builtin.NativeFn.Handle h = com.legend.builtin.NativeFn.Handle.of(frame.callee().id())
                .orElseThrow();
        List<TypedSpec> args = new ArrayList<>(frame.args());
        switch (h) {
            case EXECUTE -> {
                sameRuntime(args.get(2), on, before);
                args.set(0, swappedQuery(args.get(0), before, slice, specs, memo));
            }
            case EXECUTION_PLAN_EXECUTE -> {
                TypedSpec build = com.legend.compiler.spec.typed.Lets.bound(args.get(0), before);
                if (!(build instanceof TypedNativeCall b && com.legend.builtin.NativeFn.Handle.of(b.callee().id())
                        .orElse(null) == com.legend.builtin.NativeFn.Handle.EXECUTION_PLAN)) {
                    throw new NotImplementedException("the query run over the test-data generator's reloaded slice: an"
                            + " execution plan that does not trace to its executionPlan(...) build: not planned (B2d)");
                }
                List<TypedSpec> bargs = new ArrayList<>(b.args());
                sameRuntime(bargs.get(2), on, before);
                bargs.set(0, swappedQuery(bargs.get(0), before, slice, specs, memo));
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

    /** The query's runtime {@code runtime} is the one the slice was reloaded on ({@code on}), else refused by name. */
    private static void sameRuntime(TypedSpec runtime, TypedSpec on, List<TypedSpec> before) {
        if (!com.legend.compiler.spec.typed.Lets.bound(runtime, before).equals(on)) {
            throw new NotImplementedException("the query run over the test-data generator's reloaded slice on a runtime"
                    + " other than the one whose connection the slice was reloaded on: not planned (B2d)");
        }
    }

    /** The query {@code q} (a lambda, or a let's) with its last statement read over {@code slice}; a statement before
     *  it that reads the database is refused by name (the swap is the last statement's). */
    private static TypedSpec swappedQuery(TypedSpec q, List<TypedSpec> before, Slice slice, SpecCompiler specs,
            Map<com.legend.model.FunctionId, Boolean> memo) {
        if (!(com.legend.compiler.spec.typed.Lets.bound(q, before) instanceof TypedLambda lam)
                || !lam.parameters().isEmpty() || lam.body().isEmpty()) {
            throw new NotImplementedException("the query run over the test-data generator's reloaded slice: no"
                    + " parameterless query lambda: not planned (B2d)");
        }
        for (TypedSpec s : lam.body().subList(0, lam.body().size() - 1)) {
            if (StatementEffects.reaches(s, GeneratorSplice::touchesTheDatabase, specs, memo)) {
                throw new NotImplementedException("the query run over the test-data generator's reloaded slice reads"
                        + " the database before its last statement: not planned (B2d swaps the last statement's"
                        + " tables)");
            }
        }
        TypedSpec last = lam.body().get(lam.body().size() - 1);
        if (!Type.isRelation(last.info().type())
                && !com.legend.compiler.element.type.PlatformTypes.isTdsType(last.info().type())) {
            throw new NotImplementedException("the query run over the test-data generator's reloaded slice answers "
                    + last.info().type().typeName() + ", not a relation: not planned (B2d swaps a relation's tables)");
        }
        List<TypedSpec> body = new ArrayList<>(lam.body());
        body.set(body.size() - 1, new com.legend.compiler.spec.typed.TypedTableSwap(last, slice.reads(),
                slice.tables(), last.info()));
        return lam.withChildren(body);
    }

    /** Whether {@code n} itself writes the database: a database effect ({@code executeInDb} and its kin) or a write
     *  into a store table. {@link StatementEffects#reaches} finds it under a statement and in the bodies it calls. */
    private static boolean writesTheDatabase(TypedSpec n) {
        return n instanceof TypedNativeCall call && com.legend.builtin.NativeFn.Effect.isDbEffect(call.callee().id())
                || StatementEffects.isStoreWrite(n);
    }

    /** Whether {@code statement} binds an execution plan's build ({@code let p = executionPlan(query, ...)}): it reads
     *  nothing at its turn (the frame that executes it does). */
    private static boolean planBuild(TypedSpec statement) {
        return statement instanceof TypedLet let && let.value() instanceof TypedNativeCall b
                && com.legend.builtin.NativeFn.Handle.of(b.callee().id()).orElse(null)
                        == com.legend.builtin.NativeFn.Handle.EXECUTION_PLAN;
    }

    /** Whether {@code n} itself reads or writes the database: a class's extent, a table, a from(), a frame, a raw read,
     *  a generator or its planned statement, or a write ({@link #writesTheDatabase}). {@link StatementEffects#reaches}
     *  finds it under a statement and in the bodies it calls. */
    private static boolean touchesTheDatabase(TypedSpec n) {
        return n instanceof com.legend.compiler.spec.typed.TypedGetAll
                || n instanceof com.legend.compiler.spec.typed.TypedTableReference
                || n instanceof com.legend.compiler.spec.typed.TypedFrom
                || n instanceof com.legend.compiler.spec.typed.TypedRawSqlRelation
                || n instanceof TypedTestDataGen || n instanceof TypedPlannedValue
                || n instanceof TypedNativeCall call && com.legend.builtin.NativeFn.Handle.runsAFrame(call.callee().id())
                || writesTheDatabase(n);
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
        String lead = message != null ? message + ": " : "";
        Same same = generator.same(java.util.Objects.requireNonNull(generated.generated()),
                java.util.Objects.requireNonNull(constant.constant()), generated.db(), lead);
        ExprType bool = new ExprType(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE);
        ExprType string = new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE);
        TypedSpec condition = switch (same) {
            case Same.Fails f -> new TypedCBoolean(false, bool);
            case Same.Compared c -> new TypedPlannedValue(c.holds(), bool);
        };
        TypedSpec words = switch (same) {
            case Same.Fails f -> new TypedCString(f.why(), string);
            case Same.Compared c -> new TypedPlannedValue(c.differences(), string);
        };
        com.legend.compiler.element.TypedFunction assertFn = ctx.findFunction(
                com.legend.builtin.NativeFn.Verdict.ASSERT.fqn()).stream().filter(f -> ASSERT.equals(f.id()))
                .findFirst().orElseThrow(() -> new IllegalStateException("no assert(Boolean[1], String[1]) native"));
        return new TypedNativeCall(assertFn, List.of(condition, words), bool);
    }

    /** A generated text's loads read in {@code n} -- anything but the comparison planned above: refused by name. */
    private static void refuseGeneratedLoads(TypedSpec n, List<TypedSpec> before,
            Map<TypedPlannedValue, Generated> texts) {
        TypedSpec v = n instanceof TypedLet let ? let.value() : n;
        Loads l = loads(v, before, texts);
        if (l != null && l.generated() != null) {
            throw new NotImplementedException("the load statements of the test-data generator's text (setUpDataSQLs),"
                    + " read other than compared with a constant text's (assertSameElements) or reloaded: not planned"
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
            Map<TypedPlannedValue, Generated> texts, java.util.Set<Generated> used) {
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
                used.add(g);
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
            TypedSpec r = substituted(k, scope, generator, texts, used);
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
