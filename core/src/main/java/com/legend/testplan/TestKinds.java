// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.TypedQuery;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.executionplan.ExecutionPlan;
import com.legend.planner.Prepared;
import com.legend.planner.ProductStatements;
import com.legend.planner.ProgramKinds;
import com.legend.planner.ProgramState;
import com.legend.planner.QueryDoor;

import java.util.ArrayList;
import java.util.List;

/**
 * A TEST'S KINDS ON THE PROGRAM ROAD (the test-is-the-product plan's M3, brief B12): the statements of a test's program
 * that hold a kind the query door refuses ({@link QueryDoor.TestOnly}, found by the door's own scan), planned by this
 * library. Each kind is planned from the sub-step its brief names; until then it is refused by name, the step that
 * plans it said: the eager run at M3f, asserts at M6 (the verdict record), assertError at M9 (T10). Seeds (M3e) are
 * planned before every statement whose sources name test data ({@link #seed}).
 *
 * <p><b>Effects (M3d; R11, D34, R7 decision 14).</b> A database effect's statements are written by
 * {@link EffectPlanner} from its String arguments, each a literal folded ({@code Literals.fold}, the walk's own fold; a
 * computed one is refused by name, the T9 kind M9 plans). Consecutive effect statements are ONE script, as the walk's
 * segment rule makes them (any other statement closes it): an {@link ExecutionPlan.Effect} whose script holds the
 * statements, their dialect's bracket and abort, and whose text is the statements joined bare (the walk's
 * {@code sendScript}). An effect let binds what only connection reads may read (the walk's guard). An effect as the
 * program's last statement answers as the walk's arm does (R17 C11): {@code true} for the DDL that drops and creates,
 * no value otherwise. {@code loadCsvToDbTable} stays the walk's until its body's class moves (R17 C10).
 */
final class TestKinds implements ProgramKinds {

    private final QueryDoor.Scan scan;
    /** The open script: the effect statements since the last statement of another kind. */
    private final List<String> script = new ArrayList<>();
    /** The seed sources placed since the program's last write. */
    private final java.util.Set<Seeds.Sources> established = new java.util.HashSet<>();
    /** The seeds' steps, generated once per sources value. */
    private final java.util.Map<Seeds.Sources, List<com.legend.setup.CsvSeed.Planned>> generated =
            new java.util.HashMap<>();

    TestKinds(TypedQuery body, String runtime) {
        this.scan = new QueryDoor.Scan(body, runtime);
    }

    @Override
    public boolean plan(ProgramState program, int index, TypedSpec statement, boolean last) {
        // the statement's seeds first, as the walk establishes its contexts before every statement (M3e)
        seed(program, statement);
        java.util.EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
        kinds.remove(QueryDoor.TestOnly.INLINE_SEEDS);
        kinds.remove(QueryDoor.TestOnly.RUNTIME_SEEDS);
        if (!kinds.contains(QueryDoor.TestOnly.EFFECT)) {
            // any other statement closes the open script (the walk's segment rule)
            close(program);
        }
        if (kinds.isEmpty()) {
            return false;
        }
        if (kinds.equals(java.util.EnumSet.of(QueryDoor.TestOnly.EFFECT))) {
            effect(program, index, statement, last);
            // the effect writes: the next statement's seeds are established again (the walk's markWriting)
            established.clear();
            return true;
        }
        kinds.remove(QueryDoor.TestOnly.EFFECT);
        QueryDoor.TestOnly kind = kinds.iterator().next();
        String step = switch (kind) {
            case EFFECT, INLINE_SEEDS, RUNTIME_SEEDS -> throw new IllegalStateException("planned above");
            case LET_EXECUTE -> "M3f";
            case ASSERT -> "M6, the verdict record";
            case ASSERT_ERROR -> "M9, T10";
        };
        throw new com.legend.error.NotImplementedException(kind.what() + " on the test door's program road: not"
                + " planned yet (the test-is-the-product plan's " + step + ")");
    }

    /**
     * {@code statement}'s seeds (D35, M3e): its sources -- each {@code from()}'s inline test data and named runtime,
     * the program's runtime -- planned as a {@link ExecutionPlan.Setup} step before it, unless the program placed
     * the same sources since its last write (the walk's established state, within the program; across programs the
     * runner skips a key the session holds clean). The steps are generated once per sources value, never per
     * statement (their key is their content). A step placed while a script is open closes it first: the program's
     * order is the effects before the statement, then its seeds.
     */
    private void seed(ProgramState program, TypedSpec statement) {
        com.legend.compiler.element.ModelContext ctx = program.query().context();
        Seeds.Sources sources = Seeds.sources(statement, program.runtime(), ctx);
        if (sources.namesNothing() || !established.add(sources)) {
            return;
        }
        List<com.legend.setup.CsvSeed.Planned> planned = generated.computeIfAbsent(sources,
                s -> Seeds.planned(statement, program.runtime(), ctx, program.dialect()));
        if (planned.isEmpty()) {
            return;
        }
        close(program);
        program.add(new ExecutionPlan.Setup(Seeds.key(planned),
                planned.stream().map(com.legend.setup.CsvSeed.Planned::step).toList()));
    }

    /** The effect {@code statement}'s statements added to the open script; the script closed and the effect's answer
     *  placed where it is the program's last statement. */
    private void effect(ProgramState program, int index, TypedSpec statement, boolean last) {
        List<TypedSpec> body = program.query().body();
        if (statement instanceof com.legend.compiler.spec.typed.TypedLet let && !last
                && !com.legend.planner.ConnectionLets.onlyConnectionReads(body, index + 1, let.name())) {
            // the walk's guard: the corpus binds an effect's opaque result as a smoke check and never reads it
            throw new IllegalStateException("reading an executeInDb result binding ('" + let.name()
                    + "') is not supported");
        }
        List<TypedSpec> statements = new ArrayList<>(program.lets());
        statements.add(statement);
        ProductStatements.Statement st = ProductStatements.prepare(program.query(), statements, program.runtime(),
                index, program.features());
        if (!(st.prepared() instanceof Prepared.Effect e)) {
            throw new com.legend.error.NotImplementedException("an effect under an expression of another kind ("
                    + st.prepared().getClass().getSimpleName() + ") on the test door's program road: an effect is"
                    + " planned as a statement of its own (an effectful map is the T9 kind the test-is-the-product"
                    + " plan's M9 plans)");
        }
        com.legend.compiler.spec.typed.TypedNativeCall call = e.call();
        com.legend.builtin.NativeFn.Effect effect = com.legend.builtin.NativeFn.Effect.of(call.callee().id())
                .orElseThrow();
        if (effect == com.legend.builtin.NativeFn.Effect.LOAD_CSV_TO_DB_TABLE) {
            throw new com.legend.error.NotImplementedException("loadCsvToDbTable on the test door's program road:"
                    + " its CSV is a test resource, read where the body runs (R17 C10: the walk's until its body's"
                    + " class moves)");
        }
        List<String> values = new ArrayList<>();
        for (TypedSpec arg : EffectPlanner.stringArgs(e.call())) {
            if (!(com.legend.compiler.spec.typed.Literals.fold(arg, e.body().subList(0, e.body().size() - 1))
                    instanceof String folded)) {
                throw new com.legend.error.NotImplementedException("a computed String argument of "
                        + e.call().callee().qualifiedName() + " on the test door's program road: its value is the"
                        + " database's to compute before the effect is written (the T9 kind the test-is-the-product"
                        + " plan's M9 plans)");
            }
            values.add(folded);
        }
        for (EffectPlanner.Statement s : EffectPlanner.statements(e.call(), values, e.body(),
                e.env().withDialect(program.dialect()))) {
            script.add(s.executed());
        }
        if (last) {
            close(program);
            // the walk's arms' value: the DDL that drops and creates answers true, the others no value
            boolean dropsAndCreates = effect == com.legend.builtin.NativeFn.Effect.DROP_AND_CREATE_TABLE_IN_DB
                    || effect == com.legend.builtin.NativeFn.Effect.DROP_AND_CREATE_SCHEMA_IN_DB;
            program.add(dropsAndCreates ? ProductStatements.scalar("Boolean", "true")
                    : ProductStatements.scalar(e.call().info().type().typeName(), null));
        }
    }

    /** The open script, if any, placed as one effect step. */
    private void close(ProgramState program) {
        if (script.isEmpty()) {
            return;
        }
        com.legend.sql.dialect.SqlDialect dialect = program.dialect();
        program.add(new ExecutionPlan.Effect(new ExecutionPlan.Sql(String.join(";\n", script) + ";", List.of(),
                program.target(), null, List.of()), new ExecutionPlan.Effect.Script(script, dialect.script(script),
                dialect.scriptAbort())));
        script.clear();
    }
}
