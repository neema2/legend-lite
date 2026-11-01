// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.TypedQuery;
import com.legend.builtin.NativeFn;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.Literals;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.NotImplementedException;
import com.legend.executionplan.ExecutionPlan;
import com.legend.planner.BodyPlanner;
import com.legend.planner.ConnectionLets;
import com.legend.planner.FramePlanner;
import com.legend.planner.LetPlanner;
import com.legend.planner.PlanAllocations;
import com.legend.planner.PlanEnv;
import com.legend.planner.Prepared;
import com.legend.planner.ProductStatements;
import com.legend.planner.ProgramFrames;
import com.legend.planner.ProgramKinds;
import com.legend.planner.ProgramState;
import com.legend.planner.QueryDoor;
import com.legend.setup.CsvSeed;
import com.legend.sql.RawSql;
import com.legend.sql.dialect.SqlDialect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A TEST'S KINDS ON THE PROGRAM ROAD (the test-is-the-product plan's M3, brief B12): the statements of a test's program
 * that hold a kind the query door refuses ({@link QueryDoor.TestOnly}, found by the door's own scan), planned by this
 * library. Each kind is planned from the sub-step its brief names; until then it is refused by name, the step that
 * plans it said: the eager run at M3f, asserts from M6 (a statement-root assert as the verdict step its judge planned,
 * {@link TestDoor.Asserts}), assertError at M9 (T10). Seeds (M3e) are
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

    // ONE PROGRAM'S (ProgramKinds: one instance per program, made by TestDoor.plan): the open effect run's script and
    // the seeds placed since the program's last write are this program's planning state, owned by it alone; the
    // generated seeds are a cache of a pure function

    private final QueryDoor.Scan scan;
    /** The open script: the effect statements since the last statement of another kind. */
    private final List<EffectPlanner.Statement> script = new ArrayList<>();
    /** The places (0-based) in the body of the statements the open script holds. */
    private final List<Integer> scriptStatements = new ArrayList<>();
    /** Each effect step this program planned, its segment and ledger texts beside it (M5): beside the plan, never in
     *  it. */
    private final Map<ExecutionPlan.Node, TestDoor.Effect> effects = new IdentityHashMap<>();
    /** Each seed step this program planned, its statements' ledger texts beside it (M5). */
    private final Map<ExecutionPlan.Node, List<String>> seeds = new IdentityHashMap<>();
    /** The places in the program's sequence of the frame lets' eager-run steps this program planned (M9): the walk
     *  sends a frame's run under its own statement origin (LET), which the plan does not say; the router's sink names
     *  it to the runner. By place, not identity: the finished plan rebuilds a check whose stamp nothing reads. */
    private final Set<Integer> frameRuns = new java.util.TreeSet<>();
    /** The seed sources placed since the program's last write. */
    private final Set<Seeds.Sources> established = new HashSet<>();
    /** The seeds' steps, generated once per sources value. */
    private final Map<Seeds.Sources, List<CsvSeed.Planned>> generated =
            new HashMap<>();

    /** The body's asserts, as its judge planned them (M6). */
    private final TestDoor.Asserts asserts;

    TestKinds(TypedQuery body, String runtime, TestDoor.Asserts asserts) {
        this.scan = new QueryDoor.Scan(body, runtime);
        this.asserts = asserts;
    }

    @Override
    public boolean plan(ProgramState program, int index, TypedSpec statement, boolean last) {
        EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
        kinds.remove(QueryDoor.TestOnly.INLINE_SEEDS);
        kinds.remove(QueryDoor.TestOnly.RUNTIME_SEEDS);
        if (!kinds.contains(QueryDoor.TestOnly.EFFECT)) {
            // any other statement closes the open script (the walk's segment rule: the run's script is sent when its
            // segment ends, before the next statement's contexts are established)
            close(program);
        }
        // the statement's seeds, as the walk establishes its contexts before every statement (M3e)
        seed(program, statement);
        if (kinds.isEmpty()) {
            return false;
        }
        if (kinds.equals(EnumSet.of(QueryDoor.TestOnly.LET_EXECUTE)) && !last) {
            frame(program, index, (TypedLet) statement);
            return true;
        }
        if (kinds.equals(EnumSet.of(QueryDoor.TestOnly.ASSERT)) && !(statement instanceof TypedLet)) {
            // a statement-root assert its judge planned (M6): its verdict step, at its turn
            ExecutionPlan.Verdicts verdict = asserts.at(index);
            if (verdict != null) {
                program.add(verdict);
                return true;
            }
        }
        if (kinds.equals(EnumSet.of(QueryDoor.TestOnly.EFFECT))) {
            effect(program, index, statement, last);
            // the effect writes: the next statement's seeds are established again (the walk's markWriting)
            established.clear();
            return true;
        }
        kinds.remove(QueryDoor.TestOnly.EFFECT);
        QueryDoor.TestOnly kind = kinds.iterator().next();
        String step = switch (kind) {
            case EFFECT, INLINE_SEEDS, RUNTIME_SEEDS -> throw new IllegalStateException("planned above");
            // a let-bound execute(...) as the program's last statement, or with another kind in its binding
            case LET_EXECUTE -> "M9 (an execute(...) whose result is the value asked for)";
            case ASSERT -> "M6-M8: an assert its judge planned no verdict step for";
            case ASSERT_ERROR -> "M9, T10";
        };
        throw new NotImplementedException(kind.what() + " on the test door's program road: not"
                + " planned yet (the test-is-the-product plan's " + step + ")");
    }

    /**
     * A FRAME bound by a let (D15, B10, decision 1; the test-is-the-product plan's M3f): {@code let r =
     * execute(...)}. Pure runs it AT the let, so it is run there as a {@link ExecutionPlan.Check} step -- on every
     * pass alike (MV7) -- and its activity's comment is that step's execution-trace stamp, a parameter the runner
     * binds when the plan runs (R17 C4); its readers after paste its chain ({@code ProgramFrames}).
     */
    private void frame(ProgramState program, int index, TypedLet let) {
        TypedNativeCall ec = (TypedNativeCall) LetPlanner.rhs(let);
        ProgramFrames frames = program.frames();
        TypedQuery query = program.query();
        PlanEnv env = ProductStatements.env(query.context(), program.runtime(), program.features(), frames.planRows());
        FramePlanner.FrameChain fc = FramePlanner.chain(ec, program.lets(), query.specs(), env);
        frames.consumed(fc.env().queryLets());
        // the run's stamp: the asserts' planner's name where its sides read the frame's activity comment (M9c), the
        // program's own otherwise
        String minted = asserts.frameStamp(index);
        String stamp = minted != null ? frames.adoptStamp(minted) : frames.newStamp();
        PlanAllocations.registerActivityRows(fc.source(), fc.chain(), program.lets(), query.specs(), null, stamp, env);
        frameRuns.add(program.size());
        program.add(eagerRun(program, index, fc, stamp));
        FramePlanner.Cte cte = fc.legendQuery() ? FramePlanner.Cte.NONE : cte(program, index, fc);
        frames.let(let.name(), new ProgramFrames.Frame(fc.chain(),
                !fc.legendQuery() && fc.relationRooted(), fc.source(), fc.env().tableReplace(), cte));
    }

    /** The frame's EAGER RUN: its query, run at the let as a {@code Check} (its rows dropped) under the renames of the
     *  runtime the frame runs on (the walk runs it in the frame's environment; MR's audit S3), stamped with the let's
     *  stamp. */
    private static ExecutionPlan.Check eagerRun(ProgramState program, int index, FramePlanner.FrameChain fc,
            String stamp) {
        ProductStatements.Statement run = ProductStatements.prepare(program.query(), List.of(fc.runChain()),
                program.runtime(), index, program.features(), program.frames(), fc.env().tableReplace());
        return new ExecutionPlan.Check(program.statement(program.lowered(run, false)).stamped(stamp));
    }

    /** The frame's CTE, lowered once at the let with the product's configuration under its runtime's renames (M3g,
     *  B12 item 9): a statement that reads the frame more than once references it; one that reads it once pastes its
     *  chain. */
    private static FramePlanner.Cte cte(ProgramState program, int index, FramePlanner.FrameChain fc) {
        return FramePlanner.cte(fc, program.query().specs(), chain -> {
            ProductStatements.Statement planned = ProductStatements.prepare(program.query(), List.of(chain),
                    program.runtime(), index, program.features(), program.frames(), fc.env().tableReplace());
            return planned.prepared() instanceof Prepared.Planned p
                    ? new BodyPlanner.Bare(program.lowered(planned, false), p.root())
                    : null;
        });
    }

    /**
     * {@code statement}'s seeds (D35, M3e): its sources -- each {@code from()}'s inline test data and named runtime,
     * the program's runtime -- planned as a {@link ExecutionPlan.Setup} step before it, unless the program placed
     * the same sources since its last write (the walk's established state, within the program; across programs the
     * runner skips a key the session holds clean). The steps are generated once per sources value, never per
     * statement (their key is their content). A statement that is not an effect has closed the open script first
     * (the effects, then its seeds, then it); an effect that continues an open run and names test data is refused by
     * name (below).
     */
    private void seed(ProgramState program, TypedSpec statement) {
        ModelContext ctx = program.query().context();
        Seeds.Sources sources = Seeds.sources(statement, program.runtime(), ctx);
        if (sources.namesNothing() || !established.add(sources)) {
            return;
        }
        List<CsvSeed.Planned> planned = generated.computeIfAbsent(sources,
                s -> Seeds.planned(statement, program.runtime(), ctx, program.dialect()));
        if (planned.isEmpty()) {
            return;
        }
        if (!script.isEmpty()) {
            // the walk sends a statement's seeds ahead of the open effect run's script, the program after it: the two
            // orders differ, and no corpus body names test data inside an effect run (M3a) -- refused by name (the M3
            // audit's B8)
            throw new NotImplementedException("seeds named inside an effect run (an effect statement"
                    + " follows another, and its sources name test data): their order against the run's script is"
                    + " not designed");
        }
        List<ExecutionPlan.SetupStep> steps = planned.stream().map(CsvSeed.Planned::step).toList();
        ExecutionPlan.Setup setup = new ExecutionPlan.Setup(Seeds.key(steps), steps, program.target());
        program.add(setup);
        seeds.put(setup, planned.stream().map(CsvSeed.Planned::text).toList());
    }

    /** The effect {@code statement}'s statements added to the open script; the script closed and the effect's answer
     *  placed where it is the program's last statement. */
    private void effect(ProgramState program, int index, TypedSpec statement, boolean last) {
        List<TypedSpec> body = program.query().body();
        if (statement instanceof TypedLet let && !last
                && !ConnectionLets.onlyConnectionReads(body, index + 1, let.name())) {
            // the walk's guard: the corpus binds an effect's opaque result as a smoke check and never reads it
            throw new IllegalStateException("reading an executeInDb result binding ('" + let.name()
                    + "') is not supported");
        }
        List<TypedSpec> statements = new ArrayList<>(program.lets());
        statements.add(statement);
        ProductStatements.Statement st = ProductStatements.prepare(program.query(), statements, program.runtime(),
                index, program.features());
        if (!(st.prepared() instanceof Prepared.Effect e)) {
            throw new NotImplementedException("an effect under an expression of another kind ("
                    + st.prepared().getClass().getSimpleName() + ") on the test door's program road: an effect is"
                    + " planned as a statement of its own (an effectful map is the T9 kind the test-is-the-product"
                    + " plan's M9 plans)");
        }
        TypedNativeCall call = e.call();
        NativeFn.Effect effect = NativeFn.Effect.of(call.callee().id())
                .orElseThrow();
        if (effect == NativeFn.Effect.LOAD_CSV_TO_DB_TABLE) {
            throw new NotImplementedException("loadCsvToDbTable on the test door's program road:"
                    + " its CSV is a test resource, read where the body runs (R17 C10: the walk's until its body's"
                    + " class moves)");
        }
        List<String> values = new ArrayList<>();
        for (TypedSpec arg : EffectPlanner.stringArgs(e.call())) {
            if (!(Literals.fold(arg, e.body().subList(0, e.body().size() - 1))
                    instanceof String folded)) {
                throw new NotImplementedException("a computed String argument of "
                        + e.call().callee().qualifiedName() + " on the test door's program road: its value is the"
                        + " database's to compute before the effect is written (the T9 kind the test-is-the-product"
                        + " plan's M9 plans)");
            }
            values.add(folded);
        }
        script.addAll(EffectPlanner.statements(e.call(), values, e.body(), e.env().withDialect(program.dialect())));
        scriptStatements.add(index);
        if (last) {
            close(program);
            // the walk's arms' value: the DDL that drops and creates answers true, the others no value
            boolean dropsAndCreates = effect == NativeFn.Effect.DROP_AND_CREATE_TABLE_IN_DB
                    || effect == NativeFn.Effect.DROP_AND_CREATE_SCHEMA_IN_DB;
            program.add(ProductStatements.asATestAnswers(dropsAndCreates ? ProductStatements.scalar("Boolean", "true")
                    : ProductStatements.scalar(e.call().info().type().typeName(), null)));
        }
    }

    /** The effect steps this program planned, each with its segment and ledger texts. */
    Map<ExecutionPlan.Node, TestDoor.Effect> effects() {
        return Collections.unmodifiableMap(effects);
    }

    /** The seed steps this program planned, each with its statements' ledger texts. */
    Map<ExecutionPlan.Node, List<String>> seeds() {
        return Collections.unmodifiableMap(seeds);
    }

    /** The places of the frame lets' eager-run steps this program planned (M9). */
    Set<Integer> frameRuns() {
        return Collections.unmodifiableSet(frameRuns);
    }

    /** The open script, if any, placed as one effect step, its segment (the walk's: the body's statements it holds)
     *  and ledger texts beside it. */
    private void close(ProgramState program) {
        if (script.isEmpty()) {
            return;
        }
        SqlDialect dialect = program.dialect();
        List<String> executed = script.stream().map(EffectPlanner.Statement::executed).toList();
        ExecutionPlan.Effect effect = new ExecutionPlan.Effect(new ExecutionPlan.Sql(String.join(";\n", executed)
                + ";", List.of(), program.target(), null, List.of()), new ExecutionPlan.Effect.Script(executed,
                dialect.script(executed), dialect.scriptAbort()));
        program.add(effect);
        String where = EffectSegment.where(scriptStatements.get(0) + 1,
                scriptStatements.get(scriptStatements.size() - 1) + 1);
        effects.put(effect, new TestDoor.Effect(where, script.stream()
                .map(s -> new TestDoor.Recorded(s.recorded(), RawSql.isSingleQuery(s.executed()))).toList()));
        script.clear();
        scriptStatements.clear();
    }
}
