// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.TypedQuery;
import com.legend.builtin.NativeFn;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.StatementEffects;
import com.legend.compiler.spec.UserCallInliner;
import com.legend.compiler.spec.typed.Literals;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedMap;
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
import com.legend.testexecutionplan.TestPlan;
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
 * computed one is refused by name: lite has no text holes, the user's ruling of 2026-11-04). Consecutive effect statements are ONE script, as the walk's
 * segment rule makes them (any other statement closes it): an {@link ExecutionPlan.Effect} whose script holds the
 * statements, their dialect's bracket and abort, and whose text is the statements joined bare (the walk's
 * {@code sendScript}). An effect let binds what only connection reads may read (the walk's guard). An effect as the
 * program's last statement answers as the walk's arm does (R17 C11): {@code true} for the DDL that drops and creates,
 * no value otherwise. {@code loadCsvToDbTable} stays the walk's until its body's class moves (R17 C10). A SEED FORM
 * mapped over one such effect ({@code $sqls->map(sql | executeInDb($sql, $connection))}, the corpus's setup helpers
 * unfolded into the body; M10c item 7c) is the texts the form spells, each bound into the lambda's statement, in one
 * script ({@code Seeds.seedForm}, the one reader both roads share).
 */
final class TestKinds implements ProgramKinds {

    // ONE PROGRAM'S (ProgramKinds: one instance per program, made by TestDoor.plan): the open effect run's script and
    // the seeds placed since the program's last write are this program's planning state, owned by it alone; the
    // generated seeds are a cache of a pure function

    private final QueryDoor.Scan scan;
    /** The open script: the effect statements since the last statement of another kind. */
    private final List<EffectPlanner.Statement> script = new ArrayList<>();
    /** Each effect step this program planned, its segment and ledger texts beside it (M5): beside the plan, never in
     *  it. */
    private final Map<ExecutionPlan.Node, TestDoor.Effect> effects = new IdentityHashMap<>();
    /** Each seed step this program planned, its statements' ledger texts beside it (M5). */
    // each load's referee texts, made only when an edge asks for them (the M1 tenets audit's finding 8, 2026-11-09: a
    // file's rows were rendered as the referee's insert for every plan, with no edge too)
    private final Map<ExecutionPlan.Node, java.util.function.Supplier<List<String>>> seeds = new IdentityHashMap<>();
    /** The places in the program's sequence of the frame lets' eager-run steps this program planned (M9): the walk
     *  sends a frame's run under its own statement origin (LET), which the plan does not say; the router's sink names
     *  it to the runner. By place, not identity: the finished plan rebuilds a check whose stamp nothing reads. */
    private final Set<Integer> frameRuns = new java.util.TreeSet<>();
    /** The seed sources placed since the program's last write. */
    private final Set<Seeds.Sources> established = new HashSet<>();
    /** The seeds' steps, generated once per sources value. */
    private final Map<Seeds.Sources, CsvSeed.Planned> generated =
            new HashMap<>();

    /** The body's asserts, as its judge planned them (M6). */
    private final TestDoor.Asserts asserts;

    /** The body this program plans: a statement's stores decide its target (per-statement routing). */
    private final TypedQuery body;

    /** Where a CSV load's file is read at planning, for its reference's digest and size (data:
     *  docs/DATA_IN_THE_PLAN_2026_11_09.md L1, 2026-11-09; a resolver function to a local path until then). */
    private final com.legend.executionplan.DataStores stores;

    TestKinds(TypedQuery body, String runtime, TestDoor.Asserts asserts, com.legend.executionplan.DataStores stores) {
        this.scan = new QueryDoor.Scan(body, runtime);
        this.asserts = asserts;
        this.body = body;
        this.stores = stores;
    }

    /** A statement reading the system metamodel store alone, in a program whose target is the user's: the graph's
     *  system database beside it (per-statement routing, the walker-deletion program, 2026-11-03). */
    @Override
    public ExecutionPlan.@com.legend.base.Nullable Target targetOf(ProgramState program, int index,
            TypedSpec statement) {
        if (program.target().database() instanceof ExecutionPlan.Database.System
                || BodyClass.storesOf(statement, body, index) != BodyClass.Stores.SYSTEM) {
            return null;
        }
        return com.legend.planner.PlanMaker.systemBeside(program.target(), body.context());
    }

    /** The statements of the asserts' steps, as their judge planned them before the program (the statements' in
     *  order). */
    @Override
    public List<ExecutionPlan.Node> plannedAhead() {
        List<ExecutionPlan.Node> out = new java.util.ArrayList<>();
        for (int i = 0; i < body.body().size(); i++) {
            List<? extends TestPlan.TestStep> steps = asserts.at(i);
            if (steps != null) {
                steps.forEach(step -> out.addAll(TestPlan.statements(step)));
            }
        }
        return out;
    }

    /** The verdict steps this program placed, by their handle in the planner's program (the plan-as-data design's step
     *  3, 2026-11-08: the planner holds a test's step by its handle and the statements it reads alone). */
    private final List<TestPlan.TestStep> extensions = new ArrayList<>();

    /** The verdict step placed under {@code handle}. */
    TestPlan.TestStep extension(int handle) {
        return extensions.get(handle);
    }

    @Override
    public boolean plan(ProgramState program, int index, TypedSpec statement, boolean last) {
        EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
        kinds.remove(QueryDoor.TestOnly.INLINE_SEEDS);
        kinds.remove(QueryDoor.TestOnly.RUNTIME_SEEDS);
        if (!kinds.contains(QueryDoor.TestOnly.EFFECT)) {
            // any other statement closes the open script (the walk's segment rule: the run's script is sent when its
            // segment ends -- the walk establishes the next statement's contexts first, the door closes first: a
            // statement naming test data after an open script is refused by the class rules, the M5 audit's B1)
            close(program);
        }
        // the statement's seeds, as the walk establishes its contexts before every statement (M3e)
        seed(program, statement);
        if (statement instanceof TypedLet eagerLet && !last) {
            // an execute(...) a value let reads eagerly (size(execute(...)), through the helpers it calls) runs at the
            // let's turn, as Pure evaluates the let (2026-11-06)
            eagerReads(program, index, eagerLet.value());
        }
        if (!last && statement instanceof TypedNativeCall ec && NativeFn.Handle.runsAFrame(ec.callee().id())) {
            // a FRAME RUN AS A STATEMENT of its own (an unread frame let's execute(...), the user's ruling of
            // 2026-11-02; a program with no assert holds one since 2026-11-06, "do all including last row"): its query
            // run where it stands as a check, its rows dropped, as Pure evaluates it and the walk runs it
            TypedQuery query = program.query();
            PlanEnv env = ProductStatements.env(query.context(), program.runtime(), program.features(),
                    program.frames().planRows());
            FramePlanner.FrameChain fc = FramePlanner.chain(ec, program.lets(), query.specs(), env);
            program.add(eagerRun(program, index, fc, program.frames().newStamp()));
            return true;
        }
        if (kinds.isEmpty()) {
            return false;
        }
        if (statement instanceof TypedLet && kinds.equals(EnumSet.of(QueryDoor.TestOnly.ASSERT))) {
            // a value let whose asserts are a helper's own guards below its root (BodyClass.assertProgram): planned as
            // any value let, spliced into its readers (2026-11-06)
            return false;
        }
        if (kinds.equals(EnumSet.of(QueryDoor.TestOnly.LET_EXECUTE)) && !last) {
            frame(program, index, (TypedLet) statement);
            return true;
        }
        if (kinds.equals(EnumSet.of(QueryDoor.TestOnly.ASSERT)) && !(statement instanceof TypedLet)
                || QueryDoor.assertErrorRoot(statement, program.query().specs())) {
            // a statement-root assert its judge planned (M6), or an assertError (its function's body the step's side,
            // 2026-11-03): its verdict step(s) at its turn -- none where the judge absorbed it into a later statement's
            // groups (the database judge's segment, M7); an execute(...) its sides read eagerly (size(execute(...))) run
            // where it stands first, as a check (2026-11-06)
            // (read through the user functions it calls: the statement as the program plans it, inlined)
            eagerReads(program, index, statement);
            List<? extends TestPlan.TestStep> steps = asserts.at(index);
            if (steps != null) {
                for (TestPlan.TestStep step : steps) {
                    extensions.add(step);
                    program.addExtension(extensions.size() - 1, TestPlan.statements(step));
                }
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
        boolean lazy = !asserts.runsFrames();
        if (lazy && !readByCheck(index, let.name())) {
            // the database judge (M7): the frame is never run at its let; its readers reference its CTE inside the
            // fused statement the asserts' planner composed, and its activity comment is that statement's stamp,
            // which the planner names and the fused step binds -- adopted here so every statement's lowering binds it
            String minted = asserts.frameStamp(index);
            if (minted != null) {
                frames.adoptStamp(minted);
            }
            return;
        }
        TypedQuery query = program.query();
        PlanEnv env = ProductStatements.env(query.context(), program.runtime(), program.features(), frames.planRows());
        FramePlanner.FrameChain fc = FramePlanner.chain(ec, program.lets(), query.specs(), env);
        frames.consumed(fc.env().queryLets());
        // the run's stamp: the asserts' planner's name where its sides read the frame's activity comment (M9c), the
        // program's own otherwise
        String minted = asserts.frameStamp(index);
        String stamp = minted != null ? frames.adoptStamp(minted) : frames.newStamp();
        PlanAllocations.registerActivityRows(fc.source(), fc.chain(), program.lets(), query.specs(), null, stamp, env);
        if (!lazy) {
            frameRuns.add(program.size());
            program.add(eagerRun(program, index, fc, stamp));
        }
        // a frame the database judge does not run at its let but a CHECK reads (a println: a statement that is no
        // assert, 2026-11-06): registered for its readers as the host road's frames are, LAZY -- the check's own
        // statement runs it and binds its stamp as its own (no run at the let: the walk sent none either); the asserts
        // still read it inside their fused statement
        FramePlanner.Cte cte = fc.legendQuery() ? FramePlanner.Cte.NONE : cte(program, index, fc);
        frames.let(let.name(), new ProgramFrames.Frame(fc.chain(),
                !fc.legendQuery() && fc.relationRooted(), fc.source(), fc.env().tableReplace(), cte,
                lazy ? stamp : null));
    }

    /** The bytes the test-input resource {@code path} names (a Pure resource path: its leading {@code /} the
     *  resources' root) and their reference, read once at planning for the digest; refused by name where the run was
     *  given no store for resources or the file is missing (before anything is sent). */
    private Resource resource(String path) {
        String name = path.startsWith("/") ? path.substring(1) : path;
        byte[] bytes;
        try {
            bytes = stores.read(com.legend.executionplan.DataRef.Namespace.RESOURCE, name);
        } catch (IllegalStateException | java.io.UncheckedIOException e) {
            throw new com.legend.error.DataError("test resource '" + path + "': " + e.getMessage(), e);
        }
        try {
            // upstream's size limit, refused where the file is read for its digest (D-b, 2026-11-09)
            com.legend.base.CsvCells.requireSize(path, bytes, EffectPlanner.SIZE_LIMIT_MEGABYTES);
        } catch (com.legend.base.CsvCells.TooLarge e) {
            throw new com.legend.error.DataError(String.valueOf(e.getMessage()), e);
        }
        return new Resource(new com.legend.executionplan.DataRef(com.legend.executionplan.DataRef.Namespace.RESOURCE,
                name, com.legend.executionplan.DataStores.sha256(bytes), bytes.length,
                com.legend.executionplan.DataRef.Csv.DEFAULT), bytes);
    }

    /** A resource's reference and the bytes it digests. */
    private record Resource(com.legend.executionplan.DataRef ref, byte[] bytes) {
    }

    /** Each {@code size(execute(...))} in {@code statement} (BodyClass.eagerInlineExecute): the execute's query run
     *  where it stands as a check, its rows dropped -- Pure evaluates it, and the splice folds its size to the Result
     *  envelope's 1 (2026-11-06; the walk ran it at the side's planning). */
    private void eagerReads(ProgramState program, int index, TypedSpec statement) {
        // read through the user functions it calls -- the statement inlined, as the program plans it -- only where one
        // reaches a size(execute(...)): inlining a statement reaching none plans nothing here and may raise where its
        // judge's planner does not (2026-11-06: the corpus's testConvertSubqueryPredicates, an inlined helper's union
        // typed against a query specification)
        if (!StatementEffects.reaches(statement, BodyClass::eagerInlineExecute, program.query().specs(),
                new java.util.HashMap<>())) {
            return;
        }
        eagerReadsIn(program, index, new UserCallInliner(program.query().specs()).inlineBody(List.of(statement))
                .get(0));
    }

    private void eagerReadsIn(ProgramState program, int index, TypedSpec statement) {
        if (BodyClass.eagerInlineExecute(statement)) {
            TypedSpec arg = ((TypedNativeCall) statement).args().get(0);
            while (arg instanceof com.legend.compiler.spec.typed.TypedFrom f) {
                arg = f.source();
            }
            TypedQuery query = program.query();
            PlanEnv env = ProductStatements.env(query.context(), program.runtime(), program.features(),
                    program.frames().planRows());
            FramePlanner.FrameChain fc = FramePlanner.chain((TypedNativeCall) arg, program.lets(), query.specs(), env);
            program.add(eagerRun(program, index, fc, program.frames().newStamp()));
            return;
        }
        for (TypedSpec c : statement.children()) {
            eagerReadsIn(program, index, c);
        }
    }

    /** Whether a CHECK after the frame let at {@code index} reads the frame {@code name} -- a statement that is no let
     *  and has no test-only kind (a {@code println}), reading the frame or a let after it whose value reads it. */
    private boolean readByCheck(int index, String name) {
        List<TypedSpec> statements = body.body();
        Set<String> names = new HashSet<>(Set.of(name));
        for (int i = index + 1; i < statements.size(); i++) {
            TypedSpec s = statements.get(i);
            if (s instanceof TypedLet l) {
                if (!Collections.disjoint(com.legend.compiler.spec.typed.FreeVars.of(l.value()), names)) {
                    names.add(l.name());
                }
                continue;
            }
            EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(s);
            kinds.remove(QueryDoor.TestOnly.INLINE_SEEDS);
            kinds.remove(QueryDoor.TestOnly.RUNTIME_SEEDS);
            if (kinds.isEmpty() && !Collections.disjoint(com.legend.compiler.spec.typed.FreeVars.of(s), names)) {
                return true;
            }
        }
        return false;
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
     * the program's runtime -- planned as a {@link ExecutionPlan.Load} step before it, unless the program placed
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
        CsvSeed.Planned planned = generated.computeIfAbsent(sources,
                s -> Seeds.planned(statement, program.runtime(), ctx, program.dialect()));
        if (planned.steps().isEmpty()) {
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
        ExecutionPlan.Load setup = com.legend.planner.LoadPlanner.load(ExecutionPlan.Load.Cadence.BEFORE_STEP,
                planned.steps(), program.target());
        program.add(setup);
        // the referee's own copy, written in its dialect (A2), the edge's data
        List<String> texts = planned.refereeCopy();
        seeds.put(setup, () -> texts);
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
        List<Prepared.Effect> effects = new ArrayList<>();
        // a SEED FORM mapped over an effect, read as the class rule read it: on the statement as spelled, its source
        // through the let that binds it (Seeds.seedForm), its lambda one effect the door writes (oneEffectMapper)
        TypedMap raw = statement instanceof TypedMap tm && Seeds.oneEffectMapper(tm, program.lets()) ? tm : null;
        Seeds.SeedForm form = raw == null ? null : Seeds.seedForm(raw, program.lets());
        // or a DDL text collection mapped over it (the aggregation-aware setUp; the corpus burn-down, 2026-11-06)
        List<TypedSpec> ddl = raw == null || form != null ? null : Seeds.ddlTextSource(raw, program.lets());
        List<String> texts = form != null ? form.texts(program.query().context(), program.dialect())
                : ddl != null ? Seeds.ddlTexts(ddl, program.lets(), program.query().context()) : null;
        if (st.prepared() instanceof Prepared.Effect e) {
            effects.add(e);
        } else if (st.prepared() instanceof Prepared.EffectfulMap && raw != null && texts != null) {
            // ($sqls->map(sql | executeInDb($sql, $connection)); the test-is-the-product plan's M10c item 7c, M9's T9
            // kind): the texts the form spells, each bound into the RAW lambda's statement (one inline and resolution,
            // in prepare) and prepared as the effect it is -- the walk's effectfulMap, statement by statement, in one
            // segment
            String param = raw.mapper().parameters().get(0);
            for (String text : texts) {
                TypedSpec bound = UserCallInliner.bindStringParam(raw.mapper().body().get(0), param, text);
                List<TypedSpec> one = new ArrayList<>(program.lets());
                one.add(bound);
                ProductStatements.Statement bst = ProductStatements.prepare(program.query(), one, program.runtime(),
                        index, program.features());
                if (!(bst.prepared() instanceof Prepared.Effect be)) {
                    throw new NotImplementedException("a text form's statement bound into its map's lambda prepared as "
                            + bst.prepared().getClass().getSimpleName() + " on the test door's program road");
                }
                effects.add(be);
            }
        } else {
            throw new NotImplementedException("an effect under an expression of another kind ("
                    + st.prepared().getClass().getSimpleName() + ") on the test door's program road: an effect is"
                    + " planned as a statement of its own, or a seed form whose data folds mapped over the one effect the"
                    + " door writes (item 7c); any other effectful map is the T9 kind the test-is-the-product plan's M9"
                    + " plans");
        }
        NativeFn.Effect effect = null;
        for (Prepared.Effect e : effects) {
            TypedNativeCall call = e.call();
            effect = NativeFn.Effect.of(call.callee().id()).orElseThrow();
            List<String> values = new ArrayList<>();
            for (TypedSpec arg : EffectPlanner.stringArgs(e.call())) {
                if (!(Literals.fold(arg, e.body().subList(0, e.body().size() - 1))
                        instanceof String folded)) {
                    throw new NotImplementedException("a computed String argument of "
                            + e.call().callee().qualifiedName() + ": a SQL text built at run time is a text hole, which"
                            + " lite does not have -- write a computed value with a store write (the user's ruling,"
                            + " 2026-11-04)");
                }
                values.add(folded);
            }
            if (effect == NativeFn.Effect.LOAD_CSV_TO_DB_TABLE) {
                // a CSV load is a LOAD at its place (docs/DATA_IN_THE_PLAN_2026_11_09.md L1, 2026-11-09): the file by
                // its reference, never its path or its rows; the effects before it are their own step
                load(program, e, resource(values.get(0)));
                continue;
            }
            // a seed form's statements stay the effect script's (the M1 correctness audit's C3, 2026-11-09: as a load
            // they lost the script's transaction bracket and abort, its ledger on failure and the B8 refusal; their
            // move to a load is designed with M7-seed)
            script.addAll(EffectPlanner.statements(e.call(), values, e.body(),
                    e.env().withDialect(program.dialect())));
        }
        if (last) {
            close(program);
            // the walk's arms' value: the DDL that drops and creates answers true, the others no value (a mapped seed
            // form answers its last statement's: none)
            boolean dropsAndCreates = effect == NativeFn.Effect.DROP_AND_CREATE_TABLE_IN_DB
                    || effect == NativeFn.Effect.DROP_AND_CREATE_SCHEMA_IN_DB;
            TypedSpec value = statement instanceof TypedLet l ? l.value() : statement;
            program.add(ProductStatements.asATestAnswers(dropsAndCreates ? ProductStatements.scalar("Boolean", "true")
                    : ProductStatements.scalar(value.info().type().typeName(), null)));
        }
    }

    /** The effect steps this program planned, each with its segment and ledger texts. */
    Map<ExecutionPlan.Node, TestDoor.Effect> effects() {
        return Collections.unmodifiableMap(effects);
    }

    /** The seed steps this program planned, each with its statements' ledger texts. */
    Map<ExecutionPlan.Node, java.util.function.Supplier<List<String>>> seeds() {
        return Collections.unmodifiableMap(seeds);
    }

    /** The places of the frame lets' eager-run steps this program planned (M9). */
    Set<Integer> frameRuns() {
        return Collections.unmodifiableSet(frameRuns);
    }

    /** A CSV file's load {@code e}, placed after the open script as a load of its own, run every time it is reached
     *  ({@code AT_STEP}), the referee's copy of its rows beside it as a seed's (a file of no row: the database reads
     *  none). Its identity: the file's digest and the statements it is delivered by. */
    private void load(ProgramState program, Prepared.Effect e, Resource resource) {
        EffectPlanner.PlannedLoad planned = EffectPlanner.loadCsv(e.call(), resource.ref(), resource.bytes(),
                e.body(), e.env().withDialect(program.dialect()));
        close(program);
        ExecutionPlan.Load load = com.legend.planner.LoadPlanner.load(ExecutionPlan.Load.Cadence.AT_STEP,
                List.of(planned.step()), program.target());
        program.add(load);
        seeds.put(load, () -> List.of(planned.recorded().get()));
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
        effects.put(effect, new TestDoor.Effect(script.stream()
                .map(s -> new TestDoor.Recorded(s.recorded(), RawSql.isSingleQuery(s.executed()))).toList()));
        script.clear();
    }
}
