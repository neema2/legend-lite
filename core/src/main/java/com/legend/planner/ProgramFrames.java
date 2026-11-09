// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.base.Nullable;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.ResultEnvelopeSplice;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.NotImplementedException;
import com.legend.plan.PlanRows;
import com.legend.sql.AliasPrefix;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;

import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A PROGRAM'S FRAMES (the test-is-the-product plan's M3f, brief B12 items 8 and 10; D15, B10): the results of the
 * {@code execute(...)} calls a program reads, as the product's statement road splices them. A frame's readers paste its
 * chain where they read it (a CTE where one statement reads it more than once is M3g's); its activity rows record the
 * SQL of its query and, as their comment, the execution-trace stamp of the statement that runs it -- a plan parameter
 * the runner binds when the plan runs (R17 C4), never a text the planner writes:
 * <ul>
 *   <li>a frame bound by a let (the test door's eager run, the test-planning library's) records the stamp of the
 *       {@code Check} step that runs it at the let;</li>
 *   <li>a frame read inline and lazily (both doors) is run by the statement that reads it, which binds its own stamp
 *       ({@code ExecutionPlan.Sql.stamp}).</li>
 * </ul>
 * One object per program (and per query on the query door): the plan rows the frames register, their stamp names.
 * An {@code execute(...)} run where it stands (its result the value asked for) is planned at M9 (the plan's §7),
 * refused by name until then.
 */
public final class ProgramFrames {

    /** A frame bound by a let: its chain (from-wrapped, unresolved: its readers compose over it), whether its query's
     *  root is relation-shaped, the {@code execute(...)} call it came from, the table renames its runtime runs its
     *  query under; {@code lazyStamp}, for a frame NOT run at its let (the database judge's, read by a check:
     *  2026-11-06), the stamp its activity rows record -- a statement reading it runs it and binds that stamp as its
     *  own, as an inline run's reader does -- null for a frame run at its let. */
    public record Frame(TypedSpec chain, boolean relationRooted, TypedNativeCall source,
            Map<String, String> tableReplace, FramePlanner.Cte cte, @Nullable String lazyStamp) {
        public Frame {
            tableReplace = Collections.unmodifiableMap(new LinkedHashMap<>(tableReplace));
        }

        /** Whether a statement may read this frame as its planned CTE: a relation frame's plan, or a plain class
         *  frame's extent rows (the walk's rule); any other is pasted. */
        boolean readsAsCte() {
            return cte.plan() != null && (relationRooted || cte.extentRows() != null);
        }
    }

    /** What ONE statement read of the program's frames, frozen once its inlining ended: the stamp it binds itself
     *  (an inline lazy frame it runs; null when it runs none) and the CTE definitions it references, by CTE name, in
     *  first-read order (the frames it reads more than once, M3g). A value: nothing reads "the last statement's"
     *  facts from the program. */
    public record Reads(@Nullable String ownStamp, Map<String, SqlQuery> definitions) {
        public Reads {
            definitions = Collections.unmodifiableMap(new LinkedHashMap<>(definitions));
        }
    }

    private final Map<String, Frame> frames = new LinkedHashMap<>();
    /** The program's handle and activity rows, shared by its statements. */
    private final Map<String, Map<String, List<List<String>>>> planRows = new LinkedHashMap<>();
    /** Every stamp parameter minted, in order: each statement's lowering binds them all. */
    private final List<String> stamps = new ArrayList<>();
    /** The lets the frames' chains consumed (the query lambda's own, a graph-tree qualifier's argument keeping its
     *  spelling): a statement that reads one free -- a frame's eager run, its CTE, a reader -- binds it (the M3 audit's
     *  B6; the walk's environment shared them across the body). */
    private final Map<String, TypedSpec> consumed = new LinkedHashMap<>();
    ProgramFrames() {
    }

    /** The prefix of a stamp parameter's name (a name no Pure parameter can spell, as {@code ExecutionPlan.COUNT}). */
    public static final String STAMP = "#stamp";

    /** A new stamp parameter's name. */
    public String newStamp() {
        String name = STAMP + stamps.size();
        stamps.add(name);
        return name;
    }

    /** {@code name}, a stamp parameter minted outside this program (the asserts' planner's, for a frame let whose
     *  activity rows its sides read: the test-is-the-product plan's M9c), adopted as one of the program's: every
     *  statement's lowering binds it, and the frame's eager run binds its value. The name spells the stamp prefix. */
    public String adoptStamp(String name) {
        if (!name.startsWith(STAMP)) {
            throw new IllegalArgumentException("a stamp parameter spells the prefix '" + STAMP + "': " + name);
        }
        if (!stamps.contains(name)) {
            stamps.add(name);
        }
        return name;
    }

    /** The frame bound by the let {@code name}. */
    public void let(String name, Frame frame) {
        frames.put(name, frame);
    }

    /** The lets a frame's chain consumed ({@code FramePlanner.FrameChain.env().queryLets()}), kept for the statements
     *  that read them free -- the LATEST frame's binding of a name wins, as on the walk, whose frames write one shared
     *  environment's query lets ({@code ExecuteChainAssembly.chain}'s {@code putAll}); two frames whose query lambdas
     *  bind one name are the walk's by the class rule (its earlier frame's readers read the later binding: the walk's
     *  defect, not copied -- {@code BodyClass.frameProgram}; the M10c item 12 audit's F1), so the frames a program
     *  plans never bind one name two ways. */
    public void consumed(Map<String, TypedSpec> lets) {
        consumed.putAll(lets);
    }

    /** The lets the frames' chains consumed. */
    Map<String, TypedSpec> consumedLets() {
        return Collections.unmodifiableMap(consumed);
    }

    /** The let {@code alias} reads the same frame as the let {@code of} ({@code $r.values->at(0)}). */
    public void alias(String alias, String of) {
        frames.put(alias, Objects.requireNonNull(frames.get(of), () -> "no frame named " + of));
    }

    /** Whether the let {@code name} is a relation-rooted frame (the let kinds' alias test). */
    public boolean relationFrame(String name) {
        Frame f = frames.get(name);
        return f != null && f.relationRooted();
    }

    /** The program's plan rows (handles, activities): every statement's environment shares them. */
    public Map<String, Map<String, List<List<String>>>> planRows() {
        return planRows;
    }

    /** The stamp parameters every statement's lowering binds (as a declared parameter is bound). */
    public List<SqlExpr.PlanParam> stampSlots() {
        List<SqlExpr.PlanParam> out = new ArrayList<>(stamps.size());
        for (String s : stamps) {
            // a String[1], typed as a declared parameter is (the comment column's type)
            out.add(new QueryParameters.Declared(s, Type.Primitive.STRING,
                    Multiplicity.Bounded.ONE).slot());
        }
        return out;
    }

    /** The renames each frame runs its query under, by the frame's name ({@link FramePlanner#renames}). */
    Map<String, Map<String, String>> renames() {
        Map<String, Map<String, String>> out = new LinkedHashMap<>();
        frames.forEach((name, f) -> out.put(name, f.tableReplace()));
        return out;
    }

    /** The plan rows a new statement sees: the program's, and its own inline runs' in front of them. */
    StatementRows statementRows() {
        return new StatementRows(planRows);
    }

    /** The recorder of a statement after {@code letPrefix}, planned on {@code env} whose plan rows are {@code rows},
     *  for its first inlining. */
    Recorder recorder(List<TypedSpec> letPrefix, SpecCompiler specs, PlanEnv env, StatementRows rows) {
        return new Recorder(letPrefix, specs, env, rows, null, Set.of());
    }

    /**
     * The plan rows ONE statement sees: the program's, and in front of them the activity rows of the inline runs it
     * makes -- its own, gone with it (an inline run records the stamp of the statement that runs it: a later statement
     * that runs the same call site registers its own; the M3 audit's B7d, MR's audit S1). Every other registration (a
     * handle's rows, met while resolving) is the program's. Owned by the statement's preparation.
     */
    static final class StatementRows extends AbstractMap<String, Map<String, List<List<String>>>> {
        private final Map<String, Map<String, List<List<String>>>> program;
        private final Map<String, Map<String, List<List<String>>>> ownRows = new LinkedHashMap<>();
        private final Set<String> ownSites = new HashSet<>();

        private StatementRows(Map<String, Map<String, List<List<String>>>> program) {
            this.program = program;
        }

        /** The rows registered next for {@code site} are this statement's own. */
        void markOwn(String site) {
            ownSites.add(site);
        }

        /** This statement's own rows forgotten: a second inlining of it registers them again. */
        void forgetOwn() {
            ownRows.clear();
            ownSites.clear();
        }

        @Override
        public @Nullable Map<String, List<List<String>>> get(Object key) {
            Map<String, List<List<String>>> own = ownRows.get(key);
            return own != null ? own : program.get(key);
        }

        @Override
        public boolean containsKey(Object key) {
            return ownRows.containsKey(key) || program.containsKey(key);
        }

        @Override
        public @Nullable Map<String, List<List<String>>> put(String key, Map<String, List<List<String>>> value) {
            return ownSites.contains(key) ? ownRows.put(key, value) : program.put(key, value);
        }

        @Override
        public Set<Map.Entry<String, Map<String, List<List<String>>>>> entrySet() {
            // the program's rows, then this statement's own (the order a shared map held them in)
            Map<String, Map<String, List<List<String>>>> all = new LinkedHashMap<>(program);
            all.putAll(ownRows);
            return Collections.unmodifiableMap(all).entrySet();
        }
    }

    /** How many times {@code chain} (by identity: a pasted read places the frame's chain itself) stands in
     *  {@code body}. */
    private static int pastes(List<TypedSpec> body, TypedSpec chain) {
        int n = 0;
        ArrayDeque<TypedSpec> todo = new ArrayDeque<>(body);
        while (!todo.isEmpty()) {
            TypedSpec t = todo.pop();
            if (t == chain) {
                n++;
                continue;
            }
            todo.addAll(t.children());
        }
        return n;
    }

    /**
     * ONE INLINING'S READS of the program's frames -- the splice hook the inliner calls, re-entrantly, as it inlines a
     * statement. A recorder with one owner, the statement's preparation, which freezes it ({@link #reads}) when the
     * inlining ends; a second inlining of the statement (its frames read as their CTE) is a recorder of its own
     * ({@link #again}). The program's facts it adds to -- a stamp minted, an inline run's activity rows -- are the
     * program's, kept by {@link ProgramFrames}.
     */
    final class Recorder implements ResultEnvelopeSplice.Frames {
        private final List<TypedSpec> letPrefix;
        private final SpecCompiler specs;
        private final PlanEnv env;
        private final StatementRows rows;
        /** The frames this inlining reads by reference to their CTE (the statement reads each more than once). */
        private final Set<String> asCte;
        /** The stamp this statement binds itself (an inline lazy frame it runs), or null. */
        private @Nullable String ownStamp;
        /** The frames this inlining asked for, in first-ask order (the hook is asked by every reader of a frame). */
        private final Set<String> asked = new LinkedHashSet<>();
        /** The CTE definitions this inlining references, by CTE name, in first-read order. */
        private final Map<String, SqlQuery> definitions = new LinkedHashMap<>();
        private Recorder(List<TypedSpec> letPrefix, SpecCompiler specs, PlanEnv env, StatementRows rows,
                @Nullable String ownStamp, Set<String> asCte) {
            this.letPrefix = letPrefix;
            this.specs = specs;
            this.env = env;
            this.rows = rows;
            this.ownStamp = ownStamp;
            this.asCte = Collections.unmodifiableSet(new LinkedHashSet<>(asCte));
        }

        /** What this inlining read, frozen. */
        Reads reads() {
            return new Reads(ownStamp, definitions);
        }

        /** The recorder of a second inlining of the statement, the frames this one's {@code body} pastes more than
         *  once read as their CTE (B10's rule, per statement: a frame read once is pasted where it is read); null when
         *  none is (one inlining suffices). A read is a paste of the frame's chain in {@code body}, the statement as
         *  first inlined -- the hook is asked for a frame by its other readers too (its activities, its SQL text, its
         *  size), which paste nothing (the M3 audit's B4). The second inlining binds the same stamp. */
        @Nullable Recorder again(List<TypedSpec> body) {
            Set<String> twice = new LinkedHashSet<>();
            for (String name : asked) {
                Frame f = frames.get(name);
                if (f != null && f.readsAsCte() && pastes(body, f.chain()) > 1) {
                    twice.add(name);
                }
            }
            if (twice.isEmpty()) {
                return null;
            }
            rows.forgetOwn();
            return new Recorder(letPrefix, specs, env, rows, ownStamp, twice);
        }

        @Override
        public ResultEnvelopeSplice.@Nullable View frame(String name) {
            Frame f = frames.get(name);
            if (f == null) {
                return null;
            }
            asked.add(name);
            if (f.lazyStamp() != null && ownStamp == null) {
                // a frame not run at its let: this statement runs it, its activity comment this statement's stamp
                // (bound only where the statement reads it: PlanMaker.stampedIfRead). A statement binds one stamp: a
                // second such frame it reads keeps its stamp as an earlier reader bound it -- a statement that reads
                // the activity comment of one no statement bound raises at its run, never reads a guess
                ownStamp = f.lazyStamp();
            }
            if (!asCte.contains(name)) {
                return new ResultEnvelopeSplice.View(f.chain(), f.relationRooted(), f.source());
            }
            // read by REFERENCE: its planned CTE, defined ahead of the statement (a class frame's is its extent
            // rows, a relation frame's its plan), its aliases under the frame's own prefix (the walk's)
            String cteName = "frame_" + name;
            definitions.computeIfAbsent(cteName, n -> AliasPrefix.apply(
                    AliasPrefix.frameBody(name), f.cte().extentRows() != null
                            ? f.cte().extentRows() : Objects.requireNonNull(f.cte().plan())));
            return new ResultEnvelopeSplice.View(f.chain(), f.relationRooted(), f.source(), cteName,
                    f.cte().plan(), f.cte().plannedInfo());
        }

        @Override
        public ResultEnvelopeSplice.View inlineExecute(TypedNativeCall ec, boolean eager) {
            if (eager) {
                throw new NotImplementedException("an execute(...) run where it stands, its"
                        + " result the value asked for: planned at the test-is-the-product plan's M9; a query"
                        + " reads an execute(...)'s result through it (->values)");
            }
            FramePlanner.FrameChain fc = FramePlanner.chain(ec, letPrefix, specs, env);
            if (ownStamp == null) {
                ownStamp = newStamp();
            }
            // this statement's run of the call: its activity rows, its own, record this statement's stamp (a site a
            // frame let registered keeps the let's rows)
            String scope = PlanRows.scopeId(fc.source());
            if (!rows.containsKey(scope)) {
                rows.markOwn(scope);
                PlanAllocations.registerActivityRows(fc.source(), fc.chain(), letPrefix, specs, null, ownStamp, env);
            }
            return new ResultEnvelopeSplice.View(fc.chain(), !fc.legendQuery() && fc.relationRooted(),
                    fc.source());
        }

        @Override
        public @Nullable String relationalActivitySql(String frameName, long activityNumber) {
            Frame f = frames.get(frameName);
            if (f == null || activityNumber != 0) {
                // single-statement execution: exactly one RelationalActivity per execute; other indices fall to
                // the activity ROWS
                return null;
            }
            return PlanAllocations.activitySql(f.source(), f.chain(), letPrefix, specs, env);
        }

        @Override
        public @Nullable String relationalActivitySql(TypedNativeCall ec) {
            return PlanAllocations.activitySql(ec, FramePlanner.chain(ec, letPrefix, specs, env).chain(),
                    letPrefix, specs, env);
        }
    }
}
