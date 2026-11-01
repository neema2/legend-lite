// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.spec.ResultEnvelopeSplice;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
     *  query under. */
    public record Frame(TypedSpec chain, boolean relationRooted, TypedNativeCall source,
            Map<String, String> tableReplace) {
        public Frame {
            tableReplace = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(tableReplace));
        }
    }

    private final Map<String, Frame> frames = new LinkedHashMap<>();
    /** The program's handle and activity rows, shared by its statements. */
    private final Map<String, Map<String, List<List<String>>>> planRows = new LinkedHashMap<>();
    /** Every stamp parameter minted, in order: each statement's lowering binds them all. */
    private final List<String> stamps = new ArrayList<>();
    /** The stamp the statement being planned binds itself (an inline lazy frame it reads), or null. */
    private @com.legend.base.Nullable String ownStamp;

    ProgramFrames() {
    }

    /** A new stamp parameter's name (a name no Pure parameter can spell, as {@code ExecutionPlan.COUNT}). */
    public String newStamp() {
        String name = "#stamp" + stamps.size();
        stamps.add(name);
        return name;
    }

    /** The frame bound by the let {@code name}. */
    public void let(String name, Frame frame) {
        frames.put(name, frame);
    }

    /** The let {@code alias} reads the same frame as the let {@code of} ({@code $r.values->at(0)}). */
    public void alias(String alias, String of) {
        frames.put(alias, java.util.Objects.requireNonNull(frames.get(of), () -> "no frame named " + of));
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
    public List<com.legend.sql.SqlExpr.PlanParam> stampSlots() {
        List<com.legend.sql.SqlExpr.PlanParam> out = new ArrayList<>(stamps.size());
        for (String s : stamps) {
            // a String[1], typed as a declared parameter is (the comment column's type)
            out.add(new QueryParameters.Declared(s, com.legend.compiler.element.type.Type.Primitive.STRING,
                    com.legend.compiler.element.type.Multiplicity.Bounded.ONE).slot());
        }
        return out;
    }

    /** A statement begins: it binds no stamp of its own yet. */
    void startStatement() {
        ownStamp = null;
    }

    /** The stamp the statement just planned binds itself, or null. */
    @com.legend.base.Nullable String ownStamp() {
        return ownStamp;
    }

    /** The renames each frame runs its query under, by the frame's name ({@link FramePlanner#renames}). */
    Map<String, Map<String, String>> renames() {
        Map<String, Map<String, String>> out = new LinkedHashMap<>();
        frames.forEach((name, f) -> out.put(name, f.tableReplace()));
        return out;
    }

    /** The splice hook's frames for a statement after {@code letPrefix}, planned on {@code env}. */
    ResultEnvelopeSplice.Frames reads(List<TypedSpec> letPrefix, SpecCompiler specs, PlanEnv env) {
        return new ResultEnvelopeSplice.Frames() {
            @Override
            public ResultEnvelopeSplice.@com.legend.base.Nullable View frame(String name) {
                Frame f = frames.get(name);
                return f == null ? null : new ResultEnvelopeSplice.View(f.chain(), f.relationRooted(), f.source());
            }

            @Override
            public ResultEnvelopeSplice.View inlineExecute(TypedNativeCall ec, boolean eager) {
                if (eager) {
                    throw new com.legend.error.NotImplementedException("an execute(...) run where it stands, its"
                            + " result the value asked for: planned at the test-is-the-product plan's M9; a query"
                            + " reads an execute(...)'s result through it (->values)");
                }
                FramePlanner.FrameChain fc = FramePlanner.chain(ec, letPrefix, specs, env);
                if (ownStamp == null) {
                    ownStamp = newStamp();
                }
                PlanAllocations.registerActivityRows(fc.source(), fc.chain(), letPrefix, specs, null, ownStamp, env);
                return new ResultEnvelopeSplice.View(fc.chain(), !fc.legendQuery() && fc.relationRooted(),
                        fc.source());
            }

            @Override
            public @com.legend.base.Nullable String relationalActivitySql(String frameName, long activityNumber) {
                Frame f = frames.get(frameName);
                if (f == null || activityNumber != 0) {
                    // single-statement execution: exactly one RelationalActivity per execute; other indices fall to
                    // the activity ROWS
                    return null;
                }
                return PlanAllocations.activitySql(f.source(), f.chain(), letPrefix, specs, env);
            }

            @Override
            public @com.legend.base.Nullable String relationalActivitySql(TypedNativeCall ec) {
                return PlanAllocations.activitySql(ec, FramePlanner.chain(ec, letPrefix, specs, env).chain(),
                        letPrefix, specs, env);
            }
        };
    }
}
