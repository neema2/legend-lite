// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.compiler.element.type.ExprType;
import com.legend.sql.SqlQuery;
import com.legend.sql.dialect.SqlDialect;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** LEG 3.4 — ONE VERDICT STATEMENT PER TEST BODY (docs/DATABASE_MODE_HOMEWORK
 * _2026_09_18.md §3 D1 stage B, the V12 spike's ratified rung). In database
 * mode a statement-root assert no longer sends its verdict statement at
 * the assert: the statement is DEFERRED into this per-body batch, keyed by
 * the assert's position, and the batch is sent as ONE statement (the
 * lowering's {@link Fusion}, a {@code UNION ALL} of the deferred
 * statements) when the body reaches a statement that is not an assert, or
 * ends. Verdicts are then reported in body order and the FIRST failure
 * raises exactly as before (the runner's first-failure sequencing and gate
 * 11's per-assert ledger keys are unchanged); a root's own steps (an
 * unrolled quantified assert is several statements, a static kind gate is
 * a decided outcome) decide in their order. THE SPLIT RUNG: when the fused
 * statement itself errors (a side the database rejects), the batch is
 * judged statement by statement — today's path, so the error lands on the
 * assert that owns it — counted, never a verdict change. This class sends
 * and orders; the SQL shape is the lowering's and the judgment of a row is
 * the verdict arm's ({@link Judge}). */
public final class VerdictBatch {

    /** The lowering's fused statement over the deferred statements, with
     * the frame definitions the sides reference at its head. */
    @FunctionalInterface
    public interface Fusion {
        SqlQuery fuse(List<SqlQuery> statements, Map<String, SqlQuery> frames);
    }

    /** The verdict arm's reading of one row ({@code verdict, expected,
     * actual, unjudged, lenient}): returns on a held verdict, raises the
     * assert's own failure otherwise. */
    @FunctionalInterface
    public interface Judge {
        void judge(String name, boolean wantEqual, List<Object> row);
    }

    /** A deferred verdict statement, or an outcome the arm decided
     * without one (a static kind gate, an UNJUDGED decline, a side that
     * errored while planning). */
    private sealed interface Step permits Pending, Resolved {
    }

    private record Pending(int ix, String name, boolean wantEqual, SqlQuery query,
            Connection on, @com.legend.base.Nullable Appeal appeal) implements Step {
    }

    /** A verdict row's APPEAL (block-compiler rung 2a): run at the flush when the
     * row FAILED — returns on a pass by another judgment (the SQL-text referee's
     * rows), raises its own failure otherwise. Never consulted on a held row. */
    public interface Appeal {
        ExecutionResult appeal();
    }

    private record Resolved(RuntimeException failure) implements Step {
    }

    private static final class Root {
        final String listenerName;
        final List<Step> steps = new ArrayList<>();

        Root(String listenerName) {
            this.listenerName = listenerName;
        }
    }

    /** CENSUS (printed by the corpus lanes): fused statements sent, and the
     * batches that fell to the split rung. */




    /** CENSUS: frames built under a batch, by how the asserts read them —
     * {@code cte} (a relation-rooted frame of static schema, planned once),
     * {@code pasted} (relation-rooted but late-bound or not eager), {@code
     * class} (a class- or scalar-rooted frame: the chain pastes). */
    public enum FrameRead { CTE, PASTED, CLASS, CLASS_CTE }


    public static void frame(FrameRead how) {
        switch (how) {
            case CTE -> Census.inc(Census.Key.FRAME_CTE);
            case PASTED -> Census.inc(Census.Key.FRAME_PASTED);
            case CLASS -> Census.inc(Census.Key.FRAME_CLASS);
            case CLASS_CTE -> Census.inc(Census.Key.FRAME_CLASS_CTE);
        }
    }

    public static String frameCensus() {
        return "cte=" + Census.count(Census.Key.FRAME_CTE) + " pasted=" + Census.count(Census.Key.FRAME_PASTED)
                + " class=" + Census.count(Census.Key.FRAME_CLASS) + " class-cte=" + Census.count(Census.Key.FRAME_CLASS_CTE);
    }

    private final Fusion fusion;
    private final ExprType fusedShape;
    private final ExprType oneRow;
    private final Judge judge;
    private final List<Root> roots = new ArrayList<>();
    private @com.legend.base.Nullable Root current;
    /** The frames the body's asserts read by reference: name → the frame's
     * plan (leg 3.4 step 2), defined once per body. */
    private final Map<String, SqlQuery> frames = new LinkedHashMap<>();

    public VerdictBatch(Fusion fusion, ExprType fusedShape, ExprType oneRow, Judge judge) {
        this.fusion = fusion;
        this.fusedShape = fusedShape;
        this.oneRow = oneRow;
        this.judge = judge;
    }

    /** A statement-root assert begins: its steps accrue until {@link #close}. */
    public void open(String listenerName) {
        if (current != null) {
            throw new IllegalStateException("verdict batch: a root is already open");
        }
        current = new Root(listenerName);
        roots.add(current);
    }

    public boolean active() {
        return current != null;
    }

    public void defineFrame(String name, SqlQuery plan) {
        // the body's aliases under the frame's own prefix: the fused statement
        // keeps the renderers' invariant that an alias is unique statement-wide
        plan = com.legend.sql.AliasPrefix.apply(com.legend.sql.AliasPrefix.frameBody(name), plan);
        SqlQuery prev = frames.putIfAbsent(name, plan);
        if (prev != null && !prev.equals(plan)) {
            throw new IllegalStateException("verdict batch: two plans under the frame '" + name + "'");
        }
    }

    public Map<String, SqlQuery> frames() {
        return java.util.Collections.unmodifiableMap(frames);
    }

    public void defer(String name, boolean wantEqual, SqlQuery query, Connection on) {
        defer(name, wantEqual, query, on, null);
    }

    public void defer(String name, boolean wantEqual, SqlQuery query, Connection on,
            @com.legend.base.Nullable Appeal appeal) {
        Root r = java.util.Objects.requireNonNull(current, "verdict batch: no open root");
        int ix = 0;
        for (Root x : roots) {
            for (Step s : x.steps) {
                if (s instanceof Pending) {
                    ix++;
                }
            }
        }
        r.steps.add(new Pending(ix, name, wantEqual, query, on, appeal));
    }

    /** The verdict rows deferred so far (the next row's index). */
    public int pendingCount() {
        int ix = 0;
        for (Root x : roots) {
            for (Step s : x.steps) {
                if (s instanceof Pending) {
                    ix++;
                }
            }
        }
        return ix;
    }

    /** THE FRAGMENT MAP (block-compiler stage 2, 2026-09-21): every frame name and
     * verdict branch index → the let / assert it came from, so a database error
     * on the fused statement names the statement that owns it. */
    private final Map<String, String> fragments = new java.util.LinkedHashMap<>();

    public void fragments(Map<String, String> map) {
        fragments.putAll(map);
    }

    /** The fragments a database message names (a frame alias appears in DuckDB's
     * binder errors and H2's column errors), or an empty string. */
    public String attribute(String message) {
        StringBuilder sb = new StringBuilder();
        for (var e : fragments.entrySet()) {
            if (e.getKey().startsWith("frame_") && message.contains(e.getKey())) {
                sb.append(sb.length() == 0 ? " — near " : ", ").append(e.getValue());
            }
        }
        return sb.toString();
    }

    public void resolve(RuntimeException failure) {
        java.util.Objects.requireNonNull(current, "verdict batch: no open root")
                .steps.add(new Resolved(failure));
    }

    public void close() {
        current = null;
    }

    /** The open root was not a verdict after all (the arm answered null,
     * or left through a wall): dropped with its steps. */
    public void discard() {
        java.util.Objects.requireNonNull(current, "verdict batch: no open root");
        roots.remove(roots.size() - 1);
        current = null;
    }

    public boolean isEmpty() {
        return roots.isEmpty();
    }

    /** Send the deferred statements (one fused statement per session), then
     * report every root in body order; the first failure raises. */
    public void flush(SqlDialect dialect, ExecutionTrace trace,
            @com.legend.base.Nullable AssertListener l) {
        if (!roots.isEmpty()) {
            Census.inc(Census.Key.VERDICT_FLUSHES);
        }
        if (roots.isEmpty()) {
            return;
        }
        List<Root> batch = new ArrayList<>(roots);
        roots.clear();
        current = null;
        Map<Connection, List<Pending>> groups = new LinkedHashMap<>();
        for (Root r : batch) {
            for (Step s : r.steps) {
                if (s instanceof Pending p) {
                    groups.computeIfAbsent(p.on(), k -> new ArrayList<>()).add(p);
                }
            }
        }
        Map<Integer, List<Object>> rows = new HashMap<>();
        for (var g : groups.entrySet()) {
            List<Pending> ps = g.getValue();
            try {
                for (Pending p : ps) {
                    PrepTrace.branch(p.name(), dialect.render(p.query()).length());
                }
                for (Row row : executeFused(ps.stream().map(Pending::query).toList(),
                        g.getKey(), dialect, trace)) {
                    int local = ((Number) row.values().get(0)).intValue();
                    rows.put(ps.get(local).ix(), row.values().subList(1, row.values().size()));
                }
                Census.inc(Census.Key.VERDICT_FUSED);
            } catch (com.legend.error.DataError | com.legend.sql.dialect.DialectCapability e) {
                // the split rung: this batch judges statement by statement
                // below, so the error lands on the assert that owns it
                Census.inc(Census.Key.VERDICT_FALLBACKS);
                String m = String.valueOf(e.getMessage());
                Census.FALLBACK_REASONS.add(e.getClass().getSimpleName() + ": "
                        + m.substring(0, Math.min(m.length(), 160)).replace('\n', ' ')
                        + attribute(m));
            }
        }
        for (Root r : batch) {
            RuntimeException failure = null;
            if (r.steps.stream().noneMatch(s -> s instanceof Pending)) {
                Census.inc(Census.Key.VERDICT_HOST_DECIDED);
            }
            for (Step s : r.steps) {
                try {
                    if (s instanceof Pending p) {
                        List<Object> row = rows.get(p.ix());
                        if (row == null) {
                            row = executeOne(p.name(), com.legend.sql.FrameCtes.attach(p.query(), frames),
                                    oneRow, p.on(), dialect, trace);
                        }
                        try {
                            judge.judge(p.name(), p.wantEqual(), row);
                        } catch (com.legend.error.AssertFailed e) {
                            if (p.appeal() == null) {
                                throw e;
                            }
                            // rung 2a: the row failed — the appeal decides (returns
                            // on a pass by rows, raises its own failure otherwise)
                            p.appeal().appeal();
                        }
                    } else if (s instanceof Resolved rv) {
                        throw rv.failure();
                    }
                } catch (com.legend.error.AssertFailed | com.legend.error.DataError e) {
                    failure = e;
                    break;
                }
            }
            if (failure == null) {
                if (l != null) {
                    l.verdict(r.listenerName, true, null);
                }
                continue;
            }
            CanonicalDivergence.sqlRaised();
            if (l != null) {
                if (failure instanceof com.legend.error.AssertFailed af && af.unjudgedReason() != null) {
                    l.unjudged(r.listenerName, af.unjudgedReason());
                }
                l.verdict(r.listenerName, false, failure.getMessage());
            }
            throw failure;   // first-failure sequencing, as before
        }
    }

    private List<Row> executeFused(List<SqlQuery> statements, Connection on,
            SqlDialect dialect, ExecutionTrace trace) {
        SqlQuery b = fusion.fuse(statements, frames);
        ExecutionResult r;
        try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.BODY)) {
            r = Executor.execute(dialect.render(b), b, fusedShape,
                ResultShape.TABULAR, on, dialect, trace);
        }
        if (!(r instanceof ExecutionResult.Tabular t) || t.rows().size() != statements.size()) {
            throw new IllegalStateException("the batch verdict statement returned "
                    + (r instanceof ExecutionResult.Tabular t2 ? t2.rows().size() + " rows" : "no grid")
                    + " for " + statements.size() + " asserts");
        }
        return t.rows();
    }

    /** Execute ONE verdict statement on {@code on} and return its one row;
     * a statement the database rejects, or a canon the dialect cannot spell,
     * is counted UNJUDGED with the database's own words and surfaces as
     * itself — never a bare re-run, never a host rescue. */
    public static List<Object> executeOne(String name, SqlQuery vq, ExprType oneRow,
            Connection on, SqlDialect dialect, ExecutionTrace trace) {
        ExecutionResult r;
        try {
            try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.FALLBACK)) {
            r = Executor.execute(dialect.render(vq), vq, oneRow, ResultShape.TABULAR, on,
                    dialect, trace);
            }
        } catch (com.legend.error.DataError e) {
            CanonicalDivergence.sqlUnjudged(name, "statement-error: "
                    + String.valueOf(e.getMessage()).split("\n")[0]);
            throw e;
        } catch (com.legend.sql.dialect.DialectCapability e) {
            CanonicalDivergence.sqlUnjudged(name, "dialect-capability: "
                    + String.valueOf(e.getMessage()).split("\n")[0]);
            throw e;
        }
        if (!(r instanceof ExecutionResult.Tabular t) || t.rows().size() != 1) {
            throw new IllegalStateException(name + ": the verdict statement returned "
                    + (r instanceof ExecutionResult.Tabular t2 ? t2.rows().size() + " rows" : "no grid"));
        }
        return t.rows().get(0).values();
    }
}
