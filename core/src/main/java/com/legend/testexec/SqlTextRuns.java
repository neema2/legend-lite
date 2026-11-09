// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.testexecutionplan.AssertListener;

import static com.legend.testexecutionplan.Verdicts.ok;
import static com.legend.testexecutionplan.Verdicts.fail;
import static com.legend.testexecutionplan.Verdicts.textsDiffer;
import static com.legend.testexecutionplan.Verdicts.isPopulationGolden;
import static com.legend.testexecutionplan.Verdicts.stripChainedPlanWarning;
import static com.legend.testexecutionplan.Verdicts.scalarString;
import com.legend.exec.StatementOrigin;
import com.legend.testexecutionplan.HostJudgment;
import com.legend.testexecutionplan.TypedSide;
import java.util.List;

final class SqlTextRuns {

    private SqlTextRuns() {
    }


    /** {@code e} judged at its verdict step over the two text sides the runner handed back (the golden a constant of
     *  the plan, checked against the planned text; ours a constant likewise, or the side the step RAN for a deferred
     *  text, M10c item 8 -- its one string is our text, and the equality is computed here as the arm computes it): the
     *  decline reported, then the text decides -- equal holds; else the appeal, the walk's own rows leg, now, with the
     *  texts in hand. */
    static TypedSide judge(HostJudgment.EngineText e, List<TypedSide> sides,
            com.legend.testexecutionplan.TestPlan.JudgingRules rules, BodyRun run) {
        // the appeal is heard by its number, the plan's (step 8b): the run records the texts it read, and the edge
        // decides after the run; the step's rules say whether a failed text is appealed
        boolean compat = rules == com.legend.testexecutionplan.TestPlan.JudgingRules.COMPILED_PURE_COMPAT;
        if (sides.size() != 2) {
            throw new IllegalStateException(e.name() + ": the engine-text step handed " + sides.size()
                    + " sides, two texts expected");
        }
        String golden = scalarString(sides.get(0));
        String ours = scalarString(sides.get(1));
        if (e.golden() != null && !e.golden().equals(golden) || e.ours() != null && !e.ours().equals(ours)) {
            throw new IllegalStateException(e.name() + ": the verdict step's texts are not the texts the arm planned");
        }
        // the decline decided before the texts were compared: reported first, whatever the texts then decide -- once,
        // by the host's half under the agreement judge (its database half reports to no listener); until the slice 3
        // invariant audit (2026-11-08) a golden read at the step that differed from ours reported none
        if (e.decline() != null) {
            declinedTo(run.listener(), e.name(), e.decline());
        }
        if (e.golden() == null) {
            // a golden the plan read (2026-11-04, the core program): the two texts compare first, as any equality;
            // only a failed comparison re-runs the arm over them -- its golden processing and its appeal
            if (golden == null || ours == null) {
                return fail(e.name() + " (sql-text): " + (golden == null ? "the golden" : "our text")
                        + " side, a String to one, read no string at the verdict step");
            }
            if (golden.equals(ours)) {
                return ok();
            }
            if (!compat) {
                return textsDiffer(e.name(), golden, ours, e.decline());
            }
            // compatibility mode: the appeal PLANNED with the assert, over the golden as read (2026-11-06, the plan
            // ahead: until then the arm was re-run over the step's texts while the program ran, planning as it went).
            // The arm planned its route for a golden it could not see; a golden whose text would choose another route
            // -- an engine population statement, a numbered in-list temp -- is refused by name, never re-planned
            String g = stripChainedPlanWarning(golden);
            if (g.equals(ours)) {
                return ok();
            }
            if (com.legend.testexecutionplan.Verdicts.isPopulationGolden(g) || NAMED_IN_TEMP.matcher(g).find()) {
                return fail(e.name() + " (sql-text): a golden read at the verdict step whose text chooses a statement"
                        + " route the plan did not plan (a population or in-list temp statement): " + g);
            }
            return run.hold(e.name(), e.appeal(), g, ours, false);
        }
        boolean textEqual = e.textEqual();
        if (e.ours() == null) {
            if (ours == null) {
                return fail(e.name() + " (sql-text): our text side, a String to one, read no string at the verdict"
                        + " step");
            }
            textEqual = e.golden().equals(ours);
        }
        // the equality arm's census route is the caller's to count once this answered (AssertVerdicts.HostAssert); the
        // decline reported above, the text decides -- equal holds, else the appeal heard, or the texts' failure
        String o = java.util.Objects.requireNonNull(ours);
        return textEqual ? ok() : compat ? run.hold(e.name(), e.appeal(), e.golden(), o, textEqual)
                : textsDiffer(e.name(), e.golden(), o, e.decline());
    }

    /** The ACTUAL text of a failed verdict row (its column by position, {@code RootLoop.ACTUAL_COLUMN}): our text as
     *  the database computed it, or null. */
    private static @com.legend.base.Nullable String actualText(List<Object> row) {
        Object actual = row.get(com.legend.testexecutionplan.RootLoop.ACTUAL_COLUMN);
        return actual == null ? null : String.valueOf(actual);
    }

    /**
     * AN APPEAL DECIDED AFTER THE BODY'S RUN, FROM THE PLAN'S DATA (the plan-as-data design's step 8b, 2026-11-08):
     * {@code a} heard over the texts the run recorded -- a contract's failure, the generator's refusal (S143), or the
     * rows leg: the stores' fixtures provided by the harness, our rows read through the edge's reader (each a plan of
     * its own on the body's sessions, Appeals), the golden replayed by the {@code referee} with the plan's enumerations (no model: hole 1), its verdict
     * the appeal's. Reports go to {@code listener}. Inside a judging scope that plans nothing.
     */
    static TypedSide decideAppeal(com.legend.testexecutionplan.Appeal a, String golden, String ours, boolean textEqual,
            @com.legend.base.Nullable SqlReplayReferee referee,
            com.legend.testexecutionplan.@com.legend.base.Nullable AssertListener listener, SideReader reader,
            java.util.function.Function<com.legend.executionplan.ExecutionPlan.Target, com.legend.exec.Sessions.Session> sessions) {
        String name = a.name();
        return switch (a.kind()) {
            case CONTRACT -> fail(java.util.Objects.requireNonNull(a.contract()) + ": expected " + golden + ", got "
                    + ours);
            case TDG -> {
                // the generator's rows leg planned the generator's transcript and ran it at the verdict -- a run inside
                // the body's run (THE WALL) and planning at run; the generator is not offered (S143), so the leg is
                // refused by name (I3, 2026-11-07: its session and the planning it reached deleted)
                com.legend.base.RunScope.planning("the test-data generator's rows leg");
                throw new IllegalStateException(name + ": the test-data generator's rows leg is not offered (S143)");
            }
            case ROWS -> rowsLegNow(a, golden, ours, textEqual, referee, listener, reader, sessions);
        };
    }

    /** The database judge's failed row's appeal heard (step 8b): the texts its row compared, ours read from the row's
     *  actual cell where the plan reads it, recorded for the decision after the run. */
    static TypedSide rowAppealed(com.legend.testexecutionplan.Appeal a, List<Object> row, BodyRun run) {
        com.legend.testexecutionplan.Appeal.Batched b = java.util.Objects.requireNonNull(a.batched(), "a row appeal with no row's texts");
        String ours = b.ours() != null ? b.ours() : actualText(row);
        if (ours == null) {
            return fail(a.name() + " (sql-text): our text side, a String to one, read no string at the verdict step");
        }
        // a second hearing of an appeal another half holds (the agreement judge's database half, A1): joined to it,
        // never decided twice
        return a.joins() != com.legend.testexecutionplan.RootLoop.NO_APPEAL
                ? run.join(a.name(), a.joins(), b.golden(), ours)
                : run.hold(a.name(), a.number(), b.golden(), ours,
                        b.ours() != null ? b.textEqual() : b.golden().equals(ours));
    }

    /** The rows leg itself, from the plan's data: OUR rows as planned, the golden replayed via the referee, judged. */
    private static TypedSide rowsLegNow(com.legend.testexecutionplan.Appeal a, String golden, String ours, boolean textEqual,
            @com.legend.base.Nullable SqlReplayReferee referee,
            com.legend.testexecutionplan.@com.legend.base.Nullable AssertListener listener, SideReader reader,
            java.util.function.Function<com.legend.executionplan.ExecutionPlan.Target, com.legend.exec.Sessions.Session> sessions) {
        String name = a.name();
        com.legend.testexecutionplan.Appeal.Rows r = a.rows();
        if (r == null) {
            // no fallback (2026-11-06, the plan ahead): an appeal's rows and stores are planned with its assert
            throw new IllegalStateException(name + " (sql-text): an appeal whose rows were not planned with its assert");
        }
        if (referee == null) {
            throw new IllegalStateException(name + " (sql-text): an appeal decided with no replay referee");
        }
        // the stores' fixtures were the caller's to provide before the body's run, from the plan's data
        // (PlannedAppeals.stores; hole 4, 2026-11-08: until then the listener was asked again here, by name)
        com.legend.testexecutionplan.Appeal.Unlisted unlisted = r.rows().unlisted();
        if (unlisted != null) {
            // our rows' planning failed with a class off the closed list: the same decline, in its words
            return underivable(listener, name, unlisted.simpleName(), unlisted.message(), textEqual, golden, ours);
        }
        TypedSide rows;
        boolean priorSuspend = com.legend.testexec.SqlTypeCensus.probeSuspended();
        try {
            com.legend.testexec.SqlTypeCensus.probeSuspend(true);
            try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.REFEREE_OURS)) {
                rows = read(r.rows(), reader);
            }
        } catch (com.legend.base.RunScope.Refused e) {
            // the runtime wall's refusal is never a rows-underivable decline: it fails the body by name
            throw e;
        } catch (RuntimeException e) {
            // the rows leg is underivable -- counted, text stays the contract (§3.7: a counted decline, visible, never
            // silent; DataError joined RuntimeException at the seam)
            return underivable(listener, name, e.getClass().getSimpleName(), e.getMessage(), textEqual, golden, ours);
        } finally {
            com.legend.testexec.SqlTypeCensus.probeSuspend(priorSuspend);
        }
        SqlReplayReferee.ReplayFacts facts = new SqlReplayReferee.ReplayFacts(r.extentSubset(), r.ordered(),
                r.sortKeys());
        if (r.population().side() != null || r.population().failure() != null
                || r.population().unlisted() != null) {
            if (rows != null) {
                // the PAGE-MEMBERSHIP verdict's population (batch 0.5b): our rows for the same chain without its tail
                // page. No catch: the paged read just executed, so a failure here is a FAULT of ours and fails the
                // test loudly -- never a decline that rescues the text
                com.legend.testexec.SqlTypeCensus.probeSuspend(true);
                try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.REFEREE_OURS)) {
                    TypedSide population = read(r.population(), reader);
                    if (population != null) {
                        facts = facts.withPopulation(population);
                    }
                } finally {
                    com.legend.testexec.SqlTypeCensus.probeSuspend(priorSuspend);
                }
            }
        }
        if (rows == null) {
            declinedTo(listener, name, "rows-underivable");
            return textEqual ? ok()
                    : fail(name + " (sql-text, rows underivable):"
                            + " expected " + golden + ", got " + ours);
        }
        List<SqlReplayReferee.TempTable> temps = r.temps().stream()
                .map(t -> new SqlReplayReferee.TempTable(t.name(), t.kind(), t.values())).toList();
        com.legend.testexecutionplan.Appeal.InListSeed seed = r.seed();
        SqlReplayReferee.RowVerdict rv;
        try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.REFEREE_GOLDEN)) {
            rv = r.goldenPlan() != null
                ? referee.verifyPlan(sessions.apply(r.on()), r.goldenPlan(), r.planBindings(),
                        rows, r.mappingFqn(), r.classFqn(), facts, r.enums())
                : referee.verify(sessions.apply(r.on()),
                        r.replaySql() != null ? r.replaySql() : golden,
                        rows, r.mappingFqn(), r.classFqn(), facts, r.enums(),
                        temps.isEmpty() ? (seed == null ? List.of()
                                : inListTemps(seed.kind(), seed.values(), golden)) : temps);
        }
        if (listener != null) {
            listener.refereed(name, rv.outcome().name());
        }
        return switch (rv.outcome()) {
            case MATCH -> {
                // rows are the verdict (§0); text is a census number
                yield ok();
            }
            case DIVERGED -> fail(name + " (sql-text ROW verdict —"
                    + " golden rows vs ours diverged, whatever the text"
                    + " said): " + rv.detail());
            case FAULT -> fail(name + " (sql-text: referee FAULT — the"
                    + " referee's own machinery failed, the text cannot"
                    + " stand in): " + rv.detail());
            case DECLINED -> {
                // referee could not answer: text is the contract,
                // decline counted (§3.7)
                declinedTo(listener, name, "oracle-declined");
                yield textEqual ? ok()
                        : fail(name + " (sql-text, oracle declined: "
                                + rv.detail() + "): expected " + golden
                                + ", got " + ours);
            }
        };
    }

    /** Our rows underivable: the decline counted, the text the contract (§3.7), the failure in its words. */
    private static TypedSide underivable(com.legend.testexecutionplan.@com.legend.base.Nullable AssertListener listener, String name,
            String failedAs, @com.legend.base.Nullable String message, boolean textEqual, String golden, String ours) {
        declinedTo(listener, name, "rows-underivable");
        return textEqual ? ok()
                : fail(name + " (sql-text, rows underivable — " + failedAs + ": " + message + "): expected " + golden
                        + ", got " + ours);
    }

    /** One side of the rows leg read after the run: its failure raised as planned; a constant its text decoded; else
     *  its rows through the edge's reader (a plan of its own on the body's sessions), shaped by the plan; none: null. */
    private static @com.legend.base.Nullable TypedSide read(com.legend.testexecutionplan.Appeal.Read read,
            SideReader reader) {
        com.legend.testexecutionplan.Failure f = read.failure();
        if (f != null) {
            throw com.legend.testexecutionplan.Failures.raised(f);
        }
        com.legend.testexecutionplan.Appeal.Unlisted unlisted = read.unlisted();
        if (unlisted != null) {
            // the population's planning failed with a class off the closed list: a fault, in its words
            throw new IllegalStateException("an appeal's side whose planning failed (" + unlisted.simpleName() + "): "
                    + unlisted.message());
        }
        com.legend.testexecutionplan.TestPlan.Side side = read.side();
        com.legend.testexecutionplan.SideShape shape = read.shape();
        if (side == null || shape == null) {
            return null;
        }
        if (side instanceof com.legend.testexecutionplan.TestPlan.Side.Constant c) {
            List<List<Object>> cells = new java.util.ArrayList<>(c.cells().size());
            for (List<String> row : c.cells()) {
                cells.add(new java.util.ArrayList<Object>(row));
            }
            return com.legend.testexec.SideShapes.shape(shape, new com.legend.exec.PlanRunner.Rows(c.columns(), cells));
        }
        try {
            return com.legend.testexec.SideShapes.shape(shape, reader.read(side));
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** A decline reported to the run's listener (the edge's after the run, the body's while it runs). */
    private static void declinedTo(com.legend.testexecutionplan.@com.legend.base.Nullable AssertListener listener, String name,
            String reason) {
        if (listener != null) {
            listener.declined(name, reason);
        }
    }

    /** An in-list temp table a golden names by its name ({@code tempTableForIn_x}): a golden whose text chooses a
     *  statement route the plan did not plan. */
    private static final java.util.regex.Pattern NAMED_IN_TEMP =
            java.util.regex.Pattern.compile("tempTableForIn_([A-Za-z_][A-Za-z0-9_]*)");

    /** The in-list temp table the engine numbers ({@code tempTableForIn_N}). */
    private static final java.util.regex.Pattern NUMBERED_IN_TEMP =
            java.util.regex.Pattern.compile("tempTableForIn_(\\d+)");

    /** The temp table the golden reads, seeded by the plan's in-list values: its one numbered
     *  {@code tempTableForIn_N}, or none. */
    private static List<SqlReplayReferee.TempTable> inListTemps(String kind, List<String> values, String golden) {
        var m = NUMBERED_IN_TEMP.matcher(golden);
        java.util.Set<String> names = new java.util.LinkedHashSet<>();
        while (m.find()) {
            names.add(m.group());
        }
        return names.size() != 1 ? List.of()
                : List.of(new SqlReplayReferee.TempTable(names.iterator().next(), kind, values));
    }
}
