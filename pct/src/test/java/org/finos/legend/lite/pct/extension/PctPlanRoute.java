// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.extension;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.element.type.ExprType;
import com.legend.exec.PlanRunner;
import com.legend.executionplan.ExecutionPlan;

import java.io.IOException;

/**
 * Channel A through THE PRODUCT DOOR (2026-11-03, the user's order of fixes, step 6: one planning door, one execution
 * door): the case's expression and its injected model compiled by the one compiler and run by {@code Execution.run},
 * exactly as a product query runs -- its session the lane's for its plan ({@link PctBackend#executionSource}: as the
 * plan route's sessions chose it), its plan made for that session's server, its declared setup established, with no
 * feature flag, as upstream's PCT adapter sets none (A3 of the compatibility program, 2026-11-19: substring is Pure's
 * by default, so the 2026-09-12 written deviation, the adapter setting CORRECT_SQL_SUBSTRING_INDEXING, is reversed;
 * register S130). Its answer is the plan's TYPED rows, and, for a relation, the engine's JSON
 * answer the server writes from them ({@code edge.EngineAnswers.engine}, after the run, from the rows it collected), which the adapter reads as legend-engine's own
 * PCT adapter reads it ({@link ValueBridge#engineTDS}); a value's elements cross as the reference's own instances
 * ({@link ValueBridge}).
 */
final class PctPlanRoute {

    private PctPlanRoute() {
    }

    /** {@code -Dpct.audience}'s value for legend-engine's audience (the ENGINE lane, H7). */
    static final String ENGINE_AUDIENCE = "engine";

    /** A case's answer: what the plan answers ({@code shape}), its typed rows, the query's result type, which a
     *  value's elements are built by, and for a relation the engine's JSON answer the server writes ({@code
     *  engineJson}; null for a value). {@code order}: the output columns the answering statement's root ORDER BY
     *  sorts on, in its order, {@code "?"} for a key that names no output column; null when the root does not sort.
     *  The edge golden net's recorder writes it, so its normalizer keeps the rows' order across distinct keys
     *  (S0 of the presentation program, docs/records/presentation-program-design-2026-11-18.md §4 (d)); it is a fact
     *  of the plan, read here, never an ordering this module does. */
    record Answer(ExecutionPlan.TdsResult.Shape shape, PlanRunner.Rows rows, ExprType resultType,
            @com.legend.base.Nullable String engineJson, java.util.@com.legend.base.Nullable List<String> order) {
    }

    /** The root ORDER BY's output columns of a plan's answering statement ({@link Answer#order}): a key by its output
     *  name where the sort names one, else the projection whose expression it is, else {@code "?"}; null when the
     *  plan sends no answering statement or its root does not sort. */
    static java.util.@com.legend.base.Nullable List<String> rootOrder(ExecutionPlan plan) {
        if (!plan.sendsTheAnswer()) {
            return null;
        }
        com.legend.sql.SqlQuery tree = plan.answerSql().tree();
        while (tree instanceof com.legend.sql.SqlWith w) {
            tree = w.body();
        }
        if (!(tree instanceof com.legend.sql.SqlSelect select) || select.orderBy().isEmpty()) {
            return null;
        }
        java.util.List<String> keys = new java.util.ArrayList<>();
        for (com.legend.sql.SqlSelect.SortKey k : select.orderBy()) {
            keys.add(k.outputName() != null ? k.outputName() : projected(select, k.expr()));
        }
        return keys;
    }

    private static String projected(com.legend.sql.SqlSelect select, com.legend.sql.SqlExpr expr) {
        for (com.legend.sql.SqlSelect.Projection p : select.projections()) {
            if (p.expr().equals(expr)) {
                return p.alias() != null ? p.alias() : p.out() != null ? p.out().name() : "?";
            }
        }
        return "?";
    }

    /** Whom channel A plans a case for, one choice of three (PR #127's review, 2026-11-19: #124's ENGINE lane and the
     *  LITE-PCT lane chose on the same lines): legend-pure's audience, the PCT's own and the default (the edge's
     *  UPSTREAM text, under the substring flag: S130); legend-engine's ({@code -Dpct.audience=engine}, the ENGINE lane,
     *  H7: the pure/v1 route's text, no feature flag, as its own PCT adapters run a case); or lite's own rules
     *  ({@code -Dpct.channela.rules=LITE}, the LITE-PCT lane's channel A, the compatibility program's A4 gate: the
     *  product's style and default features, every error in lite's own words). The lane chooses its audience
     *  ONCE, and the audience is the one place a run's style and an error's words are chosen ({@link #current},
     *  {@link #words}: stage B's B1, 2026-11-21, the user's ruling on PR #130 -- no second test of the lane, none by
     *  the error's kind). */
    enum Audience {
        LEGEND_PURE, LEGEND_ENGINE, LITE;

        /** The lane's audience, as its JVM's two properties name it ({@link PctPlanRoute#audience}). */
        static Audience current() {
            return audience(System.getProperty("pct.audience"), System.getProperty("pct.channela.rules", "COMPAT"));
        }

        /** The style the audience's cases are planned in: the edge's UPSTREAM text for legend-pure's, the pure/v1
         *  route's with PCT's legend-engine run's edition for legend-engine's (a case is the one expression its own
         *  PCT adapter sends to the database, so every row of legend-engine's table answers it: the edition
         *  program's step 3, 2026-11-26), the product's under lite's own rules. */
        com.legend.database.PlanStyle style() {
            return switch (this) {
                case LEGEND_PURE -> com.legend.edge.PureText.UPSTREAM;
                case LEGEND_ENGINE -> com.legend.edge.PureText.ENGINE_PCT;
                case LITE -> com.legend.database.PlanStyle.PRODUCT;
            };
        }

        /** Any error of a case, in this audience's words, every error alike: legend-pure's for legend-pure's and
         *  legend-engine's audiences (the edge's {@link com.legend.edge.PureMessages#compile}), lite's own message
         *  under lite's own rules. */
        String words(Throwable e) {
            return switch (this) {
                case LEGEND_PURE, LEGEND_ENGINE -> com.legend.edge.PureMessages.compile(e);
                case LITE -> String.valueOf(e.getMessage());
            };
        }
    }

    /** The audience the two properties name ({@code audience}: {@code -Dpct.audience}, absent or {@code engine};
     *  {@code rules}: {@code -Dpct.channela.rules}, COMPAT or LITE): any other value, and legend-engine's audience under
     *  lite's own rules, refused by name -- the two lanes are two audiences, never one. */
    static Audience audience(@com.legend.base.Nullable String audience, String rules) {
        if (audience != null && !audience.equals(ENGINE_AUDIENCE)) {
            throw new IllegalArgumentException("-Dpct.audience=" + audience + ": absent (legend-pure's) or "
                    + ENGINE_AUDIENCE);
        }
        if (!rules.equals("COMPAT") && !rules.equals("LITE")) {
            throw new IllegalArgumentException("-Dpct.channela.rules=" + rules + ": COMPAT or LITE");
        }
        if (audience != null && rules.equals("LITE")) {
            throw new IllegalArgumentException("-Dpct.audience=" + audience + " with -Dpct.channela.rules=LITE: one"
                    + " audience a run -- legend-engine's, or lite's own rules");
        }
        return audience != null ? Audience.LEGEND_ENGINE : rules.equals("LITE") ? Audience.LITE : Audience.LEGEND_PURE;
    }

    static Answer execute(String model, String expression) throws IOException {
        // the tests' planning options: as-of joins answer the same row on every database (PlanOption, off by default;
        // the user, 2026-11-07: "our tests should use the option")
        TypedQuery typed = Compiler.query(Compiler.compileModel(PctBackend.withStorelessRuntime(model)), expression)
                .withOptions(java.util.Set.of(com.legend.platform.PlanOption.DETERMINISTIC_AS_OF));
        com.legend.result.Collector collector = new com.legend.result.Collector();
        java.io.StringWriter json = new java.io.StringWriter();
        // THE AUDIENCE, one of three (audience, above): legend-pure's by default, legend-engine's under
        // -Dpct.audience=engine (the ENGINE lane, H7), lite's own rules under -Dpct.channela.rules=LITE (the LITE-PCT
        // lane's channel A); a combination refused by name
        Audience audience = Audience.current();
        // each comparison across types as the audience's door answers it (B3 step 5, the one equality; the user's ruling
        // of 2026-11-24): legend-pure's Pure's answer, legend-engine's the database's comparison, lite's own refused
        TypedQuery query = switch (audience) {
            case LEGEND_PURE -> com.legend.edge.PureComparisons.asPures(typed);
            case LEGEND_ENGINE -> com.legend.edge.EngineComparisons.asTheDatabases(typed);
            case LITE -> typed;
        };
        // no feature flag for any audience (A3 of the compatibility program, piece 2, 2026-11-19: substring is Pure's by
        // default; S130), as upstream's own PCT adapters set none. Until then legend-pure's audience set
        // CORRECT_SQL_SUBSTRING_INDEXING; piece 2's change applied to #124's audience at the stage-A batch's merge,
        // 2026-11-20
        java.util.Set<com.legend.platform.Feature> features = java.util.Set.of();
        // counted as chosen here, the text and the flags the run is handed, for the ENGINE lane's check (RouteAudience)
        RouteAudience.planned(audience == Audience.LEGEND_ENGINE && features.isEmpty());
        // B2b S2 (stage B's B2, 2026-11-24; the plan of record's B2 and E0 = A, "E0 - yes": at legend-pure's and
        // legend-engine's doors the edge removes a final toString()/toCSV() and hands the run a table printer as its
        // receiver): a compatibility audience's case whose last step prints a relation answers legend-pure's text,
        // printed in Java from the relation's typed rows (edge.PrintedRelations, edge.PureRelationText) -- lite's own
        // rules keep the run's answer (lite's route prints a relation in the database, B2b's S5). Since the edition
        // program's step 8 (2026-11-26; tenet 1 as amended 2026-11-25) legend-pure's audience has a last step's print of
        // a value lifted too -- a Float's toString, or a computed DateTime's, printed in Java (edge.PureValueText) --
        // and the print's tail, the calls of one closed set after it (edge.PrintTail), whose answer may be an Integer
        // or a Boolean. legend-engine's audience keeps a value's print in the query (its database's cast is
        // legend-engine's own answer) and, after a relation's print, lifts the replaces alone, as before the tail
        com.legend.edge.PrintedRelations.Lifted lifted = audience == Audience.LITE ? null
                : com.legend.edge.PrintedRelations.lift(query, audience == Audience.LEGEND_PURE);
        if (lifted != null) {
            java.util.List<String> text = new java.util.ArrayList<>(1);
            if (lifted.written() != null) {
                // a literal's print, written at the door from the typed literal (edge.PrintedRelations): nothing is
                // run for it, so no database's parse of the literal reaches the text (the independent audit of step
                // 8's slice 2, 2026-11-26)
                text.add(lifted.written());
            } else {
                com.legend.Execution.run(lifted.relation(), com.legend.testbackend.StorelessRuntime.RUNTIME,
                        PctBackend.executionSource(), PctBackend.planSessions(),
                        com.legend.exec.ProductRun.Establish.DECLARED_SETUP, features, audience.style(),
                        new com.legend.result.Output.Rows(lifted.writer(text::add)),
                        com.legend.result.Batcher.DEFAULT_CAPACITY);
            }
            // the plans compared are the relation's (the review of 54d922e92: the print's own query, which the product
            // refuses at lowering, reported every lifted case as PRODUCT_REFUSES): the ones the case ran, or for a
            // literal the door wrote itself, the plan of the literal, which was not run
            PctCaseRecorder.onePlan(() -> onePlan(lifted.relation(), features));
            if (text.size() != 1) {
                throw new IllegalStateException("a lifted print answers one value; got " + text.size());
            }
            // the tail is applied here, at the door: the writer wrote the text and nothing else
            Object answered = lifted.answer(text.get(0));
            String slot = switch (lifted.answers()) {
                case "String" -> "VARCHAR";
                case "Integer" -> "BIGINT";
                case "Boolean" -> "BOOLEAN";
                default -> throw new IllegalStateException("a lifted print answers a String, an Integer or a Boolean,"
                        + " not " + lifted.answers());
            };
            ExecutionPlan.Column value = new ExecutionPlan.Column("value", lifted.answers(),
                    new ExecutionPlan.Multiplicity(1, 1), slot, null, null);
            return new Answer(ExecutionPlan.TdsResult.Shape.VALUE, new PlanRunner.Rows(java.util.List.of(value),
                    java.util.List.of(java.util.List.<Object>of(answered))), query.resultType(), null, null);
        }
        // the lane's session for the query, as its sessions choose it for the query's plan (PctBackend.executionSource):
        // a lane holding a session (a cloud's) asks the plan's answering target first
        // planned in the audience's style; a raise says the one catalog's sentence on every audience (stage B's BE,
        // 2026-11-21: until then RaisedWords reworded it for legend-pure's and legend-engine's)
        ExecutionPlan plan = com.legend.Execution.run(query, com.legend.testbackend.StorelessRuntime.RUNTIME,
                // the session for the target the plan answers on, handed to the source by the run (2026-11-09, the full
                // audit's must-fix: a supplier here planned the query again inside the run)
                PctBackend.executionSource(),
                PctBackend.planSessions(), com.legend.exec.ProductRun.Establish.DECLARED_SETUP, features,
                // the PCT compares against legend-pure's texts: the plan is spelled by the edge (phase D's D1.0)
                audience.style(),
                new com.legend.result.Output.Rows(collector), com.legend.result.Batcher.DEFAULT_CAPACITY);
        com.legend.result.Collector.Collected c = collector.collected();
        boolean relation = plan.answerShape() == ExecutionPlan.TdsResult.Shape.RELATION;
        if (relation) {
            // a relation crosses as legend-engine's PCT adapter reads one (the result boundary program's stage 1, step
            // 3): the server's own writer of the engine's answer, written from the rows the run collected, after it
            // (2026-11-09: a tee the run called back until then)
            // made after the run for the execution that ran the SQL, its door's id as the run named it to the collector
            com.legend.result.ResultSink engine = new com.legend.result.Output.Rows(com.legend.edge.EngineAnswers.engine(
                    json, com.legend.edge.HostAnswers.facts(query))).writerFor(plan, collector.execution());
            com.legend.result.Batcher batcher = new com.legend.result.Batcher(c.columns(), engine,
                    com.legend.result.Batcher.DEFAULT_CAPACITY);
            batcher.start();
            for (java.util.List<Object> row : c.rows()) {
                batcher.row(row);
            }
            batcher.end();
        }
        PctCaseRecorder.onePlan(() -> onePlan(query, features));
        return new Answer(plan.answerShape(), new PlanRunner.Rows(c.columns(), c.rows()), query.resultType(),
                relation ? json.toString() : null, relation ? rootOrder(plan) : null);
    }

    /** G3 OF THE PRESENTATION PROGRAM, REPORT-ONLY (S0, 2026-11-18, docs/records/presentation-program-design-2026-11-18.md
     *  §6): the case planned under the edge's style and under the product's, after its run (never inside it), and
     *  their statements compared, in order: {@code IDENTICAL}, {@code DIFFERS <n of m>} (the statements that differ),
     *  {@code PRODUCT_REFUSES <its class>}, or {@code EDGE_REPLAN_REFUSES <its class>} (a plan the measurement cannot
     *  remake outside the run, reported, never failing the case). Recorded beside the case when the net asks
     *  ({@code -Dlegend.edge.oneplan}). */
    static String onePlan(TypedQuery query, java.util.Set<com.legend.platform.Feature> features) {
        java.util.List<String> edge;
        try {
            edge = statements(query.executionPlan(com.legend.testbackend.StorelessRuntime.RUNTIME, features,
                    com.legend.edge.PureText.UPSTREAM));
        } catch (RuntimeException e) {
            // the measurement never fails the case it measures: a plan it cannot remake is reported by its class
            return "EDGE_REPLAN_REFUSES\t" + e.getClass().getSimpleName();
        }
        java.util.List<String> product;
        try {
            product = statements(query.executionPlan(com.legend.testbackend.StorelessRuntime.RUNTIME, features,
                    com.legend.database.PlanStyle.PRODUCT));
        } catch (RuntimeException e) {
            return "PRODUCT_REFUSES\t" + e.getClass().getSimpleName();
        }
        if (edge.equals(product)) {
            return "IDENTICAL\t" + edge.size();
        }
        int differ = Math.abs(edge.size() - product.size());
        for (int i = 0; i < Math.min(edge.size(), product.size()); i++) {
            differ += edge.get(i).equals(product.get(i)) ? 0 : 1;
        }
        return "DIFFERS\t" + differ + " of " + Math.max(edge.size(), product.size());
    }

    /** Every statement a plan sends, in the order its JSON holds them. */
    private static java.util.List<String> statements(ExecutionPlan plan) {
        java.util.List<String> out = new java.util.ArrayList<>();
        statements(com.legend.json.Json.parse(com.legend.executionplan.PlanJson.write(plan)), out);
        return out;
    }

    private static void statements(com.legend.json.Json.Node n, java.util.List<String> out) {
        if (n instanceof com.legend.json.Json.Obj o) {
            for (java.util.Map.Entry<String, com.legend.json.Json.Node> e : o.fields().entrySet()) {
                if (e.getKey().equals("statement") && e.getValue() instanceof com.legend.json.Json.Str s) {
                    out.add(s.value());
                } else {
                    statements(e.getValue(), out);
                }
            }
        } else if (n instanceof com.legend.json.Json.Arr a) {
            for (com.legend.json.Json.Node item : a.items()) {
                statements(item, out);
            }
        }
    }
}
