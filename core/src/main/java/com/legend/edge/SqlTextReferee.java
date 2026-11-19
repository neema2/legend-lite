// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import java.util.List;

/**
 * The SQL-text arm's referee residue (J33, 2026-11-18): the reads it mints over a Result (its first statement, the
 * formatting-stripped text, the activities), the referee's fixed parameter bindings, and the golden texts' shapes
 * (the chained-plan warning, the population statement, a text differing from its golden). Moved unchanged from
 * {@code compiler/spec/VerdictQueries} and {@code testexecutionplan/Verdicts}: only {@link SqlTextRuns} and
 * {@link SqlTextVerdicts} call them, so they are the edge's.
 */
public final class SqlTextReferee {

    private SqlTextReferee() {
    }

    /** The FIRST-statement form of an indexed SQL read ({@code
     * sqlRemoveFormatting($res, n)} → {@code sqlRemoveFormatting($res)}):
     * the verdict arm reads our one statement where the engine's plan
     * names its n-th (batch 67). Compiler-layer minting (Invariant 7). */
    public static com.legend.compiler.spec.typed.TypedUserCall firstStatementRead(
            com.legend.compiler.spec.typed.TypedUserCall read) {
        return new com.legend.compiler.spec.typed.TypedUserCall(
                read.callee(), List.of(read.args().get(0)), read.info());
    }

    /** assertSameSQL's OUR-TEXT read (charter §8.3b): the
     * {@code sqlRemoveFormatting($result)} call over the assert's own
     * Result argument — the envelope splice folds it to the frame's
     * EXECUTED SQL text (ResultEnvelopeSplice.sqlProducerCall). Null
     * when the model does not know the Result overload (the arm then
     * leaves the shape on its current path). */
    public static @com.legend.base.Nullable TypedSpec sqlStripRead(
            TypedSpec resultArg,
            com.legend.compiler.element.ModelContext ctx) {
        for (var f : ctx.findFunction(
                com.legend.compiler.spec.ResultEnvelopeSplice.SQL_REMOVE_FORMATTING_FQN)) {
            if (f.parameters().size() == 1
                    && f.parameters().get(0).type()
                            != Type.Primitive.STRING) {
                return new com.legend.compiler.spec.typed.TypedUserCall(f,
                        List.of(resultArg),
                        new ExprType(Type.Primitive.STRING,
                                Multiplicity.Bounded.ONE));
            }
        }
        return null;
    }

    /** SQLTEXT charter §5 (the plan replayer, slice 4) — REFEREE
     * PARAMETER BINDINGS for a plan lambda: each scalar parameter
     * binds a fixed referee value as a minted {@code TypedLet} (the
     * verdict layer appends them to the let prefix — parameters
     * resolve exactly like test-body lets, no substitution walk).
     * {@code spellings} pairs each name with the SQL literal TEXT the
     * golden's <code>${'$'}{name}</code> hole fills with (the golden
     * supplies its own quoting). Null when any parameter is not a
     * bindable scalar (enum/class/collection — the arm declines
     * COUNTED; the charter's measure-first residue). */
    public record PlanBindings(List<TypedSpec> lets,
            java.util.Map<String, String> spellings,
            java.util.Map<String, List<String>> lists) {
        public PlanBindings(List<TypedSpec> lets,
                java.util.Map<String, String> spellings) {
            this(lets, spellings, singletons(spellings));
        }

        private static java.util.Map<String, List<String>> singletons(
                java.util.Map<String, String> spellings) {
            java.util.Map<String, List<String>> out = new java.util.LinkedHashMap<>();
            spellings.forEach((k, v) -> out.put(k, List.of(v)));
            return out;
        }
    }

    public static @com.legend.base.Nullable PlanBindings refereeBindings(
            com.legend.compiler.spec.typed.TypedLambda lam) {
        Type.FunctionType ft = com.legend.compiler.element.type
                .PlatformTypes.functionTypeOf(lam.info().type());
        if (ft == null || ft.params().size() != lam.parameters().size()) {
            return null;
        }
        List<TypedSpec> lets = new java.util.ArrayList<>();
        java.util.Map<String, String> spellings =
                new java.util.LinkedHashMap<>();
        java.util.Map<String, List<String>> lists =
                new java.util.LinkedHashMap<>();
        for (int i = 0; i < lam.parameters().size(); i++) {
            String name = lam.parameters().get(i);
            Type pt = ft.params().get(i).type();
            TypedSpec value;
            String spelling;
            if (ft.params().get(i).multiplicity().isMany()
                    && (pt == Type.Primitive.STRING
                            || pt == Type.Primitive.INTEGER)) {
                // a COLLECTION parameter (batch 66): two fixed referee
                // elements — the plan's template operations
                // (collectionSize, renderCollection) evaluate over them
                // at the referee; our side runs the same two
                boolean str = pt == Type.Primitive.STRING;
                List<TypedSpec> elems = str
                        ? List.of(new com.legend.compiler.spec.typed.TypedCString(
                                        "A", scalar(pt)),
                                new com.legend.compiler.spec.typed.TypedCString(
                                        "B", scalar(pt)))
                        : List.of(new com.legend.compiler.spec.typed.TypedCInteger(
                                        22L, scalar(pt)),
                                new com.legend.compiler.spec.typed.TypedCInteger(
                                        23L, scalar(pt)));
                ExprType many = new ExprType(pt, Multiplicity.Bounded.ZERO_MANY);
                lets.add(new com.legend.compiler.spec.typed.TypedLet(name,
                        new com.legend.compiler.spec.typed.TypedCollection(
                                elems, many), many));
                lists.put(name, str ? List.of("A", "B") : List.of("22", "23"));
                continue;
            }
            if (pt == Type.Primitive.STRING) {
                value = new com.legend.compiler.spec.typed.TypedCString(
                        "A", scalar(Type.Primitive.STRING));
                spelling = "A";
            } else if (pt == Type.Primitive.INTEGER) {
                value = new com.legend.compiler.spec.typed.TypedCInteger(
                        22L, scalar(Type.Primitive.INTEGER));
                spelling = "22";
            } else if (pt == Type.Primitive.FLOAT
                    || pt == Type.Primitive.NUMBER) {
                value = new com.legend.compiler.spec.typed.TypedCFloat(
                        1.0, new java.math.BigDecimal("1.0"),
                        scalar(Type.Primitive.FLOAT));
                spelling = "1.0";
            } else if (pt == Type.Primitive.BOOLEAN) {
                value = new com.legend.compiler.spec.typed.TypedCBoolean(
                        true, scalar(Type.Primitive.BOOLEAN));
                spelling = "true";
            } else if (pt == Type.Primitive.DATE
                    || pt == Type.Primitive.STRICT_DATE) {
                value = new com.legend.compiler.spec.typed.TypedCDate(
                        com.legend.values.PureDateLiteral.parse(
                                "2015-10-16"), scalar(pt));
                spelling = "2015-10-16";
            } else if (pt == Type.Primitive.DATE_TIME) {
                value = new com.legend.compiler.spec.typed.TypedCDate(
                        com.legend.values.PureDateLiteral.parse(
                                "2015-10-16T00:00:00"), scalar(pt));
                spelling = "2015-10-16 00:00:00";
            } else {
                return null;
            }
            lets.add(new com.legend.compiler.spec.typed.TypedLet(
                    name, value, value.info()));
            spellings.put(name, spelling);
            lists.put(name, List.of(spelling));
        }
        return new PlanBindings(lets, spellings, lists);
    }

    public static TypedSpec activitiesRead(TypedSpec frameVar) {
        return new com.legend.compiler.spec.typed.TypedPropertyAccess(
                frameVar, "activities", frameVar.info());
    }

    /** A text that differs from its golden, in clean mode: the assert's failure, the first difference named. */
    public static com.legend.testexecutionplan.TypedSide textsDiffer(String name, String golden, String ours,
            @com.legend.base.Nullable String decline) {
        return com.legend.testexecutionplan.Verdicts.fail(name + " (sql-text): our text differs from the golden" + (decline != null ? " (" + decline + ")"
                : "") + ": " + com.legend.testexecutionplan.Verdicts.firstTextDiff(golden, ours));
    }

    /** The engine's index-less {@code sqlRemoveFormatting($res)} over a
     * CHAINED plan prints statement 0 and appends this warning line
     * (relationalMappingExecution.pure): the referee reads the SQL and
     * drops the message before replay — a spec-text shape, like the
     * population statement's. */
    private static final String CHAINED_PLAN_WARNING =
            "\nWarning: Results only shown for first relational query.";

    public static String stripChainedPlanWarning(String golden) {
        int at = golden.indexOf(CHAINED_PLAN_WARNING);
        return at < 0 ? golden : golden.substring(0, at);
    }

    /** The engine's population statement shape (batch 67): the ONE
     * recognizer of that golden text, shared by both routes. */
    public static boolean isPopulationGolden(String golden) {
        return golden.toLowerCase(java.util.Locale.ROOT)
                .startsWith("select distinct");
    }

    /** A side's String value, or null (moved from testexecutionplan.Verdicts, J33 narrowed, 2026-11-18). */
    public static @com.legend.base.Nullable String scalarString(
            com.legend.testexecutionplan.@com.legend.base.Nullable TypedSide r) {
        return r instanceof com.legend.testexecutionplan.TypedSide.Scalar s
                && s.value() instanceof String str ? str : null;
    }

    /** The verdict's read wrapped in {@code from(<mapping>)}, no bound context (moved from compiler.spec.VerdictQueries,
     *  J33 narrowed, 2026-11-18). */
    public static TypedSpec fromWrapped(TypedSpec query,
            com.legend.compiler.spec.typed.TypedPackageableRef mapping) {
        return fromWrapped(query, mapping,
                com.legend.compiler.spec.typed.ExecutionContext.NONE);
    }

    /** The verdict's read wrapped in the FRAME's own bound context (the
     * producer's post-processors, time zone, options) under the mapping. */
    public static TypedSpec fromWrapped(TypedSpec query,
            com.legend.compiler.spec.typed.TypedPackageableRef mapping,
            com.legend.compiler.spec.typed.ExecutionContext base) {
        com.legend.base.RunScope.planning("wrap a verdict's read");
        return new com.legend.compiler.spec.typed.TypedFrom(query,
                base.withMapping(java.util.Optional.of(mapping)), query.info());
    }

    /** A String literal as a typed node: a text the planner wrote, as a verdict step's constant side (the engine-text
     *  arms' two texts, the test-is-the-product plan's M10b; moved from compiler.spec.VerdictQueries, J33's last member,
     *  GUARD's review G, 2026-11-18). */
    public static TypedSpec text(String text) {
        return new com.legend.compiler.spec.typed.TypedCString(text, scalar(Type.Primitive.STRING));
    }

    private static ExprType scalar(Type t) {
        return new ExprType(t, Multiplicity.Bounded.ONE);
    }
}
