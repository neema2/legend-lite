// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * A PROGRAM WHOSE LAST STEP PRINTS A RELATION, LIFTED AT LEGEND-PURE'S AND LEGEND-ENGINE'S DOORS (stage B's B2, B2b S2,
 * 2026-11-24; the plan of record's B2: "at legend-engine's or legend-pure's door, the edge removes a final
 * toString()/toCSV() from the query and hands the run a table printer as its output receiver"; the user's rulings of
 * 2026-11-19 ~18:12, "Yes record it", and 2026-11-24, "also do the replace in java too"; E0 = A, 2026-11-20, "E0 - yes":
 * compatibility changes a program only at the public type-check-to-plan boundary, by removing whole calls). The door
 * reads the typed query once and, where its last statement is a relation's {@code toString}, legend-engine's
 * relational test helper's {@code meta::relational::tests::csv::toCSV} of a TDS (helperFunctions.pure:198-232, a
 * native in lite: {@code PlatformTypes.TO_CSV}) or a TDS's {@code csv}, over a relation whose columns the compiler
 * knows, followed by any number of {@code replace} calls with literal arguments, answers the query that computes the
 * relation and the print the run's receiver performs ({@link PureRelationTextWriter}). Anything else is not lifted: the
 * query is the door's as it was, and a print the door leaves is lite's own text, computed by the database
 * ({@code lowering.ProductRelationText}, B2b S5a and S5b; until S6 its Float and DateTime cells the edge's value texts,
 * the progress record's S5a window), and a TDS's {@code csv} the door does not lift lite's CSV, as {@code toCSV(t)}
 * writes it (the user's ruling of 2026-11-25, S405; refused by name from B2b S5c until then). It
 * computes no value and plans nothing; the run computes the rows and the printer spells them.
 */
public final class PrintedRelations {

    private PrintedRelations() {
    }

    /** The lifted program: {@code relation} computes the rows (its last statement the printed relation), and
     *  {@code form} and {@code replaces} are the print the receiver performs on them. */
    public record Lifted(com.legend.TypedQuery relation, PureRelationTextWriter.Form form,
            List<PureRelationText.Replace> replaces) {
        public Lifted {
            replaces = List.copyOf(replaces);
        }

        /** The receiver that prints this program's rows and hands the text to {@code receiver}. */
        public PureRelationTextWriter writer(java.util.function.Consumer<String> receiver) {
            return new PureRelationTextWriter(form, replaces, receiver);
        }
    }

    /** One expression read as a print of a relation: the relation, the print, and the literal {@code replace} calls
     *  after it, in the program's order. */
    public record Side(TypedSpec relation, PureRelationTextWriter.Form form, List<PureRelationText.Replace> replaces) {
        public Side {
            replaces = List.copyOf(replaces);
        }
    }

    /** {@code e} read as a print of a relation this edge prints, or null: THE ONE RULE for a printed expression, which
     *  the door's lift of a final statement ({@link #lift}) and B3's lift of a test body's printed side read alike (G1;
     *  the orchestrator's request of 2026-11-24). A {@code replace} whose target or replacement is no literal, or a
     *  print this edge does not write, is null. */
    public static @com.legend.base.Nullable Side side(TypedSpec e) {
        List<PureRelationText.Replace> replaces = new ArrayList<>();
        TypedSpec last = beneathReplaces(e, replaces);
        Printed printed = last == null ? null : printed(last);
        return printed == null ? null : new Side(printed.relation(), printed.form(), replaces);
    }

    /** {@code e} beneath its {@code replace} calls, each added to {@code replaces} in the program's order; null where
     *  one's target or replacement is no literal (nothing is read then). */
    private static @com.legend.base.Nullable TypedSpec beneathReplaces(TypedSpec e,
            List<PureRelationText.Replace> replaces) {
        TypedSpec last = e;
        // the replace calls, outermost first; each with literal arguments, else nothing is read
        while (last instanceof TypedNativeCall c && c.callee().qualifiedName().equals(PlatformTypes.STRING_REPLACE)
                && c.args().size() == 3) {
            if (!(c.args().get(1) instanceof TypedCString target) || !(c.args().get(2) instanceof TypedCString with)) {
                return null;
            }
            replaces.add(0, new PureRelationText.Replace(target.value(), with.value()));
            last = c.args().get(0);
        }
        return last;
    }

    /**
     * A program whose last step prints a Float's value, lifted at LEGEND-PURE'S door (stage B's B2b S6a, its legend-pure
     * door, 2026-11-25; the orchestrator's agreement and relay of the same day): {@code x->toString()}, {@code x} a Float,
     * followed by any number of {@code replace} calls with literal arguments, answers the query that computes {@code x}
     * and legend-pure's print of the value, in Java ({@link PureRelationTextWriter.Form#VALUE},
     * {@link PureValueText#value}) -- where the edge's SQL print pass writes legend-engine's in-query cast (on H2
     * {@code 1.3421E-8}, where legend-pure prints {@code 0.000000013421}). Read by legend-pure's door alone: legend-engine's
     * door's value texts, and with them the passes' deletion and a test body's value prints (B3's lift reads
     * {@link #side}, relations alone), wait for the user's ruling. Null where the last step is no such print.
     */
    public static @com.legend.base.Nullable Lifted liftValue(com.legend.TypedQuery query) {
        List<TypedSpec> body = query.body();
        if (body.isEmpty()) {
            return null;
        }
        List<PureRelationText.Replace> replaces = new ArrayList<>();
        TypedSpec last = beneathReplaces(body.get(body.size() - 1), replaces);
        if (!(last instanceof TypedNativeCall c && c.callee().qualifiedName().equals(PlatformTypes.TO_STRING)
                && c.args().size() == 1
                && c.args().get(0).info().type() == com.legend.compiler.element.type.Type.Primitive.FLOAT)) {
            return null;
        }
        List<TypedSpec> lifted = new ArrayList<>(body.subList(0, body.size() - 1));
        lifted.add(c.args().get(0));
        return new Lifted(query.withBody(lifted), PureRelationTextWriter.Form.VALUE, replaces);
    }

    /** {@code query} lifted, or null where its last statement is no print of a relation this door lifts ({@link #side}). */
    public static @com.legend.base.Nullable Lifted lift(com.legend.TypedQuery query) {
        List<TypedSpec> body = query.body();
        if (body.isEmpty()) {
            return null;
        }
        Side side = side(body.get(body.size() - 1));
        if (side == null) {
            return null;
        }
        List<TypedSpec> lifted = new ArrayList<>(body.subList(0, body.size() - 1));
        lifted.add(side.relation());
        // withBody keeps the query's model, options and parameters: only the body changes
        return new Lifted(query.withBody(lifted), side.form(), side.replaces());
    }

    /** legend-engine's relation functions' {@code toString} (toString.pure), the name the lowering matches too. */
    private static final String RELATION_TO_STRING = "meta::pure::functions::relation::toString";

    private record Printed(TypedSpec relation, PureRelationTextWriter.Form form) {
    }

    /** {@code e} as a print of a relation the printer writes, or null. Only a relation whose columns the compiler
     *  knows is lifted ({@link #knownColumns}); the lowering's own special shapes stay the run's. */
    private static @com.legend.base.Nullable Printed printed(TypedSpec e) {
        if (e instanceof TypedNativeCall c) {
            String name = c.callee().qualifiedName();
            if (c.args().isEmpty() || !knownColumns(c.args().get(0))) {
                return null;
            }
            if (name.equals(RELATION_TO_STRING)) {
                // toString(rel, true) is not lifted (PureRelationText: its header would not be legend-pure's); the run
                // answers it as it does today
                if (c.args().size() == 1 || c.args().size() == 2 && c.args().get(1) instanceof TypedCBoolean typed
                        && !typed.value()) {
                    return new Printed(c.args().get(0), PureRelationTextWriter.Form.TO_STRING);
                }
                return null;
            }
            if (name.equals(PlatformTypes.TO_CSV)) {
                if (c.args().size() == 1) {
                    return new Printed(c.args().get(0), PureRelationTextWriter.Form.TO_CSV);
                }
                // toCSV(t, renderTdsNull), and toCSV(t, dateTimeFormat, dateFormat, renderTdsNull) with the default
                // formats, which the helper's two-argument form passes (helperFunctions.pure:203-206): the door reads
                // them as that form (the zero-argument format functions are not in lite's catalog, so their text is
                // the one form that reaches it); other formats are not lifted, and the product refuses every
                // four-argument form by name (ProductRelationText, the orchestrator's Q-A of 2026-11-25)
                boolean defaultFormats = c.args().size() == 4
                        && c.args().get(1) instanceof TypedCString dt && dt.value().equals("%t{yyyy-MM-dd HH:mm:ss}")
                        && c.args().get(2) instanceof TypedCString d && d.value().equals("%t{yyyy-MM-dd}");
                if ((c.args().size() == 2 || defaultFormats)
                        && c.args().get(c.args().size() - 1) instanceof TypedCBoolean renderTdsNull) {
                    return new Printed(c.args().get(0), renderTdsNull.value()
                            ? PureRelationTextWriter.Form.TO_CSV_TDS_NULL : PureRelationTextWriter.Form.TO_CSV);
                }
                return null;
            }
            return null;
        }
        // a TDS's csv over a relation, as the lowering reads it (Render.csvOverRelation), and over a type-level cast to a
        // TDS class, peeled as the resolver peels it (ResultEnvelopeSplice.peelTdsCasts, Anchors.tdsCsvRead; stage B's
        // B2b S5c, 2026-11-25, the orchestrator's agreement: the door lifts the one the relational tests write,
        // testEnumInRelation's, to legend-engine's text; the product writes lite's CSV, toCSV(t)'s, S405)
        if (e instanceof TypedPropertyAccess p && p.property().equals(PlatformTypes.TDS_CSV_PROPERTY)) {
            TypedSpec relation = com.legend.compiler.spec.ResultEnvelopeSplice.peelTdsCasts(p.source());
            if (knownColumns(relation)) {
                return new Printed(relation, PureRelationTextWriter.Form.TDS_CSV);
            }
        }
        return null;
    }

    /** Whether {@code relation} is a relation whose columns the compiler knows: a relation type with at least one
     *  column and no column the database names when the plan runs. A raw grid ({@code executeInDb}, late bound), a
     *  dynamic pivot whose columns nothing names and a relation of no columns are not: the lowering answers each by its
     *  own rule (a raw grid's toCSV from the grid's report, a refusal by name otherwise; lowering.ProductRelationText),
     *  and the door leaves them to it. */
    private static boolean knownColumns(TypedSpec relation) {
        return com.legend.compiler.element.type.Type.relationSchema(relation.info().type())
                instanceof com.legend.compiler.element.type.Type.RelationType schema
                && !schema.columns().isEmpty() && schema.dynamicColumns().isEmpty();
    }
}
