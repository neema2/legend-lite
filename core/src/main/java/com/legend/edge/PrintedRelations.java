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
 * knows, followed by a tail of calls of one closed set with literal arguments ({@link PrintTail}: {@code replace} since
 * 2026-11-24, the rest since the edition program's step 8, 2026-11-26), answers the query that computes the
 * relation and the print the run's receiver performs ({@link PureRelationTextWriter}). At legend-pure's doors a
 * value's print is lifted too ({@code values}): a Float's {@code toString}, printed in Java by legend-pure's rule
 * ({@link PureValueText#value}) with the same tail -- tenet 1 as the user amended it on 2026-11-25, a print at a last
 * step is Java's. (B2b S6a lifted it and was held, 2026-11-25: alone it left a print used further to the edge's SQL
 * passes, two printers at one door; the tail answers that for every function of the set.) At legend-engine's door a
 * value's print inside a store query is legend-engine's own answer, a row of its table, and is not lifted. (The tail
 * after a RELATION's print is Java's at that door too: legend-engine has no SQL for a relation's text, so the print
 * and everything after it run in its own Java, which is legend-pure's.) Anything else is not lifted: the
 * query is the door's as it was, and a print the door leaves is lite's own text, computed by the database
 * ({@code lowering.ProductRelationText}, B2b S5a and S5b; until S6 its Float and DateTime cells the edge's value texts,
 * the progress record's S5a window), and a TDS's {@code csv} the door does not lift lite's CSV, as {@code toCSV(t)}
 * writes it (the user's ruling of 2026-11-25, S405; refused by name from B2b S5c until then). It
 * computes no value and plans nothing; the run computes the rows, the printer spells them, and the tail's steps are
 * applied to the printed text by the receiver ({@link PrintTail}).
 */
public final class PrintedRelations {

    private PrintedRelations() {
    }

    /** The lifted program: {@code relation} computes the rows (its last statement the printed relation, or under
     *  {@link PureRelationTextWriter.Form#VALUE} the printed value), and {@code form} and {@code tail} are the print the
     *  receiver performs on them and the steps it applies to the text. */
    public record Lifted(com.legend.TypedQuery relation, PureRelationTextWriter.Form form, List<PrintTail> tail) {
        public Lifted {
            tail = List.copyOf(tail);
        }

        /** The receiver that prints this program's rows, applies the tail and hands the answer to {@code receiver}: a
         *  String, or what the tail's last step answers ({@link #answers}). */
        public PureRelationTextWriter writer(java.util.function.Consumer<Object> receiver) {
            return new PureRelationTextWriter(form, tail, receiver);
        }

        /** The Pure type of the answer: {@code String}, or {@code Integer} or {@code Boolean} by the tail's last step. */
        public String answers() {
            return PrintTail.answers(tail);
        }
    }

    /** One expression read as a print: the relation (or the value), the print, and the tail after it, in the program's
     *  order. */
    public record Side(TypedSpec relation, PureRelationTextWriter.Form form, List<PrintTail> tail) {
        public Side {
            tail = List.copyOf(tail);
        }
    }

    /** {@code e} read as a print of a relation this edge prints, or null ({@link #side(TypedSpec, boolean)}, a
     *  relation's print alone: legend-engine's door's rule). */
    public static @com.legend.base.Nullable Side side(TypedSpec e) {
        return side(e, false);
    }

    /** {@code e} read as a print this edge writes, or null: THE ONE RULE for a printed expression, which the door's
     *  lift of a final statement ({@link #lift}) and B3's lift of a test body's printed side read alike (G1; the
     *  orchestrator's request of 2026-11-24). The tail is read first, outermost call first ({@link PrintTail#beneath}):
     *  a call outside the closed set, or one of it with an argument that is no literal, ends the tail, and what is
     *  beneath must be a print -- a relation's, or with {@code values} (legend-pure's doors) a value's -- else null. */
    public static @com.legend.base.Nullable Side side(TypedSpec e, boolean values) {
        List<PrintTail> tail = new ArrayList<>();
        Printed printed = printed(PrintTail.beneath(e, tail), values);
        return printed == null ? null : new Side(printed.relation(), printed.form(), tail);
    }

    /** {@code query} lifted, or null where its last statement is no print of a relation this door lifts ({@link #side}). */
    public static @com.legend.base.Nullable Lifted lift(com.legend.TypedQuery query) {
        return lift(query, false);
    }

    /** {@code query} lifted, or null where its last statement is no print this door lifts
     *  ({@link #side(TypedSpec, boolean)}). */
    public static @com.legend.base.Nullable Lifted lift(com.legend.TypedQuery query, boolean values) {
        List<TypedSpec> body = query.body();
        if (body.isEmpty()) {
            return null;
        }
        Side side = side(body.get(body.size() - 1), values);
        if (side == null) {
            return null;
        }
        List<TypedSpec> lifted = new ArrayList<>(body.subList(0, body.size() - 1));
        lifted.add(side.relation());
        // withBody keeps the query's model, options and parameters: only the body changes
        return new Lifted(query.withBody(lifted), side.form(), side.tail());
    }

    /** legend-engine's relation functions' {@code toString} (toString.pure), the name the lowering matches too. */
    private static final String RELATION_TO_STRING = "meta::pure::functions::relation::toString";

    private record Printed(TypedSpec relation, PureRelationTextWriter.Form form) {
    }

    /** {@code e} as a print the printer writes, or null: a relation's, or with {@code values} a Float's
     *  {@code toString}. Only a relation whose columns the compiler knows is lifted ({@link #knownColumns}); the
     *  lowering's own special shapes stay the run's. */
    private static @com.legend.base.Nullable Printed printed(TypedSpec e, boolean values) {
        if (e instanceof TypedNativeCall c) {
            String name = c.callee().qualifiedName();
            // a value's toString at legend-pure's doors (the edition program's step 8, 2026-11-26): a Float's, printed
            // by legend-pure's rule in Java from the double the database computed (PureValueText.value)
            if (values && name.equals(PlatformTypes.TO_STRING) && c.args().size() == 1
                    && c.args().get(0).info().type() == com.legend.compiler.element.type.Type.Primitive.FLOAT) {
                return new Printed(c.args().get(0), PureRelationTextWriter.Form.VALUE);
            }
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
        if (e instanceof TypedPropertyAccess p
                && PlatformTypes.isTdsCsvRead(p.property(), p.source().info().type(), p.info())) {
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
