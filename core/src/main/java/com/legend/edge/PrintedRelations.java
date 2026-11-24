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
 * query is the door's as it was, and a print the door leaves is the edge's SQL text (RelationTextLowering), which
 * differs from the printer's in the places PureRelationTextParityTest names until B2b routes every print through it
 * (the progress record's S2). It computes no value and plans nothing; the run computes the rows and the printer spells
 * them.
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

    /** {@code query} lifted, or null where its last statement is no print of a relation this door lifts. */
    public static @com.legend.base.Nullable Lifted lift(com.legend.TypedQuery query) {
        List<TypedSpec> body = query.body();
        if (body.isEmpty()) {
            return null;
        }
        TypedSpec last = body.get(body.size() - 1);
        List<PureRelationText.Replace> replaces = new ArrayList<>();
        // the replace calls, outermost first; each with literal arguments, else nothing is lifted
        while (last instanceof TypedNativeCall c && c.callee().qualifiedName().equals(PlatformTypes.STRING_REPLACE)
                && c.args().size() == 3) {
            if (!(c.args().get(1) instanceof TypedCString target) || !(c.args().get(2) instanceof TypedCString with)) {
                return null;
            }
            replaces.add(0, new PureRelationText.Replace(target.value(), with.value()));
            last = c.args().get(0);
        }
        Printed printed = printed(last);
        if (printed == null) {
            return null;
        }
        List<TypedSpec> lifted = new ArrayList<>(body.subList(0, body.size() - 1));
        lifted.add(printed.relation());
        // withBody keeps the query's model, options and parameters: only the body changes
        return new Lifted(query.withBody(lifted), printed.form(), replaces);
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
                // formats, which the helper's two-argument form passes (helperFunctions.pure:203-206) and the lowering
                // reads as that form (RelationTextLowering.isDefaultCsvFormat); other formats are not lifted: the run
                // answers them as it does today
                boolean defaultFormats = c.args().size() == 4
                        && RelationTextLowering.isDefaultCsvFormat(c.args().get(1), "%t{yyyy-MM-dd HH:mm:ss}")
                        && RelationTextLowering.isDefaultCsvFormat(c.args().get(2), "%t{yyyy-MM-dd}");
                if ((c.args().size() == 2 || defaultFormats)
                        && c.args().get(c.args().size() - 1) instanceof TypedCBoolean renderTdsNull) {
                    return new Printed(c.args().get(0), renderTdsNull.value()
                            ? PureRelationTextWriter.Form.TO_CSV_TDS_NULL : PureRelationTextWriter.Form.TO_CSV);
                }
                return null;
            }
            return null;
        }
        // a TDS's csv over a relation, as the lowering reads it (Render.csvOverRelation); a read over a cast to a TDS
        // class is the resolver's (Anchors.tdsCsvRead) and is not lifted
        if (e instanceof TypedPropertyAccess p && p.property().equals(PlatformTypes.TDS_CSV_PROPERTY)
                && knownColumns(p.source())) {
            return new Printed(p.source(), PureRelationTextWriter.Form.TDS_CSV);
        }
        return null;
    }

    /** Whether {@code relation} is a relation whose columns the compiler knows: a relation type with at least one
     *  column and no column the database names when the plan runs. A raw grid ({@code executeInDb}, late bound), a
     *  dynamic pivot whose columns nothing names and a relation of no columns are not: the lowering answers each by its
     *  own rule (a raw grid's toCSV from the grid's report, a refusal by name otherwise;
     *  RelationTextLowering.lowerToCsv and lowerToString), and the door leaves them to it. */
    private static boolean knownColumns(TypedSpec relation) {
        return com.legend.compiler.element.type.Type.relationSchema(relation.info().type())
                instanceof com.legend.compiler.element.type.Type.RelationType schema
                && !schema.columns().isEmpty() && schema.dynamicColumns().isEmpty();
    }
}
