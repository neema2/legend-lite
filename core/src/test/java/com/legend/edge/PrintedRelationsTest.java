// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The door's lift of a program whose last step prints a relation ({@link PrintedRelations}; stage B's B2, B2b S2,
 * 2026-11-24), or a Float's value at legend-pure's door (B2b S6a, 2026-11-25): what is lifted, what is left to the run,
 * and the program that remains.
 */
class PrintedRelationsTest {

    private static final String REL = "#TDS\n  a:Integer, b:String\n  1, x\n#";

    private static final String CSV = "->meta::relational::tests::csv::toCSV";

    private static com.legend.TypedQuery typed(String query) {
        return com.legend.Compiler.query(com.legend.Compiler.compileModel(""), query);
    }

    private static PureRelationTextWriter.Form form(String query) {
        PrintedRelations.Lifted lifted = PrintedRelations.lift(typed(query));
        assertNotNull(lifted, query);
        return lifted.form();
    }

    @Test
    void aFinalToStringIsLiftedAndTheRelationRemains() {
        PrintedRelations.Lifted lifted = PrintedRelations.lift(typed("|" + REL + "->toString()"));
        assertNotNull(lifted);
        assertEquals(PureRelationTextWriter.Form.TO_STRING, lifted.form());
        assertEquals(List.of(), lifted.replaces());
        // the program that remains computes the relation: its result is the relation's, no longer a String
        assertEquals(typed("|" + REL).resultType(), lifted.relation().resultType());
    }

    @Test
    void literalReplacesAfterThePrintAreLiftedInTheProgramsOrder() {
        PrintedRelations.Lifted lifted = PrintedRelations.lift(typed("|" + REL
                + "->toString()->replace('\\n', ',')->replace('x', 'y')"));
        assertNotNull(lifted);
        assertEquals(List.of(new PureRelationText.Replace("\n", ","), new PureRelationText.Replace("x", "y")),
                lifted.replaces());
    }

    @Test
    void whatTheDoorDoesNotLiftIsTheRunsAsBefore() {
        // a replace whose target, or whose replacement, is no literal
        assertNull(PrintedRelations.lift(typed("|let s = 'q'; " + REL + "->toString()->replace($s, ',');")));
        assertNull(PrintedRelations.lift(typed("|let s = 'q'; " + REL + "->toString()->replace(',', $s);")));
        // toString(rel, true): its header would not be legend-pure's (PureRelationText)
        assertNull(PrintedRelations.lift(typed("|" + REL + "->toString(true)")));
        // no print at the root
        assertNull(PrintedRelations.lift(typed("|" + REL + "->filter(r|$r.a == 1)")));
        // a print that is not the last step
        assertNull(PrintedRelations.lift(typed("|" + REL + "->toString()->length()")));
    }

    /** A last step's Float print at legend-pure's door (B2b S6a's legend-pure door, 2026-11-25, the orchestrator's
     *  agreement and relay): {@code x->toString()}, {@code x} a Float, lifted to the value and printed by legend-pure's
     *  print in Java ({@link PureValueText#value}); the one rule ({@link PrintedRelations#side}, which B3's test-body
     *  lift reads) and the relation lift read relations alone. */
    @Test
    void aLastStepsFloatPrintIsLiftedAtLegendPuresDoor() {
        PrintedRelations.Lifted lifted = PrintedRelations.liftValue(typed("|1.3421e-8->toString()"));
        assertNotNull(lifted);
        assertEquals(PureRelationTextWriter.Form.VALUE, lifted.form());
        // the program that remains computes the value: a Float, no longer a String
        assertEquals(typed("|1.3421e-8").resultType(), lifted.relation().resultType());
        assertEquals(List.of(new PureRelationText.Replace("0", "o")),
                PrintedRelations.liftValue(typed("|(1.5 * 2.0)->toString()->replace('0', 'o')")).replaces());
        // a Float's print alone: an Integer's, a DateTime's, a day's text are the run's
        assertNull(PrintedRelations.liftValue(typed("|1->toString()")));
        assertNull(PrintedRelations.liftValue(typed("|now()->toString()")));
        assertNull(PrintedRelations.liftValue(typed("|%2014-01-01->toString()")));
        // a value print that is not the last step
        assertNull(PrintedRelations.liftValue(typed("|1.5->toString()->length()")));
        // the relation lift and the one rule leave a value's print to the run
        assertNull(PrintedRelations.lift(typed("|1.3421e-8->toString()")));
    }

    /** A relation's column named csv is that column's cells, never the TDS's csv (the review of d6e94cdfe, 2026-11-25:
     *  the TDS class's csv is String[1], a column read [*]; PlatformTypes.isTdsCsvRead): not lifted. */
    @Test
    void aColumnNamedCsvIsNotATdsCsv() {
        assertNull(PrintedRelations.lift(typed("|#TDS\n  csv:String, a:Integer\n  x, 1\n#->filter(r|$r.a > 0).csv")));
        assertEquals(PureRelationTextWriter.Form.TDS_CSV, form("|" + REL + ".csv"));
    }

    /** The one rule, read off one expression (B3's test-body lift reads it too): a print and its literal replaces, or
     *  null. */
    @Test
    void theRuleReadsOneExpression() {
        com.legend.compiler.spec.typed.TypedSpec printed =
                typed("|" + REL + "->toString()->replace('x', 'y')").body().get(0);
        PrintedRelations.Side side = PrintedRelations.side(printed);
        assertNotNull(side);
        assertEquals(PureRelationTextWriter.Form.TO_STRING, side.form());
        assertEquals(List.of(new PureRelationText.Replace("x", "y")), side.replaces());
        assertEquals(((com.legend.compiler.spec.typed.TypedNativeCall) ((com.legend.compiler.spec.typed.TypedNativeCall)
                printed).args().get(0)).args().get(0), side.relation());
        assertNull(PrintedRelations.side(typed("|" + REL + "->filter(r|$r.a == 1)").body().get(0)));
    }

    @Test
    void toStringFalseIsToString() {
        PrintedRelations.Lifted lifted = PrintedRelations.lift(typed("|" + REL + "->toString(false)"));
        assertNotNull(lifted);
        assertEquals(PureRelationTextWriter.Form.TO_STRING, lifted.form());
    }

    @Test
    void theStatementsBeforeTheLastAreKept() {
        PrintedRelations.Lifted lifted = PrintedRelations.lift(typed("|let n = 1; " + REL
                + "->filter(r|$r.a == $n)->toString();"));
        assertNotNull(lifted);
        com.legend.TypedQuery original = typed("|let n = 1; " + REL + "->filter(r|$r.a == $n)->toString();");
        assertEquals(2, lifted.relation().body().size());
        // the let as it was, and the printed relation, the print's own argument, in the print's place
        assertEquals(original.body().get(0), lifted.relation().body().get(0));
        assertEquals(((com.legend.compiler.spec.typed.TypedNativeCall) original.body().get(1)).args().get(0),
                lifted.relation().body().get(1));
    }

    @Test
    void theHelpersToCsvIsLiftedInEachOfItsForms() {
        assertEquals(PureRelationTextWriter.Form.TO_CSV, form("|" + REL + CSV + "()"));
        assertEquals(PureRelationTextWriter.Form.TO_CSV, form("|" + REL + CSV + "(false)"));
        assertEquals(PureRelationTextWriter.Form.TO_CSV_TDS_NULL, form("|" + REL + CSV + "(true)"));
        // the default formats are the two-argument form's own (helperFunctions.pure:203-206: SimpleDateTimeFormat()
        // and ISO8601DateFormat(), dateExtension.pure:384-392, which return these two texts), as the lowering reads them
        assertEquals(PureRelationTextWriter.Form.TO_CSV_TDS_NULL,
                form("|" + REL + CSV + "('%t{yyyy-MM-dd HH:mm:ss}', '%t{yyyy-MM-dd}', true)"));
        assertEquals(PureRelationTextWriter.Form.TO_CSV,
                form("|" + REL + CSV + "('%t{yyyy-MM-dd HH:mm:ss}', '%t{yyyy-MM-dd}', false)"));
        // any other formats are the run's
        assertNull(PrintedRelations.lift(typed("|" + REL + CSV + "('yyyy', 'yyyy-MM-dd', false)")));
    }

    @Test
    void aTdsCsvPropertyOverARelationOrACastToATdsIsLifted() {
        assertEquals(PureRelationTextWriter.Form.TDS_CSV, form("|" + REL + ".csv"));
        // the read over a cast to a TDS class: the door peels the type-level casts as the resolver does
        // (ResultEnvelopeSplice.peelTdsCasts) and lifts it (stage B's B2b S5c, 2026-11-25: until then the edge's SQL
        // composed it, edge.RelationTextLowering, and the door left it to the run)
        assertEquals(PureRelationTextWriter.Form.TDS_CSV,
                form("|" + REL + "->cast(@meta::pure::metamodel::relation::TDS<Any>).csv"));
    }

    @Test
    void aRelationWhoseColumnsTheDatabaseNamesIsTheRuns() {
        // a dynamic pivot whose columns nothing names: the lowering refuses its text by name
        // (DynamicPivotTextRefusalTest), and the door leaves it to the run
        String pivot = "|#TDS\nYR,CITY,TREES\n2000,NYC,1\n2000,SF,2\n#->pivot(~CITY, ~total : x|$x.TREES : y|$y->sum())";
        assertNull(PrintedRelations.lift(typed(pivot + "->toString()")));
        assertNull(PrintedRelations.lift(typed(pivot + CSV + "()")));
        // a pivot whose columns a cast names is an ordinary relation: its columns the compiler's, lifted
        assertEquals(PureRelationTextWriter.Form.TO_STRING, form(pivot + "->cast(@meta::pure::metamodel::relation"
                + "::Relation<(YR:Integer, 'NYC__|__total':Integer, 'SF__|__total':Integer)>)->toString()"));
    }

    @Test
    void theLiftedQueryKeepsTheQuerysOptions() {
        com.legend.TypedQuery query = typed("|" + REL + "->toString()")
                .withOptions(java.util.Set.of(com.legend.platform.PlanOption.DETERMINISTIC_AS_OF));
        PrintedRelations.Lifted lifted = PrintedRelations.lift(query);
        assertNotNull(lifted);
        assertEquals(query.options(), lifted.relation().options());
    }
}
