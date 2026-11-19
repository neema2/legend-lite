// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.base.Nullable;
import com.legend.testexec.Equality;
import com.legend.testexec.HostRules;
import com.legend.testexec.PureAsserts;
import com.legend.testexecutionplan.TypedSide;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * LEGEND-PURE'S WORDS OF A FAILED ASSERT, THE HOST JUDGE'S (census J29, 2026-11-18): the compatibility edge's PRESENTER of
 * {@link com.legend.testexec.VerdictRecord} (H10 spike; HostWords' edge form until then) -- the assert files' own messages (assertEquals.pure, assertSameElements.pure,
 * assertError.pure, ...) and the platform's texts around them, each value by {@code toRepresentation}
 * ({@link EdgePureWords}). The decisions stay the product's; these only word them.
 */
public final class EdgeHostWords {

    public static final EdgeHostWords INSTANCE = new EdgeHostWords();

    /** The name a plan worded in legend-pure's words carries. */
    public static final String NAME = "legend-pure";

    private static final EdgePureWords W = EdgePureWords.INSTANCE;

    private EdgeHostWords() {
    }

    public String name() {
        return NAME;
    }

    private static List<Object> values(List<Equality.Typed> typed) {
        List<Object> out = new ArrayList<>(typed.size());
        typed.forEach(t -> out.add(t.value()));
        return out;
    }

    private static @Nullable Object one(List<Object> side, String what) {
        if (side.size() != 1) {
            throw new IllegalStateException(what + " must be one value, got " + side.size());
        }
        return side.get(0);
    }

    /** The synthetic class a keyless instance's wire map carries, or null. */
    private static @Nullable String wireType(@Nullable Object v) {
        return v instanceof java.util.Map<?, ?> m
                && m.get(com.legend.sql.SqlExpr.SYNTHETIC_TYPE) instanceof String t ? t : null;
    }

    /** A frame's columns and row count (the spec's toString(true) is message position only). */
    private static String summarize(TypedSide.Tabular t) {
        return t.columns().stream().map(com.legend.testexecutionplan.Column::name).toList() + " ("
                + t.rows().size() + " rows)";
    }

    private static List<List<Object>> gridRows(TypedSide.Tabular t) {
        List<List<Object>> rows = new ArrayList<>(t.rows().size());
        for (com.legend.testexecutionplan.Row r : t.rows()) {
            rows.add(r.values());
        }
        return rows;
    }

    public String rowValuesDiffer(List<Object> e, List<Object> a, List<Equality.Typed> te, List<Equality.Typed> ta,
            boolean incidental) {
        // upstream's own '\nexpected: %r\nactual:   %r' over the typed grids (the database judge's words too)
        return W.equals(values(te), values(ta), null, null);
    }

    public String notEqual(List<Object> e, List<Object> a, @Nullable String aClass) {
        return W.notEquals(e, a);
    }

    public String sameElements(List<Object> e, List<Object> a, @Nullable String eClass, @Nullable String aClass) {
        // assertSameElements.pure: assertEquals(e->sort(), a->sort()), both sides sorted
        return W.sameElements(PureAsserts.sorted(e), PureAsserts.sorted(a), eClass, aClass);
    }

    public String cellMultisetDiffers(String family, List<Object> e, List<Object> a, HostRules rules) {
        return PureAsserts.holds(PureAsserts.sorted(e), PureAsserts.sorted(a), rules)
                ? family + " (TDSRow.values): cell multiset differs" : sameElements(e, a, null, null);
    }

    public String notEquivalent(TypedSide.Tabular one, TypedSide.Tabular two, double delta,
            @Nullable String cellDiff) {
        return cellDiff != null ? cellDiff : "\n" + summarize(one) + "\n is not equivalent to:\n" + summarize(two);
    }

    public String grids(TypedSide.Tabular te, TypedSide.Tabular ta, boolean wantEqual) {
        return W.grids(gridRows(te), gridRows(ta), wantEqual);
    }

    public String size(@Nullable Object expected, long actual) {
        return W.size(expected, actual);
    }

    public String notContained(List<Object> collection, @Nullable Object value, @Nullable String collectionClass,
            @Nullable String valueClass) {
        return W.notContains(collection, value, collectionClass, valueClass);
    }

    public String notWithin(Number expected, Number actual, Number delta) {
        return W.pair(expected, null, actual, null);
    }

    public String negated(String kind, List<Object> first, List<Object> second) {
        // the prelude's bodies these natives replace: assertNotEq '%r should not equal %r', assertIsNot 'Should not be
        // identical to: %r', assertNotContains '%s should not contain %r', assertNotSize 'size should not equal: %s'
        return switch (kind) {
            case "NOT_EQ" -> W.notEquals(first, second);
            case "IS_NOT" -> W.identical(one(second, "assertIsNot actual"));
            case "NOT_CONTAINS" -> W.contains(first, one(second, "assertNotContains value"));
            case "NOT_SIZE" -> "size should not equal: " + one(first, "assertNotSize size");
            default -> throw new IllegalStateException("a negated assert of kind '" + kind + "'");
        };
    }

    public String condition(boolean wantTrue) {
        return "Assert failed";
    }

    public String emptiness(boolean wantEmpty, TypedSide side, List<Object> values) {
        return wantEmpty ? W.notEmpty(values) : "Expected non-empty collection";
    }

    public String notEq(@Nullable Object e, @Nullable Object a, @Nullable String eClass, @Nullable String aClass,
            @Nullable String eDeclared, @Nullable String aDeclared) {
        // assertEq.pure: '\nexpected: %r\nactual:   %r', an instance as %r writes it (<id instanceOf Class>); each value
        // as its carrier is (the declared kinds are lite's sentence's)
        return W.pair(e, wireType(e), a, wireType(a));
    }

    public String notInstanceOfClass(@Nullable Object v, String wireType, String type) {
        return W.notInstanceOf(v, wireType, type, wireType);
    }

    public String notInstanceOf(@Nullable Object v, String type, String carrier) {
        return W.notInstanceOf(v, null, PureAsserts.bare(type), carrier);
    }

    public String notSameInstance(String name) {
        return name + ": the element references differ";
    }

    public String atIndex(String sentence, int index) {
        // legend-pure names no element: the element's own failure
        return sentence;
    }

    public String rowNotEqual(@Nullable Object e, @Nullable Object a, @Nullable String eClass,
            @Nullable String aClass) {
        return W.equals(Collections.singletonList(e), Collections.singletonList(a), eClass, aClass);
    }

    public String sidesNotEqual(List<Object> e, List<Object> a, List<Object> eCarried, List<Object> aCarried,
            @Nullable String eClass, @Nullable String aClass) {
        return W.equals(e, a, eClass, aClass);
    }

    public String jsonDiffers(String expected, String actual, boolean incidentalRoot, String diff) {
        return "assertJsonStringsEqual: FIRST DIFF at " + diff;
    }

    public String noError(String expected) {
        // interpreted AssertError.java: PureAssertFail("No error was thrown")
        return "No error was thrown";
    }

    public String errorDiffers(String expected, String actual) {
        // assertError.pure:24, the /4 body's assertEquals format, verbatim
        return "Execution error message mismatch.\nThe actual message was \"" + actual
                + "\"\nwhere the expected message was:\"" + expected + "\"";
    }

    public String lineDiffers(long expected, long actual) {
        // assertError.pure:25
        return "Execution error line mismatch. Actual: " + actual + " where expected: " + expected;
    }

    public String columnDiffers(long expected, long actual) {
        // assertError.pure:26
        return "Execution error column mismatch. Actual: " + actual + " where expected: " + expected;
    }

    public String unjudged(String expected, String actual) {
        // the evidence as upstream's frame writes a pair (assertEquals.pure's '\nexpected: %r\nactual:   %r')
        return "\nexpected: " + expected + "\nactual:   " + actual;
    }

    /** legend-pure's words of a caught error (K10 of the edge audit, re-ported onto J29's hook, 2026-11-18): a raise
     *  of a planned site is the words the plan's edge records hold for it, by the raising statement's text and the
     *  site's number; any other error its own message. */
    static String caught(RuntimeException error, Appeals hearing) {
        if (error instanceof com.legend.error.DataError && error.getCause() instanceof com.legend.exec.RaisedErrors.Raised r
                && r.site() != null) {
            return hearing.raisedWords(java.util.Objects.requireNonNull(r.statement(),
                    "a raise of a planned site carries its statement's text"), r.site().n());
        }
        return String.valueOf(error.getMessage());
    }

    private static final EdgeHostWords SELF = INSTANCE;
    private static final HostRules RULES = HostRules.LITE;

    /** H10 SPIKE: legend-pure's words of a failure record (the presenter; the product's judge decided). SPIKE NOTE:
     *  {@code cellMultisetDiffers} picks its sentence by comparing the sorted pools; it compares under lite's rules
     *  now (the edge's 2-ULP form went with HostForms), so its wording can move only where two Float cells are within
     *  2 ULP and unequal. */
    static String present(com.legend.testexec.VerdictRecord failure) {
        return presentWith(failure);
    }

    private static String presentWith(com.legend.testexec.VerdictRecord failure) {
        return switch (failure) {
            case com.legend.testexec.VerdictRecord.RowValuesDiffer r -> SELF.rowValuesDiffer(r.e(), r.a(), r.te(), r.ta(), r.incidental());
            case com.legend.testexec.VerdictRecord.NotEqual r -> SELF.notEqual(r.e(), r.a(), r.aClass());
            case com.legend.testexec.VerdictRecord.SameElements r -> SELF.sameElements(r.e(), r.a(), r.eClass(), r.aClass());
            case com.legend.testexec.VerdictRecord.CellMultisetDiffers r -> SELF.cellMultisetDiffers(r.family(), r.e(), r.a(), RULES);
            case com.legend.testexec.VerdictRecord.NotEquivalent r -> SELF.notEquivalent(r.one(), r.two(), r.delta(), r.cellDiff());
            case com.legend.testexec.VerdictRecord.Grids r -> SELF.grids(r.te(), r.ta(), r.wantEqual());
            case com.legend.testexec.VerdictRecord.Size r -> SELF.size(r.expected(), r.actual());
            case com.legend.testexec.VerdictRecord.NotContained r -> SELF.notContained(r.collection(), r.value(), r.collectionClass(),
                    r.valueClass());
            case com.legend.testexec.VerdictRecord.NotWithin r -> SELF.notWithin(r.expected(), r.actual(), r.delta());
            case com.legend.testexec.VerdictRecord.Negated r -> SELF.negated(r.kind(), r.first(), r.second());
            case com.legend.testexec.VerdictRecord.Condition r -> SELF.condition(r.wantTrue());
            case com.legend.testexec.VerdictRecord.Emptiness r -> SELF.emptiness(r.wantEmpty(), r.side(), r.values());
            case com.legend.testexec.VerdictRecord.NotEq r -> SELF.notEq(r.e(), r.a(), r.eClass(), r.aClass(), r.eDeclared(), r.aDeclared());
            case com.legend.testexec.VerdictRecord.NotInstanceOfClass r -> SELF.notInstanceOfClass(r.v(), r.wireType(), r.type());
            case com.legend.testexec.VerdictRecord.NotInstanceOf r -> SELF.notInstanceOf(r.v(), r.type(), r.carrier());
            case com.legend.testexec.VerdictRecord.NotSameInstance r -> SELF.notSameInstance(r.name());
            case com.legend.testexec.VerdictRecord.AtIndex r -> SELF.atIndex(r.sentence(), r.index());
            case com.legend.testexec.VerdictRecord.RowNotEqual r -> SELF.rowNotEqual(r.e(), r.a(), r.eClass(), r.aClass());
            case com.legend.testexec.VerdictRecord.SidesNotEqual r -> SELF.sidesNotEqual(r.e(), r.a(), r.eCarried(), r.aCarried(), r.eClass(),
                    r.aClass());
            case com.legend.testexec.VerdictRecord.JsonDiffers r -> SELF.jsonDiffers(r.expected(), r.actual(), r.incidentalRoot(), r.diff());
            case com.legend.testexec.VerdictRecord.NoError r -> SELF.noError(r.expected());
            case com.legend.testexec.VerdictRecord.ErrorDiffers r -> SELF.errorDiffers(r.expected(), r.actual());
            case com.legend.testexec.VerdictRecord.LineDiffers r -> SELF.lineDiffers(r.expected(), r.actual());
            case com.legend.testexec.VerdictRecord.ColumnDiffers r -> SELF.columnDiffers(r.expected(), r.actual());
            case com.legend.testexec.VerdictRecord.Unjudged r -> SELF.unjudged(r.expected(), r.actual());
        };
    }
}
