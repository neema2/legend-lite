// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.error.ErrorKind;
import com.legend.error.ErrorRecord;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.Operand;
import com.legend.values.PureDateLiteral;

import java.util.ArrayList;
import java.util.List;

/**
 * A PLANNED RAISE'S WORDS, AT THE LAST STEP (stage B's BE, the one error model, 2026-11-21): a site's record -- its
 * kind and its operands printed as the kind's form prints them ({@link ErrorKind.Form}) -- and its sentence, the one
 * catalog's (upstream's words wherever upstream has them, the user's ruling of 2026-11-21). The run words a raise here
 * when it reads one back; the compatibility edge words it here too, so every audience says the same; the hosts render
 * the same forms (generated, engine-client's TypeFacts). A value only the database computed prints {@link #COMPUTED}:
 * the SQL never reads it back (the lean-SQL rule; D1, register S389).
 */
public final class RaiseWords {

    /** A value the database computed and no plan holds (D1, register S389). */
    public static final String COMPUTED = "<computed>";

    private RaiseWords() {
    }

    /** {@code site}'s record: its kind, and each operand as the form that says them prints it. */
    public static ErrorRecord record(ExecutionPlan.Site site) {
        ErrorKind kind = site.errorKind();
        List<Operand> o = site.operands();
        ErrorKind.Form form = kind.form(o.size(), i -> o.get(i) instanceof Operand.Text t && t.value().isEmpty());
        List<String> values = new ArrayList<>(o.size());
        for (int i = 0; i < o.size(); i++) {
            values.add(print(o.get(i), form.slot(i)));
        }
        return ErrorRecord.of(kind, values);
    }

    /** {@code site}'s sentence: its record's. */
    public static String sentence(ExecutionPlan.Site site) {
        return record(site).sentence();
    }

    /** {@code o} as {@code slot} prints it. */
    static String print(Operand o, ErrorKind.Slot slot) {
        return switch (slot) {
            case TEXT -> text(o);
            case DATE -> o instanceof Operand.Text t ? date(PureDateLiteral.parse(t.value())) : text(o);
            case FLOAT -> switch (o) {
                case Operand.Flt f -> CellText.carriedFloat(f.value());
                case Operand.Int n -> CellText.carriedFloat((double) n.value());
                case Operand.Dec d -> CellText.carriedFloat(d.value().doubleValue());
                case Operand.Text t -> t.value();
                case Operand.Bool b -> Boolean.toString(b.value());
                case Operand.Day d -> d.iso();
                case Operand.Instant t -> t.iso();
                case Operand.Computed c -> COMPUTED;
            };
            case PLAIN -> o instanceof Operand.Flt f ? java.math.BigDecimal.valueOf(f.value()).toPlainString() : text(o);
        };
    }

    /** {@code o} as the program wrote it: a text itself, an Integer's digits, a Float as Pure prints one, a Decimal's
     *  plain digits, a Boolean's word, a date's ISO text; a computed one {@link #COMPUTED}. */
    private static String text(Operand o) {
        return switch (o) {
            case Operand.Text x -> x.value();
            case Operand.Int n -> Long.toString(n.value());
            case Operand.Day d -> d.iso();
            case Operand.Flt f -> CellText.carriedFloat(f.value());
            case Operand.Dec d -> d.value().toPlainString();
            case Operand.Bool b -> Boolean.toString(b.value());
            case Operand.Instant ts -> ts.iso();
            case Operand.Computed c -> COMPUTED;
        };
    }

    /** A date literal at the precision it was written, {@code +0000} from a minute on: legend-pure's
     *  {@code DateFormat.append}, which the sentence is (the product's own date text, S312's {@code Z}, is a result
     *  cell's, never a sentence's). */
    private static String date(PureDateLiteral d) {
        return d.precision().atLeast(PureDateLiteral.Precision.MINUTE) ? d.canonicalText() + "+0000"
                : d.canonicalText();
    }
}
