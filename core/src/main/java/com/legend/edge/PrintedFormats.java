// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A LAST-STEP {@code format} AT LEGEND-PURE'S DOORS: ITS COMPUTED FLOATS UNDER {@code %s} AND {@code %r} ARE PRINTED IN
 * JAVA (the edition program's step 8; the user's ruling of 2026-11-28 on decision D7: "option (b) for %s and %r (Java
 * at the edge writes legend-pure's text, no rounding); option (a) for %f (record it as a difference)"). legend-pure
 * fills {@code %s} and {@code %r} of a Float with the Float's own {@code toString} text
 * ({@code PureStringFormat.format}, legend-pure 5.99.0: {@code CompiledSupport.pureToString} under {@code %s}, the
 * Pure {@code toRepresentation} under {@code %r}, which for a Number is its {@code toString}; the interpreter's
 * {@code Format} calls the same two functions). Lite's own text of a Float is another (six decimals), and since step
 * 8's deletions the edge composes no Float text in SQL. So where a program's last step is a {@code format} over a
 * LITERAL template, the door reads each such argument as a value's print ({@link PrintedRelations}: the argument run
 * as a query of its own, the double printed by {@link PureValueText}), and puts the text in the argument's place, its
 * directive made {@code %s}: a text under {@code %s} is written as it is, so the database fills the template with
 * legend-pure's digits and still computes everything else the call holds -- the other arguments, their directives,
 * the refusals.
 *
 * <p>This class is the READER of the template and the rewriter of the call: it prints nothing and computes no value.
 * It reads a template as legend-pure reads one (the directive grammar of {@code PureStringFormat.format}, the date
 * pattern's end as {@code FormatTools.findEndOfDateFormatString} finds it), only to know which argument stands under
 * which directive. A template it cannot read that way, a call whose arguments are not one value each, or as many as
 * the template's directives, is left as written: the run answers it, or refuses it, as before.
 *
 * <p>Not lifted, each left to the run: {@code %f} (the ruling; with a precision it ROUNDS, which would be a
 * computation in Java), a Float LITERAL (the lowering writes a literal's text from its written digits), a Float held
 * in a {@code Number} or an {@code Any}, a template that is no literal, and a {@code format} that is no last step
 * (used further, its text is lite's own in the middle of a program: register S434).
 */
final class PrintedFormats {

    private PrintedFormats() {
    }

    /** One directive of a template that takes an argument: the place of its letter in the template, and the letter
     *  ({@code s}, {@code r}, {@code d}, {@code f}, {@code t}; a padded {@code %0Nd} is {@code d} and a
     *  {@code %.Nf} is {@code F}, which rounds). */
    record Slot(int at, char letter) {
    }

    /** The directives of {@code template} that take an argument, in order, read as legend-pure reads them
     *  ({@code PureStringFormat.format}); null where the template is not one it reads to its end (an unknown
     *  directive, a width with no closing letter, a date pattern never closed, a {@code %} at the end). */
    static @com.legend.base.Nullable List<Slot> slots(String template) {
        List<Slot> out = new ArrayList<>();
        int n = template.length();
        int i = 0;
        while (i < n) {
            if (template.charAt(i++) != '%') {
                continue;
            }
            if (i >= n) {
                return null;
            }
            char letter = template.charAt(i++);
            switch (letter) {
                case '%' -> {
                }
                case 's', 'r', 'd', 'f' -> out.add(new Slot(i - 1, letter));
                case 't' -> {
                    out.add(new Slot(i - 1, letter));
                    if (i < n && template.charAt(i) == '{') {
                        int end = endOfDatePattern(template, i);
                        if (end < 0) {
                            return null;
                        }
                        i = end + 1;
                    }
                }
                case '0', '.' -> {
                    int j = i;
                    while (j < n && Character.isDigit(template.charAt(j))) {
                        j++;
                    }
                    char closing = letter == '0' ? 'd' : 'f';
                    if (j >= n || template.charAt(j) != closing) {
                        return null;
                    }
                    out.add(new Slot(j, letter == '0' ? 'd' : 'F'));
                    i = j + 1;
                }
                default -> {
                    return null;
                }
            }
        }
        return out;
    }

    /** The place of the brace that closes the date pattern opened at {@code open}, or -1: a quote opens a run of
     *  text, a second closes it, and inside one a backslash makes text of what follows it
     *  ({@code FormatTools.findEndOfDateFormatString}). */
    private static int endOfDatePattern(String template, int open) {
        boolean quoted = false;
        boolean escaped = false;
        for (int i = open + 1; i < template.length(); i++) {
            char c = template.charAt(i);
            if (escaped) {
                escaped = false;
            } else if (quoted) {
                if (c == '"') {
                    quoted = false;
                } else if (c == '\\') {
                    escaped = true;
                }
            } else if (c == '"') {
                quoted = true;
            } else if (c == '}') {
                return i;
            }
        }
        return -1;
    }

    /** A {@code format} call this door reads: its literal template, its arguments as written (one value each), and
     *  the template's directives, one an argument. */
    record Read(TypedNativeCall call, TypedCString template, List<TypedSpec> args, List<Slot> slots) {

        /** The places of the arguments that are a computed Float's text: under {@code %s} or {@code %r}, of the
         *  declared type Float, and no literal where they stand or through the let they are bound to. */
        List<Integer> floats(List<TypedSpec> lets) {
            List<Integer> out = new ArrayList<>();
            for (int i = 0; i < args.size(); i++) {
                char letter = slots.get(i).letter();
                if ((letter == 's' || letter == 'r') && args.get(i).info().type() == Type.Primitive.FLOAT
                        && !PrintedRelations.isAFloatLiteral(args.get(i), lets)) {
                    out.add(i);
                }
            }
            return out;
        }

        /** The call with each argument of {@code texts} replaced by its text, a String literal, under {@code %s}. */
        TypedSpec with(Map<Integer, String> texts) {
            StringBuilder template = new StringBuilder(this.template.value());
            List<TypedSpec> filled = new ArrayList<>(args);
            ExprType text = ExprType.one(Type.Primitive.STRING);
            for (Map.Entry<Integer, String> e : texts.entrySet()) {
                // a text under %r would be quoted (toRepresentation of a String); under %s it is written as it is
                template.setCharAt(slots.get(e.getKey()).at(), 's');
                filled.set(e.getKey(), new TypedCString(e.getValue(), text));
            }
            TypedSpec second = call.args().get(1);
            TypedSpec arguments = second instanceof TypedCollection list
                    // the list's own type is no longer one type's: each slot is lowered by its own (Lowerer.formatted)
                    ? list.withChildren(filled).withInfo(new ExprType(common(filled), list.info().multiplicity()))
                    : filled.get(0);
            return call.withChildren(List.of(new TypedCString(template.toString(), this.template.info()), arguments));
        }
    }

    /** The one type every argument has, or {@code Any}. */
    private static Type common(List<TypedSpec> args) {
        Type first = args.get(0).info().type();
        for (TypedSpec a : args) {
            if (!a.info().type().equals(first)) {
                return new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.ANY);
            }
        }
        return first;
    }

    /** {@code e} read as a {@code format} call this door reads, or null ({@link Read}). */
    static @com.legend.base.Nullable Read of(TypedSpec e) {
        if (!(e instanceof TypedNativeCall call)
                || !com.legend.builtin.Pure.AT_STRING_FORMAT.contains(call.callee().id())
                || call.args().size() != 2 || !(call.args().get(0) instanceof TypedCString template)) {
            return null;
        }
        List<TypedSpec> args = call.args().get(1) instanceof TypedCollection list ? list.elements()
                : List.of(call.args().get(1));
        List<Slot> slots = slots(template.value());
        if (slots == null || slots.size() != args.size() || args.isEmpty()) {
            return null;
        }
        for (TypedSpec a : args) {
            if (!Multiplicity.Bounded.ONE.equals(a.info().multiplicity())) {
                return null;
            }
        }
        return new Read(call, template, args, slots);
    }
}
