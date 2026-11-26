// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * THE TAIL OF A PRINT, COMPUTED IN JAVA AT A COMPATIBILITY DOOR (the edition program's step 8, 2026-11-26; the user's
 * words that day: "we saw toCSV-&gt;replace() and we should do that as one call at the end to replace newline in csv,
 * so maybe small set up string/count functions chained should all be in java"; the first member, {@code replace}, by
 * the user's ruling of 2026-11-24, "also do the replace in java too"). Where a program's last steps are a print the
 * door lifts ({@link PrintedRelations}) followed only by calls of this CLOSED SET with literal arguments, the door
 * takes the whole tail off the program: the text is upstream's, written in Java, and the tail is applied to it here,
 * so the database prints nothing and computes nothing over a text it did not write.
 *
 * <p>A member is a function of the text alone whose meaning here is exactly legend-pure's own Java for it (legend-pure
 * 5.99.0, {@code CompiledSupport} and {@code CoreHelper}, read 2026-11-26; the interpreter's natives call the same
 * {@code java.lang.String} methods), with no error case and nothing that depends on the machine it runs on. Left out,
 * each for that rule: {@code toUpper}/{@code toLower} (the JVM's default locale), {@code ltrim}/{@code rtrim}
 * (commons-lang's set of characters, not {@code trim}'s), {@code substring} (raises out of range), {@code split}
 * (a {@code StringTokenizer}, and it answers a list), {@code reverseString}, {@code size}, {@code toOne}, and the
 * functions legend-engine defines in Pure ({@code lpad}, {@code rpad}, {@code repeatString}). The set's members are
 * pinned ({@code PrintTailTest}): one is added with a dated note or not at all.
 *
 * <p>{@code replace} is {@link PureRelationText.Replace}, the set's first member.
 *
 * <p>Java's answer is legend-pure's, which is not always lite's own SQL's for the same call: {@code length} counts
 * UTF-16 units where a database counts characters, and {@code trim} strips a line break at either end where lite's SQL
 * strips spaces alone (measured 2026-11-26, {@code PrintTailParityTest}). On the plain texts a printed table holds the
 * two agree, {@code trim} of a text that begins or ends with a line break aside.
 *
 * <p>A step answers a String, an Integer or a Boolean. Only a String can be read further, so a step that answers
 * another type is a tail's last (the typer already holds a program to that).
 */
public sealed interface PrintTail permits PureRelationText.Replace, PrintTail.Length, PrintTail.Trim,
        PrintTail.StartsWith, PrintTail.EndsWith, PrintTail.Contains, PrintTail.IndexOf {

    /** This step applied to {@code text}. */
    Object applied(String text);

    /** {@code length()}: {@code String.length} ({@code CompiledSupport.length}), UTF-16 units as legend-pure counts. */
    record Length() implements PrintTail {
        @Override
        public Object applied(String text) {
            return (long) text.length();
        }
    }

    /** {@code trim()}: {@code String.trim} ({@code CoreHelper.trim}), every character up to U+0020 at either end. */
    record Trim() implements PrintTail {
        @Override
        public Object applied(String text) {
            return text.trim();
        }
    }

    /** {@code startsWith(prefix)}: {@code String.startsWith} ({@code CompiledSupport.startsWith}). */
    record StartsWith(String prefix) implements PrintTail {
        @Override
        public Object applied(String text) {
            return text.startsWith(prefix);
        }
    }

    /** {@code endsWith(suffix)}: {@code String.endsWith} ({@code CoreHelper.endsWith}). */
    record EndsWith(String suffix) implements PrintTail {
        @Override
        public Object applied(String text) {
            return text.endsWith(suffix);
        }
    }

    /** A String's {@code contains(part)}: {@code String.contains} ({@code CoreHelper.contains}). */
    record Contains(String part) implements PrintTail {
        @Override
        public Object applied(String text) {
            return text.contains(part);
        }
    }

    /** {@code indexOf(part)} and {@code indexOf(part, from)}: {@code String.indexOf} ({@code CompiledSupport.indexOf}),
     *  a UTF-16 index, -1 where absent; a {@code from} out of range is clamped by Java, never an error, and is read as
     *  an int as legend-pure reads it ({@code from.intValue()}). */
    record IndexOf(String part, @com.legend.base.Nullable Number from) implements PrintTail {
        @Override
        public Object applied(String text) {
            return (long) (from == null ? text.indexOf(part) : text.indexOf(part, from.intValue()));
        }
    }

    /** The Pure type a tail of {@code steps} answers over a text: the last step's, a String's where there is none. */
    static String answers(List<PrintTail> steps) {
        if (steps.isEmpty()) {
            return "String";
        }
        return switch (steps.get(steps.size() - 1)) {
            case PureRelationText.Replace r -> "String";
            case Trim t -> "String";
            case Length l -> "Integer";
            case IndexOf i -> "Integer";
            case StartsWith s -> "Boolean";
            case EndsWith e -> "Boolean";
            case Contains c -> "Boolean";
        };
    }

    /** {@code text} with {@code steps} applied in the program's order. */
    static Object applied(String text, List<PrintTail> steps) {
        Object out = text;
        for (PrintTail step : steps) {
            // only a String is read further: the typer holds a program to that, and the reader below takes no step
            // above one that answers another type
            out = step.applied((String) out);
        }
        return out;
    }

    /** One call read as a step of the set over its first argument, or null: a call outside the set, or one whose
     *  other arguments are not literals. The call is known by its resolved declaration, one of legend-pure's string
     *  natives in the catalog ({@code builtin.Pure}); a collection's {@code contains} and {@code indexOf} are other
     *  declarations. */
    static @com.legend.base.Nullable PrintTail step(TypedNativeCall c) {
        List<TypedSpec> a = c.args();
        com.legend.model.FunctionId id = c.callee().id();
        if (a.size() == 1) {
            return id.equals(Declared.LENGTH) ? new Length()
                    : id.equals(Declared.TRIM) ? new Trim() : null;
        }
        if (a.size() == 2 && a.get(1) instanceof TypedCString s) {
            if (id.equals(Declared.STARTS_WITH)) {
                return new StartsWith(s.value());
            }
            if (id.equals(Declared.ENDS_WITH)) {
                return new EndsWith(s.value());
            }
            if (id.equals(Declared.CONTAINS)) {
                return new Contains(s.value());
            }
            if (id.equals(Declared.INDEX_OF)) {
                return new IndexOf(s.value(), null);
            }
            return null;
        }
        if (a.size() == 3 && a.get(1) instanceof TypedCString s) {
            if (id.equals(Declared.REPLACE)
                    && a.get(2) instanceof TypedCString with) {
                return new PureRelationText.Replace(s.value(), with.value());
            }
            if (id.equals(Declared.INDEX_OF_FROM)
                    && a.get(2) instanceof TypedCInteger from
                    && (from.value() instanceof Long || from.value() instanceof Integer)) {
                return new IndexOf(s.value(), from.value());
            }
        }
        return null;
    }

    /** The set's declarations, each identity computed once: legend-pure's string natives, from the catalog. */
    final class Declared {
        private Declared() {
        }

        static final com.legend.model.FunctionId REPLACE =
                com.legend.model.FunctionId.of(com.legend.builtin.Pure.REPLACE__STRING_1__STRING_1__STRING_1);
        static final com.legend.model.FunctionId LENGTH =
                com.legend.model.FunctionId.of(com.legend.builtin.Pure.LENGTH__STRING_1);
        static final com.legend.model.FunctionId TRIM =
                com.legend.model.FunctionId.of(com.legend.builtin.Pure.TRIM__STRING_1);
        static final com.legend.model.FunctionId STARTS_WITH =
                com.legend.model.FunctionId.of(com.legend.builtin.Pure.STARTS_WITH__STRING_1__STRING_1);
        static final com.legend.model.FunctionId ENDS_WITH =
                com.legend.model.FunctionId.of(com.legend.builtin.Pure.ENDS_WITH__STRING_1__STRING_1);
        static final com.legend.model.FunctionId CONTAINS =
                com.legend.model.FunctionId.of(com.legend.builtin.Pure.CONTAINS__STRING_1__STRING_1);
        static final com.legend.model.FunctionId INDEX_OF =
                com.legend.model.FunctionId.of(com.legend.builtin.Pure.INDEX_OF__STRING_1__STRING_1);
        static final com.legend.model.FunctionId INDEX_OF_FROM =
                com.legend.model.FunctionId.of(com.legend.builtin.Pure.INDEX_OF__STRING_1__STRING_1__INTEGER_1);
    }

    /** {@code e} beneath its tail, each step added to {@code steps} in the program's order; {@code e} itself where its
     *  root is no step of the set. A step that answers no String is taken only at the root. */
    static TypedSpec beneath(TypedSpec e, List<PrintTail> steps) {
        TypedSpec last = e;
        while (last instanceof TypedNativeCall c && !c.args().isEmpty()) {
            PrintTail step = step(c);
            if (step == null || (!steps.isEmpty() && !answers(List.of(step)).equals("String"))) {
                break;
            }
            steps.add(0, step);
            last = c.args().get(0);
        }
        return last;
    }
}
