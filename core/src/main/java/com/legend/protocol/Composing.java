// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import java.util.ArrayList;
import java.util.List;

/**
 * The model composers' shared plumbing: upstream's {@code PureGrammarComposerUtility} (tabs, quoting) and the
 * value-specification printing every composer needs. Printing rules live in the composers, which print the
 * protocol records (the protocol program's leg 2, step 3): JSON is read by the readers alone.
 */
final class Composing {

    /** {@code PureGrammarComposerUtility.TAB}. */
    static final String TAB = "  ";

    private Composing() {
    }

    /** {@code getTabString(n)}. */
    static String tab(int n) {
        return TAB.repeat(n);
    }

    /** {@code convertPath(element.getPath())} of a record's package and name. */
    static String elementPath(String pkg, String name) {
        return PureComposer.convertPath(pkg.isEmpty() ? name : pkg + "::" + name);
    }

    static String convertPath(String path) {
        return PureComposer.convertPath(path);
    }

    static String convertIdentifier(String s) {
        return PureComposer.convertIdentifier(s);
    }

    /** {@code convertString(s, escape)}, single quotes. */
    static String convertString(String s, boolean escape) {
        return PureComposer.convertString(s, escape);
    }

    static String multiplicity(Multiplicity m) {
        return PureComposer.multiplicity(m);
    }

    /** A mapping-local property's multiplicity from its bounds, as the multiplicity reader takes them: an upper
     *  bound of {@code 2147483647} is many ({@link ProtocolReader#multiplicity}). */
    static String multiplicity(long lower, @com.legend.base.Nullable Long upper) {
        return multiplicity(Multiplicity.range(Math.toIntExact(lower),
                upper == null || upper == Integer.MAX_VALUE ? null : Math.toIntExact(upper)));
    }

    static String genericType(TypeExpression type) {
        return PureComposer.genericType(type);
    }

    /** A value specification at the top level of an element: standard style, no indentation. */
    static String valueSpecification(com.legend.protocol.spec.ValueSpecification vs) {
        return PureComposer.valueSpecification(vs, PureComposer.Style.STANDARD, "");
    }

    /** A value specification printed by a composer whose context carries {@code indentation}. */
    static String valueSpecification(com.legend.protocol.spec.ValueSpecification vs, String indentation) {
        return PureComposer.valueSpecification(vs, PureComposer.Style.STANDARD, indentation);
    }

    /**
     * A lambda's body printed as a lambda with no parameters and its first {@code |} removed: upstream's
     * {@code lambda.parameters = emptyList; lambda.accept(...).replaceFirst("\\|", "")}.
     */
    static String lambdaBodyText(List<com.legend.protocol.spec.ValueSpecification> body, String indentation) {
        String text = valueSpecification(new com.legend.protocol.spec.LambdaFunction(List.of(), body), indentation);
        int bar = text.indexOf('|');
        return bar < 0 ? text : text.substring(0, bar) + text.substring(bar + 1);
    }

    /** The lines of {@code s}, split at each newline, empty ones kept (Java's {@code split("\n", -1)}). */
    static List<String> lines(String s) {
        return PureComposer.lines(s);
    }

    /**
     * {@code s} cut at each {@code sep} not preceded by {@code unlessAfter} (none when it is {@code 0}), as
     * upstream's {@code String.split} does: no separator leaves the text whole, and trailing empty pieces
     * are dropped. Read char by char: the protocol package parses no text with a regex.
     */
    static List<String> splitDroppingTrailingEmpties(String s, char sep, char unlessAfter) {
        List<String> out = new ArrayList<>();
        int from = 0;
        boolean cut = false;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == sep && (unlessAfter == 0 || i == 0 || s.charAt(i - 1) != unlessAfter)) {
                out.add(s.substring(from, i));
                from = i + 1;
                cut = true;
            }
        }
        out.add(s.substring(from));
        if (!cut) {
            return out;
        }
        while (!out.isEmpty() && out.get(out.size() - 1).isEmpty()) {
            out.remove(out.size() - 1);
        }
        return out;
    }

    static IllegalArgumentException refused(String why) {
        return new IllegalArgumentException("model JSON: " + why);
    }

    static String join(List<String> parts, String sep) {
        return String.join(sep, parts);
    }
}
