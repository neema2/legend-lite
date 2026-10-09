// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;

/**
 * The model composers' shared plumbing: upstream's {@code PureGrammarComposerUtility} (tabs, quoting)
 * and the reads of a protocol JSON element every composer needs. Printing rules live in the composers.
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

    /** The element's {@code _type}, or {@code ""}. */
    static String type(Json.Obj o) {
        String t = o.getStringOr("_type", null);
        return t == null ? "" : t;
    }

    /** {@code PackageableElement.getPath}: the package, {@code ::}, the name. */
    static String path(Json.Obj element) {
        String pkg = element.getStringOr("package", null);
        String name = element.getStringOr("name", null);
        String n = name == null ? "null" : name;
        return pkg == null || pkg.isEmpty() ? n : pkg + "::" + n;
    }

    /** {@code convertPath(element.getPath())}. */
    static String elementPath(Json.Obj element) {
        return PureComposer.convertPath(path(element));
    }

    /** {@code convertPath(element.getPath())} of a record's package and name. */
    static String elementPath(String pkg, String name) {
        return PureComposer.convertPath(pkg.isEmpty() ? name : pkg + "::" + name);
    }

    /** The element record the JSON reads as: a printer's JSON entry reads first, then prints the record. */
    static <T extends Protocol.Element> T element(Json.Obj element, Class<T> kind) {
        Protocol.Element read = ModelReader.readElement(element);
        if (!kind.isInstance(read)) {
            throw refused("an element of _type '" + type(element) + "' that reads as " + read.getClass().getSimpleName());
        }
        return kind.cast(read);
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

    static String multiplicity(Json.Obj m) {
        return PureComposer.multiplicity(m);
    }

    static String multiplicity(Multiplicity m) {
        return PureComposer.multiplicity(m);
    }

    static String genericType(Json.Obj gt) {
        return PureComposer.genericType(gt);
    }

    static String genericType(TypeExpression type) {
        return PureComposer.genericType(type);
    }

    /** A value specification record at the top level of an element: standard style, no indentation. */
    static String valueSpecification(com.legend.protocol.spec.ValueSpecification vs) {
        return PureComposer.valueSpecification(vs, PureComposer.Style.STANDARD, "");
    }

    /** A value specification record printed by a composer whose context carries {@code indentation}. */
    static String valueSpecification(com.legend.protocol.spec.ValueSpecification vs, String indentation) {
        return PureComposer.valueSpecification(vs, PureComposer.Style.STANDARD, indentation);
    }

    /** {@link #lambdaBodyText(Json.Obj, String)} of a lambda's body: printed without parameters, its first bar gone. */
    static String lambdaBodyText(List<com.legend.protocol.spec.ValueSpecification> body, String indentation) {
        String text = valueSpecification(new com.legend.protocol.spec.LambdaFunction(List.of(), body), indentation);
        int bar = text.indexOf('|');
        return bar < 0 ? text : text.substring(0, bar) + text.substring(bar + 1);
    }

    /** A value specification at the top level of an element: standard style, no indentation. */
    static String valueSpecification(Json.Node vs) {
        return PureComposer.valueSpecification(vs, PureComposer.Style.STANDARD, "");
    }

    /** A value specification printed by a composer whose context carries {@code indentation}. */
    static String valueSpecification(Json.Node vs, String indentation) {
        return PureComposer.valueSpecification(vs, PureComposer.Style.STANDARD, indentation);
    }

    /**
     * A lambda printed with its parameters dropped and its first {@code |} removed: upstream's
     * {@code lambda.parameters = emptyList; lambda.accept(...).replaceFirst("\\|", "")}.
     */
    static String lambdaBodyText(Json.Obj lambda, String indentation) {
        java.util.LinkedHashMap<String, Json.Node> f = new java.util.LinkedHashMap<>(lambda.fields());
        f.put("parameters", Json.arr());
        String text = valueSpecification(new Json.Obj(f), indentation);
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

    static List<Json.Node> items(Json.Obj o, String key) {
        Json.Node n = o.getOr(key, null);
        return n instanceof Json.Arr a ? a.items() : List.of();
    }

    static List<Json.Obj> objs(Json.Obj o, String key) {
        List<Json.Obj> out = new ArrayList<>();
        for (Json.Node n : items(o, key)) {
            out.add(obj(n, key));
        }
        return out;
    }

    /**
     * A JSON number read as Java's {@code double}, from the token as written when the reader kept it: an
     * exact decimal has no negative zero, so {@code -0.0} would come back {@code 0.0}.
     */
    static double doubleOf(Json.Num n) {
        String token = n.token();
        return token != null ? Double.parseDouble(token) : n.doubleValue();
    }

    /** A field's value, or null when absent or JSON null. */
    static @com.legend.base.Nullable Json.Node value(Json.Obj o, String key) {
        Json.Node v = o.getOr(key, null);
        return v instanceof Json.Null ? null : v;
    }

    /** A string field, or null when absent or JSON null. */
    static @com.legend.base.Nullable String str(Json.Obj o, String key) {
        return o.getOr(key, null) instanceof Json.Str s ? s.value() : null;
    }

    /** An object field, or null when absent or JSON null. */
    static @com.legend.base.Nullable Json.Obj objOr(Json.Obj o, String key) {
        return o.getOr(key, null) instanceof Json.Obj v ? v : null;
    }

    static Json.Obj obj(Json.Node n, String what) {
        if (n instanceof Json.Obj o) {
            return o;
        }
        throw refused(what + " is not a JSON object: " + n);
    }

    static IllegalArgumentException refused(String why) {
        return new IllegalArgumentException("model JSON: " + why);
    }

    static String join(List<String> parts, String sep) {
        return String.join(sep, parts);
    }
}
