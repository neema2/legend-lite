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

    static String genericType(Json.Obj gt) {
        return PureComposer.genericType(gt);
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
