// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.ts;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE CATALOG IS THE LIBRARY (docs/TS_LIBRARY_DEFINES_THE_LANGUAGE_2026_10_31.md, M3): {@code #TS}'s words over one line
 * are the {@code timeseries::line::} functions of the model, and what a call may give a word -- its parameters by
 * place and by name, each one's kind, an option's words -- is read from the word's overloads; its documentation is its
 * {@code doc.doc}. No table of words is kept here: the binder ({@link TsFunctions#given}), the generic rule
 * ({@link TsWords}) and every reader of the catalog (a formula box's completions, the Python DSL's stubs, the guide's
 * pages) read the same overloads.
 *
 * <p>A parameter's kind from its overloads: {@code w} or {@code obs} a span where an overload pairs it with a
 * {@code unit} (a calendar period) or takes it as {@code timeseries::Scope} ({@code all}) -- else an Integer is a whole
 * number; a Float or a Number a number; an enumeration's a word of its values; a StrictDate a date (one some overloads
 * go without: a date a base is taken as of). Its place: the order of the overload that takes most.
 */
public final class TsCatalog {

    private TsCatalog() {
    }

    /** A word of the catalog: its name, its parameters by place, its forms (each overload as written: {@code
     *  volatility(x, w: Integer, type: Returns (optional), ...)}), and its documentation (none: null). */
    public record Word(String name, List<Param> params, List<String> forms, @com.legend.base.Nullable String doc) {
    }

    /** A word's parameter: its name; its kind ({@code span}: a window or a lag; {@code whole}; {@code number};
     *  {@code word}: one of {@code words}; {@code date}; {@code since}: a date a base is taken as of); what a span takes
     *  ({@code observations}, {@code period} -- a calendar span, {@code 3mo} --, {@code all}, {@code date} -- a date on);
     *  an option's words; and whether every call gives it (a parameter some overload goes without, or takes as
     *  {@code [0..1]}, has a default: the library's). */
    public record Param(String name, String kind, List<String> takes, List<String> words, boolean required) {
    }

    /** Every word over one line the model's library defines, by name. */
    public static List<Word> words(TsToPure.Model model) {
        List<Word> out = new ArrayList<>();
        for (String name : model.functionsIn(TsWords.PACKAGE.substring(0, TsWords.PACKAGE.length() - 2))) {
            List<TsToPure.Overload> overloads = model.overloads(TsWords.PACKAGE + name);
            List<Param> params = signature(model, overloads).params().stream().map(p -> new Param(p.name(),
                    p.kind().name().toLowerCase(java.util.Locale.ROOT), takes(overloads, p), p.words(),
                    required(overloads, p.name()))).toList();
            out.add(new Word(name, params, overloads.stream().map(o -> TsWords.form(name, o.params())).toList(),
                    overloads.stream().map(TsToPure.Overload::doc).filter(java.util.Objects::nonNull).findFirst()
                            .orElse(null)));
        }
        return out;
    }

    /** {@code word}'s signature, read from its overloads in {@code model}. */
    static TsFunctions.Signature signature(TsToPure.Model model, String word) {
        return signature(model, model.overloads(TsWords.PACKAGE + word));
    }

    private static TsFunctions.Signature signature(TsToPure.Model model, List<TsToPure.Overload> overloads) {
        // the order of the overload that takes most; a calendar span's unit rides its count's parameter
        List<TsToPure.Parameter> widest = overloads.stream().map(TsToPure.Overload::params)
                .max(java.util.Comparator.comparingInt(List::size)).orElse(List.of());
        Map<String, TsFunctions.Param> params = new LinkedHashMap<>();
        for (TsToPure.Parameter p : widest) {
            if (!"unit".equals(p.name())) {
                params.put(p.name(), param(model, overloads, p));
            }
        }
        // a parameter only another overload names (a window's date form under its own name) after them
        for (TsToPure.Overload o : overloads) {
            for (TsToPure.Parameter p : o.params()) {
                if (!"unit".equals(p.name()) && !params.containsKey(p.name())) {
                    params.put(p.name(), param(model, overloads, p));
                }
            }
        }
        return new TsFunctions.Signature(1, List.copyOf(params.values()));
    }

    private static TsFunctions.Param param(TsToPure.Model model, List<TsToPure.Overload> overloads,
            TsToPure.Parameter p) {
        String name = p.name();
        boolean span = overloads.stream().anyMatch(o -> spanIn(o.params(), name));
        if (span) {
            return new TsFunctions.Param(name, TsFunctions.Kind.SPAN, null, List.of());
        }
        String t = p.type();
        if ("Integer".equals(t)) {
            return new TsFunctions.Param(name, TsFunctions.Kind.WHOLE, null, List.of());
        }
        if ("Float".equals(t) || "Number".equals(t) || "Decimal".equals(t)) {
            return new TsFunctions.Param(name, TsFunctions.Kind.NUMBER, null, List.of());
        }
        if ("StrictDate".equals(t)) {
            boolean everywhere = overloads.stream().allMatch(o -> o.params().stream().anyMatch(q -> q.name()
                    .equals(name)));
            return new TsFunctions.Param(name, everywhere ? TsFunctions.Kind.DATE : TsFunctions.Kind.SINCE, null,
                    List.of());
        }
        List<String> values = model.enumeration(t);
        if (values != null) {
            return new TsFunctions.Param(name, TsFunctions.Kind.WORD, null, values);
        }
        throw new IllegalStateException("the library's word parameter " + name + " is of a type the catalog reads no"
                + " kind of: " + t);
    }

    /** What a span {@code p} takes, of its overloads' forms: observations, a calendar period, all, a date on. */
    private static List<String> takes(List<TsToPure.Overload> overloads, TsFunctions.Param p) {
        if (p.kind() != TsFunctions.Kind.SPAN) {
            return List.of();
        }
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        for (TsToPure.Overload o : overloads) {
            List<TsToPure.Parameter> ps = o.params();
            for (int i = 0; i < ps.size(); i++) {
                TsToPure.Parameter q = ps.get(i);
                if (!q.name().equals(p.name())) {
                    continue;
                }
                boolean unit = i + 1 < ps.size() && "unit".equals(ps.get(i + 1).name());
                out.add(switch (q.type()) {
                    case "Integer" -> unit ? "period" : "observations";
                    case "StrictDate" -> "date";
                    default -> (TsToPure.LIBRARY + "Scope").equals(q.type()) ? "all" : unexpected(p.name(), q.type());
                });
            }
        }
        // in the order a reader meets them, whatever the overloads' order
        return List.of("observations", "period", "all", "date").stream().filter(out::contains).toList();
    }

    private static String unexpected(String name, String type) {
        throw new IllegalStateException("the library's span " + name + " is of a type the catalog reads no form of: "
                + type);
    }

    /** Whether every overload takes {@code name}, as {@code [1]}. */
    private static boolean required(List<TsToPure.Overload> overloads, String name) {
        return overloads.stream().allMatch(o -> o.params().stream().anyMatch(q -> q.name().equals(name)
                && !q.optional()));
    }

    /** Whether {@code name} is a span in {@code ps}: an Integer a {@code unit} follows, or a {@code timeseries::Scope}. */
    private static boolean spanIn(List<TsToPure.Parameter> ps, String name) {
        for (int i = 0; i < ps.size(); i++) {
            TsToPure.Parameter p = ps.get(i);
            if (!p.name().equals(name)) {
                continue;
            }
            if ((TsToPure.LIBRARY + "Scope").equals(p.type())) {
                return true;
            }
            if ("Integer".equals(p.type()) && i + 1 < ps.size() && "unit".equals(ps.get(i + 1).name())) {
                return true;
            }
        }
        return false;
    }
}
