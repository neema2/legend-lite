// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.ts;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE CATALOG IS THE LIBRARY (docs/TS_LIBRARY_DEFINES_THE_LANGUAGE_2026_10_31.md, M3): {@code #TS}'s words are the
 * {@code timeseries::line::} functions of the model, and what a call may give a word -- its lines, its parameters by
 * place and by name, each one's kind, an option's words -- is read from the word's overloads; its documentation is its
 * {@code doc.doc}. No table of words is kept here: the binder ({@link TsFunctions#given}), the generic rule
 * ({@link TsWords}) and every reader of the catalog (a formula box's completions, the Python DSL's stubs, the guide's
 * pages) read the same overloads.
 *
 * <p>A word's lines are its leading parameters a line ({@code Relation<(key:K, date:D, value:Number)>}) is given to in
 * some overload -- one where another overload takes a number ({@code compare(x, 0)}) -- or weighted members, a basket's
 * (its lines and weights given in pairs: {@code weighted_sum(x, 0.6, y, 0.4)}). A parameter's kind from its overloads:
 * {@code w} or {@code obs} a span where an overload pairs it with a {@code unit} (a calendar period) or takes it as
 * {@code timeseries::Scope} ({@code all}) -- else an Integer is a whole number; a Float or a Number a number; an
 * enumeration's a word of its values; a StrictDate a date (one some overloads go without: a date a base is taken as of).
 * A {@code timeseries::Sample} is the query's SAMPLE BY, never given. Its place: the order of the overload that takes
 * most.
 */
public final class TsCatalog {

    private TsCatalog() {
    }

    /** A word of the catalog: its name, its parameters by place (its lines first), its forms (each overload as
     *  written: {@code volatility(x: Line, w: Integer, type: Returns (optional), ...)}), and its documentation (none:
     *  null). */
    public record Word(String name, List<Param> params, List<String> forms, @com.legend.base.Nullable String doc,
            List<String> aliases) {
        public Word {
            aliases = List.copyOf(aliases);
        }
    }

    /** A word's parameter: its name; its kind ({@code line}; {@code line_or_number}; {@code members}: lines with their
     *  weights; {@code span}: a window or a lag; {@code whole}; {@code number}; {@code word}: one of {@code words};
     *  {@code date}; {@code since}: a date a base is taken as of); what a span takes ({@code observations},
     *  {@code period} -- a calendar span, {@code 3mo} --, {@code all}, {@code date} -- a date on); an option's words; and
     *  whether every call gives it (a parameter some overload goes without, or takes as {@code [0..1]}, has a default:
     *  the library's). */
    public record Param(String name, String kind, List<String> takes, List<String> words, boolean required) {
    }

    /** The type of the parameter a word's SAMPLE BY is: never given, the query's ({@link TsWords#FILL} its FILL's). */
    static final String SAMPLE = TsToPure.LIBRARY + "Sample";

    /** Every word the model's library defines, by name. */
    public static List<Word> words(TsToPure.Model model) {
        List<Word> out = new ArrayList<>();
        for (String fn : model.functionsIn(TsWords.PACKAGE.substring(0, TsWords.PACKAGE.length() - 2))) {
            String name = TsWords.languageName(fn);
            List<TsToPure.Overload> overloads = model.overloads(TsWords.PACKAGE + fn);
            List<Param> params = new ArrayList<>();
            for (String line : lines(overloads)) {
                params.add(new Param(line, lineKind(overloads, line), List.of(), List.of(), required(overloads, line)));
            }
            signature(model, overloads).params().forEach(p -> params.add(new Param(p.name(),
                    p.kind().name().toLowerCase(java.util.Locale.ROOT), takes(model, overloads, p), p.words(),
                    required(overloads, p.name()))));
            out.add(new Word(name, List.copyOf(params), overloads.stream().map(o -> TsWords.form(name, o.params()))
                    .toList(), overloads.stream().map(TsToPure.Overload::doc).filter(java.util.Objects::nonNull)
                    .findFirst().orElse(null), overloads.stream().flatMap(o -> o.aliases().stream()).distinct().toList()));
        }
        return out;
    }

    /** {@code word}'s signature, read from its overloads in {@code model}: its lines' count (-1: weighted members, any
     *  number), its other parameters. */
    static TsFunctions.Signature signature(TsToPure.Model model, String word) {
        return signature(model, model.overloads(TsWords.fqn(model, word)));
    }

    private static TsFunctions.Signature signature(TsToPure.Model model, List<TsToPure.Overload> overloads) {
        List<String> lines = lines(overloads);
        boolean members = overloads.stream().anyMatch(o -> !o.params().isEmpty()
                && (TsToPure.MEMBERS.equals(o.params().get(0).type())
                        || TsToPure.TIMED_MEMBERS.equals(o.params().get(0).type())));
        // the order of the overload that takes most, then any another names; a calendar span's unit (a DurationUnit)
        // rides its count's parameter; SAMPLE BY's and FILL's are the query's
        List<List<TsToPure.Parameter>> orders = new ArrayList<>();
        overloads.stream().map(TsToPure.Overload::params).max(java.util.Comparator.comparingInt(List::size))
                .ifPresent(orders::add);
        overloads.forEach(o -> orders.add(o.params()));
        Map<String, TsFunctions.Param> params = new LinkedHashMap<>();
        for (List<TsToPure.Parameter> ps : orders) {
            for (TsToPure.Parameter p : ps) {
                // a calendar span's unit is its count's, SAMPLE BY's and FILL's the query's: never given (by type)
                boolean language = TsWords.DURATION.equals(p.type()) || SAMPLE.equals(p.type())
                        || TsWords.FILL.equals(p.type());
                if (!language && !lines.contains(p.name()) && !params.containsKey(p.name())) {
                    params.put(p.name(), param(model, overloads, p));
                }
            }
        }
        return new TsFunctions.Signature(members ? -1 : lines.size(), List.copyOf(params.values()));
    }

    /** A word's lines: its leading parameters some overload gives a line (or weighted members). */
    private static List<String> lines(List<TsToPure.Overload> overloads) {
        List<TsToPure.Parameter> widest = overloads.stream().map(TsToPure.Overload::params)
                .max(java.util.Comparator.comparingInt(List::size)).orElse(List.of());
        List<String> out = new ArrayList<>();
        for (TsToPure.Parameter p : widest) {
            boolean line = overloads.stream().anyMatch(o -> o.params().stream().anyMatch(q -> q.name().equals(p.name())
                    && (TsToPure.LINE.equals(q.type()) || TsToPure.TIMED_LINE.equals(q.type())
                            || TsToPure.MEMBERS.equals(q.type()) || TsToPure.TIMED_MEMBERS.equals(q.type()))));
            if (!line) {
                break;
            }
            out.add(p.name());
        }
        return out;
    }

    private static String lineKind(List<TsToPure.Overload> overloads, String name) {
        boolean members = overloads.stream().anyMatch(o -> o.params().stream().anyMatch(q -> q.name().equals(name)
                && (TsToPure.MEMBERS.equals(q.type()) || TsToPure.TIMED_MEMBERS.equals(q.type()))));
        if (members) {
            return "members";
        }
        boolean number = overloads.stream().anyMatch(o -> o.params().stream().anyMatch(q -> q.name().equals(name)
                && !TsToPure.LINE.equals(q.type()) && !TsToPure.TIMED_LINE.equals(q.type())));
        return number ? "line_or_number" : "line";
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
    private static List<String> takes(TsToPure.Model model, List<TsToPure.Overload> overloads, TsFunctions.Param p) {
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
                boolean unit = i + 1 < ps.size() && TsWords.DURATION.equals(ps.get(i + 1).type());
                switch (q.type()) {
                    case "Integer" -> out.add(unit ? "period" : "observations");
                    case "StrictDate" -> out.add("date");
                    default -> {
                        if (!scope(q.type())) {
                            unexpected(p.name(), q.type());
                        }
                        out.addAll(java.util.Objects.requireNonNull(model.enumeration(q.type())));
                    }
                }
            }
        }
        // in the order a reader meets them, whatever the overloads' order
        return List.of("observations", "period", "all", "whole", "date").stream().filter(out::contains).toList();
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

    /** Whether {@code type} is a span's word: {@code timeseries::Scope} ({@code all}, the line's start to date) or
     *  {@code timeseries::Whole} ({@code whole}, the whole line: D3). */
    private static boolean scope(String type) {
        return (TsToPure.LIBRARY + "Scope").equals(type) || (TsToPure.LIBRARY + "Whole").equals(type);
    }

    /** Whether {@code name} is a span in {@code ps}: an Integer a DurationUnit follows (a calendar span's count and
     *  unit), or a span's word ({@link #scope}). */
    private static boolean spanIn(List<TsToPure.Parameter> ps, String name) {
        for (int i = 0; i < ps.size(); i++) {
            TsToPure.Parameter p = ps.get(i);
            if (!p.name().equals(name)) {
                continue;
            }
            if (scope(p.type())) {
                return true;
            }
            if ("Integer".equals(p.type()) && i + 1 < ps.size() && TsWords.DURATION.equals(ps.get(i + 1).type())) {
                return true;
            }
        }
        return false;
    }
}
