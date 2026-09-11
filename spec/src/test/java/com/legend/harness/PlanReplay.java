// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.harness;

import com.legend.exec.SqlReplayOracle;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The REFEREE's replay of a golden PLAN text (batch 66 — the engine's
 * multi-node plans as row verdicts): the plan's nodes run in order,
 * exactly as the engine's plan executor runs them. An {@code
 * Allocation} binds its name to its value — a {@code Constant}'s
 * literal values, or a {@code Relational} node's rows fetched on the
 * oracle — and the later nodes' {@code ${...}} holes fill from the
 * bindings: a name, a {@code name.column} read of a bound row, the
 * freemarker {@code ?replace} builtin, or one of the engine's own
 * template helper functions (relationalMappingExecution.pure:
 * collectionSize, renderCollection, varPlaceHolderToString,
 * optionalVarPlaceHolderOperationSelector, GMTtoTZ — each evaluated
 * here by its published body). The FINAL Relational node's filled SQL
 * is the statement the caller replays for rows. Everything here is
 * golden-side: a parse of the engine's spec text, never an emission of
 * ours. Unmodeled shapes throw {@link H2Verify.Unverifiable} with the
 * reason named (counted declines).
 */
final class PlanReplay {

    private PlanReplay() {
    }

    /** The node events of a formatted plan text in order: an Allocation
     * header, its {@code name =}, a Constant's {@code values=[...]}, a
     * Relational's {@code sql = ...} (up to its {@code connection =}). */
    private static final Pattern NODE = Pattern.compile(
            "(Allocation\\s*\\()|(name = ([A-Za-z_][A-Za-z0-9_]*))"
                    + "|(values=\\[([^\\]]*)\\])"
                    + "|(sql = (.*?)(?=\\s+connection\\s*=))",
            Pattern.DOTALL);

    /** The final SQL of {@code plan} with every hole filled; {@code run}
     * fetches an Allocation's Relational value on the oracle. */
    /** {@code materialize(name, sql, labels)}: the oracle keeps the
     * allocation's rows as a table and returns the relation read that
     * stands in for the placeholder. */
    static String finalSql(String plan, Map<String, List<String>> params,
            Function<String, SqlReplayOracle.OracleRows> run,
            Materializer materialize) {
        if (plan.contains("sql=select") || plan.contains("Sequence(type=")) {
            // planToStringWithoutFormatting strips the SQL's own spaces
            // (select"root".LEGALNAMEas"name"...) — text, never a statement
            throw new H2Verify.Unverifiable("plan-text unformatted"
                    + " (planToStringWithoutFormatting) — its SQL is not"
                    + " a statement", null);
        }
        Map<String, List<String>> bindings = new LinkedHashMap<>(params);
        Map<String, Map<String, String>> rowBindings = new LinkedHashMap<>();
        // a MULTI-column allocation used as a relation (the cross-store
        // TDS join's `from (${tdsVar_0}) as …`): the ORACLE keeps its rows
        // as a table named by the allocation — the values never leave the
        // database (batch 112; the engine realizes the rows in Java and its
        // template re-spells them, with the placeholder's column names
        // BARE — the table is created with those bare names)
        Map<String, String> relations = new LinkedHashMap<>();
        String pending = null;
        boolean inAllocation = false;
        String lastSql = null;
        Matcher m = NODE.matcher(plan);
        while (m.find()) {
            if (m.group(1) != null) {
                inAllocation = true;
            } else if (m.group(2) != null) {
                if (inAllocation) {
                    pending = m.group(3);
                    inAllocation = false;
                }
            } else if (m.group(4) != null) {
                if (pending != null) {
                    List<String> vals = new ArrayList<>();
                    for (String v : m.group(5).split(",")) {
                        if (!v.isBlank()) {
                            vals.add(v.strip());
                        }
                    }
                    bindings.put(pending, vals);
                    pending = null;
                }
            } else if (m.group(6) != null) {
                String sql = fill(m.group(7).strip(),
                        new Scope(bindings, rowBindings, relations));
                if (pending != null) {
                    SqlReplayOracle.OracleRows rows = run.apply(sql);
                    Map<String, String> row = new LinkedHashMap<>();
                    List<String> scalars = new ArrayList<>();
                    for (List<Object> r : rows.rows()) {
                        for (int i = 0; i < rows.labels().size(); i++) {
                            String cell = spell(r.get(i));
                            row.putIfAbsent(rows.labels().get(i), cell);
                            if (i == 0) {
                                scalars.add(cell);
                            }
                        }
                    }
                    rowBindings.put(pending, row);
                    bindings.put(pending, scalars);
                    if (rows.labels().size() > 1) {
                        relations.put(pending,
                                materialize.table(pending, sql, rows.labels()));
                    }
                    pending = null;
                } else {
                    lastSql = sql;
                }
            }
        }
        if (lastSql == null) {
            throw new H2Verify.Unverifiable("plan-text: no Relational sql"
                    + " node to replay", null);
        }
        return lastSql;
    }

    /** The raw spelling of a fetched cell — the template supplies its own
     * quoting, so the value is bare (a temporal cell prints in the
     * driver's own 'yyyy-MM-dd[ HH:mm:ss.f]' form, which the oracle's H2
     * reads back; no JDBC type is named here — the oracle owns JDBC). */
    private static String spell(Object v) {
        return v == null ? "null" : String.valueOf(v);
    }

    /** Every {@code ${...}} hole filled — the hole ends at ITS closing
     * brace (a map argument's braces nest inside it). */
    /** The oracle-side materialization of an allocation's rows. */
    @FunctionalInterface
    interface Materializer {
        String table(String name, String sql, List<String> labels);
    }

    /** The three binding kinds one replay carries. */
    private record Scope(Map<String, List<String>> bindings,
            Map<String, Map<String, String>> rows,
            Map<String, String> relations) {}

    private static String fill(String sql, Scope sc) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < sql.length()) {
            int start = sql.indexOf("${", i);
            if (start < 0) {
                out.append(sql, i, sql.length());
                break;
            }
            out.append(sql, i, start);
            int depth = 0;
            int j = start + 1;
            boolean quoted = false;
            for (; j < sql.length(); j++) {
                char c = sql.charAt(j);
                if (c == '"' && sql.charAt(j - 1) != '\\') {
                    quoted = !quoted;
                } else if (!quoted && c == '{') {
                    depth++;
                } else if (!quoted && c == '}') {
                    depth--;
                    if (depth == 0) {
                        break;
                    }
                }
            }
            if (j >= sql.length()) {
                throw new H2Verify.Unverifiable("plan-text: unterminated hole", null);
            }
            out.append(evaluate(sql.substring(start + 2, j).strip(), sc));
            i = j + 1;
        }
        return out.toString();
    }

    /** One hole: {@code name}, {@code name.column}, {@code name![]},
     * {@code name?replace("a", "b")} (chained), or {@code fn(args)}. */
    private static String evaluate(String expr, Scope sc) {
        int paren = expr.indexOf('(');
        int q = expr.indexOf("?replace(");
        if (q > 0 && (paren < 0 || q < paren)) {
            String base = evaluate(expr.substring(0, q), sc);
            String rest = expr.substring(q);
            while (rest.startsWith("?replace(")) {
                int close = matching(rest, 8);
                List<Object> a = args(rest.substring(9, close), sc);
                base = base.replace((String) a.get(0), (String) a.get(1));
                rest = rest.substring(close + 1);
            }
            if (!rest.isBlank()) {
                throw new H2Verify.Unverifiable("plan-text: template builtin"
                        + " not modeled: " + rest, null);
            }
            return base;
        }
        if (paren > 0 && expr.endsWith(")")) {
            String fn = expr.substring(0, paren).strip();
            return call(fn, args(expr.substring(paren + 1, expr.length() - 1),
                    sc));
        }
        String name = expr.endsWith("![]") ? expr.substring(0, expr.length() - 3)
                : expr;
        int dot = name.indexOf('.');
        if (dot > 0) {
            Map<String, String> row = sc.rows().get(name.substring(0, dot));
            String col = name.substring(dot + 1);
            if (row == null || !row.containsKey(col)) {
                throw new H2Verify.Unverifiable("plan-text: hole ${" + expr
                        + "} reads no bound row column", null);
            }
            return row.get(col);
        }
        String rel = sc.relations().get(name);
        if (rel != null) {
            return rel;
        }
        List<String> v = sc.bindings().get(name);
        if (v == null) {
            throw new H2Verify.Unverifiable("plan-text: unbound hole ${"
                    + expr + "}", null);
        }
        return String.join(", ", v);
    }

    /** Index of the parenthesis matching the one at {@code open}. */
    private static int matching(String s, int open) {
        int depth = 0;
        boolean quoted = false;
        for (int i = open; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' && s.charAt(i - 1) != '\\') {
                quoted = !quoted;
            } else if (!quoted && c == '(') {
                depth++;
            } else if (!quoted && c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        throw new H2Verify.Unverifiable("plan-text: unbalanced template call", null);
    }

    /** Freemarker call arguments: quoted strings, {@code {"k":"v"}}
     * maps, nested calls, and identifiers (with the engine's {@code
     * ![]} default) — whitespace-separated, as the engine spells them.
     * A binding or a nested call's value is a list; a literal is a
     * String or a Map. */
    private static List<Object> args(String text, Scope sc) {
        List<Object> out = new ArrayList<>();
        int i = 0;
        while (i < text.length()) {
            char c = text.charAt(i);
            if (Character.isWhitespace(c) || c == ',') {
                i++;
            } else if (c == '"') {
                int j = i + 1;
                while (j < text.length() && !(text.charAt(j) == '"'
                        && text.charAt(j - 1) != '\\')) {
                    j++;
                }
                out.add(unescape(text.substring(i + 1, j)));
                i = j + 1;
            } else if (c == '{') {
                int j = text.indexOf('}', i);
                Map<String, String> map = new LinkedHashMap<>();
                Matcher pm = Pattern.compile(
                        "\"((?:[^\"\\\\]|\\\\.)*)\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"")
                        .matcher(text.substring(i + 1, j));
                while (pm.find()) {
                    map.put(unescape(pm.group(1)), unescape(pm.group(2)));
                }
                out.add(map);
                i = j + 1;
            } else {
                int j = i;
                while (j < text.length() && !Character.isWhitespace(text.charAt(j))
                        && text.charAt(j) != ',' && text.charAt(j) != '(') {
                    j++;
                }
                if (j < text.length() && text.charAt(j) == '(') {
                    // a nested call: its value is one scalar
                    int close = matching(text, j);
                    out.add(List.of(evaluate(text.substring(i, close + 1), sc)));
                    i = close + 1;
                    continue;
                }
                String id = text.substring(i, j);
                if (id.endsWith("![]")) {
                    id = id.substring(0, id.length() - 3);
                }
                List<String> v = sc.bindings().get(id);
                if (v == null) {
                    throw new H2Verify.Unverifiable("plan-text: unbound"
                            + " template argument " + id, null);
                }
                out.add(v);
                i = j;
            }
        }
        return out;
    }

    private static String unescape(String s) {
        return s.replace("\\\"", "\"").replace("\\'", "'");
    }

    @SuppressWarnings("unchecked")
    private static String call(String fn, List<Object> args) {
        switch (fn) {
            case "collectionSize" -> {
                // '<#function collectionSize collection> <#return collection?size?c>'
                return String.valueOf(((List<String>) args.get(0)).size());
            }
            case "renderCollection" -> {
                // '<#function renderCollection collection separator prefix
                // suffix replacementMap defaultValue>' — empty = default;
                // each element with the replacements applied; prefix +
                // join(suffix + separator + prefix) + suffix
                List<String> coll = (List<String>) args.get(0);
                String sep = (String) args.get(1);
                String prefix = (String) args.get(2);
                String suffix = (String) args.get(3);
                Map<String, String> repl = (Map<String, String>) args.get(4);
                String dflt = (String) args.get(5);
                if (coll.isEmpty()) {
                    return dflt;
                }
                List<String> mapped = new ArrayList<>();
                for (String e : coll) {
                    mapped.add(replaced(e, repl));
                }
                return prefix + String.join(suffix + sep + prefix, mapped) + suffix;
            }
            case "varPlaceHolderToString" -> {
                // '<#function varPlaceHolderToString optionalParameter prefix
                // suffix replacementMap defaultValue>': an EMPTY optional is
                // the default; else prefix + replaced value + suffix
                List<String> p = (List<String>) args.get(0);
                if (p.isEmpty()) {
                    return (String) args.get(4);
                }
                return (String) args.get(1)
                        + replaced(String.join(", ", p), (Map<String, String>) args.get(3))
                        + (String) args.get(2);
            }
            case "optionalVarPlaceHolderOperationSelector" -> {
                // '<#if optionalParameter?has_content ||
                // optionalParameter?is_string><#return trueClause>'
                List<String> p = (List<String>) args.get(0);
                return (String) (p.isEmpty() ? args.get(2) : args.get(1));
            }
            case "GMTtoTZ" -> {
                // '<#function GMTtoTZ tz paramDate>': an empty parameter is
                // itself; else (tz + " " + paramDate)?date.@alloyDate — the
                // GMT instant moved into the zone, printed in the input's
                // own pattern (PlanDateParameter.withZoneSameInstant)
                String tz = ((String) args.get(0)).replace("[", "").replace("]", "")
                        .strip();
                List<String> dates = (List<String>) args.get(1);
                List<String> out = new ArrayList<>();
                for (String d : dates) {
                    out.add(gmtToZone(d, tz));
                }
                return String.join(", ", out);
            }
            default -> throw new H2Verify.Unverifiable("plan-text: template"
                    + " operation " + fn + " not modeled", null);
        }
    }

    private static String replaced(String value, Map<String, String> repl) {
        String x = value;
        for (var r : repl.entrySet()) {
            x = x.replace(r.getKey(), r.getValue());
        }
        return x;
    }

    private static String gmtToZone(String date, String tz) {
        boolean withT = date.contains("T");
        String norm = date.replace('T', ' ');
        String pattern = norm.contains(".") ? "yyyy-MM-dd HH:mm:ss.SSS"
                : norm.length() > 10 ? "yyyy-MM-dd HH:mm:ss" : "yyyy-MM-dd";
        java.time.format.DateTimeFormatter f =
                java.time.format.DateTimeFormatter.ofPattern(pattern);
        java.time.LocalDateTime gmt = pattern.equals("yyyy-MM-dd")
                ? java.time.LocalDate.parse(norm, f).atStartOfDay()
                : java.time.LocalDateTime.parse(norm, f);
        java.time.LocalDateTime local = gmt.atZone(java.time.ZoneId.of("GMT"))
                .withZoneSameInstant(java.time.ZoneId.of(tz)).toLocalDateTime();
        String s = local.format(f);
        return withT ? s.replace(' ', 'T') : s;
    }
}
