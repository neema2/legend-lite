// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;


import com.legend.sql.DialectCapability;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A regular expression for SQLite, which has none (REGEXP is an application-defined function, and none is registered:
 * the database executes, never the host). A LITERAL pattern -- plan text, known when the statement is written -- is
 * compiled here to a Thompson NFA: a constant table of its transitions, the build-time half. The MATCH is the
 * database's: a correlated {@code WITH RECURSIVE} walks the input's characters through the table ({@link #matches}),
 * the query-time half (docs/JAVA_EVICTION_PLAN.md §1: query-time values to the database, build-time text to typed
 * constants).
 *
 * <p>The RE2 syntax the platform's patterns use: literals and escapes ({@code \d \w \s} and their negations,
 * {@code \t \n \r}, an escaped metacharacter), {@code .}, character classes with ranges and negation, groups
 * (capturing or not: a match's truth needs no capture), alternation, {@code * + ? {n} {n,} {n,m}} (lazy forms match
 * the same strings), the anchors {@code ^ $} and the leading flags {@code (?i)} (ASCII case folding, as SQLite's
 * lower/upper), {@code (?m)} and {@code (?s)}. Anything else -- a backreference, a lookaround, a word boundary, a
 * POSIX class, a flag group inside the pattern -- is refused by name.
 */
final class SqliteRegex {

    /** A set of characters: code-point ranges, possibly negated. */
    private record CharSet(List<int[]> ranges, boolean negated) {
    }

    private sealed interface Node permits Lit, Seq, Alt, Repeat, Anchor, Group {
    }

    /** A capturing group, numbered by its '(' in the pattern. */
    private record Group(int number, Node inner) implements Node {
    }

    private record Lit(CharSet set) implements Node {
    }

    private record Seq(List<Node> items) implements Node {
    }

    private record Alt(List<Node> branches) implements Node {
    }

    /** {@code node} at least {@code min} times, at most {@code max} (-1: unbounded). */
    private record Repeat(Node node, int min, int max) implements Node {
    }

    /** {@code ^} (start) or {@code $} (end). */
    private record Anchor(boolean start) implements Node {
    }

    private final String pattern;
    private final int[] at = {0};
    private final int[] groupCount = {0};
    /** Whether a repetition is lazy: the match preferred is then the shortest, not the longest. */
    private final boolean[] lazy = {false};
    /** The capturing groups whose spans the walk tracks, in column order. */
    private final List<Integer> tracked = new ArrayList<>();
    private final boolean[] flags = new boolean[3];   // i, m, s

    private SqliteRegex(String pattern) {
        this.pattern = pattern;
    }

    private static final int CI = 0;
    private static final int MULTILINE = 1;
    private static final int DOTALL = 2;

    // ==================================================================
    // Parsing
    // ==================================================================

    private boolean more() {
        return at[0] < pattern.length();
    }

    private char peek() {
        return pattern.charAt(at[0]);
    }

    private char next() {
        return pattern.charAt(at[0]++);
    }

    private DialectCapability refused(String what) {
        return new DialectCapability("a regular expression with " + what + " reached SQLite (pattern '" + pattern
                + "'): its matcher takes RE2's literals, classes, groups, alternation, repetition, anchors and the"
                + " leading flags i, m, s");
    }

    private Node parse() {
        // the leading flags: (?ims)
        while (pattern.startsWith("(?", at[0]) && at[0] + 2 < pattern.length()
                && "ims".indexOf(pattern.charAt(at[0] + 2)) >= 0) {
            int close = pattern.indexOf(')', at[0]);
            if (close < 0) {
                throw refused("an unclosed flag group");
            }
            for (char f : pattern.substring(at[0] + 2, close).toCharArray()) {
                switch (f) {
                    case 'i' -> flags[CI] = true;
                    case 'm' -> flags[MULTILINE] = true;
                    case 's' -> flags[DOTALL] = true;
                    default -> throw refused("the flag '" + f + "'");
                }
            }
            at[0] = close + 1;
        }
        Node n = alternation();
        if (more()) {
            throw refused("an unbalanced ')'");
        }
        return n;
    }

    private Node alternation() {
        List<Node> branches = new ArrayList<>();
        branches.add(sequence());
        while (more() && peek() == '|') {
            next();
            branches.add(sequence());
        }
        return branches.size() == 1 ? branches.get(0) : new Alt(branches);
    }

    private Node sequence() {
        List<Node> items = new ArrayList<>();
        while (more() && peek() != '|' && peek() != ')') {
            items.add(repetition());
        }
        return items.size() == 1 ? items.get(0) : new Seq(items);
    }

    private Node repetition() {
        Node atom = atom();
        while (more()) {
            char q = peek();
            int min;
            int max;
            if (q == '*') {
                min = 0;
                max = -1;
            } else if (q == '+') {
                min = 1;
                max = -1;
            } else if (q == '?') {
                min = 0;
                max = 1;
            } else if (q == '{' && bounded()) {
                int close = pattern.indexOf('}', at[0]);
                String[] parts = pattern.substring(at[0] + 1, close).split(",", -1);
                min = Integer.parseInt(parts[0]);
                max = parts.length == 1 ? min : parts[1].isEmpty() ? -1 : Integer.parseInt(parts[1]);
                at[0] = close;
            } else {
                break;
            }
            next();
            if (more() && peek() == '?') {
                next();   // lazy: the same strings match; a find prefers the shortest
                lazy[0] = true;
            } else if (more() && peek() == '+') {
                throw refused("a possessive quantifier");
            }
            if (atom instanceof Anchor) {
                throw refused("a repeated anchor");
            }
            atom = new Repeat(atom, min, max);
        }
        return atom;
    }

    /** At a {@code {n}}, {@code {n,}} or {@code {n,m}} (else a literal brace). */
    private boolean bounded() {
        int close = pattern.indexOf('}', at[0]);
        return close > 0 && pattern.substring(at[0] + 1, close).matches("[0-9]+(,[0-9]*)?");
    }

    private Node atom() {
        char c = next();
        return switch (c) {
            case '(' -> {
                if (pattern.startsWith("?:", at[0])) {
                    at[0] += 2;
                } else if (pattern.startsWith("?P<", at[0]) || pattern.startsWith("?<", at[0])
                        && !pattern.startsWith("?<=", at[0]) && !pattern.startsWith("?<!", at[0])) {
                    at[0] = pattern.indexOf('>', at[0]) + 1;
                    yield capture();
                } else if (more() && peek() == '?') {
                    throw refused("the group '(" + pattern.substring(at[0], Math.min(pattern.length(), at[0] + 3))
                            + "'");
                } else {
                    yield capture();
                }
                Node inner = alternation();
                if (!more() || next() != ')') {
                    throw refused("an unclosed group");
                }
                yield inner;
            }
            case '[' -> new Lit(charClass());
            case '.' -> new Lit(flags[DOTALL] ? new CharSet(List.of(), true)
                    : new CharSet(List.<int[]>of(new int[] {'\n', '\n'}), true));
            case '^' -> new Anchor(true);
            case '$' -> new Anchor(false);
            case '\\' -> new Lit(escape(false));
            case ')', '*', '+', '?' -> throw refused("a misplaced '" + c + "'");
            default -> new Lit(literal(Character.isHighSurrogate(c) && more()
                    ? Character.toCodePoint(c, next()) : c));
        };
    }

    /** A capturing group's body, after its '(': numbered in order of its '('. */
    private Node capture() {
        int number = ++groupCount[0];
        Node inner = alternation();
        if (!more() || next() != ')') {
            throw refused("an unclosed group");
        }
        return new Group(number, inner);
    }

    private CharSet literal(int cp) {
        return new CharSet(List.<int[]>of(new int[] {cp, cp}), false);
    }

    /** An escape after its backslash: a class ({@code \d \w \s}, negated in upper case) or a character. */
    private CharSet escape(boolean inClass) {
        if (!more()) {
            throw refused("a trailing backslash");
        }
        char e = next();
        List<int[]> digits = List.of(new int[] {'0', '9'});
        List<int[]> word = List.of(new int[] {'0', '9'}, new int[] {'A', 'Z'}, new int[] {'a', 'z'},
                new int[] {'_', '_'});
        List<int[]> space = List.of(new int[] {'\t', '\r'}, new int[] {' ', ' '});
        return switch (e) {
            case 'd' -> new CharSet(digits, false);
            case 'w' -> new CharSet(word, false);
            case 's' -> new CharSet(space, false);
            case 'D', 'W', 'S' -> {
                if (inClass) {
                    throw refused("a negated class escape inside a class");
                }
                yield new CharSet(e == 'D' ? digits : e == 'W' ? word : space, true);
            }
            case 't' -> literal('\t');
            case 'n' -> literal('\n');
            case 'r' -> literal('\r');
            case 'f' -> literal('\f');
            case 'v' -> literal(0x0b);
            case 'b', 'B', 'A', 'z', 'Z', 'Q', 'E', 'p', 'P', 'x', 'k', 'g' ->
                    throw refused("the escape '\\" + e + "'");
            default -> {
                if (Character.isLetterOrDigit(e)) {
                    throw refused("the escape '\\" + e + "'");
                }
                yield literal(e);
            }
        };
    }

    private CharSet charClass() {
        boolean negated = more() && peek() == '^';
        if (negated) {
            next();
        }
        List<int[]> ranges = new ArrayList<>();
        boolean first = true;
        while (more() && (peek() != ']' || first)) {
            first = false;
            if (pattern.startsWith("[:", at[0])) {
                throw refused("a POSIX class");
            }
            int lo;
            if (peek() == '\\') {
                next();
                CharSet esc = escape(true);
                if (esc.ranges().size() != 1 || esc.ranges().get(0)[0] != esc.ranges().get(0)[1]) {
                    ranges.addAll(esc.ranges());
                    continue;
                }
                lo = esc.ranges().get(0)[0];
            } else {
                lo = next();
            }
            int hi = lo;
            if (more() && peek() == '-' && at[0] + 1 < pattern.length() && pattern.charAt(at[0] + 1) != ']') {
                next();
                char h = next();
                hi = h == '\\' ? escape(true).ranges().get(0)[0] : h;
            }
            ranges.add(new int[] {lo, hi});
        }
        if (!more()) {
            throw refused("an unclosed class");
        }
        next();
        return new CharSet(ranges, negated);
    }

    // ==================================================================
    // The NFA: states, character edges, anchor edges, epsilon edges
    // ==================================================================

    private record Edge(int from, int label, int to) {
    }

    private final List<Edge> chars = new ArrayList<>();
    private final List<Edge> anchors = new ArrayList<>();
    private final List<Edge> marks = new ArrayList<>();
    private final List<int[]> epsilons = new ArrayList<>();
    private final Map<String, Integer> predicates = new LinkedHashMap<>();
    private final int[] states = {0};

    private int state() {
        return states[0]++;
    }

    /** {@code n}'s states from {@code start}; its exit state. */
    private int build(Node n, int start) {
        return switch (n) {
            case Lit l -> {
                int e = state();
                chars.add(new Edge(start, predicate(l.set()), e));
                yield e;
            }
            case Seq s -> {
                int cur = start;
                for (Node item : s.items()) {
                    cur = build(item, cur);
                }
                yield cur;
            }
            case Alt a -> {
                int e = state();
                for (Node b : a.branches()) {
                    int s = state();
                    epsilons.add(new int[] {start, s});
                    epsilons.add(new int[] {build(b, s), e});
                }
                yield e;
            }
            case Repeat r -> {
                int cur = start;
                for (int i = 0; i < r.min(); i++) {
                    cur = build(r.node(), cur);
                }
                if (r.max() < 0) {
                    int loop = state();
                    epsilons.add(new int[] {cur, loop});
                    epsilons.add(new int[] {build(r.node(), loop), loop});
                    yield loop;
                }
                int e = state();
                epsilons.add(new int[] {cur, e});
                for (int i = r.min(); i < r.max(); i++) {
                    cur = build(r.node(), cur);
                    epsilons.add(new int[] {cur, e});
                }
                yield e;
            }
            case Anchor a -> {
                int e = state();
                anchors.add(new Edge(start, a.start() ? 0 : 1, e));
                yield e;
            }
            case Group g -> {
                int k = tracked.indexOf(g.number());
                if (k < 0) {
                    yield build(g.inner(), start);
                }
                // its span's ends set as the walk passes them: open (2k), close (2k + 1)
                int open = state();
                marks.add(new Edge(start, 2 * k, open));
                int e = state();
                marks.add(new Edge(build(g.inner(), open), 2 * k + 1, e));
                yield e;
            }
        };
    }

    /** The index of the SQL predicate testing one character ({@code ?} stands for it) against {@code set}. */
    private int predicate(CharSet set) {
        List<int[]> ranges = new ArrayList<>(set.ranges());
        if (flags[CI]) {
            for (int[] r : set.ranges()) {
                fold(r, 'A', 'Z', 'a' - 'A', ranges);
                fold(r, 'a', 'z', 'A' - 'a', ranges);
            }
        }
        StringBuilder sql = new StringBuilder(set.negated() ? "NOT (" : "(");
        for (int i = 0; i < ranges.size(); i++) {
            int[] r = ranges.get(i);
            sql.append(i > 0 ? " OR " : "").append(r[0] == r[1] ? "unicode(?) = " + r[0]
                    : "unicode(?) BETWEEN " + r[0] + " AND " + r[1]);
        }
        if (ranges.isEmpty()) {
            sql.append("0");
        }
        String text = sql.append(")").toString();
        return predicates.computeIfAbsent(text, k -> predicates.size());
    }

    /** A range's part within [lo, hi], shifted to the other case. */
    private static void fold(int[] r, char lo, char hi, int shift, List<int[]> into) {
        int a = Math.max(r[0], lo);
        int b = Math.min(r[1], hi);
        if (a <= b) {
            into.add(new int[] {a + shift, b + shift});
        }
    }

    private BitSet closure(int s) {
        BitSet seen = new BitSet();
        Deque<Integer> todo = new ArrayDeque<>();
        todo.push(s);
        while (!todo.isEmpty()) {
            int x = todo.pop();
            if (seen.get(x)) {
                continue;
            }
            seen.set(x);
            for (int[] e : epsilons) {
                if (e[0] == x) {
                    todo.push(e[1]);
                }
            }
        }
        return seen;
    }

    // ==================================================================
    // The match: the database's
    // ==================================================================

    /**
     * Whether {@code input} matches {@code pattern} -- the whole text ({@code full}), or anywhere in it (a search: a
     * leading state reads any character). NULL for a NULL input. The walk is {@code (position, state)} pairs, each
     * step one character through the table (or an anchor's test in place); UNION keeps each pair once, so it ends.
     */
    static SqlWriter.Piece matches(SqlWriter.Piece input, String pattern, boolean full) {
        SqliteRegex rx = new SqliteRegex(pattern);
        Node n = rx.parse();
        int start = rx.state();
        int entry = start;
        if (!full) {
            // a search: any prefix first
            entry = rx.state();
            rx.epsilons.add(new int[] {start, entry});
            rx.chars.add(new Edge(start, rx.predicate(new CharSet(List.of(), true)), start));
        }
        int accept = rx.build(n, entry);
        return rx.sql(input, start, accept, full);
    }

    /**
     * The leftmost match of {@code pattern} in {@code input}: a JSON array of its 0-based start and end and, for each
     * of {@code groups}, its group's start and end (NULL when the group took no part), or NULL for no match (or a NULL
     * input). Among the matches at the leftmost start, the longest -- the shortest when the pattern repeats lazily
     * (RE2's leftmost-first preference, for patterns whose only ambiguity is the repetition's; a group's span the one
     * its end prefers alike). A search: a leading state reads any character, each read moving the match's start past
     * it; a group's ends are set as the walk passes its parentheses.
     */
    static SqlWriter.Piece find(SqlWriter.Piece input, String pattern, List<Integer> groups) {
        SqliteRegex rx = new SqliteRegex(pattern);
        rx.tracked.addAll(groups);
        Node n = rx.parse();
        for (int g : groups) {
            if (g < 1 || g > rx.groupCount[0]) {
                throw rx.refused("no group " + g);
            }
        }
        int start = rx.state();
        int entry = rx.state();
        rx.epsilons.add(new int[] {start, entry});
        rx.chars.add(new Edge(start, rx.predicate(new CharSet(List.of(), true)), start));
        int accept = rx.build(n, entry);
        return rx.findSql(input, start, accept);
    }

    private SqlWriter.Piece findSql(SqlWriter.Piece input, int prefix, int accept) {
        String ch = "substr(__in.t, __rx.p + 1, 1)";
        int spans = tracked.size() * 2;
        StringBuilder cols = new StringBuilder();
        StringBuilder carry = new StringBuilder();
        StringBuilder nulls = new StringBuilder();
        for (int i = 0; i < spans; i++) {
            cols.append(", g").append(i);
            carry.append(", __rx.g").append(i);
            nulls.append(", NULL");
        }
        String chars0 = rows(chars);
        String anchors0 = rows(anchors);
        String marks0 = rows(marks);
        StringBuilder startRows = new StringBuilder();
        BitSet first = closure(prefix);
        for (int u = first.nextSetBit(0); u >= 0; u = first.nextSetBit(u + 1)) {
            startRows.append(startRows.length() > 0 ? ", " : "").append("(" + u + ")");
        }
        StringBuilder tests = new StringBuilder("CASE __e.column2");
        for (Map.Entry<String, Integer> p : predicates.entrySet()) {
            tests.append(" WHEN ").append(p.getValue()).append(" THEN ").append(p.getKey().replace("?", ch));
        }
        tests.append(" END");
        String bol = flags[MULTILINE] ? "(__rx.p = 0 OR substr(__in.t, __rx.p, 1) = char(10))" : "__rx.p = 0";
        String eol = flags[MULTILINE] ? "(__rx.p = length(__in.t) OR substr(__in.t, __rx.p + 1, 1) = char(10))"
                : "__rx.p = length(__in.t)";
        StringBuilder setMarks = new StringBuilder();
        for (int i = 0; i < spans; i++) {
            setMarks.append(", CASE WHEN __k.column2 = ").append(i).append(" THEN __rx.p ELSE __rx.g").append(i)
                    .append(" END");
        }
        StringBuilder result = new StringBuilder("json_array(m, p");
        String dir = lazy[0] ? " ASC" : " DESC";
        StringBuilder order = new StringBuilder("m ASC, p" + dir);
        for (int i = 0; i < spans; i++) {
            result.append(", g").append(i);
            order.append(", g").append(i).append(dir);
        }
        result.append(")");
        return w -> {
            w.append("(WITH RECURSIVE __in(t) AS (SELECT ").piece(input).append("), __rx(p, s, m" + cols + ") AS"
                    + " (SELECT 0, column1, 0" + nulls + " FROM (VALUES " + startRows + ")");
            w.append(" UNION SELECT __rx.p + 1, __e.column3, CASE WHEN __e.column1 = " + prefix
                    + " THEN __rx.p + 1 ELSE __rx.m END" + carry + " FROM __rx, __in JOIN (VALUES " + chars0
                    + ") AS __e ON __e.column1 = __rx.s WHERE __rx.p < length(__in.t) AND " + tests);
            if (!anchors0.isEmpty()) {
                w.append(" UNION SELECT __rx.p, __a.column3, __rx.m" + carry + " FROM __rx, __in JOIN (VALUES "
                        + anchors0 + ") AS __a ON __a.column1 = __rx.s WHERE CASE __a.column2 WHEN 0 THEN " + bol
                        + " ELSE " + eol + " END");
            }
            if (!marks0.isEmpty()) {
                w.append(" UNION SELECT __rx.p, __k.column3, __rx.m" + setMarks + " FROM __rx JOIN (VALUES " + marks0
                        + ") AS __k ON __k.column1 = __rx.s");
            }
            w.append(") SELECT " + result + " FROM __rx WHERE s = " + accept + " ORDER BY " + order + " LIMIT 1)");
        };
    }

    /** The table of {@code edges}, each to every state of its target's closure. */
    private String rows(List<Edge> edges) {
        StringBuilder out = new StringBuilder();
        for (Edge e : edges) {
            BitSet to = closure(e.to());
            for (int u = to.nextSetBit(0); u >= 0; u = to.nextSetBit(u + 1)) {
                out.append(out.length() > 0 ? ", " : "").append("(" + e.from() + ", " + e.label() + ", " + u + ")");
            }
        }
        return out.toString();
    }

    private SqlWriter.Piece sql(SqlWriter.Piece input, int start, int accept, boolean full) {
        String ch = "substr(__in.t, __rx.p + 1, 1)";
        StringBuilder charRows = new StringBuilder();
        for (Edge e : chars) {
            BitSet to = closure(e.to());
            for (int u = to.nextSetBit(0); u >= 0; u = to.nextSetBit(u + 1)) {
                charRows.append(charRows.length() > 0 ? ", " : "").append("(" + e.from() + ", " + e.label() + ", " + u
                        + ")");
            }
        }
        StringBuilder anchorRows = new StringBuilder();
        for (Edge e : anchors) {
            BitSet to = closure(e.to());
            for (int u = to.nextSetBit(0); u >= 0; u = to.nextSetBit(u + 1)) {
                anchorRows.append(anchorRows.length() > 0 ? ", " : "").append("(" + e.from() + ", " + e.label() + ", "
                        + u + ")");
            }
        }
        StringBuilder startRows = new StringBuilder();
        BitSet first = closure(start);
        for (int u = first.nextSetBit(0); u >= 0; u = first.nextSetBit(u + 1)) {
            startRows.append(startRows.length() > 0 ? ", " : "").append("(" + u + ")");
        }
        StringBuilder tests = new StringBuilder("CASE __e.column2");
        for (Map.Entry<String, Integer> p : predicates.entrySet()) {
            tests.append(" WHEN ").append(p.getValue()).append(" THEN ").append(p.getKey().replace("?", ch));
        }
        tests.append(" END");
        String bol = flags[MULTILINE] ? "(__rx.p = 0 OR substr(__in.t, __rx.p, 1) = char(10))" : "__rx.p = 0";
        String eol = flags[MULTILINE] ? "(__rx.p = length(__in.t) OR substr(__in.t, __rx.p + 1, 1) = char(10))"
                : "__rx.p = length(__in.t)";
        String chars0 = charRows.toString();
        String anchors0 = anchorRows.toString();
        return w -> {
            w.append("(WITH RECURSIVE __in(t) AS (SELECT ").piece(input).append("), __rx(p, s) AS (SELECT 0, column1"
                    + " FROM (VALUES " + startRows + ")");
            if (!chars0.isEmpty()) {
                w.append(" UNION SELECT __rx.p + 1, __e.column3 FROM __rx, __in JOIN (VALUES " + chars0 + ") AS __e ON"
                        + " __e.column1 = __rx.s WHERE __rx.p < length(__in.t) AND " + tests);
            }
            if (!anchors0.isEmpty()) {
                w.append(" UNION SELECT __rx.p, __a.column3 FROM __rx, __in JOIN (VALUES " + anchors0 + ") AS __a ON"
                        + " __a.column1 = __rx.s WHERE CASE __a.column2 WHEN 0 THEN " + bol + " ELSE " + eol + " END");
            }
            w.append(") SELECT CASE WHEN (SELECT t FROM __in) IS NULL THEN NULL ELSE EXISTS(SELECT 1 FROM __rx, __in"
                    + " WHERE __rx.s = " + accept + (full ? " AND __rx.p = length(__in.t)" : "") + ") END)");
        };
    }
}
