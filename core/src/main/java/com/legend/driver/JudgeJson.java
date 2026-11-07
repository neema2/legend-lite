// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.base.Nullable;
import com.legend.executionplan.ExecutionPlan;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * THE TWO TEXTS OF {@code assertJsonStringsEqual} as both judges read them (register S144, the user's ruling of
 * 2026-11-07, option 2): lite's own runs read strict JSON -- a text that is not JSON is refused; the compatibility
 * rules read them as upstream does. Upstream's {@code equalJsonStrings} ({@code CompiledSupport.jsonStringEquals})
 * parses both with json-simple 1.1.1, legend-pure's pin, whose grammar this is, probed against the 1.1.1 jar
 * (GATES 2026-11-07): commas and colons are skipped wherever they stand, so a missing or extra one reads; an
 * unterminated string at the very end reads as the end; any other token after the root value is refused; an escape
 * json-simple does not know keeps its backslash; {@code true}, {@code false} and {@code null} are lowercase; a number
 * is {@code -?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?}, an integer past a Long refused; whitespace is space, tab, CR
 * and LF alone; a {@code \\u} escape takes four ASCII hex digits. Values are lite's (a Long, a BigDecimal for a
 * fraction or an exponent), as {@code com.legend.sql.Json} reads them: the comparison is the judges' own.
 * Measured on the corpus (2026-11-07, both databases): two goldens read only under the compatibility grammar, the
 * graph-fetch milestoning goldens ending {@code ]"}. Compatibility code: the edge's when the edge package exists (P2).
 */
final class JudgeJson {

    private JudgeJson() {
    }

    /** {@code text} read under {@code rules}; a text the rules refuse raises {@link IllegalStateException}. */
    static @Nullable Object read(String text, ExecutionPlan.JudgingRules rules) {
        if (rules == ExecutionPlan.JudgingRules.COMPILED_PURE_COMPAT) {
            return new JsonSimple(text).parse();
        }
        try {
            com.legend.json.Json.parse(text);
        } catch (IllegalArgumentException notJson) {
            throw new IllegalStateException("not JSON: " + notJson.getMessage(), notJson);
        }
        return com.legend.sql.Json.parse(text);
    }

    /** json-simple 1.1.1's lexer and parser state machine ({@code org.json.simple.parser.JSONParser#parse}), ported. */
    private static final class JsonSimple {
        private enum Kind { LEFT_BRACE, RIGHT_BRACE, LEFT_SQUARE, RIGHT_SQUARE, COMMA, COLON, VALUE, EOF }

        private static final int INIT = 0;
        private static final int FINISHED_VALUE = 1;
        private static final int IN_OBJECT = 2;
        private static final int IN_ARRAY = 3;
        private static final int PASSED_PAIR_KEY = 4;
        private static final int IN_ERROR = -1;

        private final String s;
        private int at;
        private Kind kind = Kind.EOF;
        private @Nullable Object value;

        JsonSimple(String s) {
            this.s = s;
        }

        @Nullable Object parse() {
            LinkedList<Integer> statuses = new LinkedList<>();
            LinkedList<@Nullable Object> values = new LinkedList<>();
            int status = INIT;
            do {
                next();
                switch (status) {
                    case INIT -> {
                        switch (kind) {
                            case VALUE -> {
                                status = FINISHED_VALUE;
                                statuses.addFirst(status);
                                values.addFirst(value);
                            }
                            case LEFT_BRACE -> {
                                status = IN_OBJECT;
                                statuses.addFirst(status);
                                values.addFirst(new LinkedHashMap<String, Object>());
                            }
                            case LEFT_SQUARE -> {
                                status = IN_ARRAY;
                                statuses.addFirst(status);
                                values.addFirst(new ArrayList<Object>());
                            }
                            default -> status = IN_ERROR;
                        }
                    }
                    case FINISHED_VALUE -> {
                        if (kind == Kind.EOF) {
                            return values.removeFirst();
                        }
                        throw unexpected();
                    }
                    case IN_OBJECT -> {
                        switch (kind) {
                            case COMMA -> {
                            }
                            case VALUE -> {
                                if (value instanceof String key) {
                                    values.addFirst(key);
                                    status = PASSED_PAIR_KEY;
                                    statuses.addFirst(status);
                                } else {
                                    status = IN_ERROR;
                                }
                            }
                            case RIGHT_BRACE -> status = closed(statuses, values);
                            default -> status = IN_ERROR;
                        }
                    }
                    case PASSED_PAIR_KEY -> {
                        switch (kind) {
                            case COLON -> {
                            }
                            case VALUE -> {
                                statuses.removeFirst();
                                String key = (String) values.removeFirst();
                                map(values.getFirst()).put(key, value);
                                status = statuses.getFirst();
                            }
                            case LEFT_SQUARE, LEFT_BRACE -> {
                                statuses.removeFirst();
                                String key = (String) values.removeFirst();
                                Object container = kind == Kind.LEFT_SQUARE ? new ArrayList<Object>()
                                        : new LinkedHashMap<String, Object>();
                                map(values.getFirst()).put(key, container);
                                status = kind == Kind.LEFT_SQUARE ? IN_ARRAY : IN_OBJECT;
                                statuses.addFirst(status);
                                values.addFirst(container);
                            }
                            default -> status = IN_ERROR;
                        }
                    }
                    case IN_ARRAY -> {
                        switch (kind) {
                            case COMMA -> {
                            }
                            case VALUE -> list(values.getFirst()).add(value);
                            case RIGHT_SQUARE -> status = closed(statuses, values);
                            case LEFT_SQUARE, LEFT_BRACE -> {
                                Object container = kind == Kind.LEFT_SQUARE ? new ArrayList<Object>()
                                        : new LinkedHashMap<String, Object>();
                                list(values.getFirst()).add(container);
                                status = kind == Kind.LEFT_SQUARE ? IN_ARRAY : IN_OBJECT;
                                statuses.addFirst(status);
                                values.addFirst(container);
                            }
                            default -> status = IN_ERROR;
                        }
                    }
                    default -> throw unexpected();
                }
                if (status == IN_ERROR) {
                    throw unexpected();
                }
            } while (kind != Kind.EOF);
            throw unexpected();
        }

        /** A container closed: the one it sits in resumes, or the root is finished. */
        private static int closed(LinkedList<Integer> statuses, LinkedList<@Nullable Object> values) {
            if (values.size() > 1) {
                statuses.removeFirst();
                values.removeFirst();
                return statuses.getFirst();
            }
            return FINISHED_VALUE;
        }

        @SuppressWarnings("unchecked")
        private static Map<String, Object> map(@Nullable Object o) {
            return (Map<String, Object>) java.util.Objects.requireNonNull(o);
        }

        @SuppressWarnings("unchecked")
        private static List<Object> list(@Nullable Object o) {
            return (List<Object>) java.util.Objects.requireNonNull(o);
        }

        private IllegalStateException unexpected() {
            return new IllegalStateException("Unexpected token " + kind + " at position " + at
                    + " (json-simple 1.1.1's grammar)");
        }

        /** The next token: whitespace skipped; an unterminated string at the end is the end. */
        private void next() {
            while (at < s.length() && " \t\n\r".indexOf(s.charAt(at)) >= 0) {
                at++;
            }
            value = null;
            if (at >= s.length()) {
                kind = Kind.EOF;
                return;
            }
            char c = s.charAt(at);
            switch (c) {
                case '{' -> single(Kind.LEFT_BRACE);
                case '}' -> single(Kind.RIGHT_BRACE);
                case '[' -> single(Kind.LEFT_SQUARE);
                case ']' -> single(Kind.RIGHT_SQUARE);
                case ',' -> single(Kind.COMMA);
                case ':' -> single(Kind.COLON);
                case '"' -> string();
                default -> {
                    if (word("true", Boolean.TRUE) || word("false", Boolean.FALSE) || word("null", null)) {
                        return;
                    }
                    if (c == '-' || (c >= '0' && c <= '9')) {
                        number();
                        return;
                    }
                    throw new IllegalStateException("Unexpected character (" + c + ") at position " + at
                            + " (json-simple 1.1.1's grammar)");
                }
            }
        }

        private void single(Kind k) {
            at++;
            kind = k;
        }

        private boolean word(String w, @Nullable Object v) {
            if (!s.startsWith(w, at)) {
                return false;
            }
            at += w.length();
            kind = Kind.VALUE;
            value = v;
            return true;
        }

        private void number() {
            int start = at;
            if (s.charAt(at) == '-') {
                at++;
            }
            int digits = digits();
            if (digits == 0) {
                throw new IllegalStateException("Unexpected character (" + s.charAt(start) + ") at position " + start
                        + " (json-simple 1.1.1's grammar)");
            }
            boolean decimal = false;
            if (at + 1 < s.length() && s.charAt(at) == '.' && isDigit(s.charAt(at + 1))) {
                at++;
                digits();
                decimal = true;
            }
            if (at < s.length() && (s.charAt(at) == 'e' || s.charAt(at) == 'E')) {
                int mark = at;
                at++;
                if (at < s.length() && (s.charAt(at) == '+' || s.charAt(at) == '-')) {
                    at++;
                }
                if (digits() == 0) {
                    at = mark;   // no exponent: the number ends before the e, which lexes on its own
                } else {
                    decimal = true;
                }
            }
            String t = s.substring(start, at);
            kind = Kind.VALUE;
            if (decimal) {
                value = new BigDecimal(t);
            } else {
                BigInteger big = new BigInteger(t);
                if (big.bitLength() >= 64) {
                    // json-simple reads an integer as a Long, and one past it raises (NumberFormatException)
                    throw new IllegalStateException("For input string: \"" + t + "\" (an integer past a Long, refused"
                            + " by json-simple 1.1.1's grammar)");
                }
                value = big.longValue();
            }
        }

        private int digits() {
            int start = at;
            while (at < s.length() && isDigit(s.charAt(at))) {
                at++;
            }
            return at - start;
        }

        private static boolean isDigit(char c) {
            return c >= '0' && c <= '9';
        }

        /** An ASCII hex digit: json-simple's {@code \\u} takes no other (a fullwidth or Arabic-Indic digit keeps the
         *  escape's backslash, probed 2026-11-07). */
        private static boolean isHexDigit(int c) {
            return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F';
        }

        /** A string's escapes as json-simple reads them; one unterminated at the end is the end (no token). */
        private void string() {
            at++;
            StringBuilder b = new StringBuilder();
            while (at < s.length()) {
                char c = s.charAt(at);
                if (c == '"') {
                    at++;
                    kind = Kind.VALUE;
                    value = b.toString();
                    return;
                }
                if (c != '\\' || at + 1 >= s.length()) {
                    b.append(c);
                    at++;
                    continue;
                }
                char e = s.charAt(at + 1);
                switch (e) {
                    case '"' -> b.append('"');
                    case '\\' -> b.append('\\');
                    case '/' -> b.append('/');
                    case 'b' -> b.append('\b');
                    case 'f' -> b.append('\f');
                    case 'n' -> b.append('\n');
                    case 'r' -> b.append('\r');
                    case 't' -> b.append('\t');
                    case 'u' -> {
                        if (at + 6 <= s.length() && s.substring(at + 2, at + 6).chars()
                                .allMatch(JsonSimple::isHexDigit)) {
                            b.append((char) Integer.parseInt(s.substring(at + 2, at + 6), 16));
                            at += 6;
                        } else {
                            b.append('\\');
                            at++;
                        }
                        continue;
                    }
                    default -> {
                        // an escape json-simple does not know keeps its backslash; the character reads on its own
                        b.append('\\');
                        at++;
                        continue;
                    }
                }
                at += 2;
            }
            kind = Kind.EOF;
        }
    }
}
