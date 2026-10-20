// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testcases;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The openCypher TCK's value grammar (tck/README.adoc, "Values"), the one language both sides of the Cypher parity
 * judge speak (docs/CYPHER_STUDY_2026_10_15.md §8.3): the TCK's expected tables are written in it, the oracle writes
 * Neo4j's rows in it ({@code tools/neo4j-oracle/snap.py}, {@code fmt}), and a lite cell is read into it here. Values
 * compare structurally: a node by its label set and properties, never by an id (C5); a map by its keys; a list in
 * order, or as a bag where the scenario says so; a float within a tolerance.
 */
public final class CypherValues {

    private CypherValues() {
    }

    /** A value of the grammar. */
    public sealed interface Value permits Null, Bool, Int, Flt, Str, Lst, Mp, Node, Rel, Path, Opaque {
    }

    public record Null() implements Value {
    }

    public record Bool(boolean value) implements Value {
    }

    public record Int(BigInteger value) implements Value {
    }

    public record Flt(double value) implements Value {
    }

    public record Str(String value) implements Value {
    }

    public record Lst(List<Value> items) implements Value {
    }

    public record Mp(Map<String, Value> entries) implements Value {
    }

    /** {@code (:A:B {k: v})}: labels as a sorted set. */
    public record Node(TreeSet<String> labels, Map<String, Value> props) implements Value {
    }

    /** {@code [:T {k: v}]}. */
    public record Rel(String type, Map<String, Value> props) implements Value {
    }

    /** {@code <(a)-[:T]->(b)>}: its text, compared as written (nodes and paths are not values lite returns yet). */
    /** A path: its nodes and its relationships in traversal order, and each relationship's direction as traversed
     *  (forward: the node before it to the node after); Neo4j's {@code <(:A)-[:T]->(:B)>}, lite's
     *  {@code {"@path":[node, rel, node, ...], "@dirs":[true, ...]}} (the study's §12.27). */
    public record Path(List<Value> nodes, List<Value> rels, List<Boolean> forward) implements Value {
    }

    /** A value of a kind the grammar has no spelling for (a temporal, a point), as the oracle tags it. */
    public record Opaque(String text) implements Value {
    }

    // ------------------------------------------------------------------ reading

    /** The value {@code text} spells. */
    public static Value parse(String text) {
        Reader r = new Reader(text);
        Value v = r.value();
        r.ws();
        if (r.i != text.length()) {
            throw new IllegalArgumentException("value grammar: trailing text in " + text);
        }
        return v;
    }

    private static final class Reader {
        private final String s;
        private int i;

        Reader(String s) {
            this.s = s;
        }

        void ws() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
        }

        private char peek() {
            return i < s.length() ? s.charAt(i) : '\0';
        }

        private boolean take(String word) {
            if (s.startsWith(word, i)) {
                i += word.length();
                return true;
            }
            return false;
        }

        private void expect(char c) {
            ws();
            if (peek() != c) {
                throw new IllegalArgumentException("value grammar: expected '" + c + "' at " + i + " in " + s);
            }
            i++;
        }

        Value value() {
            ws();
            char c = peek();
            if (c == '\'') {
                return new Str(string());
            }
            if (c == '[') {
                int start = i;
                i++;
                ws();
                if (peek() == ':') {
                    i = start;
                    return rel();
                }
                i = start;
                return list();
            }
            if (c == '{') {
                return new Mp(map());
            }
            if (c == '(') {
                return node();
            }
            if (c == '<') {
                return path();
            }
            if (take("true")) {
                return new Bool(true);
            }
            if (take("false")) {
                return new Bool(false);
            }
            if (take("null")) {
                return new Null();
            }
            if (take("NaN")) {
                return new Flt(Double.NaN);
            }
            if (take("Inf")) {
                return new Flt(Double.POSITIVE_INFINITY);
            }
            if (take("-Inf")) {
                return new Flt(Double.NEGATIVE_INFINITY);
            }
            int start = i;
            if (peek() == '-' || peek() == '+') {
                i++;
            }
            while (i < s.length() && Character.isDigit(s.charAt(i))) {
                i++;
            }
            boolean flt = false;
            if (peek() == '.' && i + 1 < s.length() && Character.isDigit(s.charAt(i + 1))) {
                flt = true;
                i++;
                while (i < s.length() && Character.isDigit(s.charAt(i))) {
                    i++;
                }
            }
            if (peek() == 'e' || peek() == 'E') {
                int save = i;
                i++;
                if (peek() == '-' || peek() == '+') {
                    i++;
                }
                if (Character.isDigit(peek())) {
                    flt = true;
                    while (i < s.length() && Character.isDigit(s.charAt(i))) {
                        i++;
                    }
                } else {
                    i = save;
                }
            }
            String number = s.substring(start, i);
            if (number.isEmpty() || number.equals("-") || number.equals("+")) {
                // an opaque tagged value: Kind('text')
                i = start;
                return opaque();
            }
            return flt ? new Flt(Double.parseDouble(number)) : new Int(new BigInteger(number));
        }

        private Value opaque() {
            int start = i;
            while (i < s.length() && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_')) {
                i++;
            }
            if (i == start || peek() != '(') {
                throw new IllegalArgumentException("value grammar: unexpected '" + peek() + "' at " + i + " in " + s);
            }
            int depth = 0;
            while (i < s.length()) {
                char c = s.charAt(i);
                if (c == '\'') {
                    string();
                    continue;
                }
                if (c == '(') {
                    depth++;
                } else if (c == ')') {
                    depth--;
                    if (depth == 0) {
                        i++;
                        break;
                    }
                }
                i++;
            }
            return new Opaque(s.substring(start, i));
        }

        private String string() {
            expect('\'');
            StringBuilder out = new StringBuilder();
            while (true) {
                if (i >= s.length()) {
                    throw new IllegalArgumentException("value grammar: unterminated string in " + s);
                }
                char c = s.charAt(i++);
                if (c == '\'') {
                    return out.toString();
                }
                if (c == '\\') {
                    char e = s.charAt(i++);
                    out.append(switch (e) {
                        case 'n' -> '\n';
                        case 't' -> '\t';
                        case 'r' -> '\r';
                        default -> e;
                    });
                } else {
                    out.append(c);
                }
            }
        }

        private Value list() {
            expect('[');
            List<Value> items = new ArrayList<>();
            ws();
            if (peek() == ']') {
                i++;
                return new Lst(items);
            }
            while (true) {
                items.add(value());
                ws();
                if (peek() == ',') {
                    i++;
                    continue;
                }
                expect(']');
                return new Lst(items);
            }
        }

        private Map<String, Value> map() {
            expect('{');
            Map<String, Value> out = new TreeMap<>();
            ws();
            if (peek() == '}') {
                i++;
                return out;
            }
            while (true) {
                ws();
                String key = key();
                expect(':');
                out.put(key, value());
                ws();
                if (peek() == ',') {
                    i++;
                    continue;
                }
                expect('}');
                return out;
            }
        }

        private String key() {
            if (peek() == '`') {
                i++;
                int start = i;
                while (i < s.length() && s.charAt(i) != '`') {
                    i++;
                }
                String k = s.substring(start, i);
                i++;
                return k;
            }
            int start = i;
            while (i < s.length() && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_')) {
                i++;
            }
            if (start == i) {
                throw new IllegalArgumentException("value grammar: expected a key at " + i + " in " + s);
            }
            return s.substring(start, i);
        }

        private Value node() {
            expect('(');
            TreeSet<String> labels = new TreeSet<>();
            ws();
            while (peek() == ':') {
                i++;
                labels.add(key());
                ws();
            }
            Map<String, Value> props = peek() == '{' ? map() : Map.of();
            expect(')');
            return new Node(labels, props);
        }

        private Value rel() {
            expect('[');
            expect(':');
            String type = key();
            ws();
            Map<String, Value> props = peek() == '{' ? map() : Map.of();
            expect(']');
            return new Rel(type, props);
        }

        /** {@code <(node)-[rel]->(node)<-[rel]-(node)>}: a node, then arrows each with its relationship. */
        private Value path() {
            expect('<');
            List<Value> nodes = new ArrayList<>();
            List<Value> rels = new ArrayList<>();
            List<Boolean> forward = new ArrayList<>();
            ws();
            nodes.add(node());
            ws();
            while (peek() != '>') {
                boolean back = peek() == '<';
                if (back) {
                    i++;
                }
                expect('-');
                ws();
                rels.add(rel());
                ws();
                expect('-');
                if (!back) {
                    expect('>');
                }
                forward.add(!back);
                ws();
                nodes.add(node());
                ws();
            }
            expect('>');
            return new Path(nodes, rels, forward);
        }
    }

    // ------------------------------------------------------------------ lite's cells

    /** A cell of a lite result as a value: integers, floats, strings, booleans and null; another kind as the text
     *  the result spells it. */
    public static Value ofCell(@com.legend.base.Nullable Object cell) {
        if (cell == null) {
            return new Null();
        }
        if (cell instanceof Boolean b) {
            return new Bool(b);
        }
        if (cell instanceof BigInteger n) {
            return new Int(n);
        }
        if (cell instanceof Long || cell instanceof Integer || cell instanceof Short || cell instanceof Byte) {
            return new Int(BigInteger.valueOf(((Number) cell).longValue()));
        }
        if (cell instanceof BigDecimal d) {
            return d.scale() <= 0 && d.stripTrailingZeros().scale() <= 0 && isWhole(d) ? new Int(d.toBigInteger())
                    : new Flt(d.doubleValue());
        }
        if (cell instanceof Double || cell instanceof Float) {
            return new Flt(((Number) cell).doubleValue());
        }
        if (cell instanceof String s) {
            return new Str(s);
        }
        // a date or a date-time (lite's own literal values, or the driver's): as Neo4j's Python driver spells them
        // in the answers, Date('yyyy-MM-dd') and DateTime('yyyy-MM-ddTHH:mm:ss.nnnnnnnnn'), the nanoseconds always
        // nine digits
        switch (cell) {
            case com.legend.values.PureDateLiteral.StrictDate d -> {
                return new Opaque("Date('" + date(d.year(), d.month(), d.day()) + "')");
            }
            case com.legend.values.PureDateLiteral.DateWithHour d -> {
                return new Opaque(dateTime(d.year(), d.month(), d.day(), d.hour(), 0, 0, ""));
            }
            case com.legend.values.PureDateLiteral.DateWithMinute d -> {
                return new Opaque(dateTime(d.year(), d.month(), d.day(), d.hour(), d.minute(), 0, ""));
            }
            case com.legend.values.PureDateLiteral.DateWithSecond d -> {
                return new Opaque(dateTime(d.year(), d.month(), d.day(), d.hour(), d.minute(), d.second(), ""));
            }
            case com.legend.values.PureDateLiteral.DateWithSubsecond d -> {
                return new Opaque(dateTime(d.year(), d.month(), d.day(), d.hour(), d.minute(), d.second(),
                        d.subsecond()));
            }
            case java.sql.Date d -> {
                java.time.LocalDate l = d.toLocalDate();
                return new Opaque("Date('" + date(l.getYear(), l.getMonthValue(), l.getDayOfMonth()) + "')");
            }
            case java.time.LocalDate l -> {
                return new Opaque("Date('" + date(l.getYear(), l.getMonthValue(), l.getDayOfMonth()) + "')");
            }
            case java.sql.Timestamp ts -> {
                return ofCell(ts.toLocalDateTime());
            }
            case java.time.LocalDateTime l -> {
                return new Opaque(dateTime(l.getYear(), l.getMonthValue(), l.getDayOfMonth(), l.getHour(),
                        l.getMinute(), l.getSecond(), String.format(java.util.Locale.ROOT, "%09d", l.getNano())));
            }
            // a time of day (lite's StrictTime, or the driver's): Time('HH:mm:ss.nnnnnnnnn'), as the Python driver
            // spells a local time, the nanoseconds always nine digits
            case com.legend.values.PureTimeLiteral.TimeWithMinute t -> {
                return new Opaque(time(t.hour(), t.minute(), 0, ""));
            }
            case com.legend.values.PureTimeLiteral.TimeWithSecond t -> {
                return new Opaque(time(t.hour(), t.minute(), t.second(), ""));
            }
            case com.legend.values.PureTimeLiteral.TimeWithSubsecond t -> {
                return new Opaque(time(t.hour(), t.minute(), t.second(), t.subsecond()));
            }
            case java.sql.Time t -> {
                return ofCell(t.toLocalTime());
            }
            case java.time.LocalTime l -> {
                return new Opaque(time(l.getHour(), l.getMinute(), l.getSecond(), String.format(
                        java.util.Locale.ROOT, "%09d", l.getNano())));
            }
            default -> {
            }
        }
        // a struct cell (an instance of the model's Duration class on DuckDB): the executor's ordered field map
        if (cell instanceof Map<?, ?> m) {
            Map<String, Value> entries = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : m.entrySet()) {
                entries.put(String.valueOf(e.getKey()), ofCell(e.getValue()));
            }
            return new Mp(entries);
        }
        // a list or map, as the Variant lite returns (C8): DuckDB's JsonNode, Postgres's json/jsonb PGobject
        String cls = cell.getClass().getName();
        if (cls.equals("org.duckdb.JsonNode")) {
            return ofJson(NonFiniteJson.parse(cell.toString()));
        }
        if (cls.equals("org.postgresql.util.PGobject")) {
            try {
                String type = (String) cell.getClass().getMethod("getType").invoke(cell);
                if (type.equals("json") || type.equals("jsonb")) {
                    return ofJson(com.legend.json.Json.parse((String) cell.getClass().getMethod("getValue")
                            .invoke(cell)));
                }
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("pgjdbc's PGobject", e);
            }
        }
        return new Opaque(cell.getClass().getSimpleName() + "(" + new Str(cell.toString()) + ")");
    }

    private static String date(int year, int month, int day) {
        return String.format(java.util.Locale.ROOT, "%04d-%02d-%02d", year, month, day);
    }

    /** A duration of Neo4j's ({@code Duration('P12Y5M14DT16H13M10S')}) against lite's, an instance of the model's
     *  Duration class (a map of its four slots: months, days, seconds, nanoseconds, docs/SEMANTICS_REGISTER.md S67):
     *  equal when the text's units sum to the slots (years are twelve months, weeks seven days, hours and minutes
     *  seconds; the seconds' fraction nanoseconds, the seconds floored so the nanoseconds lie in [0, 1e9), as Neo4j
     *  keeps them). A key of the map starting with {@code __} (a layout's identity) is not a slot. */
    static boolean sameDuration(String text, Map<String, Value> slots) {
        long[] expected = durationSlots(text);
        if (expected == null) {
            return false;
        }
        String[] names = {"months", "days", "seconds", "nanoseconds"};
        int seen = 0;
        for (String k : slots.keySet()) {
            if (!k.startsWith("__")) {
                seen++;
            }
        }
        if (seen != 4) {
            return false;
        }
        for (int i = 0; i < 4; i++) {
            Value v = slots.get(names[i]);
            if (!(v instanceof Int n) || n.value().longValueExact() != expected[i]) {
                return false;
            }
        }
        return true;
    }

    /** Two local times of day ({@code Time('HH:mm:ss.nnnnnnnnn')}, no offset): equal when they name one time, whatever
     *  the fraction's digits (Neo4j spells nine, the island's text of a stored time Neo4j's toString, which cuts the
     *  trailing zeros: the study's §12.59). */
    static boolean sameLocalTime(String a, String b) {
        java.time.LocalTime x = localTime(a);
        java.time.LocalTime y = localTime(b);
        return x != null && x.equals(y);
    }

    /** Two local date-times of the value grammar ({@code DateTime('2017-01-01T12:00:00')}, Neo4j's nine digits of
     *  the second or the island's trimmed ones, a list's or map's element, the study's §12.76): equal when they
     *  name one local date-time. */
    static boolean sameLocalDateTime(String a, String b) {
        java.time.LocalDateTime x = localDateTime(a);
        java.time.LocalDateTime y = localDateTime(b);
        return x != null && x.equals(y);
    }

    private static java.time.@com.legend.base.Nullable LocalDateTime localDateTime(String text) {
        if (!text.startsWith("DateTime('") || !text.endsWith("')")) {
            return null;
        }
        try {
            return java.time.LocalDateTime.parse(text.substring(10, text.length() - 2));
        } catch (java.time.format.DateTimeParseException e) {
            return null;   // a zoned date-time
        }
    }

    private static java.time.@com.legend.base.Nullable LocalTime localTime(String text) {
        if (!text.startsWith("Time('") || !text.endsWith("')")) {
            return null;
        }
        try {
            return java.time.LocalTime.parse(text.substring(6, text.length() - 2));
        } catch (java.time.format.DateTimeParseException e) {
            return null;   // a zoned time, or a date-time's text
        }
    }

    /** The four slots a duration text of Neo4j's spells; null when the text is not one. */
    static long @com.legend.base.Nullable [] durationSlots(String text) {
        if (!text.startsWith("Duration('") || !text.endsWith("')")) {
            return null;
        }
        String body = text.substring(10, text.length() - 2);
        if (!body.startsWith("P")) {
            return null;
        }
        long months = 0;
        long days = 0;
        java.math.BigDecimal nanos = java.math.BigDecimal.ZERO;
        boolean time = false;
        int i = 1;
        while (i < body.length()) {
            char ch = body.charAt(i);
            if (ch == 'T') {
                time = true;
                i++;
                continue;
            }
            int start = i;
            if (ch == '-' || ch == '+') {
                i++;
            }
            while (i < body.length() && (Character.isDigit(body.charAt(i)) || body.charAt(i) == '.')) {
                i++;
            }
            if (i >= body.length()) {
                return null;
            }
            java.math.BigDecimal n = new java.math.BigDecimal(body.substring(start, i));
            char unit = body.charAt(i++);
            switch (unit) {
                case 'Y' -> months += 12 * n.longValueExact();
                case 'M' -> {
                    if (time) {
                        nanos = nanos.add(n.multiply(java.math.BigDecimal.valueOf(60_000_000_000L)));
                    } else {
                        months += n.longValueExact();
                    }
                }
                case 'W' -> days += 7 * n.longValueExact();
                case 'D' -> days += n.longValueExact();
                case 'H' -> nanos = nanos.add(n.multiply(java.math.BigDecimal.valueOf(3_600_000_000_000L)));
                case 'S' -> nanos = nanos.add(n.multiply(java.math.BigDecimal.valueOf(1_000_000_000L)));
                default -> {
                    return null;
                }
            }
        }
        // The seconds and their nanoseconds apart: a duration across the millennia (9998 years and more, the year 1's
        // date-time to the year 9999's) is past a long's nanoseconds, its seconds well inside one.
        java.math.BigDecimal[] split = nanos.divideAndRemainder(java.math.BigDecimal.valueOf(1_000_000_000L));
        long seconds = split[0].longValueExact();
        long rest = split[1].longValueExact();
        if (rest < 0) {
            seconds--;
            rest += 1_000_000_000L;
        }
        return new long[] {months, days, seconds, rest};
    }

    /** A zoned date-time of Neo4j's ({@code DateTime('...T...+01:00')}) against lite's, which keeps the instant and
     *  no zone ({@code DateTime('...')}, UTC): equal when they name one instant (docs/SEMANTICS_REGISTER.md S65).
     *  Only a pair of one zoned and one local text compares so; two of one kind compare as text. */
    static boolean sameInstant(String a, String b) {
        java.time.Instant x = instantOf(a);
        java.time.Instant y = instantOf(b);
        return x != null && y != null && x.equals(y) && zonedText(a) != zonedText(b);
    }

    /** A zoned time of day of Neo4j's ({@code Time('12:31:14.645876000+01:00')}) against lite's, which keeps its
     *  instant on the epoch day and no zone ({@code DateTime('1970-01-01T11:31:14.645876000')}): equal when they
     *  name one instant (docs/SEMANTICS_REGISTER.md S68). Only a pair of one zoned time and one local date-time
     *  text compares so. */
    static boolean sameEpochTime(String a, String b) {
        String zonedTime = zonedTimeText(a) ? a : zonedTimeText(b) ? b : null;
        if (zonedTime == null) {
            return false;
        }
        String local = zonedTimeText(a) ? b : a;
        if (!local.startsWith("DateTime('") || !local.endsWith("')") || zonedText(local)) {
            return false;
        }
        try {
            java.time.Instant x = java.time.OffsetTime.parse(zonedTime.substring(6, zonedTime.length() - 2))
                    .atDate(java.time.LocalDate.EPOCH).toInstant();
            java.time.Instant y = java.time.LocalDateTime.parse(local.substring(10, local.length() - 2))
                    .toInstant(java.time.ZoneOffset.UTC);
            return x.equals(y);
        } catch (java.time.format.DateTimeParseException e) {
            return false;
        }
    }

    /** A local time of day of Neo4j's ({@code Time('12:31:14.645876123')}, no offset) against lite's nanosecond one,
     *  which is the date-time at it on the epoch day ({@code DateTime('1970-01-01T12:31:14.645876123')},
     *  docs/SEMANTICS_REGISTER.md S70): equal when the clocks agree. Only a pair of one local time and one
     *  epoch-day date-time text compares so. */
    static boolean sameEpochLocalTime(String a, String b) {
        String time = localTimeText(a) ? a : localTimeText(b) ? b : null;
        if (time == null) {
            return false;
        }
        String dt = localTimeText(a) ? b : a;
        String prefix = "DateTime('1970-01-01T";
        if (!dt.startsWith(prefix) || !dt.endsWith("')")) {
            return false;
        }
        return dt.substring(prefix.length(), dt.length() - 2).equals(time.substring(6, time.length() - 2));
    }

    private static boolean localTimeText(String text) {
        return text.startsWith("Time('") && text.endsWith("')") && !zonedTimeText(text);
    }

    static boolean zonedTimeText(String text) {
        if (!text.startsWith("Time('") || !text.endsWith("')")) {
            return false;
        }
        String body = text.substring(6, text.length() - 2);
        return body.indexOf('+') > 0 || body.indexOf('-') > 0 || body.endsWith("Z");
    }

    static boolean zonedText(String text) {
        if (!text.startsWith("DateTime('") || !text.endsWith("')")) {
            return false;
        }
        String body = text.substring(10, text.length() - 2);
        int t = body.indexOf('T');
        return t > 0 && (body.indexOf('+', t) > 0 || body.indexOf('-', t) > 0 || body.endsWith("Z"));
    }

    /** A zoned value's text ({@code Time('HH:mm:ss.n+01:00')}, {@code DateTime('...T...+00:15')}) split for the
     *  store (the Cypher island's stored zoned values, docs/CYPHER_STUDY_2026_10_15.md §12.65): its instant's text
     *  as a seed binds it (a zoned time's on the epoch day, 1969-12-31 or 1970-01-02 when the offset crosses
     *  midnight) and its offset in seconds; null for another text. */
    static @com.legend.base.Nullable ZonedParts zonedParts(String text) {
        try {
            if (zonedTimeText(text)) {
                java.time.OffsetTime t = java.time.OffsetTime.parse(text.substring(6, text.length() - 2));
                java.time.LocalDateTime utc = java.time.LocalDateTime.ofInstant(t.atDate(java.time.LocalDate.EPOCH)
                        .toInstant(), java.time.ZoneOffset.UTC);
                return new ZonedParts(seedText(utc), t.getOffset().getTotalSeconds(), null);
            }
            if (zonedText(text)) {
                String body = text.substring(10, text.length() - 2);
                // (a region's name in brackets after the offset, as the oracle dumps a value of a named zone:
                // tools/neo4j-oracle/README.md)
                int bracket = body.indexOf('[');
                java.time.OffsetDateTime t = java.time.OffsetDateTime.parse(bracket < 0 ? body
                        : body.substring(0, bracket));
                java.time.LocalDateTime utc = java.time.LocalDateTime.ofInstant(t.toInstant(),
                        java.time.ZoneOffset.UTC);
                return new ZonedParts(seedText(utc), t.getOffset().getTotalSeconds(), bracket < 0 ? null
                        : body.substring(bracket + 1, body.length() - 1));
            }
        } catch (java.time.format.DateTimeParseException e) {
            return null;
        }
        return null;
    }

    /** A stored zoned value's parts: the instant's seed text, the offset in seconds, and the region's name where
     *  the value is of a named zone (null for an offset alone). */
    record ZonedParts(String instant, long offsetSeconds, @com.legend.base.Nullable String region) {
    }

    /** A date-time as the seed binds it: {@code yyyy-MM-dd HH:mm:ss.nnnnnnnnn}, nine digits. */
    private static String seedText(java.time.LocalDateTime dt) {
        return dt.toLocalDate() + " " + String.format(java.util.Locale.ROOT, "%02d:%02d:%02d.%09d", dt.getHour(),
                dt.getMinute(), dt.getSecond(), dt.getNano());
    }

    private static java.time.@com.legend.base.Nullable Instant instantOf(String text) {
        if (!text.startsWith("DateTime('") || !text.endsWith("')")) {
            return null;
        }
        String body = text.substring(10, text.length() - 2);
        try {
            if (zonedText(text)) {
                return java.time.OffsetDateTime.parse(body).toInstant();
            }
            return java.time.LocalDateTime.parse(body).toInstant(java.time.ZoneOffset.UTC);
        } catch (java.time.format.DateTimeParseException e) {
            return null;
        }
    }

    private static String time(int hour, int minute, int second, String fraction) {
        String nanos = (fraction + "000000000").substring(0, 9);
        return "Time('" + String.format(java.util.Locale.ROOT, "%02d:%02d:%02d.", hour, minute, second) + nanos + "')";
    }

    private static String dateTime(int year, int month, int day, int hour, int minute, int second, String fraction) {
        String nanos = (fraction + "000000000").substring(0, 9);
        return "DateTime('" + date(year, month, day) + String.format(java.util.Locale.ROOT, "T%02d:%02d:%02d.", hour, minute, second) + nanos
                + "')";
    }

    /** The oracle's spelling of a temporal value: {@code Date('...')}, {@code DateTime('...')}, ... */
    private static final java.util.regex.Pattern TEMPORAL_TEXT = java.util.regex.Pattern.compile(
            "(Date|LocalDateTime|DateTime|LocalTime|Time|Duration)\\('[^']*'\\)");

    /** A JSON value as a Cypher value: a number with no fraction an integer, else a float. */
    private static Value ofJson(com.legend.json.Json.Node node) {
        return switch (node) {
            case com.legend.json.Json.Null n -> new Null();
            case com.legend.json.Json.Bool b -> new Bool(b.value());
            // a temporal value inside a node or map, spelled as the oracle spells one (Date('2015-07-21'), ...): read
            // back as that value, as a cell of its type is (the study's §12.21)
            case com.legend.json.Json.Str s -> TEMPORAL_TEXT.matcher(s.value()).matches() ? new Opaque(s.value())
                    : new Str(s.value());
            case com.legend.json.Json.Num n -> n.isInteger() ? new Int(BigInteger.valueOf(n.longValue()))
                    : new Flt(n.doubleValue());
            case com.legend.json.Json.Arr a -> {
                List<Value> items = new ArrayList<>();
                for (com.legend.json.Json.Node item : a.items()) {
                    items.add(ofJson(item));
                }
                yield new Lst(items);
            }
            case com.legend.json.Json.Obj o -> {
                if (o.fields().get("@path") instanceof com.legend.json.Json.Arr steps) {
                    // a path as lite returns it (the study's §12.27): nodes and relationships alternating, each
                    // relationship's traversed direction beside them
                    List<Value> nodes = new ArrayList<>();
                    List<Value> rels = new ArrayList<>();
                    for (int k = 0; k < steps.items().size(); k++) {
                        (k % 2 == 0 ? nodes : rels).add(ofJson(steps.items().get(k)));
                    }
                    List<Boolean> forward = new ArrayList<>();
                    for (com.legend.json.Json.Node d : ((com.legend.json.Json.Arr) o.fields().get("@dirs")).items()) {
                        forward.add(((com.legend.json.Json.Bool) d).value());
                    }
                    yield new Path(nodes, rels, forward);
                }
                if (o.fields().get("@type") instanceof com.legend.json.Json.Str type) {
                    // a relationship as lite returns it (the study's §12.19): its type, its properties beside it
                    // (its identity, "@id", the island's own key, read past: Neo4j's values carry none, §12.71)
                    Map<String, Value> props = new TreeMap<>();
                    o.fields().forEach((k, v) -> {
                        if (!k.equals("@type") && !k.equals("@id")) {
                            props.put(k, ofJson(v));
                        }
                    });
                    yield new Rel(type.value(), props);
                }
                if (o.fields().get("@labels") instanceof com.legend.json.Json.Arr labels) {
                    // a node as lite returns it (C8, the study's §12.14): its labels, its properties beside them
                    TreeSet<String> ls = new TreeSet<>();
                    for (com.legend.json.Json.Node l : labels.items()) {
                        ls.add(((com.legend.json.Json.Str) l).value());
                    }
                    Map<String, Value> props = new TreeMap<>();
                    o.fields().forEach((k, v) -> {
                        if (!k.equals("@labels") && !k.equals("@id")) {
                            props.put(k, ofJson(v));
                        }
                    });
                    yield new Node(ls, props);
                }
                Map<String, Value> entries = new TreeMap<>();
                o.fields().forEach((k, v) -> entries.put(k, ofJson(v)));
                yield new Mp(entries);
            }
        };
    }

    private static boolean isWhole(BigDecimal d) {
        return d.signum() == 0 || d.stripTrailingZeros().scale() <= 0;
    }

    // ------------------------------------------------------------------ comparing

    /** Two doubles agree within this many units in the last place (an average summed in another order; the host
     *  judge's rule, docs/SEMANTICS_REGISTER.md S15); a signed zero is its own value (Neo4j's -0.0). */
    private static final int ULPS = 2;

    /** Whether {@code a} and {@code b} are the same value; {@code bagLists} compares lists as bags. */
    public static boolean equal(Value a, Value b, boolean bagLists) {
        if (a instanceof Int x && b instanceof Int y) {
            return x.value().equals(y.value());
        }
        if (a instanceof Opaque x && b instanceof Opaque y) {
            return x.text().equals(y.text()) || sameInstant(x.text(), y.text()) || sameEpochTime(x.text(), y.text())
                    || sameEpochLocalTime(x.text(), y.text()) || sameLocalTime(x.text(), y.text())
                    || sameLocalDateTime(x.text(), y.text());
        }
        if (a instanceof Opaque x && b instanceof Mp y || a instanceof Mp x2 && b instanceof Opaque y2) {
            Opaque o = a instanceof Opaque x3 ? x3 : (Opaque) b;
            Mp m = a instanceof Mp x4 ? x4 : (Mp) b;
            return sameDuration(o.text(), m.entries());
        }
        if (a instanceof Flt x && b instanceof Flt y) {
            return sameDouble(x.value(), y.value());
        }
        if (a instanceof Int x && b instanceof Flt y || a instanceof Flt x2 && b instanceof Int y2) {
            // an integer and a float are different values in Cypher (1 = 1.0 is true, but a result cell typed one
            // way is not the other): the grammar keeps them apart
            return false;
        }
        if (a instanceof Lst x && b instanceof Lst y) {
            if (x.items().size() != y.items().size()) {
                return false;
            }
            if (!bagLists) {
                for (int i = 0; i < x.items().size(); i++) {
                    if (!equal(x.items().get(i), y.items().get(i), false)) {
                        return false;
                    }
                }
                return true;
            }
            return sameBag(x.items(), y.items(), true);
        }
        if (a instanceof Mp x && b instanceof Mp y) {
            return sameMap(x.entries(), y.entries(), bagLists);
        }
        if (a instanceof Node x && b instanceof Node y) {
            return x.labels().equals(y.labels()) && sameMap(x.props(), y.props(), bagLists);
        }
        if (a instanceof Rel x && b instanceof Rel y) {
            return x.type().equals(y.type()) && sameMap(x.props(), y.props(), bagLists);
        }
        if (a instanceof Path x && b instanceof Path y) {
            if (x.nodes().size() != y.nodes().size() || !x.forward().equals(y.forward())) {
                return false;
            }
            for (int k = 0; k < x.nodes().size(); k++) {
                if (!equal(x.nodes().get(k), y.nodes().get(k), bagLists)) {
                    return false;
                }
            }
            for (int k = 0; k < x.rels().size(); k++) {
                if (!equal(x.rels().get(k), y.rels().get(k), bagLists)) {
                    return false;
                }
            }
            return true;
        }
        return a.equals(b);
    }

    private static boolean sameMap(Map<String, Value> x, Map<String, Value> y, boolean bagLists) {
        if (!x.keySet().equals(y.keySet())) {
            return false;
        }
        for (Map.Entry<String, Value> e : x.entrySet()) {
            if (!equal(e.getValue(), y.get(e.getKey()), bagLists)) {
                return false;
            }
        }
        return true;
    }

    private static boolean sameDouble(double x, double y) {
        if (Double.isNaN(x) || Double.isNaN(y)) {
            return Double.isNaN(x) && Double.isNaN(y);
        }
        if (x == y) {
            return Double.doubleToLongBits(x) == Double.doubleToLongBits(y);
        }
        if (x == 0 || y == 0 || Double.isInfinite(x) || Double.isInfinite(y)) {
            return false;
        }
        return Math.abs(x - y) <= ULPS * Math.ulp(Math.max(Math.abs(x), Math.abs(y)));
    }

    /** Whether two lists hold the same values, each as many times. */
    public static boolean sameBag(List<Value> x, List<Value> y, boolean bagLists) {
        if (x.size() != y.size()) {
            return false;
        }
        List<Value> left = new ArrayList<>(y);
        for (Value v : x) {
            int found = -1;
            for (int i = 0; i < left.size(); i++) {
                if (equal(v, left.get(i), bagLists)) {
                    found = i;
                    break;
                }
            }
            if (found < 0) {
                return false;
            }
            left.remove(found);
        }
        return true;
    }

    /** A row of values. */
    public static List<Value> row(List<String> cells) {
        List<Value> out = new ArrayList<>();
        for (String c : cells) {
            out.add(parse(c));
        }
        return out;
    }

    /** Whether two tables of rows agree: in order, or as bags of rows. */
    public static boolean sameRows(List<List<Value>> expected, List<List<Value>> actual, boolean ordered,
            boolean bagLists) {
        if (expected.size() != actual.size()) {
            return false;
        }
        if (ordered) {
            for (int i = 0; i < expected.size(); i++) {
                if (!sameRow(expected.get(i), actual.get(i), bagLists)) {
                    return false;
                }
            }
            return true;
        }
        List<List<Value>> left = new ArrayList<>(actual);
        for (List<Value> row : expected) {
            int found = -1;
            for (int i = 0; i < left.size(); i++) {
                if (sameRow(row, left.get(i), bagLists)) {
                    found = i;
                    break;
                }
            }
            if (found < 0) {
                return false;
            }
            left.remove(found);
        }
        return true;
    }

    private static boolean sameRow(List<Value> a, List<Value> b, boolean bagLists) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!equal(a.get(i), b.get(i), bagLists)) {
                return false;
            }
        }
        return true;
    }

    // ------------------------------------------------------------------ writing

    /** A value in the grammar, as the oracle writes it. */
    public static String text(Value v) {
        return switch (v) {
            case Null n -> "null";
            case Bool b -> String.valueOf(b.value());
            case Int i -> i.value().toString();
            case Flt f -> Double.isNaN(f.value()) ? "NaN" : Double.isInfinite(f.value()) ? (f.value() > 0 ? "Inf"
                    : "-Inf") : Double.toString(f.value());
            case Str s -> "'" + s.value().replace("\\", "\\\\").replace("'", "\\'") + "'";
            case Lst l -> "[" + String.join(", ", l.items().stream().map(CypherValues::text).toList()) + "]";
            case Mp m -> mapText(m.entries());
            case Node n -> "(" + String.join("", n.labels().stream().map(l -> ":" + l).toList())
                    + (n.props().isEmpty() ? "" : " " + mapText(n.props())) + ")";
            case Rel r -> "[:" + r.type() + (r.props().isEmpty() ? "" : " " + mapText(r.props())) + "]";
            case Path p -> {
                StringBuilder sb = new StringBuilder("<").append(text(p.nodes().get(0)));
                for (int k = 0; k < p.rels().size(); k++) {
                    sb.append(p.forward().get(k) ? "-" : "<-").append(text(p.rels().get(k)))
                            .append(p.forward().get(k) ? "->" : "-").append(text(p.nodes().get(k + 1)));
                }
                yield sb.append(">").toString();
            }
            case Opaque o -> o.text();
        };
    }

    private static String mapText(Map<String, Value> m) {
        List<String> parts = new ArrayList<>();
        new TreeMap<>(m).forEach((k, v) -> parts.add(k + ": " + text(v)));
        return "{" + String.join(", ", parts) + "}";
    }

    /** A row's cells as text, for a report. */
    public static String rowText(List<Value> row) {
        List<String> parts = new ArrayList<>();
        for (Value v : row) {
            parts.add(text(v));
        }
        return "[" + String.join(", ", parts) + "]";
    }

    /** The values of a map literal's text, in key order. */
    public static Map<String, Value> mapOf(String text) {
        Value v = parse(text);
        if (v instanceof Mp m) {
            return new LinkedHashMap<>(m.entries());
        }
        throw new IllegalArgumentException("not a map: " + text);
    }
}
