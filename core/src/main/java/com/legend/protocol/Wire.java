// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * One protocol JSON object being READ (the model reader's cursor, {@link ModelReader}): every field
 * the reader takes is marked, and {@link #done} refuses the object when a field was left untaken --
 * naming it. A field the reader has no rule for is never dropped, and a value it cannot map is never
 * guessed: both are refused by name ({@link #refuse}). A field the emitter writes as a CONSTANT is
 * taken through {@link #constant}, which refuses any other value, because the record could not carry
 * it back to the wire.
 *
 * <p>Absent optional fields read as {@code null}; the caller maps them to the record's own absent
 * value. The reader is pure: no I/O, no regex.
 */
final class Wire {

    private final Json.Obj obj;
    private final String where;
    private final Set<String> taken = new HashSet<>();

    private Wire(Json.Obj obj, String where) {
        this.obj = obj;
        this.where = where;
    }

    /** {@code node} as an object, refused when it is anything else. */
    static Wire of(Json.Node node, String where) {
        if (node instanceof Json.Obj o) {
            return new Wire(o, where);
        }
        throw refuse(where + " is not a JSON object: " + abbreviate(node));
    }

    /** The refusal every reader raises: a named reason, never a guess. */
    static IllegalArgumentException refuse(String why) {
        return new IllegalArgumentException("protocol JSON: " + why);
    }

    Json.Obj json() {
        return obj;
    }

    String where() {
        return where;
    }

    boolean has(String key) {
        return obj.fields().containsKey(key);
    }

    /**
     * The fields not yet taken, as an object, every one of them taken now: for a rule that reads the rest of
     * this object as another shape (an older spelling read through today's rule, which then refuses what it
     * does not take).
     */
    Json.Obj rest() {
        java.util.LinkedHashMap<String, Json.Node> out = new java.util.LinkedHashMap<>();
        obj.fields().forEach((k, v) -> {
            if (taken.add(k)) {
                out.put(k, v);
            }
        });
        return new Json.Obj(out);
    }

    /** The object's {@code _type}, taken; {@code null} when it has none. */
    @com.legend.base.Nullable String type() {
        return optStr("_type");
    }

    /** A required field, taken. */
    Json.Node take(String key) {
        Json.Node n = obj.fields().get(key);
        if (n == null) {
            throw refuse(where + " has no '" + key + "'");
        }
        taken.add(key);
        return n;
    }

    /** An optional field, taken; {@code null} when absent (a JSON {@code null} is returned as itself). */
    @com.legend.base.Nullable Json.Node opt(String key) {
        Json.Node n = obj.fields().get(key);
        if (n != null) {
            taken.add(key);
        }
        return n;
    }

    String str(String key) {
        return asStr(take(key), key);
    }

    @com.legend.base.Nullable String optStr(String key) {
        Json.Node n = opt(key);
        return n == null ? null : asStr(n, key);
    }

    boolean bool(String key) {
        return asBool(take(key), key);
    }

    @com.legend.base.Nullable Boolean optBool(String key) {
        Json.Node n = opt(key);
        return n == null ? null : asBool(n, key);
    }

    long lng(String key) {
        return asLong(take(key), key);
    }

    @com.legend.base.Nullable Long optLong(String key) {
        Json.Node n = opt(key);
        return n == null ? null : asLong(n, key);
    }

    int integer(String key) {
        return Math.toIntExact(lng(key));
    }

    @com.legend.base.Nullable Integer optInt(String key) {
        Long v = optLong(key);
        return v == null ? null : Integer.valueOf(Math.toIntExact(v));
    }

    /** A number's exact value, as written (a decimal keeps its digits). */
    BigDecimal decimal(String key) {
        return exact(take(key), where + "." + key);
    }

    /** A number a record holds as a {@code double} (its sign kept, {@code -0.0} included). */
    double dbl(String key) {
        return asDouble(take(key), where + "." + key);
    }

    static double asDouble(Json.Node v, String what) {
        if (v instanceof Json.Num n) {
            // the token, not the BigDecimal: a decimal has no negative zero
            return n.token() != null ? com.legend.json.PortableText.doubleOf(n.token()) : n.doubleValue();
        }
        throw refuse(what + " is not a number: " + abbreviate(v));
    }

    List<Json.Node> arr(String key) {
        return asArr(take(key), key);
    }

    @com.legend.base.Nullable List<Json.Node> optArr(String key) {
        Json.Node n = opt(key);
        return n == null ? null : asArr(n, key);
    }

    /** An optional array, read as empty when absent. */
    List<Json.Node> arrOrEmpty(String key) {
        List<Json.Node> a = optArr(key);
        return a == null ? List.of() : a;
    }

    List<String> strings(String key) {
        return strings(arr(key), key);
    }

    @com.legend.base.Nullable List<String> optStrings(String key) {
        List<Json.Node> a = optArr(key);
        return a == null ? null : strings(a, key);
    }

    private List<String> strings(List<Json.Node> a, String key) {
        List<String> out = new ArrayList<>(a.size());
        for (Json.Node n : a) {
            out.add(asStr(n, key + "[]"));
        }
        return out;
    }

    /** Each item of a required array, read by {@code read}. */
    <T> List<T> list(String key, Function<Json.Node, T> read) {
        List<Json.Node> a = arr(key);
        List<T> out = new ArrayList<>(a.size());
        for (Json.Node n : a) {
            out.add(read.apply(n));
        }
        return out;
    }

    /** Each item of an optional array; {@code null} when absent. */
    <T> @com.legend.base.Nullable List<T> optList(String key, Function<Json.Node, T> read) {
        List<Json.Node> a = optArr(key);
        if (a == null) {
            return null;
        }
        List<T> out = new ArrayList<>(a.size());
        for (Json.Node n : a) {
            out.add(read.apply(n));
        }
        return out;
    }

    /** Each item of an optional array, empty when absent. */
    <T> List<T> listOrEmpty(String key, Function<Json.Node, T> read) {
        List<T> l = optList(key, read);
        return l == null ? List.of() : l;
    }

    /** A nested object (the caller reads it and calls {@link #done}). */
    Wire obj(String key) {
        return of(take(key), where + "." + key);
    }

    @com.legend.base.Nullable Wire optObj(String key) {
        Json.Node n = opt(key);
        return n == null ? null : of(n, where + "." + key);
    }

    /** {@code sourceInformation}, or {@code null} when the JSON carries none. */
    @com.legend.base.Nullable SourceInfo span() {
        return span("sourceInformation");
    }

    /** A span-valued field, or {@code null} when absent -- or written as a JSON {@code null}, which the engine reads
     *  as none (older JSON writes it so). */
    @com.legend.base.Nullable SourceInfo span(String key) {
        Json.Node n = opt(key);
        return n == null || n instanceof Json.Null ? null : sourceInfo(n, where + "." + key);
    }

    /** A field the emitter writes as a constant: taken, and refused when it holds anything else. */
    void constant(String key, String expected) {
        String v = str(key);
        if (!v.equals(expected)) {
            throw refuse(where + "." + key + " is '" + v + "', the only readable value is '" + expected + "'");
        }
    }

    /** A boolean field the emitter writes as a constant. */
    void constant(String key, boolean expected) {
        if (bool(key) != expected) {
            throw refuse(where + "." + key + " is " + !expected + ", the only readable value is " + expected);
        }
    }

    /** An array field the emitter always writes empty. */
    void emptyArray(String key) {
        if (!arr(key).isEmpty()) {
            throw refuse(where + "." + key + " is not empty: no record carries it");
        }
    }

    /** An array field the emitter always writes empty, which older JSON may leave out (where the engine's class
     *  starts it empty). */
    void emptyOrAbsent(String key) {
        if (!arrOrEmpty(key).isEmpty()) {
            throw refuse(where + "." + key + " is not empty: no record carries it");
        }
    }

    /** Every field taken, or the read is refused naming the first one left. */
    <T extends @com.legend.base.Nullable Object> T done(T result) {
        if (taken.size() != obj.fields().size()) {
            for (String k : obj.fields().keySet()) {
                if (!taken.contains(k)) {
                    throw refuse(where + " has a field no reader rule takes: '" + k + "'");
                }
            }
        }
        return result;
    }

    // ---------------------------------------------------------------------
    // Scalars
    // ---------------------------------------------------------------------

    String asStr(Json.Node n, String key) {
        if (n instanceof Json.Str s) {
            return s.value();
        }
        throw refuse(where + "." + key + " is not a string: " + abbreviate(n));
    }

    private boolean asBool(Json.Node n, String key) {
        if (n instanceof Json.Bool b) {
            return b.value();
        }
        throw refuse(where + "." + key + " is not a boolean: " + abbreviate(n));
    }

    private long asLong(Json.Node n, String key) {
        if (n instanceof Json.Num num && num.isInteger()) {
            return num.longValue();
        }
        throw refuse(where + "." + key + " is not an integer: " + abbreviate(n));
    }

    private List<Json.Node> asArr(Json.Node n, String key) {
        if (n instanceof Json.Arr a) {
            return a.items();
        }
        throw refuse(where + "." + key + " is not an array: " + abbreviate(n));
    }

    /** A number's exact value: the decimal token when the JSON parser kept one. */
    static BigDecimal exact(Json.Node v, String what) {
        if (!(v instanceof Json.Num n)) {
            throw refuse(what + " is not a number: " + abbreviate(v));
        }
        if (n.decimalValue() != null) {
            return n.decimalValue();
        }
        if (n.token() != null) {
            return new BigDecimal(n.token());
        }
        return n.isInteger() ? BigDecimal.valueOf(n.longValue())
                : new BigDecimal(com.legend.json.PortableText.doubleText(n.doubleValue()));
    }

    /** {@code {endColumn, endLine, sourceId, startColumn, startLine}}, every field required. */
    static SourceInfo sourceInfo(Json.Node node, String where) {
        Wire s = of(node, where);
        SourceInfo out = new SourceInfo(s.str("sourceId"), s.integer("startLine"), s.integer("startColumn"),
                s.integer("endLine"), s.integer("endColumn"));
        return s.done(out);
    }

    static String abbreviate(@com.legend.base.Nullable Object n) {
        String s = n instanceof Json.Node node ? Json.toCompact(node) : String.valueOf(n);
        return s.length() > 160 ? s.substring(0, 160) + "..." : s;
    }

    /** The entries of a {@code _type}-dispatch table, looked up or refused by name. */
    static <V> V rule(Map<String, V> table, @com.legend.base.Nullable String type, String family) {
        V r = type == null ? null : table.get(type);
        if (r == null) {
            throw refuse("no reader rule for " + family + " _type '" + type + "' -- add the rule, do not drop it");
        }
        return r;
    }
}
