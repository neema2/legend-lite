// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.deephaven;

import com.legend.deephaven.DeephavenMessages.Card;
import com.legend.deephaven.DeephavenMessages.Field;
import com.legend.deephaven.DeephavenMessages.Message;
import com.legend.json.Json;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Protobuf's wire form of a message written in its JSON form, and back, by {@link DeephavenMessages}' table
 * (docs/DEEPHAVEN_PLAN_2026_10_22.md DH11, DH22): the plan is Deephaven's JSON form of its request, and this is the
 * one place that turns it into the bytes a server reads. Fields are written in number order and a field without
 * presence is left out at its type's default, so the bytes are protobuf's canonical form of the message. A key the
 * table does not know, or a value of the wrong kind, is refused by name: the target and the table disagree.
 */
final class ProtoWire {

    private ProtoWire() {
    }

    private static final int VARINT = 0;
    private static final int FIXED64 = 1;
    private static final int LENGTH = 2;
    private static final int FIXED32 = 5;

    /** {@code value}, a {@code type} in its JSON form, as protobuf's bytes; each ticket's bytes pass {@code ticket}. */
    static byte[] write(String type, Json.Obj value, UnaryOperator<byte[]> ticket) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        message(out, DeephavenMessages.message(type), value, ticket);
        return out.toByteArray();
    }

    /** {@code value}, a {@code type} in its JSON form, with each table reference it holds (a {@code TableReference},
     *  at any depth: an operation's source, a join's two sides, a merge's list) replaced by {@code reference}'s answer
     *  for it; the rest as it is, its fields in the table's order. The walk is the table's, so a key it does not know
     *  is refused as {@link #write} refuses it. */
    static Json.Obj references(String type, Json.Obj value, UnaryOperator<Json.Obj> reference) {
        Message m = DeephavenMessages.message(type);
        known(m, value);
        LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>();
        for (Field f : m.fields().values()) {
            Json.Node v = value.fields().get(f.json());
            if (v == null) {
                continue;
            }
            if (f.kind() != DeephavenMessages.Kind.MESSAGE) {
                out.put(f.json(), v);
            } else if (f.card() == Card.REPEATED) {
                if (!(v instanceof Json.Arr items)) {
                    throw kind(m, f, v);
                }
                List<Json.Node> each = new ArrayList<>();
                for (Json.Node item : items.items()) {
                    each.add(referencesIn(m, f, item, reference));
                }
                out.put(f.json(), new Json.Arr(each));
            } else {
                out.put(f.json(), referencesIn(m, f, v, reference));
            }
        }
        return new Json.Obj(out);
    }

    private static Json.Obj referencesIn(Message m, Field f, Json.Node v, UnaryOperator<Json.Obj> reference) {
        if (!(v instanceof Json.Obj obj)) {
            throw kind(m, f, v);
        }
        if (typeOf(f).equals("TableReference")) {
            known(DeephavenMessages.message("TableReference"), obj);
            return reference.apply(obj);
        }
        return references(typeOf(f), obj, reference);
    }

    private static void known(Message m, Json.Obj value) {
        for (String key : value.fields().keySet()) {
            if (!m.fields().containsKey(key)) {
                throw new IllegalArgumentException("Deephaven message " + m.name() + " has no field '" + key
                        + "' lite writes (it writes " + m.fields().keySet() + ")");
            }
        }
    }

    private static void message(ByteArrayOutputStream out, Message m, Json.Obj value, UnaryOperator<byte[]> ticket) {
        known(m, value);
        for (Field f : m.fields().values()) {
            Json.Node v = value.fields().get(f.json());
            if (v == null) {
                continue;
            }
            if (f.card() == Card.REPEATED) {
                if (!(v instanceof Json.Arr items)) {
                    throw kind(m, f, v);
                }
                for (Json.Node item : items.items()) {
                    one(out, m, f, item, true, ticket);
                }
            } else {
                one(out, m, f, v, f.card() == Card.PRESENT, ticket);
            }
        }
    }

    /** One value of {@code f}; {@code always}: written even at its type's default (a oneof's member, an element). */
    private static void one(ByteArrayOutputStream out, Message m, Field f, Json.Node v, boolean always,
            UnaryOperator<byte[]> ticket) {
        if (f.card() == Card.REPEATED && f.kind() != DeephavenMessages.Kind.MESSAGE
                && f.kind() != DeephavenMessages.Kind.STRING && f.kind() != DeephavenMessages.Kind.BYTES) {
            // proto3 packs a repeated number into one length-delimited run; no such field is in the table
            throw new IllegalStateException("Deephaven field " + m.name() + "." + f.json()
                    + " is a repeated number, which lite does not write");
        }
        switch (f.kind()) {
            case MESSAGE -> {
                if (!(v instanceof Json.Obj obj)) {
                    throw kind(m, f, v);
                }
                ByteArrayOutputStream inner = new ByteArrayOutputStream();
                message(inner, DeephavenMessages.message(typeOf(f)), obj, ticket);
                delimited(out, f.number(), inner.toByteArray());
            }
            case STRING -> {
                String s = string(m, f, v);
                if (always || !s.isEmpty()) {
                    delimited(out, f.number(), s.getBytes(StandardCharsets.UTF_8));
                }
            }
            case BYTES -> {
                byte[] b = Base64.getDecoder().decode(string(m, f, v));
                if (m.name().equals("Ticket")) {
                    b = ticket.apply(b);
                }
                if (always || b.length > 0) {
                    delimited(out, f.number(), b);
                }
            }
            case BOOL -> {
                if (!(v instanceof Json.Bool bool)) {
                    throw kind(m, f, v);
                }
                if (always || bool.value()) {
                    tag(out, f.number(), VARINT);
                    varint(out, bool.value() ? 1 : 0);
                }
            }
            case INT32, INT64, UINT32, UINT64, SINT32, SINT64 -> {
                long n = integer(m, f, v);
                if (always || n != 0) {
                    tag(out, f.number(), VARINT);
                    varint(out, switch (f.kind()) {
                        case SINT32 -> (n << 1) ^ (n >> 31);
                        case SINT64 -> (n << 1) ^ (n >> 63);
                        case INT32, INT64, UINT32, UINT64 -> n;
                        case MESSAGE, STRING, BYTES, BOOL, DOUBLE, ENUM -> throw kind(m, f, v);
                    });
                }
            }
            case DOUBLE -> {
                if (!(v instanceof Json.Num num)) {
                    throw kind(m, f, v);
                }
                if (always || num.doubleValue() != 0.0) {
                    tag(out, f.number(), FIXED64);
                    long bits = Double.doubleToLongBits(num.doubleValue());
                    for (int i = 0; i < 8; i++) {
                        out.write((int) (bits >>> (8 * i)) & 0xFF);
                    }
                }
            }
            case ENUM -> {
                int n = DeephavenMessages.enumNumber(typeOf(f), string(m, f, v));
                if (always || n != 0) {
                    tag(out, f.number(), VARINT);
                    varint(out, n);
                }
            }
        }
    }

    /** An integer field's value: a JSON number, or a string of one (the JSON form's 64-bit integers), in range. */
    private static long integer(Message m, Field f, Json.Node v) {
        long n;
        if (v instanceof Json.Num num && num.isInteger()) {
            n = num.longValue();
        } else if (v instanceof Json.Str s && s.value().matches("-?[0-9]{1,19}")) {
            n = Long.parseLong(s.value());
        } else {
            throw kind(m, f, v);
        }
        boolean in = switch (f.kind()) {
            case INT32, SINT32 -> n >= Integer.MIN_VALUE && n <= Integer.MAX_VALUE;
            case UINT32 -> n >= 0 && n <= 0xFFFFFFFFL;
            case UINT64 -> n >= 0;
            case INT64, SINT64 -> true;
            case MESSAGE, STRING, BYTES, BOOL, DOUBLE, ENUM -> throw kind(m, f, v);
        };
        if (!in) {
            throw new IllegalArgumentException("Deephaven field " + m.name() + "." + f.json() + " cannot hold " + n);
        }
        return n;
    }

    private static String string(Message m, Field f, Json.Node v) {
        if (!(v instanceof Json.Str s)) {
            throw kind(m, f, v);
        }
        return s.value();
    }

    private static String typeOf(Field f) {
        String type = f.type();
        if (type == null) {
            throw new IllegalStateException("Deephaven field " + f.json() + " names no type");
        }
        return type;
    }

    private static IllegalArgumentException kind(Message m, Field f, Json.Node v) {
        return new IllegalArgumentException("Deephaven field " + m.name() + "." + f.json() + " is a " + f.kind()
                + (f.card() == Card.REPEATED ? " list" : "") + ", not " + v);
    }

    private static void delimited(ByteArrayOutputStream out, int number, byte[] b) {
        tag(out, number, LENGTH);
        varint(out, b.length);
        out.write(b, 0, b.length);
    }

    private static void tag(ByteArrayOutputStream out, int number, int wire) {
        varint(out, ((long) number << 3) | wire);
    }

    /** A base-128 varint; a negative int32 or enum is written as its 64-bit two's complement, as protobuf does. */
    private static void varint(ByteArrayOutputStream out, long v) {
        while ((v & ~0x7FL) != 0) {
            out.write((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        out.write((int) v);
    }

    /** {@code bytes}, a {@code type} the server sent, in its JSON form. A field the table does not list is skipped by
     *  its wire type, as every protobuf reader skips a field it does not know (the server sends some lite does not
     *  read: a created table's schema). */
    static Json.Obj read(String type, byte[] bytes) {
        Message m = DeephavenMessages.message(type);
        Map<Integer, Field> byNumber = new LinkedHashMap<>();
        for (Field f : m.fields().values()) {
            byNumber.put(f.number(), f);
        }
        Map<String, Json.Node> out = new LinkedHashMap<>();
        Map<String, List<Json.Node>> lists = new LinkedHashMap<>();
        int[] at = {0};
        while (at[0] < bytes.length) {
            long key = readVarint(bytes, at);
            int number = (int) (key >>> 3);
            int wire = (int) (key & 7);
            Field f = byNumber.get(number);
            if (f == null) {
                skip(bytes, at, wire, m);
                continue;
            }
            Json.Node v = value(m, f, wire, bytes, at);
            if (f.card() == Card.REPEATED) {
                lists.computeIfAbsent(f.json(), k -> new ArrayList<>()).add(v);
            } else {
                out.put(f.json(), v);
            }
        }
        lists.forEach((k, items) -> out.put(k, new Json.Arr(items)));
        return new Json.Obj(new LinkedHashMap<>(out));
    }

    private static Json.Node value(Message m, Field f, int wire, byte[] bytes, int[] at) {
        int expected = switch (f.kind()) {
            case MESSAGE, STRING, BYTES -> LENGTH;
            case DOUBLE -> FIXED64;
            case BOOL, INT32, INT64, UINT32, UINT64, SINT32, SINT64, ENUM -> VARINT;
        };
        if (wire != expected) {
            throw new IllegalArgumentException("Deephaven field " + m.name() + "." + f.json() + " came as wire type "
                    + wire + ", not " + expected);
        }
        return switch (f.kind()) {
            case MESSAGE -> read(typeOf(f), delimitedBytes(bytes, at));
            case STRING -> new Json.Str(new String(delimitedBytes(bytes, at), StandardCharsets.UTF_8));
            case BYTES -> new Json.Str(Base64.getEncoder().encodeToString(delimitedBytes(bytes, at)));
            case BOOL -> new Json.Bool(readVarint(bytes, at) != 0);
            case INT32 -> Json.Num.ofLong((int) readVarint(bytes, at));
            case ENUM -> new Json.Str(DeephavenMessages.enumName(typeOf(f), (int) readVarint(bytes, at)));
            case INT64, UINT32, UINT64 -> Json.Num.ofLong(readVarint(bytes, at));
            case SINT32, SINT64 -> {
                long z = readVarint(bytes, at);
                yield Json.Num.ofLong((z >>> 1) ^ -(z & 1));
            }
            case DOUBLE -> {
                long bits = 0;
                for (int i = 0; i < 8; i++) {
                    bits |= (long) (bytes[at[0] + i] & 0xFF) << (8 * i);
                }
                at[0] += 8;
                yield Json.Num.ofDouble(Double.longBitsToDouble(bits));
            }
        };
    }

    private static void skip(byte[] bytes, int[] at, int wire, Message m) {
        switch (wire) {
            case VARINT -> readVarint(bytes, at);
            case FIXED64 -> at[0] += 8;
            case LENGTH -> delimitedBytes(bytes, at);
            case FIXED32 -> at[0] += 4;
            default -> throw new IllegalArgumentException("Deephaven message " + m.name() + " came with wire type "
                    + wire + ", which no message lite reads uses");
        }
    }

    private static byte[] delimitedBytes(byte[] bytes, int[] at) {
        int n = (int) readVarint(bytes, at);
        if (n < 0 || at[0] + n > bytes.length) {
            throw new IllegalArgumentException("a Deephaven message ended inside a field");
        }
        byte[] b = java.util.Arrays.copyOfRange(bytes, at[0], at[0] + n);
        at[0] += n;
        return b;
    }

    private static long readVarint(byte[] bytes, int[] at) {
        long v = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            if (at[0] >= bytes.length) {
                throw new IllegalArgumentException("a Deephaven message ended inside a number");
            }
            int b = bytes[at[0]++] & 0xFF;
            v |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return v;
            }
        }
        throw new IllegalArgumentException("a Deephaven message holds a number longer than 64 bits");
    }
}
