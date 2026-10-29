// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.deephaven;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The columns of the table a Deephaven server made, as its answer's schema names them (docs/DEEPHAVEN_PLAN_2026_10_22.md
 * DH16): each column's name and its {@code deephaven:type}, the Java type the server holds it as -- {@code int},
 * {@code long}, {@code java.lang.String} (measured on Deephaven 42.7, 2026-10-29: {@code byte}, {@code short},
 * {@code int}, {@code long}, {@code float}, {@code double}, {@code java.lang.Boolean}, {@code java.lang.String},
 * {@code java.time.Instant}, {@code java.time.LocalDate}, {@code java.time.LocalTime}, {@code java.math.BigDecimal}).
 *
 * <p>The schema is Arrow's {@code Message} with a {@code Schema} header, a flatbuffer, framed as an Arrow IPC message
 * (the continuation marker and the length before it, as 42.7 writes it). Only what the check reads is read: the
 * schema's fields, each one's name and custom metadata.
 */
public final class ArrowSchema {

    private ArrowSchema() {
    }

    /** One column of a made table: its name and its Deephaven type, null when the server wrote none. */
    public record Column(String name, @com.legend.base.Nullable String type) {
    }

    /** Arrow's {@code MessageHeader} union: the member that is a schema. */
    private static final int SCHEMA_HEADER = 1;

    /** {@code bytes}, a schema message, as its columns in order; bytes that are not one refused by name. */
    static List<Column> columns(byte[] bytes) {
        byte[] b = bytes;
        int start = bytes.length >= 8 && i32(b, 0) == -1 ? 8 : 0;
        try {
            int message = start + i32(b, start);
            if (u8(b, message, 1) != SCHEMA_HEADER) {
                throw new IllegalArgumentException("a Deephaven answer's schema is a message of kind "
                        + u8(b, message, 1) + ", not a schema");
            }
            int schema = table(b, message, 2);
            List<Column> columns = new ArrayList<>();
            int fields = vector(b, schema, 1);
            for (int i = 0, n = fields < 0 ? 0 : i32(b, fields); i < n; i++) {
                int field = element(b, fields, i);
                String name = string(b, field, 0);
                if (name == null) {
                    throw new IllegalArgumentException("a Deephaven answer's schema names no column " + i);
                }
                String type = null;
                int metadata = vector(b, field, 6);
                for (int j = 0, m = metadata < 0 ? 0 : i32(b, metadata); j < m; j++) {
                    int kv = element(b, metadata, j);
                    if ("deephaven:type".equals(string(b, kv, 0))) {
                        type = string(b, kv, 1);
                    }
                }
                columns.add(new Column(name, type));
            }
            return List.copyOf(columns);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("a Deephaven answer's schema is cut short or malformed: " + e.getMessage(),
                    e);
        }
    }

    /** Where field {@code slot} of the table at {@code table} is, or -1 when the table leaves it out. */
    private static int field(byte[] b, int table, int slot) {
        int vtable = table - i32(b, table);
        int size = u16(b, vtable);
        int at = 4 + 2 * slot;
        if (at + 2 > size) {
            return -1;
        }
        int offset = u16(b, vtable + at);
        return offset == 0 ? -1 : table + offset;
    }

    private static int u8(byte[] b, int table, int slot) {
        int at = field(b, table, slot);
        return at < 0 ? 0 : b[at] & 0xff;
    }

    /** The position an offset field points to, or -1 when it is left out. */
    private static int target(byte[] b, int table, int slot) {
        int at = field(b, table, slot);
        return at < 0 ? -1 : at + i32(b, at);
    }

    private static int table(byte[] b, int table, int slot) {
        int t = target(b, table, slot);
        if (t < 0) {
            throw new IllegalArgumentException("a Deephaven answer's schema message has no schema");
        }
        return t;
    }

    private static int vector(byte[] b, int table, int slot) {
        return target(b, table, slot);
    }

    /** The table that is element {@code i} of the vector of tables at {@code vector}. */
    private static int element(byte[] b, int vector, int i) {
        int at = vector + 4 + 4 * i;
        return at + i32(b, at);
    }

    private static @com.legend.base.Nullable String string(byte[] b, int table, int slot) {
        int s = target(b, table, slot);
        if (s < 0) {
            return null;
        }
        int length = i32(b, s);
        if (length < 0 || s + 4 + length > b.length) {
            throw new IndexOutOfBoundsException("a string of " + length + " bytes at " + s);
        }
        return new String(b, s + 4, length, StandardCharsets.UTF_8);
    }

    /** The little-endian 32-bit number at {@code at}. */
    private static int i32(byte[] b, int at) {
        return (b[at] & 0xff) | (b[at + 1] & 0xff) << 8 | (b[at + 2] & 0xff) << 16 | (b[at + 3] & 0xff) << 24;
    }

    private static int u16(byte[] b, int at) {
        return (b[at] & 0xff) | (b[at + 1] & 0xff) << 8;
    }
}
