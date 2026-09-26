package com.legend.warehouse.server.duck;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Just enough of a FlatBuffers builder to write Arrow's message headers,
 * built back to front the way FlatBuffers' own builder is: tables with a
 * vtable each, strings, vectors of offsets, vectors of structs. Little-endian
 * throughout. Offsets are distances from the END of the buffer ({@link #offset}).
 */
final class FlatBuilder {

    private byte[] buf = new byte[256];
    /** The first used byte; data occupies {@code [head, buf.length)}. */
    private int head = buf.length;
    private int minAlign = 1;
    private int[] vtable = new int[0];
    private int objectStart;

    /** The current position, as a distance from the end. */
    int offset() {
        return buf.length - head;
    }

    private void ensure(int bytes) {
        while (head < bytes) {
            int used = buf.length - head;
            byte[] bigger = new byte[buf.length * 2];
            System.arraycopy(buf, head, bigger, bigger.length - used, used);
            head += bigger.length - buf.length;
            buf = bigger;
        }
    }

    /** Pads so that after {@code additional} more bytes, a {@code size}-byte value lands aligned. */
    private void prep(int size, int additional) {
        if (size > minAlign) minAlign = size;
        int align = (-(offset() + additional)) & (size - 1);
        ensure(align + size + additional);
        head -= align;   // the padding is already zero
    }

    private void putByte(int v) {
        ensure(1);
        buf[--head] = (byte) v;
    }

    private void putShort(int v) {
        ensure(2);
        head -= 2;
        buf[head] = (byte) v;
        buf[head + 1] = (byte) (v >>> 8);
    }

    private void putInt(int v) {
        ensure(4);
        head -= 4;
        writeInt(head, v);
    }

    private void writeInt(int at, int v) {
        buf[at] = (byte) v;
        buf[at + 1] = (byte) (v >>> 8);
        buf[at + 2] = (byte) (v >>> 16);
        buf[at + 3] = (byte) (v >>> 24);
    }

    private void putLong(long v) {
        ensure(8);
        head -= 8;
        for (int i = 0; i < 8; i++) buf[head + i] = (byte) (v >>> (8 * i));
    }

    void addByte(int v) {
        prep(1, 0);
        putByte(v);
    }

    void addShort(int v) {
        prep(2, 0);
        putShort(v);
    }

    void addInt(int v) {
        prep(4, 0);
        putInt(v);
    }

    void addLong(long v) {
        prep(8, 0);
        putLong(v);
    }

    /** A reference to something already written, as the unsigned distance forward to it. */
    void addOffset(int target) {
        prep(4, 0);
        putInt(offset() - target + 4);
    }

    // -- tables --------------------------------------------------------------

    void startTable(int fields) {
        vtable = new int[fields];
        objectStart = offset();
    }

    private void slot(int field) {
        vtable[field] = offset();
    }

    void fieldByte(int field, int v) {
        addByte(v);
        slot(field);
    }

    void fieldBool(int field, boolean v) {
        fieldByte(field, v ? 1 : 0);
    }

    void fieldShort(int field, int v) {
        addShort(v);
        slot(field);
    }

    void fieldInt(int field, int v) {
        addInt(v);
        slot(field);
    }

    void fieldLong(int field, long v) {
        addLong(v);
        slot(field);
    }

    void fieldOffset(int field, int target) {
        addOffset(target);
        slot(field);
    }

    /** Ends the table and writes its vtable just before it; the table's offset. */
    int endTable() {
        addInt(0);   // where the table points at its vtable
        int table = offset();
        int n = vtable.length;
        while (n > 0 && vtable[n - 1] == 0) n--;
        for (int i = n - 1; i >= 0; i--) addShort(vtable[i] != 0 ? table - vtable[i] : 0);
        addShort(table - objectStart);
        addShort((n + 2) * 2);
        int vt = offset();
        writeInt(buf.length - table, vt - table);
        return table;
    }

    // -- strings and vectors -------------------------------------------------

    int string(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        addByte(0);
        startVector(1, b.length, 1);
        head -= b.length;
        System.arraycopy(b, 0, buf, head, b.length);
        return endVector(b.length);
    }

    /** Offsets written back to front, so the vector reads them in the given order. */
    int offsets(int[] targets) {
        startVector(4, targets.length, 4);
        for (int i = targets.length - 1; i >= 0; i--) addOffset(targets[i]);
        return endVector(targets.length);
    }

    /** A vector of structs of two longs each (Arrow's FieldNode and Buffer), in the given order. */
    int longPairs(long[] firsts, long[] seconds) {
        int n = firsts.length;
        startVector(16, n, 8);
        for (int i = n - 1; i >= 0; i--) {
            prep(8, 16);
            putLong(seconds[i]);
            putLong(firsts[i]);
        }
        return endVector(n);
    }

    private void startVector(int elementSize, int count, int alignment) {
        prep(4, elementSize * count);
        prep(alignment, elementSize * count);
    }

    private int endVector(int count) {
        putInt(count);
        return offset();
    }

    /** The finished buffer, rooted at {@code root}. */
    byte[] finish(int root) {
        prep(minAlign, 4);
        addOffset(root);
        return Arrays.copyOfRange(buf, head, buf.length);
    }
}
