package com.legend.arrowipc;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Just enough of a FlatBuffers builder to write Arrow's message headers,
 * built back to front the way FlatBuffers' own builder is: tables with a
 * vtable each, strings, vectors of offsets, vectors of structs. Little-endian
 * throughout. Offsets are distances from the END of the buffer ({@link #offset}).
 * (From the warehouse server's {@code duck/FlatBuilder}, moved here 2026-10-29 so the warehouse and core's writers
 * share one framer: the result boundary program's track A, docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md step 2.)
 */
public final class FlatBuilder {

    private byte[] buf = new byte[256];
    /** The first used byte; data occupies {@code [head, buf.length)}. */
    private int head = buf.length;
    private int minAlign = 1;
    private int[] vtable = new int[0];
    private int objectStart;

    /** The current position, as a distance from the end. */
    public int offset() {
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

    public void addByte(int v) {
        prep(1, 0);
        putByte(v);
    }

    public void addShort(int v) {
        prep(2, 0);
        putShort(v);
    }

    public void addInt(int v) {
        prep(4, 0);
        putInt(v);
    }

    public void addLong(long v) {
        prep(8, 0);
        putLong(v);
    }

    /** A reference to something already written, as the unsigned distance forward to it. */
    public void addOffset(int target) {
        prep(4, 0);
        putInt(offset() - target + 4);
    }

    // -- tables --------------------------------------------------------------

    public void startTable(int fields) {
        vtable = new int[fields];
        objectStart = offset();
    }

    private void slot(int field) {
        vtable[field] = offset();
    }

    public void fieldByte(int field, int v) {
        addByte(v);
        slot(field);
    }

    public void fieldBool(int field, boolean v) {
        fieldByte(field, v ? 1 : 0);
    }

    public void fieldShort(int field, int v) {
        addShort(v);
        slot(field);
    }

    public void fieldInt(int field, int v) {
        addInt(v);
        slot(field);
    }

    public void fieldLong(int field, long v) {
        addLong(v);
        slot(field);
    }

    public void fieldOffset(int field, int target) {
        addOffset(target);
        slot(field);
    }

    /** Ends the table and writes its vtable just before it; the table's offset. */
    public int endTable() {
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

    public int string(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        addByte(0);
        startVector(1, b.length, 1);
        head -= b.length;
        System.arraycopy(b, 0, buf, head, b.length);
        return endVector(b.length);
    }

    /** Offsets written back to front, so the vector reads them in the given order. */
    public int offsets(int[] targets) {
        startVector(4, targets.length, 4);
        for (int i = targets.length - 1; i >= 0; i--) addOffset(targets[i]);
        return endVector(targets.length);
    }

    /** A vector of 32-bit integers (a Union's type ids), in the given order. */
    public int ints(int[] values) {
        startVector(4, values.length, 4);
        for (int i = values.length - 1; i >= 0; i--) addInt(values[i]);
        return endVector(values.length);
    }

    /** A vector of structs of two longs each (Arrow's FieldNode and Buffer), in the given order. */
    public int longPairs(long[] firsts, long[] seconds) {
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
    public byte[] finish(int root) {
        prep(minAlign, 4);
        addOffset(root);
        return Arrays.copyOfRange(buf, head, buf.length);
    }
}
