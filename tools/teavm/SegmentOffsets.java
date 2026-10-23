package com.legend.tools.teavm;

import java.io.ByteArrayOutputStream;

/**
 * TeaVM 0.15.0 writes each data segment's start address wrongly, and this writes it again (2026-10-23, found when the
 * planner's module grew past 1 MiB of static data with the Deephaven wire, docs/DEEPHAVEN_PLAN_2026_10_22.md L3b).
 *
 * <p>An active segment's address is an {@code i32.const} expression, whose number WebAssembly reads as a SIGNED
 * LEB128. TeaVM's {@code WasmBinaryRenderer.renderInitializer} writes it with {@code WasmBinaryWriter.writeLEB}, the
 * UNSIGNED form, so an address whose last seven-bit group has its top bit set (64 to 127, 8 to 16 KiB, 1 to 2 MiB,
 * ...) reads as negative, and the engine refuses the module when it instantiates it ("data segment 1 is out of bounds
 * (offset 4293918936)"). Elsewhere the two forms are the same bytes, which is why every module built before was right:
 * its one other segment began at 0, and the planner's second began below 1 MiB until 2026-10-23. This reads each
 * active segment's address as the unsigned number TeaVM meant and writes it as the signed LEB128 WebAssembly reads,
 * and changes nothing else; a module whose segments are not the {@code i32.const} form TeaVM writes is refused, not
 * guessed at. It goes when a TeaVM release writes the address signed.
 */
final class SegmentOffsets {

    private SegmentOffsets() {
    }

    private static final int DATA_SECTION = 11;

    /** {@code module} with every active data segment's address written as a signed LEB128. */
    static byte[] fix(byte[] module) {
        if (module.length < 8 || module[0] != 0 || module[1] != 'a' || module[2] != 's' || module[3] != 'm') {
            throw new IllegalArgumentException("not a WebAssembly module");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(module.length + 16);
        out.write(module, 0, 8);
        int[] at = {8};
        while (at[0] < module.length) {
            int id = module[at[0]++] & 0xFF;
            int size = (int) uleb(module, at);
            int start = at[0];
            byte[] body = java.util.Arrays.copyOfRange(module, start, start + size);
            at[0] = start + size;
            if (id == DATA_SECTION) {
                body = data(body);
            }
            out.write(id);
            writeUleb(out, body.length);
            out.write(body, 0, body.length);
        }
        return out.toByteArray();
    }

    private static byte[] data(byte[] body) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(body.length + 8);
        int[] at = {0};
        long count = uleb(body, at);
        writeUleb(out, count);
        for (long i = 0; i < count; i++) {
            long kind = uleb(body, at);
            writeUleb(out, kind);
            if (kind == 0) {
                // active, memory 0: i32.const <address> end, then the bytes
                if ((body[at[0]++] & 0xFF) != 0x41) {
                    throw new IllegalArgumentException("data segment " + i + "'s address is not an i32.const");
                }
                long address = uleb(body, at);
                if ((body[at[0]++] & 0xFF) != 0x0B) {
                    throw new IllegalArgumentException("data segment " + i + "'s address is not one i32.const");
                }
                if (address > 0xFFFFFFFFL) {
                    throw new IllegalArgumentException("data segment " + i + "'s address " + address + " is not 32-bit");
                }
                out.write(0x41);
                writeSleb(out, (int) address);
                out.write(0x0B);
            } else if (kind != 1) {
                // passive (1) has no address; the forms naming a memory TeaVM does not write
                throw new IllegalArgumentException("data segment " + i + " is of kind " + kind
                        + ", which TeaVM 0.15.0 does not write");
            }
            long length = uleb(body, at);
            writeUleb(out, length);
            out.write(body, at[0], (int) length);
            at[0] += (int) length;
        }
        if (at[0] != body.length) {
            throw new IllegalArgumentException("the data section holds " + (body.length - at[0]) + " bytes past its segments");
        }
        return out.toByteArray();
    }

    private static long uleb(byte[] b, int[] at) {
        long v = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            int x = b[at[0]++] & 0xFF;
            v |= (long) (x & 0x7F) << shift;
            if ((x & 0x80) == 0) {
                return v;
            }
        }
        throw new IllegalArgumentException("a LEB128 longer than 64 bits");
    }

    private static void writeUleb(ByteArrayOutputStream out, long v) {
        do {
            int x = (int) (v & 0x7F);
            v >>>= 7;
            out.write(v != 0 ? x | 0x80 : x);
        } while (v != 0);
    }

    /** {@code v}'s signed LEB128: an address of 2 GiB and up is the negative i32 of the same bits, as WebAssembly
     *  reads it back. */
    static void writeSleb(ByteArrayOutputStream out, int v) {
        long n = v;
        while (true) {
            int x = (int) (n & 0x7F);
            n >>= 7;
            boolean done = (n == 0 && (x & 0x40) == 0) || (n == -1 && (x & 0x40) != 0);
            out.write(done ? x : x | 0x80);
            if (done) {
                return;
            }
        }
    }
}
