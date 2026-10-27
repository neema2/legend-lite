// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.teavm;

import java.io.ByteArrayOutputStream;

/**
 * The data segment offsets of a module TeaVM 0.15.0 wrote, re-encoded as the specification reads them (PARK-40,
 * docs/PARKED_WORK_LEDGER.md).
 *
 * <p>An active data segment's offset is an {@code i32.const} whose operand the specification reads as a SIGNED LEB128
 * integer. TeaVM 0.15.0's binary renderer writes it UNSIGNED ({@code WasmBinaryRenderer.renderInitializer} calls
 * {@code writeLEB}; 0.16.0 calls {@code writeSignedLEB}). The two encodings agree while the last byte's bit 6 is clear
 * and differ when it is set: an offset in [2^20, 2^21) is three bytes ending in 0x40..0x7F, which a reader takes as
 * negative. The planner's string pool crossed 1 MiB on 2026-10-26 (1,056,011 bytes: {@code 8b ba 40}, read as
 * -1,041,141), and every host refused the module ("data segment 1 is out of bounds").
 *
 * <p>The fix reads each offset the way TeaVM wrote it (unsigned) and writes it the way the specification reads it
 * (signed), recomputing the section's size. It is idempotent: a non-negative value's signed encoding decodes to the
 * same value when read unsigned (its trailing 0x00, when it has one, continues a zero), so a module whose offsets are
 * already the specification's comes back byte for byte. A data offset is a byte address, never negative, so no module
 * is read wrongly. Everything but the data section (id 11) is copied unchanged; an offset expression that is not one
 * {@code i32.const} is refused by its opcode, never guessed at.
 */
final class WasmDataOffsets {

    private static final byte[] HEADER = {0, 'a', 's', 'm', 1, 0, 0, 0};
    private static final int DATA_SECTION = 11;
    private static final int I32_CONST = 0x41;
    private static final int END = 0x0B;
    private static final int ACTIVE = 0;
    private static final int PASSIVE = 1;
    private static final int ACTIVE_IN_MEMORY = 2;

    private WasmDataOffsets() {
    }

    /** {@code module} with every active data segment's offset in signed LEB128: the same bytes when they already are. */
    static byte[] fix(byte[] module) {
        if (module.length < HEADER.length) {
            throw new IllegalArgumentException("not a WebAssembly module: " + module.length + " bytes");
        }
        for (int i = 0; i < HEADER.length; i++) {
            if (module[i] != HEADER[i]) {
                throw new IllegalArgumentException("not a WebAssembly module (version 1): byte " + i + " is 0x"
                        + Integer.toHexString(module[i] & 0xFF));
            }
        }
        Reader in = new Reader(module, HEADER.length);
        ByteArrayOutputStream out = new ByteArrayOutputStream(module.length + 16);
        out.write(module, 0, HEADER.length);
        while (!in.atEnd()) {
            int at = in.pos;
            int id = in.u8();
            long size = in.unsigned();
            int start = in.pos;
            long end = start + size;
            if (end > module.length) {
                throw new IllegalArgumentException("section " + id + " at " + at + " runs " + (end - module.length)
                        + " bytes past the module's end");
            }
            if (id == DATA_SECTION) {
                byte[] content = rewriteData(module, start, (int) end);
                out.write(id);
                writeUnsigned(out, content.length);
                out.write(content, 0, content.length);
            } else {
                out.write(module, at, (int) end - at);
            }
            in.pos = (int) end;
        }
        return out.toByteArray();
    }

    /** The data section's content, each active segment's offset re-encoded. */
    private static byte[] rewriteData(byte[] module, int start, int end) {
        Reader in = new Reader(module, start);
        ByteArrayOutputStream out = new ByteArrayOutputStream(end - start + 8);
        long count = in.unsigned();
        writeUnsigned(out, count);
        for (long i = 0; i < count; i++) {
            long flags = in.unsigned();
            writeUnsigned(out, flags);
            if (flags == ACTIVE || flags == ACTIVE_IN_MEMORY) {
                if (flags == ACTIVE_IN_MEMORY) {
                    writeUnsigned(out, in.unsigned());    // the memory's index
                }
                int op = in.u8();
                if (op != I32_CONST) {
                    throw new IllegalArgumentException("data segment " + i + ": its offset expression starts with opcode 0x"
                            + Integer.toHexString(op) + ", not i32.const; TeaVM writes i32.const, and this fixer reads"
                            + " nothing else");
                }
                long offset = in.unsigned();    // as TeaVM 0.15.0 wrote it
                int close = in.u8();
                if (close != END) {
                    throw new IllegalArgumentException("data segment " + i + ": its offset expression continues past"
                            + " i32.const with opcode 0x" + Integer.toHexString(close));
                }
                out.write(I32_CONST);
                writeSigned(out, (int) offset);    // as the specification reads it: an i32, signed
                out.write(END);
            } else if (flags != PASSIVE) {
                throw new IllegalArgumentException("data segment " + i + ": flags " + flags + " (0, 1 and 2 are defined)");
            }
            long length = in.unsigned();
            if (in.pos + length > end) {
                throw new IllegalArgumentException("data segment " + i + ": " + length + " bytes run past the section");
            }
            writeUnsigned(out, length);
            out.write(module, in.pos, (int) length);
            in.pos += (int) length;
        }
        if (in.pos != end) {
            throw new IllegalArgumentException("the data section holds " + (end - in.pos) + " bytes after its "
                    + count + " segments");
        }
        return out.toByteArray();
    }

    private static void writeUnsigned(ByteArrayOutputStream out, long value) {
        long v = value;
        do {
            int b = (int) (v & 0x7F);
            v >>>= 7;
            out.write(v == 0 ? b : b | 0x80);
        } while (v != 0);
    }

    private static void writeSigned(ByteArrayOutputStream out, int value) {
        int v = value;
        while (true) {
            int b = v & 0x7F;
            v >>= 7;
            boolean last = (v == 0 && (b & 0x40) == 0) || (v == -1 && (b & 0x40) != 0);
            out.write(last ? b : b | 0x80);
            if (last) {
                return;
            }
        }
    }

    /** A cursor over the module's bytes. */
    private static final class Reader {
        private final byte[] bytes;
        int pos;

        Reader(byte[] bytes, int pos) {
            this.bytes = bytes;
            this.pos = pos;
        }

        boolean atEnd() {
            return pos >= bytes.length;
        }

        int u8() {
            if (pos >= bytes.length) {
                throw new IllegalArgumentException("the module ends at " + pos + ", inside a section");
            }
            return bytes[pos++] & 0xFF;
        }

        /** An unsigned LEB128 integer of at most 32 bits. */
        long unsigned() {
            long value = 0;
            int shift = 0;
            while (true) {
                int b = u8();
                value |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return value;
                }
                shift += 7;
                if (shift > 28) {
                    throw new IllegalArgumentException("an integer of more than 32 bits at " + pos);
                }
            }
        }
    }
}
