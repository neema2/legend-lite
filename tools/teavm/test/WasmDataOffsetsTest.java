// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.teavm;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The fixer held to the specification's encoding, and PARK-40's anchor (docs/PARKED_WORK_LEDGER.md): the fixer runs
 * while MODULE.bazel pins the TeaVM whose binary renderer writes data segment offsets unsigned.
 */
class WasmDataOffsetsTest {

    // the planner module of 2026-10-26, in miniature: segment 0 at 0, segment 1 at 1,056,011 (TeaVM 0.15.0 wrote it
    // 8b ba 40, which a reader of the specification takes as -1,041,141), between sections copied unchanged
    private static final byte[] TYPES = section(1, bytes(0));
    private static final byte[] CUSTOM = section(0, bytes(1, 'x', 1, 2));
    private static final byte[] FIRST = bytes(0, 0x41, 0x00, 0x0B, 3, 'a', 'b', 'c');

    @Test
    @DisplayName("TeaVM 0.15.0's unsigned offset is re-encoded as the specification's signed one, the section resized")
    void theUnsignedOffsetIsReEncodedSigned() {
        byte[] written = module(TYPES, section(11, concat(bytes(2), FIRST,
                bytes(0, 0x41, 0x8B, 0xBA, 0x40, 0x0B, 2, 'd', 'e'))), CUSTOM);
        byte[] read = module(TYPES, section(11, concat(bytes(2), FIRST,
                bytes(0, 0x41, 0x8B, 0xBA, 0xC0, 0x00, 0x0B, 2, 'd', 'e'))), CUSTOM);
        assertArrayEquals(read, WasmDataOffsets.fix(written));
        assertEquals(1_056_011, offsetOf(WasmDataOffsets.fix(written), 1));
        assertArrayEquals(read, WasmDataOffsets.fix(read), "a module already in the specification's encoding is untouched");
    }

    @Test
    @DisplayName("every width of the encoding reads back as the offset TeaVM meant")
    void everyWidthReadsBack() {
        int[] offsets = {0, 1, 63, 64, 127, 128, 8191, 8192, 16383, 16384, (1 << 20) - 1, 1 << 20, 1_056_011,
                (1 << 21) - 1, 1 << 21, (1 << 28) - 1, 1 << 28, Integer.MAX_VALUE};
        for (int offset : offsets) {
            byte[] written = module(section(11, concat(bytes(1, 0, 0x41), unsigned(offset), bytes(0x0B, 0))));
            byte[] fixed = WasmDataOffsets.fix(written);
            assertEquals(offset, offsetOf(fixed, 0), () -> "offset " + offset);
            assertArrayEquals(fixed, WasmDataOffsets.fix(fixed), () -> "offset " + offset + ": idempotent");
        }
    }

    @Test
    @DisplayName("a passive segment is copied; an active segment in a named memory keeps its memory index")
    void theOtherSegmentKindsAreCarried() {
        byte[] written = module(section(11, concat(bytes(3), FIRST,
                bytes(1, 2, 'p', 'q'),
                bytes(2, 0, 0x41, 0x8B, 0xBA, 0x40, 0x0B, 1, 'z'))));
        byte[] read = module(section(11, concat(bytes(3), FIRST,
                bytes(1, 2, 'p', 'q'),
                bytes(2, 0, 0x41, 0x8B, 0xBA, 0xC0, 0x00, 0x0B, 1, 'z'))));
        assertArrayEquals(read, WasmDataOffsets.fix(written));
    }

    @Test
    @DisplayName("an offset expression that is not i32.const is refused by its opcode, and a non-module by its bytes")
    void whatItDoesNotReadIsRefused() {
        byte[] globalGet = module(section(11, bytes(1, 0, 0x23, 0, 0x0B, 0)));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> WasmDataOffsets.fix(globalGet));
        assertTrue(e.getMessage().contains("opcode 0x23"), e.getMessage());
        assertThrows(IllegalArgumentException.class, () -> WasmDataOffsets.fix(bytes(0, 'a', 's', 'm', 2, 0, 0, 0)));
        assertThrows(IllegalArgumentException.class, () -> WasmDataOffsets.fix(bytes('M', 'Z')));
    }

    @Test
    @DisplayName("PARK-40's anchor: TeaVM is still the release whose data segment offsets are unsigned")
    void teaVmIsStillTheReleaseWhoseOffsetsAreUnsigned_park40() throws IOException {
        String module = Files.readString(Runfile.property("teavm.module"), StandardCharsets.UTF_8);
        assertTrue(module.contains("\"org.teavm:teavm-core:0.15.0\""),
                "PARKED WORK CHANGED — PARK-40: MODULE.bazel no longer pins TeaVM 0.15.0, the release whose binary"
                        + " renderer writes a data segment's offset unsigned (0.16.0 writes it signed). Delete"
                        + " WasmDataOffsets, its call in TeaVmCompile and this test, and close the row in"
                        + " docs/PARKED_WORK_LEDGER.md in the same commit.");
    }

    /** The offset of segment {@code index}, read as the specification reads it: a signed LEB128 i32. */
    private static int offsetOf(byte[] module, int index) {
        int pos = 8;
        while (pos < module.length) {
            int id = module[pos++] & 0xFF;
            long[] size = unsignedAt(module, pos);
            pos = (int) size[1];
            if (id != 11) {
                pos += (int) size[0];
                continue;
            }
            long[] count = unsignedAt(module, pos);
            pos = (int) count[1];
            for (int i = 0; i < count[0]; i++) {
                int flags = module[pos++];
                if (flags == 2) {
                    pos = (int) unsignedAt(module, pos)[1];
                }
                int offset = 0;
                if (flags != 1) {
                    assertEquals(0x41, module[pos++] & 0xFF, "i32.const");
                    int shift = 0;
                    int b;
                    do {
                        b = module[pos++] & 0xFF;
                        offset |= (b & 0x7F) << shift;
                        shift += 7;
                    } while ((b & 0x80) != 0);
                    if (shift < 32 && (b & 0x40) != 0) {
                        offset |= -1 << shift;
                    }
                    assertEquals(0x0B, module[pos++] & 0xFF, "end");
                }
                long[] length = unsignedAt(module, pos);
                pos = (int) (length[1] + length[0]);
                if (i == index) {
                    return offset;
                }
            }
        }
        throw new AssertionError("no data segment " + index);
    }

    private static long[] unsignedAt(byte[] bytes, int pos) {
        long value = 0;
        int shift = 0;
        int p = pos;
        int b;
        do {
            b = bytes[p++] & 0xFF;
            value |= (long) (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return new long[] {value, p};
    }

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }

    private static byte[] unsigned(int value) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long v = value;
        do {
            int b = (int) (v & 0x7F);
            v >>>= 7;
            out.write(v == 0 ? b : b | 0x80);
        } while (v != 0);
        return out.toByteArray();
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.write(part, 0, part.length);
        }
        return out.toByteArray();
    }

    private static byte[] section(int id, byte[] content) {
        return concat(bytes(id), unsigned(content.length), content);
    }

    private static byte[] module(byte[]... sections) {
        return concat(bytes(0, 'a', 's', 'm', 1, 0, 0, 0), concat(sections));
    }
}
