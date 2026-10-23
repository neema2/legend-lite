package com.legend.tools.teavm;

import org.junit.jupiter.api.Test;

import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** TeaVM 0.15.0's unsigned data-segment addresses written as the signed LEB128 WebAssembly reads (SegmentOffsets). */
class SegmentOffsetsTest {

    private static final HexFormat HEX = HexFormat.of();

    /** A module: the header, a type section left as is, and a data section of the segments given (hex). */
    private static byte[] module(String dataBody) {
        byte[] data = HEX.parseHex(dataBody);
        return HEX.parseHex("0061736d01000000" + "0104" + "01600000" + "0b"
                + String.format(java.util.Locale.ROOT, "%02x", data.length) + dataBody);
    }

    @Test
    void anAddressFromOneToTwoMebibytesIsWrittenSigned() {
        // TeaVM's 1,048,651 (the planner's, 2026-10-23): unsigned cb8040, which WebAssembly reads as -1,048,501;
        // signed it takes a fourth byte, cb80c000
        byte[] fixed = SegmentOffsets.fix(module("02" + "00" + "4100" + "0b" + "0161" + "00" + "41cb8040" + "0b" + "0162"));
        assertEquals(HEX.formatHex(module("02" + "00" + "4100" + "0b" + "0161" + "00" + "41cb80c000" + "0b" + "0162")),
                HEX.formatHex(fixed));
    }

    @Test
    void anAddressBelowOneMebibyteIsTheSameBytes() {
        byte[] m = module("01" + "00" + "41ffff3f" + "0b" + "0263" + "64");
        assertEquals(HEX.formatHex(m), HEX.formatHex(SegmentOffsets.fix(m)));
        // and 64, whose seventh bit is set, already takes its second byte signed: 41c000
        byte[] sixtyFour = module("01" + "00" + "4140" + "0b" + "00");
        assertEquals(HEX.formatHex(module("01" + "00" + "41c000" + "0b" + "00")),
                HEX.formatHex(SegmentOffsets.fix(sixtyFour)));
    }

    @Test
    void aPassiveSegmentAndOtherSectionsAreLeftAsTheyAre() {
        byte[] m = module("01" + "01" + "0365" + "6667");
        assertEquals(HEX.formatHex(m), HEX.formatHex(SegmentOffsets.fix(m)));
    }

    @Test
    void whatTeaVmDoesNotWriteIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> SegmentOffsets.fix(module("01" + "00" + "4200" + "0b" + "00")));
        assertThrows(IllegalArgumentException.class, () -> SegmentOffsets.fix(module("01" + "02" + "00" + "4100" + "0b" + "00")));
        assertThrows(IllegalArgumentException.class, () -> SegmentOffsets.fix(HEX.parseHex("00")));
    }
}
