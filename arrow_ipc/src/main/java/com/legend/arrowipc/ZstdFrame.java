// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.arrowipc;

import java.io.ByteArrayOutputStream;

/**
 * ONE ZSTANDARD FRAME OF RAW BLOCKS (RFC 8878 §3.1): the bytes stored as they are, in blocks of at most 128 KiB,
 * each flagged raw -- a frame any zstd decoder reads (pyarrow's, the browser's fzstd, libzstd), written with no
 * compressor and no library. legend-engine's Arrow answer to execute ({@code ?serializationFormat=ARROW_IPC}) is one
 * zstd frame around an Arrow IPC stream (docs/SEMANTICS_REGISTER.md S28; measured 2026-10-08), so legend-lite's is
 * the same frame, uncompressed: the format is upstream's, the bytes inside are the writer's own (track A, 2026-10-29).
 */
public final class ZstdFrame {

    private ZstdFrame() {
    }

    /** The frame's first four bytes, little-endian 0xFD2FB528. */
    public static final byte[] MAGIC = {0x28, (byte) 0xB5, 0x2F, (byte) 0xFD};

    private static final int BLOCK = 128 * 1024;

    /** {@code content} as one frame: the magic, a header stating the content size (an 8-byte field) and a 128 KiB
     *  window, then raw blocks, the last flagged; no checksum. */
    public static byte[] of(byte[] content) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(content.length + 16 + 3 * (content.length / BLOCK + 1));
        put(out, MAGIC);
        // Frame_Header_Descriptor: Frame_Content_Size_flag 3 (8 bytes), Single_Segment 0, Content_Checksum 0,
        // Dictionary_ID 0
        out.write(0xC0);
        // Window_Descriptor: exponent 7, mantissa 0 -> 1 << (10 + 7) = 128 KiB, a block's largest size
        out.write(7 << 3);
        long size = content.length;
        for (int i = 0; i < 8; i++) out.write((int) (size >>> (8 * i)) & 0xFF);
        int at = 0;
        do {
            int n = Math.min(BLOCK, content.length - at);
            boolean last = at + n == content.length;
            // Block_Header, 3 bytes little-endian: Last_Block (bit 0), Block_Type raw (bits 1-2: 0), Block_Size
            int header = (n << 3) | (last ? 1 : 0);
            out.write(header & 0xFF);
            out.write((header >>> 8) & 0xFF);
            out.write((header >>> 16) & 0xFF);
            out.write(content, at, n);
            at += n;
        } while (at < content.length);
        return out.toByteArray();
    }

    /** {@code out.write(b)} whole: the WebAssembly tab's class library (TeaVM) has no
     *  {@code ByteArrayOutputStream.writeBytes} (track A, 2026-10-29, the l5 chain's PG group). */
    private static void put(java.io.ByteArrayOutputStream out, byte[] b) {
        out.write(b, 0, b.length);
    }
}
