// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

/**
 * THE ASOR store-object-reference protocol — ONE owner (F3.4; audit
 * A11 found the format spelled three times: GraphEmission's encode,
 * Substitution's resolve-time decode via a {@code lastIndexOf(":{")}
 * heuristic, and the harness's full codec with its own copy of the
 * prefix). Layout, decoded from the engine goldens:
 * {@code 001:010:} then len10-prefixed segments
 * ({@code %010d:value:}): store kind, the DEFINING (include-aware)
 * mapping, the root set id, the set id, the canonical test-H2
 * connection protocol JSON, and the per-row pk-map JSON. References
 * travel base64-encoded with an {@code ASOR:} prefix.
 */
public final class AsorRef {

    private AsorRef() {
    }

    public static final String KIND = "Relational";

    /** The reference marker prefixed to the base64 payload. */
    public static final String MARKER = "ASOR:";

    /** Segment-length framing width ({@code %010d}) — the SQL-side
     *  encoder spells it as {@code lpad(.., SEG_LEN_WIDTH, '0')}. */
    public static final int SEG_LEN_WIDTH = 10;

    /** The engine protocol's canonical test-H2 connection segment —
     * part of the WIRE FORMAT (both producer and consumer are ours;
     * the goldens pin these exact bytes), not an execution choice. */
    public static final String CANONICAL_H2_CONNECTION =
            "{\"_type\":\"RelationalDatabaseConnection\","
            + "\"authenticationStrategy\":{\"_type\":\"h2Default\"},"
            + "\"datasourceSpecification\":{\"_type\":\"h2Local\"},"
            + "\"element\":\"\",\"postProcessorWithParameter\":[],"
            + "\"postProcessors\":[],\"timeZone\":\"GMT\","
            + "\"type\":\"H2\"}";

    public static String seg(String v) {
        return String.format("%010d", v.length()) + ":" + v + ":";
    }

    /** The STATIC prefix — everything before the per-row pk segment. */
    public static String prefix(String definingMapping, String rootSetId,
            String setId) {
        return "001:010:" + seg(KIND) + seg(definingMapping)
                + seg(rootSetId) + seg(setId) + seg(CANONICAL_H2_CONNECTION);
    }

    /** The COMPLETE reference for one row — prefix + the per-row
     *  pk-map segment (len10-framed, NO trailing colon), base64 without
     *  padding, MARKER-prefixed. The harness encoder (ObjectRefs) and
     *  the SQL-side emitter (SnapshotEnvelope) both follow this shape;
     *  the harness delegates here (F3.4b). */
    public static String ref(String definingMapping, String rootSetId,
            String setId, String pkJson) {
        String full = prefix(definingMapping, rootSetId, setId)
                + String.format("%0" + SEG_LEN_WIDTH + "d", pkJson.length())
                + ":" + pkJson;
        return MARKER + java.util.Base64.getEncoder().withoutPadding()
                .encodeToString(full.getBytes(
                        java.nio.charset.StandardCharsets.UTF_8));
    }

    /** The DEFINING mapping named by a static prefix (segment 2 of the
     *  framing) — the resolver's decode arm keys its pk-column facts on
     *  it. Null when the text is not a well-formed prefix. */
    public static @com.legend.Nullable String prefixMapping(String prefix) {
        try {
            int i = "001:010:".length();
            String seg = null;
            for (int k = 0; k < 2; k++) {
                int len = Integer.parseInt(prefix.substring(i, i + 10));
                seg = prefix.substring(i + 11, i + 11 + len);
                i += 11 + len + 1;
            }
            return seg;
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            return null;
        }
    }

    /** A decoded reference: the segments a consumer reads. */
    public record Ref(String mapping, String rootSetId, String setId,
            String pkJson) {
    }

    /** Decode an {@code ASOR:}-prefixed (or bare) base64 reference by
     * the REAL segment walk — never a substring heuristic. Null when
     * the text is not a well-formed reference (callers keep their loud
     * walls). */
    public static @com.legend.Nullable Ref decode(String ref) {
        try {
            String b64 = ref.startsWith(MARKER)
                    ? ref.substring(MARKER.length()) : ref;
            String d = new String(java.util.Base64.getDecoder().decode(
                    b64 + "=".repeat((4 - b64.length() % 4) % 4)),
                    java.nio.charset.StandardCharsets.UTF_8);
            java.util.List<String> segs = new java.util.ArrayList<>();
            int i = "001:010:".length();
            while (i < d.length() && segs.size() < 6) {
                int len = Integer.parseInt(d.substring(i, i + 10));
                segs.add(d.substring(i + 11, i + 11 + len));
                i += 11 + len + 1;
            }
            if (segs.size() < 6) {
                return null;
            }
            return new Ref(segs.get(1), segs.get(2), segs.get(3),
                    segs.get(5));
        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            // bad base64 / bad length digits / short segment — not a
            // reference: the caller's wall owns it
            return null;
        }
    }
}
