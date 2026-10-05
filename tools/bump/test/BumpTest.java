package com.legend.tools.bump;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BumpTest {

    @Test
    void integrityIsSubresourceIntegrity() throws Exception {
        // the SHA-256 of nothing, as Bazel's http rules spell it
        assertEquals("sha256-47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=",
                Bump.integrity(MessageDigest.getInstance("SHA-256").digest(new byte[0])));
    }

    @Test
    void rewritesThePinsBlockOfTheRealSegment() throws Exception {
        String segment = Files.readString(Runfile.property("release.segment"), StandardCharsets.UTF_8);
        Map<String, String> pins = Bump.readPins(segment);
        assertEquals("finos/legend-engine", pins.get("LEGEND_ENGINE_REPO"));
        Map<String, String> moved = new LinkedHashMap<>();
        int n = 0;
        for (String k : Bump.PINS) {
            moved.put(k, "moved-" + n++ + "$1\\");  // taken literally: no regex, no escapes
        }
        String out = Bump.writePins(segment, moved);
        assertEquals(moved, Bump.readPins(out), "every pin moved, and reads back");
        // nothing outside the block moved: the same lines, in the same places
        List<String> before = segment.lines().toList();
        List<String> after = out.lines().toList();
        assertEquals(before.size(), after.size());
        for (int i = 0; i < before.size(); i++) {
            if (!before.get(i).equals(after.get(i))) {
                String name = before.get(i).substring(0, before.get(i).indexOf(" = "));
                assertTrue(Bump.PINS.contains(name), "line " + (i + 1) + " moved: " + before.get(i));
            }
        }
        assertEquals(segment.endsWith("\n"), out.endsWith("\n"));
    }

    @Test
    void refusesAPinsBlockItDidNotWrite() {
        String block = "x\n# ── PINS: written whole\nLEGEND_ENGINE_RELEASE = \"1\"\n# ── END PINS ──\n";
        assertThrows(IllegalStateException.class, () -> Bump.readPins(block), "a missing name");
        assertThrows(IllegalStateException.class, () -> Bump.readPins("no block"), "no block");
        assertThrows(IllegalStateException.class,
                () -> Bump.readPins(block.replace("= \"1\"", "= other")), "not NAME = \"value\"");
    }

    @Test
    void tagCommitReadsTheRefAdvertisement() {
        String a = "a".repeat(40);
        String b = "b".repeat(40);
        String c = "c".repeat(40);
        String d = "d".repeat(40);
        // GitHub's smart-HTTP advertisement: pkt-lines, capabilities after a NUL on the first ref
        String advertisement = "001e# service=git-upload-pack\n0000"
                + "0123" + a + " refs/tags/legend-engine-4.145.0\0multi_ack side-band-64k\n"
                + "004e" + b + " refs/tags/legend-engine-4.145.01\n"
                + "004b" + c + " refs/tags/legend-pure-5.99.0\n"
                + "004f" + d + " refs/tags/legend-pure-5.99.0^{}\n"
                + "0000";
        assertEquals(a, Bump.tagCommit(advertisement, "legend-engine-4.145.0"), "a lightweight tag, the first ref");
        assertEquals(b, Bump.tagCommit(advertisement, "legend-engine-4.145.01"), "no prefix match");
        assertEquals(d, Bump.tagCommit(advertisement, "legend-pure-5.99.0"), "an annotated tag's peeled commit");
        assertNull(Bump.tagCommit(advertisement, "legend-engine-4.145"), "a tag that only prefixes others");
        assertNull(Bump.tagCommit(advertisement, "legend-engine-4.146.0"));
    }

}
