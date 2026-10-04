package com.legend.tools.bump;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BumpTest {

    private static final String ENGINE_INTEGRITY = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    private static final String PURE_INTEGRITY = "sha256-/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+8=";

    @Test
    void integrityIsSubresourceIntegrity() throws Exception {
        // the SHA-256 of nothing, as Bazel's http rules spell it
        assertEquals("sha256-47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=",
                Bump.integrity(MessageDigest.getInstance("SHA-256").digest(new byte[0])));
    }

    @Test
    void rewritesEveryPinOfTheRealModule() throws Exception {
        String module = Files.readString(Runfile.property("module.bazel"), StandardCharsets.UTF_8);
        Map<String, String> managed = new LinkedHashMap<>();
        managed.put("com.zaxxer:HikariCP", "9.9.1");
        managed.put("org.apache.commons:commons-lang3", "9.9.2");
        managed.put("org.apache.httpcomponents:httpcore", "9.9.3");
        managed.put("junit:junit", "9.9.4");
        managed.put("com.google.guava:guava", "9.9.5-jre$1");  // taken literally, never as a group reference
        String out = Bump.rewriteModule(module, "9.1.0", "8.2.0", ENGINE_INTEGRITY, PURE_INTEGRITY, managed);
        assertTrue(out.contains("\nLEGEND_ENGINE_RELEASE = \"9.1.0\"\n"), "engine release");
        assertTrue(out.contains("\nLEGEND_PURE_RELEASE = \"8.2.0\"\n"), "pure release");
        assertTrue(out.matches("(?s).*name = \"legend_engine_src\",[^)]*integrity = \"" + quote(ENGINE_INTEGRITY) + "\".*"),
                "engine archive");
        assertTrue(out.matches("(?s).*name = \"legend_pure_src\",[^)]*integrity = \"" + quote(PURE_INTEGRITY) + "\".*"),
                "pure archive");
        managed.forEach((k, v) -> assertTrue(out.contains("\"" + k + ":" + v + "\""), k));
        // nothing else moved: the same lines, the same count
        assertEquals(module.lines().count(), out.lines().count());
    }

    @Test
    void rewritesEveryPinOfTheRealOraclePins() throws Exception {
        String pins = Files.readString(Runfile.property("oracle.pins"), StandardCharsets.UTF_8);
        String engineSha = "1".repeat(40);
        String pureSha = "2".repeat(40);
        String out = Bump.rewritePins(pins, "9.1.0", "8.2.0", engineSha, "legend-engine-9.1.0", pureSha,
                "legend-pure-8.2.0");
        for (String line : new String[] {"LEGEND_ENGINE_RELEASE=9.1.0", "LEGEND_PURE_RELEASE=8.2.0",
                "LEGEND_ENGINE_SHA=" + engineSha, "LEGEND_ENGINE_DESCRIBE=legend-engine-9.1.0",
                "LEGEND_PURE_SHA=" + pureSha, "LEGEND_PURE_DESCRIBE=legend-pure-8.2.0"}) {
            assertTrue(out.lines().anyMatch(line::equals), line);
        }
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

    private static String quote(String s) {
        return java.util.regex.Pattern.quote(s);
    }
}
