package com.legend.cache;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Our SHA-256 must be byte-identical to the platform's.
 *
 * <p>This is not a "does it look like a hash" test. {@link Hash} is
 * the key type of a CONTENT-ADDRESSED store, so a single differing
 * bit silently invalidates every cached artifact and, worse, could
 * make two different artifacts collide onto one key. The equivalence
 * is therefore asserted directly against {@link MessageDigest}, over
 * the shapes that break hand-rolled implementations: empty input, the
 * exact block boundary, one byte either side of it, the length-field
 * boundary, and a spread of random sizes.
 */
class Sha256Test {

    private static String platform(byte[] in) throws Exception {
        return HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(in));
    }

    private static String ours(byte[] in) {
        return Sha256.hex(Sha256.digest(in));
    }

    @Test
    void matchesThePublishedVectors() {
        // FIPS 180-4 / NIST CAVP.
        assertEquals(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                ours(new byte[0]), "empty");
        assertEquals(
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                ours("abc".getBytes(StandardCharsets.UTF_8)), "abc");
        assertEquals(
                "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
                ours(("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")
                        .getBytes(StandardCharsets.UTF_8)), "two-block");
    }

    @Test
    void matchesThePlatformAtEveryPaddingBoundary() throws Exception {
        // 55/56/57 straddle "the length no longer fits in this block",
        // and 63/64/65 the block size itself — where a padding bug hides.
        for (int n : new int[] {0, 1, 54, 55, 56, 57, 63, 64, 65, 119, 120,
                121, 127, 128, 129}) {
            byte[] in = new byte[n];
            for (int i = 0; i < n; i++) {
                in[i] = (byte) (i * 31 + 7);
            }
            assertEquals(platform(in), ours(in), "length " + n);
        }
    }

    @Test
    void matchesThePlatformOnRandomInput() throws Exception {
        Random r = new Random(20260920);
        for (int i = 0; i < 400; i++) {
            byte[] in = new byte[r.nextInt(2000)];
            r.nextBytes(in);
            assertEquals(platform(in), ours(in),
                    "random input of " + in.length + " bytes");
        }
    }

    @Test
    void theHashTypeItselfStillAgreesWithThePlatform() throws Exception {
        // The real invariant: existing cache entries stay valid.
        for (String s : new String[] {"", "a", "Class a::B {}",
                "###Relational\nDatabase d::D ( Table T ( c VARCHAR(1) ) )"}) {
            assertEquals(platform(s.getBytes(StandardCharsets.UTF_8)),
                    Hash.ofUtf8(s).hex(),
                    "Hash.ofUtf8 drifted from the platform for " + s);
        }
    }
}
