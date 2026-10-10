package conformance;

/**
 * The cases' random inputs: splitmix64, seeded per family, so both sides draw the same numbers without any class
 * under test (no {@code java.util.Random}, no {@code SplittableRandom}).
 */
final class Rng {

    private long state;

    Rng(long seed) {
        state = seed;
    }

    long next() {
        long z = (state += 0x9E3779B97F4A7C15L);
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /** In [0, bound). */
    int below(int bound) {
        return (int) ((next() >>> 1) % bound);
    }

    /** {@code n} decimal digits, the first not zero unless {@code n} is 1. */
    String digits(int n) {
        StringBuilder out = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            int d = below(10);
            if (i == 0 && n > 1 && d == 0) {
                d = 1 + below(9);
            }
            out.append((char) ('0' + d));
        }
        return out.toString();
    }

    /** An identifier-like word of 1 to 12 characters. */
    String word() {
        String letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789";
        int n = 1 + below(12);
        StringBuilder out = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            out.append(letters.charAt(i == 0 ? below(53) : below(letters.length())));
        }
        return out.toString();
    }
}
