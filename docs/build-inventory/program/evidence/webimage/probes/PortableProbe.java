package probe;

import java.math.BigDecimal;

/**
 * SPIKE (2026-10-10): ClasslibProbe's families with nothing TeaVM's class library lacks (no MessageDigest, HexFormat,
 * SplittableRandom): splitmix64 inputs, FNV-1a 64 per family. Run on the JVM, in Web Image and in TeaVM, so the control
 * (TeaVM, known to differ since leg 5) shows the probe can see a difference. Arguments: count seed.
 */
public final class PortableProbe {

    private static long state;

    private static long next() {
        long z = (state += 0x9E3779B97F4A7C15L);
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    private static long below(long bound) {
        return (next() >>> 1) % bound;
    }

    private static long fnv(long hash, String text) {
        for (int i = 0; i < text.length(); i++) {
            hash = (hash ^ text.charAt(i)) * 0x100000001B3L;
        }
        return (hash ^ '\n') * 0x100000001B3L;
    }

    private static final long BASIS = 0xCBF29CE484222325L;

    public static void main(String[] args) {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 1_000_000;
        state = args.length > 1 ? Long.parseLong(args[1]) : 42L;
        long toText = BASIS, fromText = BASIS, fromLongText = BASIS, decimalOf = BASIS, floats = BASIS;
        int toTextDiffs = 0;
        for (int i = 0; i < count; i++) {
            double d = Double.longBitsToDouble(next());
            String text = Double.toString(d);
            toText = fnv(toText, text);
            fromText = fnv(fromText, Long.toHexString(Double.doubleToRawLongBits(Double.parseDouble(text))));
            String digits = (1 + below(Long.MAX_VALUE - 1)) + "" + below(Long.MAX_VALUE) + "E" + (below(640) - 340);
            fromLongText = fnv(fromLongText, Long.toHexString(Double.doubleToRawLongBits(Double.parseDouble(digits))));
            if (!Double.isNaN(d) && !Double.isInfinite(d)) {
                decimalOf = fnv(decimalOf, Long.toHexString(Double.doubleToRawLongBits(new BigDecimal(digits).doubleValue())));
            }
            floats = fnv(floats, Float.toString(Float.intBitsToFloat((int) next())));
        }
        long blank = BASIS;
        for (int c = 0; c <= 0xFFFF; c++) {
            String s = String.valueOf((char) c);
            blank = fnv(blank, s.isBlank() + "," + s.strip().isEmpty() + "," + Character.isWhitespace(c));
        }
        long shortText = BASIS;
        for (int i = 0; i < count; i++) {
            String text = below(100_000) + "." + below(1_000_000);
            shortText = fnv(shortText, Double.toString(Double.parseDouble(text)));
        }
        StringBuilder out = new StringBuilder();
        out.append("count ").append(count).append('\n');
        out.append("Double.toString(random bits)      ").append(Long.toHexString(toText)).append('\n');
        out.append("parseDouble(shortest text)        ").append(Long.toHexString(fromText)).append('\n');
        out.append("parseDouble(long decimal)         ").append(Long.toHexString(fromLongText)).append('\n');
        out.append("BigDecimal(long decimal).double   ").append(Long.toHexString(decimalOf)).append('\n');
        out.append("Float.toString(random bits)       ").append(Long.toHexString(floats)).append('\n');
        out.append("isBlank/strip/isWhitespace 0-FFFF ").append(Long.toHexString(blank)).append('\n');
        out.append("toString(parse(short decimal))    ").append(Long.toHexString(shortText));
        System.out.println(out);
    }
}
