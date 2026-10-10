package probe;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.SplittableRandom;

/**
 * SPIKE (2026-10-10): does a module run the JDK's own class library? The calls TeaVM's rewrites answered differently
 * (leg 5: Double.toString, parseDouble, BigDecimal.doubleValue, String.isBlank), over many inputs, each family folded
 * to one SHA-256 -- run on the JVM and in the module, and compare the lines. Arguments: count seed.
 */
public final class ClasslibProbe {

    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 1_000_000;
        long seed = args.length > 1 ? Long.parseLong(args[1]) : 42L;
        SplittableRandom random = new SplittableRandom(seed);

        MessageDigest toText = MessageDigest.getInstance("SHA-256");
        MessageDigest fromText = MessageDigest.getInstance("SHA-256");
        MessageDigest decimalOf = MessageDigest.getInstance("SHA-256");
        MessageDigest floats = MessageDigest.getInstance("SHA-256");
        MessageDigest fromLongText = MessageDigest.getInstance("SHA-256");
        for (int i = 0; i < count; i++) {
            double d = Double.longBitsToDouble(random.nextLong());
            String text = Double.toString(d);
            feed(toText, text);
            // the shortest text back to its double, and a long decimal (beyond 17 digits) to its nearest double
            feed(fromText, Long.toHexString(Double.doubleToRawLongBits(Double.parseDouble(text))));
            String digits = random.nextLong(1, Long.MAX_VALUE) + "" + random.nextLong(0, Long.MAX_VALUE)
                    + "E" + random.nextInt(-340, 300);
            feed(fromLongText, Long.toHexString(Double.doubleToRawLongBits(Double.parseDouble(digits))));
            if (Double.isFinite(d)) {
                feed(decimalOf, Long.toHexString(Double.doubleToRawLongBits(new BigDecimal(digits).doubleValue())));
            }
            feed(floats, Float.toString(Float.intBitsToFloat(random.nextInt())));
        }
        MessageDigest blank = MessageDigest.getInstance("SHA-256");
        for (int c = 0; c <= 0xFFFF; c++) {
            String s = String.valueOf((char) c);
            feed(blank, s.isBlank() + "," + s.strip().isEmpty() + "," + Character.isWhitespace(c));
        }
        // the decimals a person writes: short ones, where TeaVM's Double.toString picked the other shortest text
        MessageDigest shortText = MessageDigest.getInstance("SHA-256");
        for (int i = 0; i < count; i++) {
            String text = random.nextInt(0, 100_000) + "." + random.nextInt(0, 1_000_000);
            feed(shortText, Double.toString(Double.parseDouble(text)));
        }
        System.out.println("count " + count + " seed " + seed);
        System.out.println("Double.toString(random bits)      " + hex(toText));
        System.out.println("parseDouble(shortest text)        " + hex(fromText));
        System.out.println("parseDouble(long decimal)         " + hex(fromLongText));
        System.out.println("BigDecimal(long decimal).double   " + hex(decimalOf));
        System.out.println("Float.toString(random bits)       " + hex(floats));
        System.out.println("isBlank/strip/isWhitespace 0-FFFF " + hex(blank));
        System.out.println("toString(parse(short decimal))    " + hex(shortText));
    }

    private static void feed(MessageDigest digest, String text) {
        digest.update(text.getBytes(StandardCharsets.UTF_8));
        digest.update((byte) '\n');
    }

    private static String hex(MessageDigest digest) {
        return HexFormat.of().formatHex(digest.digest());
    }
}
