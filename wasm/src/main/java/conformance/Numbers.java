package conformance;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;

/** The number families: a double's text both ways, a float's text, BigDecimal, BigInteger, int and long text, Math. */
final class Numbers {

    private Numbers() {
    }

    // ---- a double to text

    /** Every power of two (normal and subnormal) with its neighbours, and the edges where the notation changes. */
    static String doubleToStringEdges() {
        Out out = new Out();
        for (long e = 0; e <= 2046; e++) {
            long bits = e << 52;
            for (long b : new long[] {bits - 1, bits, bits + 1}) {
                if (b >= 0 && b < 0x7FF0000000000000L) {
                    out.add(Out.hex(b), Double.toString(Double.longBitsToDouble(b)));
                }
            }
        }
        for (int k = 0; k < 52; k++) {
            long b = 1L << k;
            out.add(Out.hex(b), Double.toString(Double.longBitsToDouble(b)));
        }
        double[] edges = {0.0, -0.0, Double.MIN_VALUE, -Double.MIN_VALUE, Double.MAX_VALUE, Double.MIN_NORMAL,
            Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0.1, 0.2, 0.3, 1.0 / 3, 2.0 / 3,
            0.001, 0.01, 1e7, 1e-3, 9999999.999999998, 1e21, 1e22, 1e23, 9007199254740992.0, 9007199254740993.0,
            123456789012345680.0, 5e-324, 2.2250738585072014E-308, 2.225073858507201E-308, 4.35, 2.675, 1.005};
        for (double d : edges) {
            long b = Double.doubleToRawLongBits(d);
            for (long x : new long[] {b - 1, b, b + 1}) {
                out.add(Out.hex(x), Double.toString(Double.longBitsToDouble(x)));
            }
        }
        return out.toString();
    }

    static String doubleToStringRandom() {
        Out out = new Out();
        Rng r = new Rng(1);
        for (int i = 0; i < 40_000; i++) {
            long b = r.next();
            out.add(Out.hex(b), Double.toString(Double.longBitsToDouble(b)));
        }
        return out.toString();
    }

    /** {@code StringBuilder.append(double)} and {@code String.valueOf(double)}: the other two ways a double is spelled. */
    static String doubleAppend() {
        Out out = new Out();
        Rng r = new Rng(2);
        for (int i = 0; i < 4_000; i++) {
            double d = Double.longBitsToDouble(r.next());
            out.add(Out.hex(Double.doubleToRawLongBits(d)), new StringBuilder().append(d) + " " + String.valueOf(d));
        }
        return out.toString();
    }

    /** Decimals as a person writes them, read and written back: what a model's literal goes through. */
    static String doubleShortDecimals() {
        Out out = new Out();
        Rng r = new Rng(3);
        for (int i = 0; i < 40_000; i++) {
            String s = r.digits(1 + r.below(6)) + "." + r.digits(1 + r.below(8));
            out.add(s, readAndWrite(s));
        }
        return out.toString();
    }

    // ---- text to a double

    static String doubleParseForms() {
        Out out = new Out();
        String[] forms = {"0", "-0", "+0", "1", "-1", "+1.5", ".5", "5.", "-.5", "1e5", "1E5", "1e+5", "1E-5",
            "1.5e308", "1.7976931348623157e308", "1.7976931348623158e308", "1.7976931348623159e308", "1e309",
            "4.9e-324", "2.4703282292062327e-324", "2.4703282292062328e-324", "1e-325", "2.2250738585072011e-308",
            "2.2250738585072012e-308", "9007199254740993", "9007199254740993.0000000001", "0.1", "0.30000000000000004",
            "  2.5", "2.5  ", "\t2.5\n", "2.5d", "2.5D", "2.5f", "2.5F", "1e5d", "Infinity", "-Infinity", "+Infinity",
            "NaN", "-NaN", "infinity", "nan", "0x1.8p1", "0X10P0", "0x.8p-1", "1_000", "", " ", "1e", "e5", ".", "-",
            "--1", "1.2.3", "1e5.5", "1,5", "١٢٣", "５", "1 ", "00000000000000000000001.5",
            "1.500000000000000000000000000000", "0.000000000000000000000000000000000000000001",
            "123456789012345678901234567890", "1e2147483647", "1e-2147483648", "1e9999999999"};
        for (String s : forms) {
            out.add(s, readBits(s));
        }
        for (int k = -345; k <= 310; k++) {
            String s = "1e" + k;
            out.add(s, readAndWrite(s));
        }
        return out.toString();
    }

    /** Random decimals: 1 to 20 significant digits, any exponent a double can hold and beyond. */
    static String doubleParseRandom() {
        Out out = new Out();
        Rng r = new Rng(4);
        for (int i = 0; i < 40_000; i++) {
            String digits = r.digits(1 + r.below(20));
            int point = r.below(digits.length() + 1);
            String mantissa = digits.substring(0, point) + "." + digits.substring(point);
            if (mantissa.startsWith(".")) {
                mantissa = "0" + mantissa;
            }
            String s = (r.below(4) == 0 ? "-" : "") + mantissa + (r.below(3) == 0 ? "" : (r.below(2) == 0 ? "e" : "E")
                    + (r.below(330) - 340 + r.below(320)));
            out.add(s, readBits(s));
        }
        return out.toString();
    }

    /**
     * The hardest decimals to read: exactly halfway between two neighbouring doubles (round half to even decides),
     * and a hair above and below it, written in full by integer arithmetic (no conversion under test makes them).
     */
    static String doubleParseMidpoints() {
        Out out = new Out();
        Rng r = new Rng(5);
        for (int i = 0; i < 2_500; i++) {
            long bits;
            if (i % 10 == 0) {
                bits = r.next() & 0x7FEFFFFFFFFFFFFFL;   // any finite exponent
            } else {
                bits = ((long) (923 + r.below(200)) << 52) | (r.next() & 0xFFFFFFFFFFFFFL);   // about 1e-30 to 1e30
            }
            String mid = midpoint(bits);
            out.add(mid, readBits(mid));
            String above = mid.contains(".") ? mid + "1" : mid + ".0000000001";
            out.add(above, readBits(above));
            String below = below(mid);
            out.add(below, readBits(below));
        }
        return out.toString();
    }

    private static String readBits(String s) {
        try {
            return Out.hex(Double.doubleToRawLongBits(Double.parseDouble(s)));
        } catch (RuntimeException e) {
            return Out.err(e);
        }
    }

    private static String readAndWrite(String s) {
        try {
            double d = Double.parseDouble(s);
            return Out.hex(Double.doubleToRawLongBits(d)) + " " + Double.toString(d);
        } catch (RuntimeException e) {
            return Out.err(e);
        }
    }

    /** The exact decimal halfway between the finite positive double {@code bits} and the next one up. */
    private static String midpoint(long bits) {
        long frac = bits & 0xFFFFFFFFFFFFFL;
        int exp = (int) (bits >>> 52);
        long m = exp == 0 ? frac : frac | (1L << 52);
        int e = (exp == 0 ? -1074 : exp - 1075) - 1;
        BigInteger odd = BigInteger.valueOf(m).shiftLeft(1).add(BigInteger.ONE);
        if (e >= 0) {
            return odd.shiftLeft(e).toString();
        }
        String digits = odd.multiply(BigInteger.valueOf(5).pow(-e)).toString();
        int k = -e;
        if (digits.length() <= k) {
            return "0." + "0".repeat(k - digits.length()) + digits;
        }
        return digits.substring(0, digits.length() - k) + "." + digits.substring(digits.length() - k);
    }

    /** A hair below {@code mid}: its last digit lowered by one, then nines. */
    private static String below(String mid) {
        if (!mid.contains(".")) {
            return new BigInteger(mid).subtract(BigInteger.ONE) + ".9999999999";
        }
        char last = mid.charAt(mid.length() - 1);
        return mid.substring(0, mid.length() - 1) + (char) (last - 1) + "9999999999";
    }

    // ---- a float to text

    static String floatToString() {
        Out out = new Out();
        for (int e = 0; e <= 254; e++) {
            int bits = e << 23;
            for (int b : new int[] {bits - 1, bits, bits + 1}) {
                if (b >= 0) {
                    out.add(Out.hex(b), Float.toString(Float.intBitsToFloat(b)));
                }
            }
        }
        Rng r = new Rng(6);
        for (int i = 0; i < 20_000; i++) {
            int b = (int) r.next();
            out.add(Out.hex(b & 0xFFFFFFFFL), Float.toString(Float.intBitsToFloat(b)));
        }
        return out.toString();
    }

    // ---- BigDecimal

    static String bigDecimal() {
        Out out = new Out();
        Rng r = new Rng(7);
        for (int i = 0; i < 3_000; i++) {
            String a = decimal(r);
            String b = decimal(r);
            out.add(a + " " + b, bigDecimalOps(a, b));
        }
        for (int i = 0; i < 2_000; i++) {
            long l = r.next() >> r.below(64);
            out.add("valueOf(long) " + l, BigDecimal.valueOf(l).toString());
            double d = Double.longBitsToDouble(r.next());
            if (!Double.isNaN(d) && !Double.isInfinite(d)) {
                out.add("valueOf(double) " + Out.hex(Double.doubleToRawLongBits(d)), BigDecimal.valueOf(d).toString());
                out.add("new(double) " + Out.hex(Double.doubleToRawLongBits(d)), new BigDecimal(d).toString());
            }
        }
        return out.toString();
    }

    private static String decimal(Rng r) {
        String digits = r.digits(1 + r.below(30));
        String s = (r.below(3) == 0 ? "-" : "") + digits;
        int point = r.below(digits.length() + 1);
        if (point < digits.length() && r.below(2) == 0) {
            s = s.substring(0, s.length() - digits.length() + point) + "." + digits.substring(point);
        }
        if (r.below(4) == 0) {
            s += "E" + (r.below(100) - 50);
        }
        return s;
    }

    private static String bigDecimalOps(String a, String b) {
        try {
            BigDecimal x = new BigDecimal(a);
            BigDecimal y = new BigDecimal(b);
            StringBuilder s = new StringBuilder();
            s.append(x).append('|').append(x.toPlainString()).append('|').append(x.stripTrailingZeros())
                    .append('|').append(x.scale()).append('|').append(x.precision()).append('|').append(x.signum())
                    .append('|').append(x.unscaledValue()).append('|')
                    .append(Out.hex(Double.doubleToRawLongBits(x.doubleValue())))
                    .append('|').append(x.setScale(2, RoundingMode.HALF_UP)).append('|')
                    .append(x.setScale(0, RoundingMode.HALF_EVEN)).append('|').append(x.negate()).append('|')
                    .append(x.abs()).append('|').append(x.scaleByPowerOfTen(3)).append('|').append(x.longValue())
                    .append('|').append(x.compareTo(y)).append('|').append(x.add(y)).append('|').append(x.subtract(y))
                    .append('|').append(x.equals(y)).append('|').append(x.hashCode());
            if (y.signum() != 0) {
                s.append('|').append(x.divide(y, 10, RoundingMode.HALF_EVEN)).append('|')
                        .append(x.divide(y, MathContext.DECIMAL64));
            }
            try {
                s.append('|').append(x.toBigIntegerExact());
            } catch (ArithmeticException e) {
                s.append('|').append(Out.err(e));
            }
            return s.toString();
        } catch (RuntimeException e) {
            return Out.err(e);
        }
    }

    // ---- BigInteger

    static String bigInteger() {
        Out out = new Out();
        Rng r = new Rng(8);
        for (int i = 0; i < 3_000; i++) {
            String a = (r.below(3) == 0 ? "-" : "") + r.digits(1 + r.below(60));
            String b = (r.below(3) == 0 ? "-" : "") + r.digits(1 + r.below(30));
            BigInteger x = new BigInteger(a);
            BigInteger y = new BigInteger(b);
            StringBuilder s = new StringBuilder();
            s.append(x).append('|').append(x.add(y)).append('|').append(x.multiply(y)).append('|').append(x.pow(3))
                    .append('|').append(x.shiftLeft(7)).append('|').append(x.shiftRight(5)).append('|')
                    .append(x.bitLength()).append('|').append(x.testBit(10)).append('|').append(x.longValue())
                    .append('|').append(x.compareTo(y));
            if (y.signum() != 0) {
                BigInteger[] qr = x.divideAndRemainder(y);
                s.append('|').append(qr[0]).append('|').append(qr[1]);
            }
            out.add(a + " " + b, s.toString());
        }
        return out.toString();
    }

    // ---- int and long text, Math

    static String integers() {
        Out out = new Out();
        String[] ints = {"0", "-0", "+0", "1", "-1", "+1", "007", "2147483647", "2147483648", "-2147483648",
            "-2147483649", "9223372036854775807", "9223372036854775808", "-9223372036854775808",
            "-9223372036854775809", "", " ", " 1", "1 ", "+", "-", "0x10", "1e3", "1.0", "1_000",
            "١٢٣", "１２", "१", "12٣"};
        for (String s : ints) {
            out.add("parseInt " + s, parse(s, false));
            out.add("parseLong " + s, parse(s, true));
        }
        Rng r = new Rng(9);
        for (int i = 0; i < 5_000; i++) {
            long l = r.next() >> r.below(64);
            int n = (int) l;
            out.add(Out.hex(l), Long.toString(l) + " " + Integer.toString(n) + " " + Integer.toHexString(n) + " "
                    + Integer.rotateRight(n, r.below(40)) + " " + Integer.valueOf(n).hashCode());
        }
        for (int radix = 2; radix <= 36; radix++) {
            StringBuilder s = new StringBuilder();
            for (int d = -1; d <= 37; d++) {
                s.append(Character.forDigit(d, radix) == 0 ? "-" : String.valueOf(Character.forDigit(d, radix)));
            }
            out.add("forDigit radix " + radix, s.toString());
        }
        double[] specials = {0.0, -0.0, 1.5, -1.5, 2.5, -2.5, 0.49999999999999994, -0.5, 1e300, -1e-300,
            Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.MIN_VALUE, 4503599627370497.0};
        for (double a : specials) {
            for (double b : specials) {
                out.add(Out.hex(Double.doubleToRawLongBits(a)) + " " + Out.hex(Double.doubleToRawLongBits(b)),
                        Out.hex(Double.doubleToRawLongBits(Math.max(a, b))) + " "
                                + Out.hex(Double.doubleToRawLongBits(Math.min(a, b))));
            }
            out.add("floor/abs " + Out.hex(Double.doubleToRawLongBits(a)),
                    Out.hex(Double.doubleToRawLongBits(Math.floor(a))) + " "
                            + Out.hex(Double.doubleToRawLongBits(Math.abs(a))));
        }
        long[] longs = {0, 1, -1, Integer.MAX_VALUE, Integer.MIN_VALUE, 1L << 32, Long.MAX_VALUE, Long.MIN_VALUE,
            3037000499L, 3037000500L};
        for (long a : longs) {
            out.add("toIntExact " + a, exact(() -> String.valueOf(Math.toIntExact(a))));
            out.add("abs " + a, String.valueOf(Math.abs(a)));
            for (long b : longs) {
                out.add("multiplyExact " + a + " " + b, exact(() -> String.valueOf(Math.multiplyExact(a, b))));
            }
        }
        return out.toString();
    }

    private static String parse(String s, boolean asLong) {
        try {
            return asLong ? Long.toString(Long.parseLong(s)) : Integer.toString(Integer.parseInt(s));
        } catch (RuntimeException e) {
            return Out.err(e);
        }
    }

    private interface Answer {
        String get();
    }

    private static String exact(Answer a) {
        try {
            return a.get();
        } catch (RuntimeException e) {
            return Out.err(e);
        }
    }
}
