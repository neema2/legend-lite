import java.math.BigDecimal;
import java.math.BigInteger;
import org.teavm.classlib.impl.text.ExactDecimal;

/** Scratch: ExactDecimal held to the JDK on the JVM. Arguments: count seed. */
public class ExactCheck {
    static long state;
    static long next() {
        long z = (state += 0x9E3779B97F4A7C15L);
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    static String text(double v) {
        if (v == 0 || Double.isNaN(v) || Double.isInfinite(v)) return Double.toString(v);
        ExactDecimal.Decimal d = new ExactDecimal.Decimal();
        ExactDecimal.shortest(Math.abs(v), d);
        return (v < 0 ? "-" : "") + d.digits + "E" + d.exponent;
    }
    static String text(float v) {
        if (v == 0 || Float.isNaN(v) || Float.isInfinite(v)) return Float.toString(v);
        ExactDecimal.Decimal d = new ExactDecimal.Decimal();
        ExactDecimal.shortest(Math.abs(v), d);
        return (v < 0 ? "-" : "") + d.digits + "E" + d.exponent;
    }
    /** The JDK's choice as digits and exponent, from its text. */
    static String jdk(String s) {
        if (s.contains("Infinity") || s.contains("NaN")) return s;
        BigDecimal b = new BigDecimal(s);
        if (b.signum() == 0) return s;
        b = b.stripTrailingZeros();
        return (b.signum() < 0 ? "-" : "") + b.unscaledValue().abs() + "E" + (-b.scale());
    }

    static int bad = 0;
    static void same(String what, String mine, String theirs) {
        if (!mine.equals(theirs)) {
            if (bad++ < 25) System.out.println("DIFF " + what + ": mine " + mine + " jdk " + theirs);
        }
    }

    public static void main(String[] args) {
        int count = Integer.parseInt(args[0]);
        state = Long.parseLong(args[1]);
        long t0 = System.nanoTime();
        // toString: random bits, every power of two and its neighbours
        for (int i = 0; i < count; i++) {
            double v = Double.longBitsToDouble(next());
            if (!Double.isNaN(v)) same("double " + Long.toHexString(Double.doubleToRawLongBits(v)), text(v), jdk(Double.toString(v)));
            float f = Float.intBitsToFloat((int) next());
            if (!Float.isNaN(f)) same("float " + Integer.toHexString(Float.floatToRawIntBits(f)), text(f), jdk(Float.toString(f)));
        }
        for (long e = 0; e <= 2047; e++) for (long b : new long[] {(e << 52) - 1, e << 52, (e << 52) + 1}) {
            if (b < 0 || b >= 0x7FF0000000000000L) continue;
            double v = Double.longBitsToDouble(b);
            same("pow2 " + Long.toHexString(b), text(v), jdk(Double.toString(v)));
        }
        for (int k = 0; k < 53; k++) { double v = Double.longBitsToDouble(1L << k); same("sub " + k, text(v), jdk(Double.toString(v))); }
        for (int e = 0; e <= 255; e++) for (int b : new int[] {(e << 23) - 1, e << 23, (e << 23) + 1}) {
            if (b < 0 || b >= 0x7F800000) continue;
            float v = Float.intBitsToFloat(b);
            same("fpow2 " + Integer.toHexString(b), text(v), jdk(Float.toString(v)));
        }
        // short decimals written by people
        for (int i = 0; i < count; i++) {
            String s = (next() >>> 1) % 100000 + "." + (next() >>> 1) % 1000000;
            double v = Double.parseDouble(s);
            same("short " + s, text(v), jdk(Double.toString(v)));
        }
        long t1 = System.nanoTime();
        // reading: random decimals, exact halfway points and a hair either side, powers of ten, forms
        for (int i = 0; i < count; i++) {
            StringBuilder d = new StringBuilder();
            int n = 1 + (int) ((next() >>> 1) % 25);
            for (int j = 0; j < n; j++) d.append((char) ('0' + (next() >>> 1) % 10));
            int exp = (int) ((next() >>> 1) % 700) - 360;
            String s = d + "e" + exp;
            same("parse " + s, Long.toHexString(Double.doubleToRawLongBits(ExactDecimal.parseDouble(s))), Long.toHexString(Double.doubleToRawLongBits(Double.parseDouble(s))));
            same("parsef " + s, Integer.toHexString(Float.floatToRawIntBits(ExactDecimal.parseFloat(s))), Integer.toHexString(Float.floatToRawIntBits(Float.parseFloat(s))));
        }
        for (int i = 0; i < count / 10; i++) {
            long bits = next() & 0x7FEFFFFFFFFFFFFFL;
            double v = Double.longBitsToDouble(bits);
            BigDecimal lo = new BigDecimal(v), hi = new BigDecimal(Math.nextUp(v));
            BigDecimal mid = lo.add(hi).divide(BigDecimal.valueOf(2));
            for (BigDecimal x : new BigDecimal[] {mid, mid.add(mid.ulp()), mid.subtract(mid.ulp())}) {
                String s = x.toString();
                same("mid " + s, Long.toHexString(Double.doubleToRawLongBits(ExactDecimal.parseDouble(s))), Long.toHexString(Double.doubleToRawLongBits(Double.parseDouble(s))));
            }
            float f = Float.intBitsToFloat((int) next() & 0x7F7FFFFF);
            BigDecimal flo = new BigDecimal(f), fhi = new BigDecimal(Math.nextUp(f));
            BigDecimal fmid = flo.add(fhi).divide(BigDecimal.valueOf(2));
            for (BigDecimal x : new BigDecimal[] {fmid, fmid.add(fmid.ulp()), fmid.subtract(fmid.ulp())}) {
                String s = x.toString();
                same("fmid " + s, Integer.toHexString(Float.floatToRawIntBits(ExactDecimal.parseFloat(s))), Integer.toHexString(Float.floatToRawIntBits(Float.parseFloat(s))));
            }
        }
        String[] forms = {"0", "-0", "+0", "1", "-1", "+1.5", ".5", "5.", "-.5", "1e5", "1E5", "1e+5", "1E-5", "1.7976931348623157e308",
            "1.7976931348623158e308", "1.7976931348623159e308", "1e309", "4.9e-324", "2.4703282292062327e-324", "2.4703282292062328e-324",
            "1e-325", "2.2250738585072011e-308", "2.2250738585072012e-308", "9007199254740993", "  2.5", "2.5  ", "\t2.5\n", "2.5d", "2.5f",
            "Infinity", "-Infinity", "NaN", "-NaN", "infinity", "0x1.8p1", "0X10P0", "0x.8p-1", "0x1p-1074", "0x1p-1075", "0x1.0000000000001p-1075",
            "0x1.fffffffffffff8p1023", "0x1.fffffffffffff7ffp1023", "1_000", "", " ", "1e", "e5", ".", "-", "--1", "1.2.3", "1e5.5", "1,5", "1.f",
            "١٢٣", "1e2147483648", "1e-9999999999999", "0.000000000000000000000000000000000000000000000001e40", "1.401298464324817e-45",
            "7.006492321624085e-46", "7.006492321624086e-46", "3.4028235e38", "3.4028236e38", "3.40282357e38"};
        for (String s : forms) {
            same("form " + s, val(() -> Long.toHexString(Double.doubleToRawLongBits(ExactDecimal.parseDouble(s)))), val(() -> Long.toHexString(Double.doubleToRawLongBits(Double.parseDouble(s)))));
            same("formf " + s, val(() -> Integer.toHexString(Float.floatToRawIntBits(ExactDecimal.parseFloat(s)))), val(() -> Integer.toHexString(Float.floatToRawIntBits(Float.parseFloat(s)))));
        }
        // BigDecimal.doubleValue
        for (int i = 0; i < count / 10; i++) {
            BigInteger u = new BigInteger(1 + (int) ((next() >>> 1) % 120), new java.util.Random(next()));
            int scale = (int) ((next() >>> 1) % 800) - 400;
            BigDecimal b = new BigDecimal(u, scale);
            same("bd " + b, Long.toHexString(Double.doubleToRawLongBits(ExactDecimal.toDouble(false, u.toString(), -scale))), Long.toHexString(Double.doubleToRawLongBits(b.doubleValue())));
            same("bdf " + b, Integer.toHexString(Float.floatToRawIntBits(ExactDecimal.toFloat(false, u.toString(), -scale))), Integer.toHexString(Float.floatToRawIntBits(b.floatValue())));
        }
        long t2 = System.nanoTime();
        System.out.println("differences: " + bad + "  (writing " + (t1 - t0) / 1_000_000 + " ms, reading " + (t2 - t1) / 1_000_000 + " ms)");
    }

    interface S { String get(); }
    static String val(S s) { try { return s.get(); } catch (NumberFormatException e) { return "ERR " + e.getMessage(); } }
}
