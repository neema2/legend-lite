package conformance;

/**
 * One family's answers, one {@code input<TAB>answer} line per case, every character outside printable ASCII written
 * {@code \\uXXXX} (and a backslash as two), so a line is a line on both sides and a difference reads in a diff. Built
 * with nothing whose spelling is under test: hex by hand, no {@code String.format}.
 */
final class Out {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private final StringBuilder text = new StringBuilder(1 << 16);

    void add(String input, String answer) {
        escape(input, text);
        text.append('\t');
        escape(answer, text);
        text.append('\n');
    }

    @Override
    public String toString() {
        return text.toString();
    }

    /**
     * A thrown answer: the class always, the message after it (a difference in the message alone is told apart). A
     * message's line breaks are written as "\n": the JDK builds some (PatternSyntaxException's) with the platform's
     * line separator, so on Windows its own answer differs from macOS's (CI, 2026-10-10) -- this compares TeaVM with
     * the JDK, not one platform with another.
     */
    static String err(Throwable t) {
        return "ERR " + t.getClass().getName() + ": " + String.valueOf(t.getMessage()).replace("\r\n", "\n");
    }

    static String hex(long bits) {
        char[] out = new char[16];
        for (int i = 15; i >= 0; i--) {
            out[i] = HEX[(int) (bits & 0xF)];
            bits >>>= 4;
        }
        return new String(out);
    }

    static String hex4(int c) {
        return new String(new char[] {HEX[(c >> 12) & 0xF], HEX[(c >> 8) & 0xF], HEX[(c >> 4) & 0xF], HEX[c & 0xF]});
    }

    static String bytes(byte[] b) {
        StringBuilder out = new StringBuilder(b.length * 2);
        for (byte x : b) {
            out.append(HEX[(x >> 4) & 0xF]).append(HEX[x & 0xF]);
        }
        return out.toString();
    }

    private static void escape(String s, StringBuilder out) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\') {
                out.append("\\\\");
            } else if (c >= 0x20 && c < 0x7F) {
                out.append(c);
            } else {
                out.append("\\u").append(hex4(c));
            }
        }
    }
}
