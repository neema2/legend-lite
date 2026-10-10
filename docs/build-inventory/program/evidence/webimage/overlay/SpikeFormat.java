package com.legend.base;

/**
 * SPIKE (2026-10-10): what the product's String.format calls actually need, without java.util.Formatter -- zero-padded
 * integers (%0Nd, the sign counted in the width as Formatter counts it), four-digit lower-case hex (%04x), and a
 * template's %N$s, %s and %% (SystemMetamodel's text block). Scratch only: measures what dropping Formatter saves.
 */
public final class SpikeFormat {

    private SpikeFormat() {
    }

    public static String pad(long value, int width) {
        String digits = Long.toString(Math.abs(value));
        if (value == Long.MIN_VALUE) {
            digits = "9223372036854775808";
        }
        String sign = value < 0 ? "-" : "";
        StringBuilder b = new StringBuilder(sign);
        for (int i = sign.length() + digits.length(); i < width; i++) {
            b.append('0');
        }
        return b.append(digits).toString();
    }

    public static String hex4(int c) {
        String h = Integer.toHexString(c);
        return "0000".substring(Math.min(4, h.length())) + h;
    }

    public static String template(String text, Object... args) {
        StringBuilder b = new StringBuilder(text.length() + 1024);
        int next = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c != '%') {
                b.append(c);
                continue;
            }
            char d = text.charAt(++i);
            if (d == '%') {
                b.append('%');
            } else if (d == 's') {
                b.append(args[next++]);
            } else {
                int dollar = text.indexOf('$', i);
                int n = Integer.parseInt(text.substring(i, dollar));
                if (text.charAt(dollar + 1) != 's') {
                    throw new IllegalArgumentException("unsupported conversion at " + i);
                }
                b.append(args[n - 1]);
                i = dollar + 1;
            }
        }
        return b.toString();
    }
}
