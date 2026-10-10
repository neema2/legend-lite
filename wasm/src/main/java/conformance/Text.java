package conformance;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** The text families: Character, String, String.format, the regular expressions lite writes, and the codecs. */
final class Text {

    private Text() {
    }

    /** Strings chosen for where a class library can go wrong: Unicode whitespace, case pairs that are not one to one,
     *  surrogates, digits of other scripts, and the shapes lite's own text takes. */
    static final String[] CORPUS = {"", " ", "  ", "\t", "\n", "\r\n", "\u000b", "\f", "\u001c", "\u001f", "\u0085",
        " ", " ", " ", " ", "​", " ", " ", " ", " ", "　", "﻿",
        " a ", " a ", " a　", "abc", "ABC", "aBc", "İstanbul", "ıi", "ß", "ﬁ",
        "ΣΑΣ", "σς", "Ǆǅǆ", "ǰ", "ŉ", "ΐ", "ẞ", "K",
        "Å", "ſ", "µ", "ÿ", "Ÿ", "café", "é", "😀", "\ud800", "a\ud800b",
        "\udc00x", "x\u0000y", "١٢٣", "５", "  padded  ", "meta::pure::functions::string::toLower",
        "my::Class.prop", "2026-01-15T12:00:00.123+0000", "1,2,,3,,", "a|b||c", "SELECT * FROM t WHERE x = 'y'",
        "tempTableForIn_foo", "tempTableForIn_12", "a\r\nb\nc\rd", "ÀàĀā", "և", "ᾈ",
        "Hello, World!", "x\ty\tz", "line1 line2"};

    // ---- Character, every char

    static String character() {
        Out out = new Out();
        for (int c = 0; c <= 0xFFFF; c++) {
            char ch = (char) c;
            String flags = (Character.isWhitespace(ch) ? "W" : "-") + (Character.isDigit(ch) ? "D" : "-")
                    + (Character.isLetter(ch) ? "L" : "-") + (Character.isLetterOrDigit(ch) ? "A" : "-")
                    + (Character.isUpperCase(ch) ? "U" : "-");
            out.add(Out.hex4(c), flags + " " + Out.hex4(Character.toLowerCase(ch)) + " "
                    + Out.hex4(Character.toUpperCase(ch)) + " " + Character.digit(ch, 10) + " "
                    + Character.digit(ch, 16) + " " + Character.digit(ch, 36));
        }
        for (int cp = 0x10000; cp <= 0x10FFFF; cp += 97) {
            out.add("cp " + Integer.toHexString(cp), Character.charCount(cp) + " "
                    + new String(Character.toChars(cp)).length() + " " + (Character.isLetter(cp) ? "L" : "-")
                    + (Character.isDigit(cp) ? "D" : "-") + " " + Integer.toHexString(Character.toLowerCase(cp)));
        }
        return out.toString();
    }

    // ---- String

    static String string() {
        Out out = new Out();
        for (String s : CORPUS) {
            out.add(s, s.isBlank() + "|" + s.isEmpty() + "|" + s.strip() + "|" + s.trim() + "|"
                    + s.toLowerCase(Locale.ROOT) + "|" + s.toUpperCase(Locale.ROOT) + "|" + s.length() + "|"
                    + s.codePointCount(0, s.length()) + "|" + codePoints(s) + "|" + s.chars().sum() + "|"
                    + s.hashCode() + "|" + Out.bytes(s.getBytes(StandardCharsets.UTF_8)) + "|"
                    + new String(s.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8) + "|"
                    + s.repeat(2).length() + "|" + s.indexOf('a') + "|" + s.lastIndexOf("a") + "|" + s.contains("::")
                    + "|" + String.join("/", s.split(",")) + "|" + s.split(",").length + "|"
                    + String.join("/", s.split("\\s+")) + "|" + s.replace("a", "<a>") + "|"
                    + s.equalsIgnoreCase(s.toUpperCase(Locale.ROOT)) + "|"
                    + s.equalsIgnoreCase(s.toLowerCase(Locale.ROOT)) + "|" + Integer.signum(s.compareTo("m")) + "|"
                    + (s.isEmpty() ? "" : s.substring(1)) + "|" + s.matches(".*\\p{L}.*") + "|"
                    + (s.isEmpty() ? -1 : s.codePointAt(0)));
        }
        // a malformed UTF-8 sequence decoded: what a decoder puts in its place
        int[][] malformed = {{0xC0, 0x80}, {0xE2, 0x82}, {0xF0, 0x9F, 0x98}, {0xFF}, {0xED, 0xA0, 0x80},
            {0xF4, 0x90, 0x80, 0x80}, {0x61, 0x80, 0x62}, {0xE2, 0x82, 0xAC}, {0xC3}};
        for (int[] m : malformed) {
            byte[] b = new byte[m.length];
            for (int i = 0; i < m.length; i++) {
                b[i] = (byte) m[i];
            }
            out.add("decode " + Out.bytes(b), new String(b, StandardCharsets.UTF_8));
        }
        return out.toString();
    }

    private static String codePoints(String s) {
        StringBuilder out = new StringBuilder();
        s.codePoints().forEach(cp -> out.append(Integer.toHexString(cp)).append(','));
        return out.toString();
    }

    // ---- String.format, as lite calls it (always Locale.ROOT; Error Prone refuses it without one)

    static String format() {
        Out out = new Out();
        long[] values = {0, 1, 7, 9, 10, 59, 99, 100, 999, 1000, 123456789, 1234567890, -1, -7, -10, -100,
            Integer.MAX_VALUE, Integer.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE};
        String[] patterns = {"%d", "%02d", "%03d", "%09d", "%010d", "%d-%02d"};
        for (long v : values) {
            for (String p : patterns) {
                out.add(p + " " + v, String.format(Locale.ROOT, p, v, v));
            }
            out.add("%04x " + v, String.format(Locale.ROOT, "%04x", (int) v));
        }
        for (int c : new int[] {0, 1, 0x1f, 0x7f, 0xff, 0x2028, 0xfeff, 0xffff}) {
            out.add("\\u%04x " + c, String.format(Locale.ROOT, "\\u%04x", c));
        }
        out.add("%s", String.format(Locale.ROOT, "%s|%s|%s|%s", "a", 1, 1.5, true));
        out.add("%1$s", String.format(Locale.ROOT, "%1$s-%2$s-%1$s %%", "x", "y"));
        out.add("formatted", "%s and %s".formatted("a", "b"));
        return out.toString();
    }

    // ---- the regular expressions lite writes (every literal under core/, json/ and base/ main, 2026-10-10)

    static final String[] PATTERNS = {"-?\\d{4,}-\\d{2}-\\d{2}", "-?\\d{4,}-\\d{2}-\\d{2}[T ]\\d.*", "-{3,}", "-+",
        ", ", ",", "::", ".*([+-]\\d{4}|[+-]\\d{2}:\\d{2}|Z)$", "'", "(?i)\\bBIT\\b", "(?i)\\bCLOB\\b",
        "(?i)\\bFLOAT\\b", "(?i)\\bH2VERSION\\s*\\(\\s*\\)", "(\\+0000|Z)$", "[^0-9].*$", "[^A-Za-z0-9_]",
        "[+-]?\\d*\\.\\d+([eE][+-]?\\d+)?", "[+-]?\\d+", "[+-]?\\d+(\\.\\d+)?[dD]", "[+-]?\\d+[eE][+-]?\\d+", "[0-9]+",
        "[0-9]+\\.\\.([0-9]+|\\*)", "[A-Za-z_][A-Za-z0-9_]*", "[A-Za-z_][A-Za-z0-9_$]*", "[dD]$", "/", "\\.",
        "\\(\\?([ims]+)\\)", "\\), \\(", "\\[[^]]*\\]$", "\\|", "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}",
        "\\d{4}-\\d{2}-\\d{2} \\d{2}", "\\d{4}-\\d{2}-\\d{2}",
        "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?([+-]\\d{4}|Z)", "\\d+", "\\n", "\\s*\\R\\s*", "\\s+", "\n",
        "\r?\n", "\t", "\u0000", "&", "^_+", "^[a-z][a-z0-9_]*+(-[a-z][a-z0-9_]*+)*+$",
        "^[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)*$", "^[a-zA-Z0-9_]+$", "^\\n", "^DIFFER", "$1", "0+",
        "0+$", "BOOLEAN", "count(*) AS \"COUNT(*)\"", "CURRENT_TIMESTAMP", "DOUBLE", "tempTableForIn_([A-Za-z_][A-Za-z0-9_]*)",
        "tempTableForIn_(\\d+)", "TEXT"};

    /** Inputs for the patterns: the corpus, and text in the shapes the patterns are for. */
    static final String[] REGEX_INPUTS = {"2026-01-15", "-12026-01-15", "2026-01-15T12:00:00", "2026-01-15 12:00",
        "2026-01-15 12", "2026-01-15T12:00:00.123+0000", "2026-01-15T12:00:00Z", "2026-01-15T12:00:00+05:30",
        "12:00:00", "--- x ---", "a-b--c", "a, b, c", "my::pkg::Class", "it's", "CAST(x AS bit)", "a BIT b",
        "clob CLOB Clob", "FLOAT(53)", "H2VERSION ( )", "h2version()", "1.5", "-1.5e10", "+.5E-3", "42", "-0",
        "1.5d", "2D", "1e5", "1..*", "0..1", "10..20", "abc_1", "_x$y", "1.0d", "a/b/c", "a.b.c", "(?i)abc",
        "(?ims)x", "(a), (b)", "Integer[1]", "String[*]", "a|b||c", "line1\nline2", "a\r\nb", " \n   \u0085 ",
        "  x  y\t\tz  ", "a\u0000b", "a&b", "__init", "kebab-case-id", "Kebab-Case", "a.b.$c", "ab_12", "\nx",
        "DIFFERS here", "x$1y", "000", "1000", "BOOLEAN", "count(*) AS \"COUNT(*)\"", "CURRENT_TIMESTAMP", "DOUBLE",
        "tempTableForIn_foo", "tempTableForIn_12", "TEXT", "١٢٣", "１", "été", "K"};

    static String regex() {
        Out out = new Out();
        for (String p : PATTERNS) {
            Pattern pattern;
            String refused;
            try {
                pattern = Pattern.compile(p);
                refused = "";
            } catch (RuntimeException e) {
                pattern = Pattern.compile("");
                refused = Out.err(e);
            }
            out.add(p, refused);
            for (String s : REGEX_INPUTS) {
                // one line per case whatever happens, so a pattern one side refuses moves no other line
                out.add(p + " ~ " + s, refused.isEmpty() ? matchAll(pattern, s) : refused);
            }
        }
        out.add("quote", Pattern.quote("a.b*c\\E") + " " + Matcher.quoteReplacement("$1\\x"));
        return out.toString();
    }

    private static String matchAll(Pattern p, String s) {
        try {
            Matcher m = p.matcher(s);
            StringBuilder out = new StringBuilder();
            out.append(m.matches()).append(' ').append(p.matcher(s).lookingAt()).append(' ');
            m.reset();
            while (m.find()) {
                out.append('[').append(m.start()).append(',').append(m.end());
                for (int g = 0; g <= m.groupCount(); g++) {
                    out.append(',').append(m.group(g));
                }
                out.append(']');
            }
            out.append(' ').append(p.matcher(s).replaceAll("<$0>")).append(' ')
                    .append(String.join("/", p.split(s))).append(' ').append(p.split(s, -1).length);
            Matcher a = p.matcher(s);
            StringBuffer appended = new StringBuffer();
            while (a.find()) {
                a.appendReplacement(appended, "{}");
            }
            a.appendTail(appended);
            return out.append(' ').append(appended).toString();
        } catch (RuntimeException e) {
            return Out.err(e);
        }
    }

    // ---- codecs

    static String codecs() {
        Out out = new Out();
        Rng r = new Rng(10);
        for (int i = 0; i < 300; i++) {
            byte[] b = new byte[r.below(20)];
            for (int j = 0; j < b.length; j++) {
                b[j] = (byte) r.next();
            }
            String enc = Base64.getEncoder().encodeToString(b);
            out.add("base64 " + Out.bytes(b), enc + " " + Base64.getEncoder().withoutPadding().encodeToString(b) + " "
                    + Out.bytes(Base64.getDecoder().decode(enc)));
        }
        for (String s : new String[] {"", "QQ", "QQ=", "QQ==", "QUI=", "Q", "Q===", "QQ==QQ==", "a b", "-_", "+/",
            "QUJD\n", "Zm9v"}) {
            try {
                out.add("decode " + s, Out.bytes(Base64.getDecoder().decode(s)));
            } catch (RuntimeException e) {
                out.add("decode " + s, Out.err(e));
            }
        }
        for (String s : new String[] {"a+b", "%20", "%2", "%zz", "%E2%82%AC", "%C0%80", "100%", "a%2Bb", "%e2%82%ac",
            "%F0%9F%98%80", "%ED%A0%80", "+%2B+"}) {
            try {
                out.add("urldecode " + s, URLDecoder.decode(s, StandardCharsets.UTF_8));
            } catch (RuntimeException e) {
                out.add("urldecode " + s, Out.err(e));
            }
        }
        for (String s : new String[] {"http://host:9200", "https://user@host/path?q=1#f", "host:9200",
            "http://[::1]:80/x", "not a uri", "http://host/%zz", "jdbc:postgresql://localhost:5432/db",
            "file:///tmp/x", "HTTP://Host:80", "http://hé/", "", "#frag", "http://host:-1", "http://host:99999"}) {
            try {
                URI u = new URI(s);
                out.add("uri " + s, u + " " + URI.create(s));
            } catch (Exception e) {
                out.add("uri " + s, Out.err(e));
            }
        }
        List<Object> list = new ArrayList<>(List.of(1, "a", 1.5, true));
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("k", list);
        map.put("n", 2L);
        out.add("toString", list + " " + map + " " + java.util.Arrays.toString(new int[] {1, 2}) + " "
                + java.util.Optional.of("x") + " " + java.util.Optional.empty());
        return out.toString();
    }
}
