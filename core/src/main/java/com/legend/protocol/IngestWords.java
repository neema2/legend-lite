// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

/**
 * The words of an ingest ({@link Protocol.PIngest}) that the grammar, the JSON reader and the printer share: the write
 * modes as written and as the wire spells them, and the shapes of the values written as strings -- a time of day, a
 * time zone, an ISO 8601 duration, an application directory id. Checked by shape only: a named zone's existence is
 * the runner's to check, where it runs (TeaVM's planner carries no zone database).
 */
public final class IngestWords {

    private IngestWords() {
    }

    /** The write modes, written and on the wire: Legend's three (the wire's values, legend-studio's
     *  {@code V1_WriteModeType}), then lite's. */
    private static final String[][] WRITE_MODES = {
            {"AppendOnly", "append_only"},
            {"BatchMilestoned", "batch_milestoned"},
            {"BatchMilestonedBusinessTemporal", "batch_milestoned_business_temporal"},
            {"LatestByKey", "latest_by_key"},
    };

    /** The wire's value of a written write mode, null for a word that is none. */
    public static @com.legend.base.Nullable String writeModeWire(String word) {
        for (String[] m : WRITE_MODES) {
            if (m[0].equals(word)) {
                return m[1];
            }
        }
        return null;
    }

    /** The written word of a wire write mode, null for a value that is none. */
    public static @com.legend.base.Nullable String writeModeWord(String wire) {
        for (String[] m : WRITE_MODES) {
            if (m[1].equals(wire)) {
                return m[0];
            }
        }
        return null;
    }

    /** {@code HH:MM}, a 24-hour time of day. */
    public static boolean isTimeOfDay(String s) {
        if (s.length() != 5 || s.charAt(2) != ':' || !digits(s.substring(0, 2)) || !digits(s.substring(3))) {
            return false;
        }
        return Integer.parseInt(s.substring(0, 2)) < 24 && Integer.parseInt(s.substring(3)) < 60;
    }

    /** {@code UTC}, or an IANA name: {@code Area/Location}, letters, digits, {@code _ + -} in each part. */
    public static boolean isZone(String s) {
        if (s.equals("UTC")) {
            return true;
        }
        // character by character: each part between '/' non-empty, at least two parts
        int parts = 1;
        int partLength = 0;
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (ch == '/') {
                if (partLength == 0) {
                    return false;
                }
                parts++;
                partLength = 0;
            } else if (Character.isLetterOrDigit(ch) || ch == '_' || ch == '+' || ch == '-') {
                partLength++;
            } else {
                return false;
            }
        }
        return parts >= 2 && partLength > 0;
    }

    /** An ISO 8601 duration of days and time: {@code P1D}, {@code PT15M}, {@code PT8H}, {@code PT1H30M},
     *  {@code PT0.5S}; at least one part, none empty. */
    public static boolean isDuration(String s) {
        if (!s.startsWith("P") || s.length() < 3) {
            return false;
        }
        int i = 1;
        boolean any = false;
        int days = s.indexOf('D');
        int t = s.indexOf('T');
        if (days > 0 && (t < 0 || days < t)) {
            if (!digits(s.substring(1, days))) {
                return false;
            }
            any = true;
            i = days + 1;
        }
        if (i == s.length()) {
            return any;
        }
        if (s.charAt(i) != 'T' || i + 1 == s.length()) {
            return false;
        }
        i++;
        String[] units = {"H", "M", "S"};
        int u = 0;
        while (i < s.length()) {
            int j = i;
            while (j < s.length() && (Character.isDigit(s.charAt(j)) || s.charAt(j) == '.')) {
                j++;
            }
            if (j == i || j == s.length()) {
                return false;
            }
            String unit = String.valueOf(s.charAt(j));
            while (u < units.length && !units[u].equals(unit)) {
                u++;
            }
            if (u == units.length) {
                return false;
            }
            String number = s.substring(i, j);
            if (number.indexOf('.') >= 0 && (!unit.equals("S") || number.startsWith(".") || number.endsWith("."))) {
                return false;
            }
            u++;
            any = true;
            i = j + 1;
        }
        return any;
    }

    /** An application directory id: digits, no sign, no leading zero. */
    public static boolean isAppDirId(String s) {
        return digits(s) && (s.length() == 1 || s.charAt(0) != '0') && s.length() <= 18;
    }

    private static boolean digits(String s) {
        if (s.isEmpty()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) < '0' || s.charAt(i) > '9') {
                return false;
            }
        }
        return true;
    }
}
