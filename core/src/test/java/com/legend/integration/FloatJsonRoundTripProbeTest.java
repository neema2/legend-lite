// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PROBE, NOT A GUARD (the compatibility program's slice 0.4 (i), homework H9, 2026-11-19; on the scratch branch
 * claude/compat-b1-homework only): whether each database's JSON writer, as lite's dialect drives it (a graph fetch's
 * serialize, its checked form), keeps a Float's round-trip digits. Each value is stored as a DOUBLE, fetched, and its
 * JSON number's text read back with {@link Double#parseDouble}: the same bits, or not. It writes one TSV line per
 * (database, query form, value) to {@code $TEST_UNDECLARED_OUTPUTS_DIR/float_json.tsv}, and asserts nothing.
 */
class FloatJsonRoundTripProbeTest {

    /** The hard doubles: a computed fraction, thirds and roots, the extremes, whole values past 2^53 and past 10^15,
     *  a negative zero, small and large exponents. */
    private static final double[] VALUES = {
            0.1 + 0.2, 1.0 / 3.0, Math.sqrt(2.0), Math.PI, 1e300, 4.9e-324, Double.MAX_VALUE, Double.MIN_NORMAL,
            123456789012345678.0, 9007199254740994.0, 1e22, 1e23, 12345678901234567.0, 1234567890123456.0,
            -0.0, 5e-7, 1.5, 100.0, 0.1, 2.5e-5, 1.0E21, 123456.789e3, 0.000001234, -2.0 / 3.0};

    private static final String MODEL = """
            ###Pure
            Class p::F { id: Integer[1]; v: Float[1]; }
            ###Relational
            Database p::DB ( Table T ( ID INTEGER PRIMARY KEY, V DOUBLE ) )
            ###Mapping
            Mapping p::M ( p::F: Relational { ~mainTable [p::DB]T id: [p::DB]T.ID, v: [p::DB]T.V } )
            ###Connection
            RelationalDatabaseConnection p::Conn { %s }
            ###Runtime
            Runtime p::RT { mappings: [ p::M ]; connections: [ p::DB: [ c1: p::Conn ] ]; }
            """;

    private static final String[][] QUERIES = {
            {"serialize", "|p::F.all()->sortBy(f|$f.id)->graphFetch(#{p::F{id,v}}#)->serialize(#{p::F{id,v}}#)"},
            {"checked", "|p::F.all()->sortBy(f|$f.id)->graphFetchChecked(#{p::F{id,v}}#)->serialize(#{p::F{id,v}}#)"},
    };

    @Test
    void floatsThroughEachDatabasesJsonWriter() throws Exception {
        List<String> out = new ArrayList<>();
        out.add("database\tform\tid\tvalue (Java shortest)\tJSON text\tround-trips\tshortest");
        for (TestBackend b : TestBackends.inProcess()) {
            String model = MODEL.formatted(b.connectionBody());
            for (String[] q : QUERIES) {
                try (Connection c = b.open()) {
                    try (Statement s = c.createStatement()) {
                        s.execute("CREATE TABLE T (ID INTEGER PRIMARY KEY, V DOUBLE PRECISION)");
                        for (int i = 0; i < VALUES.length; i++) {
                            s.execute("INSERT INTO T VALUES (" + (i + 1) + ", " + literal(VALUES[i]) + ")");
                        }
                    }
                    try (Statement s = c.createStatement();
                            java.sql.ResultSet rs = s.executeQuery("SELECT V FROM T WHERE ID = 15")) {
                        rs.next();
                        double stored = rs.getDouble(1);
                        out.add(b.name() + "\t" + q[0] + "\tstored-15\t-0.0\t" + stored + "\t"
                                + (Double.doubleToLongBits(stored) == Double.doubleToLongBits(-0.0)) + "\t-");
                    }
                    String json;
                    try {
                        json = Lean.run(model, q[1], "p::RT", c).json();
                    } catch (RuntimeException | AssertionError refused) {
                        out.add(b.name() + "\t" + q[0] + "\t-\t-\tREFUSED: "
                                + String.valueOf(refused.getMessage()).replace('\n', ' ') + "\t-\t-");
                        continue;
                    }
                    Matcher m = Pattern.compile("\"id\"\\s*:\\s*(\\d+)\\s*,\\s*\"v\"\\s*:\\s*([^,}\\]]+)").matcher(json);
                    int seen = 0;
                    while (m.find()) {
                        seen++;
                        int id = Integer.parseInt(m.group(1));
                        double want = VALUES[id - 1];
                        String text = m.group(2).trim();
                        boolean trips;
                        try {
                            trips = Double.doubleToLongBits(Double.parseDouble(text))
                                    == Double.doubleToLongBits(want);
                        } catch (NumberFormatException e) {
                            trips = false;
                        }
                        boolean shortest = trips && shortestDigits(text).equals(shortestDigits(Double.toString(want)));
                        out.add(b.name() + "\t" + q[0] + "\t" + id + "\t" + Double.toString(want) + "\t" + text + "\t"
                                + trips + "\t" + shortest);
                    }
                    if (seen != VALUES.length) {
                        out.add(b.name() + "\t" + q[0] + "\t-\t-\tUNPARSED (" + seen + " of " + VALUES.length + "): "
                                + json.replace('\n', ' ') + "\t-\t-");
                    }
                }
            }
        }
        String dir = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        Path file = Path.of(dir == null ? System.getProperty("java.io.tmpdir") : dir, "float_json.tsv");
        Files.write(file, out);
        out.forEach(System.out::println);
    }

    /** A double as a SQL literal every in-process database reads exactly: its shortest round-trip digits. */
    private static String literal(double d) {
        if (d == 0.0 && 1 / d < 0) {
            return "CAST('-0.0' AS DOUBLE PRECISION)";   // a literal -0.0 is the negation of a DECIMAL 0: 0
        }
        return Double.toString(d);
    }

    /** The significant digits and the exponent of a number's text, so 1.0E21 and 1000000000000000000000.0 compare. */
    private static String shortestDigits(String text) {
        java.math.BigDecimal v = new java.math.BigDecimal(text.replace("E+", "E"));
        v = v.stripTrailingZeros();
        return v.unscaledValue().abs().toString() + "e" + (-v.scale()) + (v.signum() < 0 ? "-" : "");
    }
}
