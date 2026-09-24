// SPDX-License-Identifier: Apache-2.0

package com.legend.rcorpus;

import com.legend.test.PureTestRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** THE PER-ASSERT VERDICT LEDGER (leg 3.3, docs/DATABASE_MODE_HOMEWORK_2026_09_18.md §4x):
 * one row per adjudicated assert of every test a corpus lane ran, in statement
 * order — {@code test, ordinal, assert family, verdict} — written by a lane run
 * with {@code -Dlegend.judge.ledger=<path>} and read back by the differential
 * gate, which joins a HOST-mode ledger against a DATABASE-mode ledger on
 * {@code (test, ordinal)}. The verdict vocabulary: {@code PASS}; {@code FAIL};
 * {@code UNJUDGED} (database mode declined the shape — a FAIL whose test reason
 * names it). A test's asserts after its first failure were never adjudicated
 * (first-failure sequencing) and simply have no row. */
final class JudgeLedger {

    private JudgeLedger() {
    }

    static final String PROPERTY = "legend.judge.ledger";

    record Row(String test, int ordinal, String family, String verdict) {
    }

    /** Append the test's verdict rows when a ledger path is configured. */
    static void record(String test, PureTestRunner.Result r) {
        String path = System.getProperty(PROPERTY, "").trim();
        if (path.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (PureTestRunner.Verdict v : r.verdicts()) {
            // the UNJUDGED state is the verdict's own fact (the database judge's
            // typed decline), never read from the failure text
            String verdict = v.pass() ? "PASS" : v.unjudgedReason() != null ? "UNJUDGED" : "FAIL";
            sb.append(test).append('\t').append(i++).append('\t').append(v.assertName())
                    .append('\t').append(verdict).append('\n');
        }
        try {
            Files.writeString(Path.of(path), sb, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException("judge ledger " + path, e);
        }
    }

    /** The ledger's rows keyed by {@code test\tordinal}, in file order. */
    static Map<String, Row> read(Path path) throws IOException {
        Map<String, Row> rows = new LinkedHashMap<>();
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            String[] f = line.split("\t", -1);
            if (f.length != 4) {
                throw new IllegalStateException("judge ledger row: " + line);
            }
            rows.put(f[0] + "\t" + f[1], new Row(f[0], Integer.parseInt(f[1]), f[2], f[3]));
        }
        return rows;
    }

    /** The differential: the two ledgers joined per assert. A pair the
     * REGISTERS name (the lane's database-mode lost / gained / accepted
     * registers and the host lane's accepted roster — every row with its
     * written reason) is a registered divergence; anything else is a bug in
     * one mode. {@code hostOnly} / {@code databaseOnly}: an assert one judge
     * adjudicated and the other never reached (the other's body raised
     * first) — the same asymmetry as an unjudged shape, pinned the same way. */
    record Differential(int agree, List<Row[]> disagree, List<Row> unjudged,
            List<Row> hostOnly, List<Row> databaseOnly) {

        /** Every test with a disagreement or a one-sided adjudication. */
        java.util.Set<String> tests() {
            java.util.Set<String> out = new java.util.HashSet<>();
            disagree.forEach(pair -> out.add(pair[0].test()));
            hostOnly.forEach(r -> out.add(r.test()));
            databaseOnly.forEach(r -> out.add(r.test()));
            return out;
        }

        List<String> unregistered(java.util.Set<String> registered) {
            List<String> out = new ArrayList<>();
            for (Row[] pair : disagree) {
                if (!registered.contains(pair[0].test())) {
                    out.add(pair[0].test() + " #" + pair[0].ordinal() + " " + pair[0].family()
                            + ": host " + pair[0].verdict() + ", database " + pair[1].verdict());
                }
            }
            for (Row r : hostOnly) {
                if (!registered.contains(r.test())) {
                    out.add(r.test() + " #" + r.ordinal() + " " + r.family()
                            + ": host " + r.verdict() + ", database never reached it");
                }
            }
            for (Row r : databaseOnly) {
                if (!registered.contains(r.test())) {
                    out.add(r.test() + " #" + r.ordinal() + " " + r.family()
                            + ": database " + r.verdict() + ", host never reached it");
                }
            }
            return out;
        }

        Map<String, Integer> unjudgedByFamily() {
            Map<String, Integer> m = new LinkedHashMap<>();
            for (Row r : unjudged) {
                m.merge(r.family(), 1, Integer::sum);
            }
            return m;
        }
    }

    static Differential diff(Map<String, Row> host, Map<String, Row> database) {
        int agree = 0;
        List<Row> hostOnly = new ArrayList<>();
        List<Row> databaseOnly = new ArrayList<>();
        List<Row[]> disagree = new ArrayList<>();
        List<Row> unjudged = new ArrayList<>();
        for (Map.Entry<String, Row> e : host.entrySet()) {
            Row h = e.getValue();
            Row d = database.get(e.getKey());
            if (d == null) {
                hostOnly.add(h);
            } else if (d.verdict().equals("UNJUDGED")) {
                unjudged.add(d);
            } else if (h.verdict().equals(d.verdict())) {
                agree++;
            } else {
                disagree.add(new Row[] {h, d});
            }
        }
        for (Map.Entry<String, Row> e : database.entrySet()) {
            if (!host.containsKey(e.getKey())) {
                databaseOnly.add(e.getValue());
            }
        }
        return new Differential(agree, disagree, unjudged, hostOnly, databaseOnly);
    }
}
