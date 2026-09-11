// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ERROR-SHAPE GUARDRAILS — the Phase-2b lock on the try/catch/finally
 * review, upgraded per NULL_GATE_VERIFICATION G4: broad catches are
 * pinned at EXACT per-file counts (shrink-only, the
 * ObservabilityGuardrailTest shape), the broad-type pattern matches
 * multi-catch in any position, and the two §6.5 rules that were missing
 * (catch-that-returns-a-value, {@code endsWith} on FQN strings) are
 * pinned. Known residual OUTSIDE these rules' static reach:
 * the old runner's {@code unknownTypePull} (deleted) regexed an exception message
 * through a local — retired by the deferred unknown-element
 * rebucketing (typed exception from the resolver), tracked there.
 */
class ErrorShapeGuardrailTest {

    /**
     * Broad-catch census at review time — EXACT counts per file, each a
     * reviewed, documented boundary (module drop-and-wall, or-null
     * probes, harness advisory paths). Shrink-only: a new broad catch
     * anywhere, including in these files, fails until reviewed.
     */
    private static final Map<String, Integer> BROAD_CATCH_COUNTS = Map.ofEntries(
            // batch 7a (2026-09-11): the product's test runner — resolve,
            // type, setup and body failures each become the test's FAIL
            // result with the platform's whole message; the set of what
            // the platform may raise for a test body is the platform's
            // business, not the runner's (a narrowed list would silently
            // abort the run on the next kind); the harness carried these
            // same four before the extraction
            Map.entry("PureTestRunner.java", 4),
            // F3.1b (2026-08-16): isValidJson delegates the VARIANT gate
            // to the platform reader; ANY parse failure means not-JSON —
            // a designed total catch (the reader throws ISE/SIOOBE/NFE
            // on garbage and the set is the reader's business, not the
            // gate's)
            Map.entry("TdsChecker.java", 1),
            Map.entry("ClassSources.java", 1),
            Map.entry("Compiler.java", 2),
            Map.entry("FunctionCompiler.java", 1),
            // SQLTEXT slice 3a (reviewed): the OUR-ROWS leg's designed
            // total catch — ANY failure executing the producer's query
            // (resolution, lowering, execution) becomes a COUNTED
            // text-verdict decline (charter §3.7: counted, visible,
            // never silent) and TEXT stays the contract; the verdict
            // is never swallowed, it degrades to the §4 policy
            Map.entry("SqlTextVerdicts.java", 1),
            // D5 (reviewed): the generic checked-exception carrier —
            // getOrOpen tunnels the caller's E through compute's
            // unchecked boundary; catch(RuntimeException) rethrows
            // as-is, catch(Exception) wraps in the carrier, unwrapped
            // and rethrown as E at the method boundary. Both catches
            // ARE the tunnel, not swallows.
            Map.entry("HandleStore.java", 2),
            // 4 = the derived/implicit-child PROBE-AND-FALLBACK set
            // (reviewed): navHeadRelation's assoc probe, hopJoin's
            // assoc-vs-slot probe, inlineDerivedCalls' compilable-callee
            // probe, implicitLeaves' child-source probe — each falls back
            // to the next resolution route, never swallows a verdict
            Map.entry("GraphEmission.java", 4),
            // 5 = the HTTP request boundary (reviewed): each handler's
            // catch converts ANY failure into a JSON error response and
            // keeps the server alive — LSP handler, execute, executeSql,
            // diagram (whose response-write fallback is the 5th)
            Map.entry("LegendHttpServer.java", 5),
            // 2 = the LSP protocol boundary (reviewed): dispatch converts
            // failures to JSON-RPC error responses; rebuild converts a
            // compile crash into published diagnostics
            Map.entry("PureLspServer.java", 2),
            Map.entry("ScanRelations.java", 1),
            // the seedability trial-lowering probe — broad by design,
            // reviewed 2026-08-21 (see the catch-site comment)
            Map.entry("SeedableLets.java", 1),
            // V2/V6 (OPEN_REGISTER, adjudicated 2026-08-22): the byte
            // verdict's DECLINE TUNNELS — prepCanon (lowering refusal)
            // and runCanon (render/execution refusal) each fall back to
            // the host lattice with the decline COUNTED
            // (CanonicalDivergence.sqlDeclined), so neither broad catch
            // can become a silent rescue.
            // 2 -> 3 (2026-08-23 F10 v1): the canon-exec tunnel's
            // MIDDLE RUNG (drop the literal candidate, keep the bare
            // byte channel) catches the same designed-sentinel class as
            // its two siblings — a caught failure becomes a counted
            // decline, never a rescue (witness testRepeatStringNoString:
            // the BLOB wire under a STRING stamp).
            Map.entry("StatementExecutor.java", 3),
            // contract program (2026-08-23): the wire census's
            // INSTRUMENT-ISOLATION catch — measurement must never throw
            // into execution (the Dual.alias lesson); an unreadable
            // ResultSetMetaData is a counted unknown, never a failure
            Map.entry("SqlTypeCensus.java", 1),
            Map.entry("StaticFold.java", 1),
            // 3 -> 4 (2026-08-28 V7 batch 1): the dual channel's decline
            // tunnel — a production-route probe failure becomes a COUNTED
            // per-form decline (CanonicalDivergence.v7Declined), never a
            // swallow and never a verdict (the host verdict of record is
            // computed before the probe runs); the V2/V6 tunnel idiom
            // 4 -> 5 (slice 3 equality half, reviewed): evalSideText's
            // counted-decline boundary — the F2.3 catch inherited from
            // the deleted ExecCallFinder.sideSqlText, same discipline
            // (every failure lands in the H2Verify decline census)
            Map.entry("QuotedSpecParser.java", 1),
            Map.entry("Typer.java", 1),
            Map.entry("ValidateDesugar.java", 2));

    /** Broad type ANYWHERE in the catch parameter — multi-catch included
     * (the first version anchored it first and missed a
     * {@code catch (SQLException | RuntimeException)}). */
    private static final Pattern BROAD_CATCH = Pattern.compile(
            "catch \\(([^)]*)\\)");

    private static final Pattern BROAD_TYPE = Pattern.compile(
            "\\b(?:RuntimeException|Exception|Throwable)\\b");

    /** Designed catch-return sentinels at review time: harness
     * Unsupported buckets, UnfoldableRef isolation, overflow to
     * BigInteger, join-side search. Shrink-only. */
    // re-pinned 2026-08-16 F1.2: harness left src/main (was 20).
    // 13→15 (2026-08-22 V11): the single-query canon's decline tunnel
    // adds two catch-returns (wrapped→bare, bare→fold) — each IS the
    // designed sentinel this guardrail asks for: caught failure →
    // counted rider decline + derived fallback value, never a silent
    // rescue. (prepCanon/runCanon's dead catches returned null and were
    // never in this count.)
    // 15→17 (2026-09-11, batch 7a): the product's test runner turns a
    // platform failure at resolve/typing time into the test's own FAIL
    // result carrying the platform's whole message — the designed sentinel
    // of a test runner (a failing test must never abort the run); the
    // harness had the same two catches before it moved into the product.
    private static final int CATCH_RETURNS_VALUE = 17;

    /** {@code endsWith("::…")} identification sites — the suffix-match
     * idiom exact-FQN doctrine retires; may only shrink. */
    private static final int ENDS_WITH_FQN = 18;   // re-pinned 2026-08-16 F1.2: harness left src/main (was 23)

    // F1.7 (Charter C2.4): a `default -> "<literal>"` in a type/name
    // mapping switch silently LOSES the unmatched kind — the exact
    // defect audit 15 removed from Executor.pureOfSqlType and PCT
    // reintroduced. Shrink-only; a new site must throw instead.
    private static final int DEFAULT_LITERAL_FALLBACKS = 5;

    @Test
    void noReturnOrThrowInsideFinally() throws IOException {
        List<String> bad = new ArrayList<>();
        for (Path p : mainSources()) {
            String src = Files.readString(p);
            Matcher m = Pattern.compile("\\} finally \\{").matcher(src);
            while (m.find()) {
                String body = blockAfter(src, m.end());
                if (Pattern.compile("\\breturn\\b|\\bthrow\\b")
                        .matcher(body).find()) {
                    bad.add(p.getFileName() + ":" + lineOf(src, m.start()));
                }
            }
        }
        assertTrue(bad.isEmpty(),
                "finally block contains return/throw (swallows in-flight"
                + " exceptions) — restructure: " + bad);
    }

    @Test
    void broadCatchCountsPinnedPerFile() throws IOException {
        Map<String, Integer> found = new HashMap<>();
        Map<String, List<Integer>> where = new HashMap<>();
        for (Path p : mainSources()) {
            String src = Files.readString(p);
            Matcher m = BROAD_CATCH.matcher(src);
            while (m.find()) {
                if (BROAD_TYPE.matcher(m.group(1)).find()) {
                    String f = p.getFileName().toString();
                    found.merge(f, 1, Integer::sum);
                    where.computeIfAbsent(f, k -> new ArrayList<>())
                            .add(lineOf(src, m.start()));
                }
            }
        }
        List<String> bad = new ArrayList<>();
        found.forEach((f, n) -> {
            int allowed = BROAD_CATCH_COUNTS.getOrDefault(f, 0);
            if (n > allowed) {
                bad.add(f + " has " + n + " broad catches (pinned at "
                        + allowed + ") at lines " + where.get(f));
            }
        });
        assertTrue(bad.isEmpty(),
                "broad catches grew — catch the specific exception, or"
                + " review + document the site and re-pin: " + bad);
    }

    /** A catch that RETURNS a value silently converts a failure into an
     * answer — the population was reviewed (all designed sentinels) and
     * is pinned; growth means a new site needs the same review. */
    @Test
    void catchThatReturnsValueOnlyShrinks() throws IOException {
        int n = 0;
        List<String> where = new ArrayList<>();
        Set<String> sentinels = Set.of("null", "Optional.empty()",
                "false", "true");
        for (Path p : mainSources()) {
            String src = Files.readString(p);
            Matcher m = Pattern.compile("catch \\([^)]+\\) \\{").matcher(src);
            while (m.find()) {
                String body = blockAfter(src, m.end());
                Matcher r = Pattern.compile("\\breturn ([^;]{1,80});")
                        .matcher(body);
                while (r.find()) {
                    if (!sentinels.contains(r.group(1).strip())) {
                        n++;
                        where.add(p.getFileName() + ":"
                                + lineOf(src, m.start()));
                        break;
                    }
                }
            }
        }
        assertTrue(n <= CATCH_RETURNS_VALUE,
                "catch-that-returns-a-value grew to " + n + " (pinned at "
                + CATCH_RETURNS_VALUE + ") — a caught failure must become"
                + " a designed sentinel or rethrow: " + where);
    }

    /** Identify elements by EXACT FQN, never suffix match. */
    /** F1.11: the reflection ENTRY SPELLINGS the ArchUnit package rule
     *  cannot see (calls on java.lang.Class/ClassLoader carry no
     *  java.lang.reflect dependency until the result is USED). Zero in
     *  production; stays zero. */
    @Test
    void reflectionSpellingsStayAtZero() throws IOException {
        List<String> bad = new ArrayList<>();
        Pattern p1 = Pattern.compile("Class\\.forName\\(|\\.setAccessible\\("
                + "|getDeclaredMethod\\(|getDeclaredField\\("
                + "|getDeclaredConstructor\\(|loadClass\\("
                // PX.1: subprocess escape — the remaining way to compute
                // outside every dependency rule (zero uses; stays zero)
                + "|new ProcessBuilder|Runtime\\.getRuntime\\(\\)\\.exec");
        for (Path p : mainSources()) {
            String src = Files.readString(p);
            Matcher m = p1.matcher(src);
            while (m.find()) {
                bad.add(p.getFileName() + ":" + lineOf(src, m.start()));
            }
        }
        assertTrue(bad.isEmpty(),
                "reflection entry spellings in production: " + bad
                + " — reflection bypasses every dependency rule and is"
                + " banned (F1.11)");
    }

    /** Tier-2 audit adjudication (2026-08-18, ADVERSARIAL_TENET_AUDIT
     * probe 9): the rule's SCOPE is defaults that fabricate a DATA
     * VALUE — string literals and numeric literals standing in for a
     * cell/type/count. Measured and ruled OUT of scope: {@code default
     * -> false/true} (22 sites — predicate methods, where the default
     * IS the total answer for unmatched kinds) and {@code default ->
     * null} (59 sites — the not-found idiom the null-policy decision
     * procedure governs; e.g. GridReads' "not a shape this compiler
     * owns" contract). Numeric fabricators measured ZERO and pin
     * there. */
    @Test
    void defaultLiteralFallbacksOnlyShrink() throws IOException {
        int n = 0;
        int numeric = 0;
        for (Path p : mainSources()) {
            String src = Files.readString(p);
            Matcher m = Pattern.compile("default -> \"").matcher(src);
            while (m.find()) {
                n++;
            }
            Matcher num = Pattern.compile(
                    "default -> \\d[\\dxXlLfF_.]*[,;)]").matcher(src);
            while (num.find()) {
                numeric++;
            }
        }
        assertTrue(n <= DEFAULT_LITERAL_FALLBACKS,
                "type-losing default->literal sites grew to " + n
                + " (pinned at " + DEFAULT_LITERAL_FALLBACKS
                + ") — an unmatched kind must THROW, never become a"
                + " plausible literal (Charter C2.4)");
        assertTrue(numeric == 0,
                "default->NUMERIC fabricators appeared (" + numeric
                + ", pinned at 0) — an unmatched kind must THROW, never"
                + " become a plausible number (Charter C2.4)");
    }

    @Test
    void fqnSuffixMatchingOnlyShrinks() throws IOException {
        int n = 0;
        for (Path p : mainSources()) {
            Matcher m = Pattern.compile("\\.endsWith\\(\"::")
                    .matcher(Files.readString(p));
            while (m.find()) {
                n++;
            }
        }
        assertTrue(n <= ENDS_WITH_FQN,
                "endsWith-on-FQN sites grew to " + n + " (pinned at "
                + ENDS_WITH_FQN + ") — identify by exact FQN");
    }

    /** Dispatching on exception MESSAGE TEXT couples control flow to
     * wording. Zero in core AND engine sources; stays zero. */
    @Test
    void noControlFlowOnExceptionMessageText() throws IOException {
        List<String> bad = new ArrayList<>();
        Pattern p1 = Pattern.compile(
                "getMessage\\(\\)\\s*\\.\\s*(contains|matches|startsWith|endsWith|equals)\\(");
        for (Path p : allSources()) {
            String src = Files.readString(p);
            Matcher m = p1.matcher(src);
            while (m.find()) {
                bad.add(p.getFileName() + ":" + lineOf(src, m.start()));
            }
        }
        assertTrue(bad.isEmpty(),
                "control flow inspects exception message text — use a typed"
                + " exception or a field instead: " + bad);
    }

    /** A catch body with NO statements and NO comment is an undocumented
     * swallow ({@code {}} and {@code { ; }} alike). */
    @Test
    void noUndocumentedEmptyCatch() throws IOException {
        List<String> bad = new ArrayList<>();
        Pattern empty = Pattern.compile(
                "catch \\([^)]+\\)\\s*\\{[\\s;]*\\}");
        for (Path p : mainSources()) {
            String src = Files.readString(p);
            Matcher m = empty.matcher(src);
            while (m.find()) {
                bad.add(p.getFileName() + ":" + lineOf(src, m.start()));
            }
        }
        assertTrue(bad.isEmpty(),
                "empty catch without a policy comment — document why the"
                + " swallow is safe: " + bad);
    }

    private static String blockAfter(String src, int open) {
        int i = open;
        int depth = 1;
        StringBuilder body = new StringBuilder();
        while (i < src.length() && depth > 0) {
            char c = src.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
            }
            body.append(c);
            i++;
        }
        return body.toString();
    }

    private static List<Path> mainSources() throws IOException {
        Path root = Path.of("src/main/java");
        try (Stream<Path> s = Files.walk(root)) {
            List<Path> out = s.filter(p -> p.toString().endsWith(".java"))
                    .toList();
            GuardCoverage.assertFloor(/* 499->498: HostEval DELETED, Phase 1 batch 2 */ "ErrorShapeGuardrailTest",
                    out.size(), 498);
            return out;
        }
    }

    /** Core main + engine main (engine tests hold the ONE documented
     * residual, named in the class javadoc). */
    private static List<Path> allSources() throws IOException {
        List<Path> out = new ArrayList<>(mainSources());
        Path engine = Path.of("../engine/src/main/java");
        if (Files.isDirectory(engine)) {
            try (Stream<Path> s = Files.walk(engine)) {
                out.addAll(s.filter(p -> p.toString().endsWith(".java"))
                        .toList());
            }
        }
        return out;
    }

    private static int lineOf(String src, int offset) {
        return (int) src.chars().limit(offset).filter(c -> c == '\n').count() + 1;
    }
}
