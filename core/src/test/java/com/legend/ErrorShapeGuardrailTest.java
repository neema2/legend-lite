// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

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
@Tag("guardrail")
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
            // the service test-suite runner (2026-09-16): the same four
            // designed boundaries — a test's harness failure, its resolve
            // and execute failures, each becoming a FAIL result carrying the
            // platform's whole message (a failing test never aborts the
            // run), and a session that fails to close
            Map.entry("ServiceTestRunner.java", 4),
            // F3.1b (2026-08-16): isValidJson delegates the VARIANT gate
            // to the platform reader; ANY parse failure means not-JSON —
            // a designed total catch (the reader throws ISE/SIOOBE/NFE
            // on garbage and the set is the reader's business, not the
            // gate's)
            Map.entry("TdsChecker.java", 1),
            // 0 -> 1 (2026-11-11, the B checkpoint's correctness audit, C8): a run's catalog turns end in one loop
            // (SystemDatabase.endTurns) that releases EVERY turn whatever a turn's end throws, marks the catalog
            // broken where it failed, and rethrows the first failure with the later ones suppressed -- a release that
            // never rescues (catching DataError alone left the turns after any other failure locked)
            Map.entry("SystemDatabase.java", 1),
            Map.entry("ClassSources.java", 1),
            Map.entry("Compiler.java", 2),
            Map.entry("FunctionCompiler.java", 1),
            // SQLTEXT slice 3a (reviewed): the OUR-ROWS leg's designed
            // total catch — ANY failure executing the producer's query
            // (resolution, lowering, execution) becomes a COUNTED
            // text-verdict decline (charter §3.7: counted, visible,
            // never silent) and TEXT stays the contract; the verdict
            // is never swallowed, it degrades to the §4 policy
            // 1 -> 2 (2026-09-17): lookThrough — the platform's inliner fails a
            // helper it cannot splice (a call cycle, an un-typeable callee) with
            // its own walls; for the plan arm that is "no producer", the answer
            // it gave before the look-through existed, never a verdict
            // 2 -> 3 (2026-11-05, zero exemptions on the runtime wall): sideAhead -- an appeal's side planned with its
            // assert keeps ANY planning failure and raises it where the appeal reads the side, as the side's evaluation
            // there raised it (the rows leg's decline, or a fault); nothing judged at the catch
            // 3 -> 2 + 1 (2026-11-09, slice 5: the rows leg's read of our rows, decided after the run, is the test's
            // run's -- SqlTextRuns -- the planning's two catches stay here)
            Map.entry("SqlTextVerdicts.java", 2),
            Map.entry("SqlTextRuns.java", 1),
            // 2026-10-18 (the plan path program's step 6): the agreement gate turns ANY failure of the database
            // judge's arm -- a statement the database rejects, a dialect's refusal, a planner wall -- into the one
            // outcome Errored, raised as a failure naming both judges' verdicts (AgreementArm.agree); the class
            // cannot be narrowed without letting some failure of that arm escape the gate's comparison
            Map.entry("AgreementArm.java", 1),
            // D5 (reviewed): the generic checked-exception carrier —
            // getOrOpen tunnels the caller's E through compute's
            // unchecked boundary; catch(RuntimeException) rethrows
            // as-is, catch(Exception) wraps in the carrier, unwrapped
            // and rethrown as E at the method boundary. Both catches
            // ARE the tunnel, not swallows.
            Map.entry("HandleStore.java", 2),
            // step 3 (reviewed 2026-10-09, docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9; the audit's S5): the runner's
            // shared sessions close a connection whose setup failed, whatever the failure, and rethrow it as it is, a
            // failed close suppressed on it -- a narrowed list would leak the connection on the next kind
            Map.entry("PlanSessions.java", 1),
            // the dynamic pivot's probe plan (reviewed 2026-10-20, docs/PLAN_PATH_PROGRAM_2026_10_17.md §9): a keyed
            // plan's snapshot sets the session back whatever the work's failure (rolled back, its isolation and
            // auto-commit restored, a failed restore suppressed on it) and rethrows it as it is -- the
            // PlanSessions.java shape; a narrowed list would leave a shared session in an open transaction
            Map.entry("JdbcStatements.java", 1),
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
            // 5 -> 6 (2026-09-23, /engine/plan from datacube/dual-plane): a plan
            // call's non-honest failure is a BUG, logged whole and answered
            // "internal": letting it escape dropped the TCP connection and the
            // client saw a socket error with no message
            // 6 -> 5 (2026-09-27, upstream pure/v1 API): /engine/plan deleted with its
            // handler; its bug-vs-honest split moved to PureV1Api (below)
            // 5 -> 4 (2026-09-29, plan W0.1): /engine/sql deleted with its handler
            Map.entry("LegendHttpServer.java", 4),
            // 1 = the pure/v1 call boundary (2026-09-27): one catch for every upstream
            // call; a non-honest failure is a BUG, logged whole and answered 500 in the
            // engine's error shape, the split /engine/plan's handler made before it
            Map.entry("PureV1Api.java", 1),
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
            // the host lattice with the decline COUNTED (the decline
            // counter went with the byte channel, 2026-11-07), so neither
            // broad catch can become a silent rescue.
            // 2 -> 3 (2026-08-23 F10 v1): the canon-exec tunnel's
            // MIDDLE RUNG (drop the literal candidate, keep the bare
            // byte channel) catches the same designed-sentinel class as
            // its two siblings — a caught failure becomes a counted
            // decline, never a rescue (witness testRepeatStringNoString:
            // the BLOB wire under a STRING stamp).
            Map.entry("BodyDriver.java", 3),   // StatementExecutor.java until 2026-10-29 (track D)
            // (2026-11-01, the test-is-the-product plan's M6; its audit's S1, S2 and N2; R14's planned raise): an
            // assert program's asserts are planned BEFORE the program runs, so a failure the walk would raise at the
            // assert's turn -- the classification's, a side's planning, a let's handle registration, the frame
            // environment's, of whatever type -- is captured into a raise the sink throws at that turn, nothing
            // swallowed (HostAsserts: 4 sites; AssertVerdicts.planHost: 1); and the runner hands a host side's read
            // failure of any type to the program sink, which counts it as the walk counts it and returns the error
            // the runner then throws (PlanRunner.verdicts: 1)
            // 4 -> 6 (2026-11-03, the walker-deletion program): assertError planned as a verdict step -- its frame
            // environment's failure raised before the arm at its turn (the walk's), and its side's planning failure:
            // the error the arm takes is the judgment's answer, any other a wall raised at its turn, loud
            Map.entry("HostAsserts.java", 6),
            // 0 -> 1 (2026-11-07, the P1 milestone audit's 2): a body whose run FAILED with its compatibility appeals
            // held -- the catch always rethrows: the run's own failure, the first held appeal's failure (which
            // precedes it, the run's suppressed in it), or a named refusal where the failed run wrote (its rollback
            // undid what the appeals read). An Error (the wall's refusal) is not caught. Nothing swallowed
            // (the router's until slice 5, 2026-11-09: the run's entry, TestRunner, holds it since)
            Map.entry("TestRunner.java", 1),
            // M7 (2026-11-01): the database judge's planner -- AssertVerdicts.planDatabase records an arm's WALL as a
            // step raised at the root's turn (the walk discards the root and surfaces the wall after its flush;
            // nothing may run at planning, so the raise is planned, its exit counted there: 1 more); DatabaseAsserts
            // (4): its static pass leaves a body whose let or frame environment raises to the walk (2: the walk raises
            // at that turn, before anything is flushed -- the audit's F3, F4), leaves a gate that raises to the arm
            // (1: the arm raises the same at its turn), and its planning pass records the gate's raise as the root's
            // wall (1). Nothing swallowed
            // 2 -> 3 (2026-11-04, the assert bar): a caller's message the database judge cannot plan is no refusal of
            // the assert -- upstream evaluates the message only where the assert fails -- so its planning failure
            // becomes the verdict's failure text (AssertMessages.unevaluated), raised where the row does not hold.
            // Nothing swallowed: the failure is the assert's message
            Map.entry("AssertVerdicts.java", 3),
            // 0 -> 1 (2026-11-04, the assert bar): HostSides.planChecked -- a check whose sides fail to plan is
            // planned again without its caller's message where the message is what raised (planning its own sides
            // again raises theirs), the message's failure the judgment's raise where the assert fails
            // (HostJudgment.Messaged#unplanned). Nothing swallowed
            Map.entry("HostSides.java", 1),
            // 4 -> 6 (2026-11-02, the walker-deletion program: the agreement judge's database half, planEach): a let
            // the database half cannot build declines the asserts after it (the host half raises it at its place: a
            // planned raise), and an assert's frame environment, gate or arm that raises is planned as the arm's wall
            // at the root's place, as the planning pass records it. Nothing swallowed
            Map.entry("DatabaseAsserts.java", 6),
            // 2026-11-08, the plan-as-data design's step 3 (docs/PLAN_AS_DATA_2026_11_07.md 2.8): the host side read that fails is handed to the judges as the failure of that verdict's sides,
            // moved with the verdict steps from the runner (PlanRunner, 1 -> 0) to the test run (TestRun, 0 -> 1);
            // 2026-11-08, the slice 1 audit (docs/PLAN_AS_DATA_2026_11_07.md section 10): the open program's guarded work -- its failure, a runtime one or an I/O
            // one, handed to the caller once the savepoint is rolled back to (TestRun 1 -> 0, ProgramRunner 0 -> 2:
            // the work without a savepoint and with one); a savepoint statement's own failure is the program's
            Map.entry("ProgramRunner.java", 2),
            // 2026-11-11, the A delta audit's note 11 (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §3 rule 2): a file load's
            // reading undoes what it set on the session (H2's path variable) on every exit, its failure of any kind
            // rethrown with an undo's failure kept beside it -- nothing swallowed
            Map.entry("PlanRunner.java", 1),
            // contract program (2026-08-23): the wire census's
            // INSTRUMENT-ISOLATION catch — measurement must never throw
            // into execution (the Dual.alias lesson); an unreadable
            // ResultSetMetaData is a counted unknown, never a failure
            // (SqlTypeCensus.java 1 -> deleted 2026-11-09, the full audit: the census had no feeder; with its static
            // state, the class went)
            // 0 -> 1 (2026-10-18, the writes design's leg 2, reviewed): Executor.inTransaction's rollback -- any
            // failure of the work (a RuntimeException or an Error) rolls the transaction back and restores
            // autocommit, a failure of either suppressed into it, and is RETHROWN unchanged; never a swallow, never
            // a value. A finally cannot attach the rollback's own failure to the work's (and may not throw).
            // Executor.java's one (the rollback below) moved to Sessions.java with inTransaction, 2026-10-27 (the result
            // boundary program's phase 5, the legacy executor deleted): the same catch, rethrown unchanged
            Map.entry("Sessions.java", 1),
            Map.entry("StaticFold.java", 1),
            // 3 -> 4 (2026-08-28 V7 batch 1): the dual channel's decline
            // tunnel — a production-route probe failure becomes a COUNTED
            // per-form decline (CanonicalDivergence.v7Declined), never a
            // swallow and never a verdict (the host verdict of record is
            // computed before the probe runs); the V2/V6 tunnel idiom
            // 4 -> 5 (slice 3 equality half, reviewed): evalSideText's
            // counted-decline boundary — the F2.3 catch inherited from
            // the deleted ExecCallFinder.sideSqlText, same discipline
            // (every failure lands in the RefereeVerify decline census)
            Map.entry("QuotedSpecParser.java", 1),
            // Typer.java's one catch moved with the overload machinery (2026-09-29, plan W1.6)
            Map.entry("Overloads.java", 1),
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
    private static final int CATCH_RETURNS_VALUE = 31;   // 30 -> 31 (2026-11-12, phase D's D1.0b, the orchestrator's ruling: pure/v1's upstream routes to the edge): upstream's error node for a test-data root no identifier names left the product's answer (PureV1Api.answer lost that branch) for the edge's own wrapper, edge.TestDataRoutes.answer, whose catch hands the Unseeded failure to the edge's answer of it -- the designed sentinel, re-answered as upstream answers it; nothing is swallowed. 29 -> 30 (2026-11-08, the slice 1 audit (docs/PLAN_AS_DATA_2026_11_07.md section 10)): OpenProgram.guarded's two catches return the caller's handling of the guarded work's failure (Failed), the designed sentinel that keeps a savepoint statement's failure the program's; nothing is swallowed. 28 -> 29 (2026-11-05, the deep audit's runtime wall): VerdictBatch.planAhead's catch returns the dialect's refusal of a statement planned ahead as the designed sentinel Ahead.refused, raised (and counted UNJUDGED) where the root loop reaches it -- as executeOne raised it before, now planned before the run; nothing is swallowed. 27 -> 28 (2026-11-04, the assert bar): VerdictBatch.messaged's catch returns the error reading a failing root's caller's message as that root's failure -- upstream evaluates the message only where the assert fails, and its evaluation's error is the test's; nothing is swallowed. 26 -> 27 (2026-11-04, the assert bar): HostSides.planChecked's catch returns the check planned without its caller's message, the message's planning failure carried as the designed sentinel HostJudgment.Messaged#unplanned and raised where the assert fails (upstream evaluates the message only there); nothing is swallowed. 24 -> 26 (2026-10-18, the plan path program's step 6, docs/PLAN_PATH_PROGRAM_2026_10_17.md §6): AgreementArm.databaseOutcome's two arms ARE the designed sentinel of the agreement gate -- the database judge's outcome as data (Failed with its assert failure, Errored with its statement's failure), compared with the host's verdict of record and re-raised naming both when they differ; nothing is swallowed. 22 -> 24 (2026-10-13, docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B6's review): SavedQueries.answer's and SavedCubes.answer's 500 arm -- the store's designed sentinel, as PureV1Api.answer's arms are: the engine's error JSON for a failure, naming it. Both stores answered so before; the arm's return was then longer than this matcher's 80 characters, and one helper (SavedQueries.failed, the fields in the engine's order) made it short enough to be counted. 23 -> 22 (2026-10-12, the independent review of neema2/legend-lite#30): Executor.fetch's catch that kept a Timestamp for every driver is gone -- a dialect states its driver delivers no java.time (DatabaseConnector.decoding().javaTime), any other refusal fails. 22 -> 23 (2026-10-12, the rebase of the cloud-backends branch onto main, which took 22 with Protocol.java's own site: Executor.fetch's BC-faithful re-fetch as java.time is an upgrade of a cell the driver already delivered; Snowflake's driver refuses that re-fetch with a plain SQLException, and the cell keeps the Timestamp already read -- the read itself never fails into a value); 17 -> 19 (2026-09-16: ServiceTestRunner's resolve/execute FAIL sentinels, the PureTestRunner pair's twins); 19 -> 21 (2026-09-27: PureV1Api.answer's two arms ARE the designed sentinel of an HTTP API -- the engine's error JSON, 400 for an honest failure, 500 for a logged bug); 21 -> 22 (2026-09-27, E8: its third arm, a malformed request or the database refusing -- the engine's measured 500 with no errorType)

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
        // core's main sources: src/main/java and the DuckDB-only src/main/duckdb (Bazel workplan P3-27, CT-N10)
        String root = "core/src/main/java";
        try (Stream<Path> s = SourceFiles.under(root, "core/src/main/duckdb").stream()) {
            List<Path> out = s.filter(p -> p.toString().endsWith(".java"))
                    .toList();
            GuardCoverage.assertFloor(/* 499->498: HostEval DELETED, Phase 1 batch 2 */ "ErrorShapeGuardrailTest",
                    out.size(), 498);
            return out;
        }
    }

    /** Core main. The engine module this once also walked was deleted on
     * 2026-08-11; its walk stayed behind as an `if (Files.isDirectory(...))`
     * that skipped silently for six weeks — a guard over nothing, removed
     * rather than carried into the Bazel build. */
    private static List<Path> allSources() throws IOException {
        return new ArrayList<>(mainSources());
    }

    private static int lineOf(String src, int offset) {
        return (int) src.chars().limit(offset).filter(c -> c == '\n').count() + 1;
    }
}
