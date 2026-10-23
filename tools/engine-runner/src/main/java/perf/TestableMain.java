package perf;

import org.finos.legend.engine.language.pure.compiler.Compiler;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.protocol.pure.v1.model.test.assertion.status.AssertFail;
import org.finos.legend.engine.protocol.pure.v1.model.test.assertion.status.AssertionStatus;
import org.finos.legend.engine.protocol.pure.v1.model.test.assertion.status.EqualToJsonAssertFail;
import org.finos.legend.engine.protocol.pure.v1.model.test.result.TestError;
import org.finos.legend.engine.protocol.pure.v1.model.test.result.TestExecuted;
import org.finos.legend.engine.protocol.pure.v1.model.test.result.TestResult;
import org.finos.legend.engine.shared.core.deployment.DeploymentMode;
import org.finos.legend.engine.testable.TestableRunner;
import org.finos.legend.engine.testable.model.RunTestsResult;
import org.finos.legend.engine.testable.model.RunTestsTestableInput;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs the test suites declared INSIDE a .pure file, using Legend's own testable framework.
 *
 * This is the portable form: the model, the seed data (###Data) and the assertions
 * (testSuites + EqualToJson) all live in .pure grammar, so any Legend-compatible engine
 * can execute them with its own runner. Nothing here is specific to this harness.
 */
public class TestableMain
{
    public static void main(String[] args) throws Exception
    {
        StringBuilder src = new StringBuilder();
        List<String> testables = new ArrayList<>();
        // --dump=<dir> writes the FULL expected/actual payloads per failing assertion.
        // Without it a wide TDS failure is a 300-character prefix that is identical on
        // both sides, which says nothing about which of 60 columns diverged.
        String dumpDir = null;
        // --pmcd-json=<file> writes the compiled protocol document. The census probes
        // measure which protocol _type tags the UPSTREAM corpus exercises; this is how we
        // measure which ones OUR corpus exercises, which is a different question and the
        // one that makes corpus work countable.
        String pmcdJson = null;
        // --rows=<dir> (rebuild D23): the engine's ACTUAL rows for EVERY test, pass or
        // fail, one JSON file per test. Every EqualToJson expectation is replaced by a
        // sentinel before the run, so the framework reports each test as failing and
        // hands over what it computed; the summary counts ran/errored, never passed.
        String rowsDir = null;
        // --data=<file.pure> (rebuild D23): elements in this file REPLACE the model's
        // elements of the same path (a damaged ###Data element in place of the seed);
        // the seed files are never edited. Several allowed; later ones win.
        List<String> dataOverrides = new ArrayList<>();
        // --report=<file>: one line per test, `status \t testable \t suite \t test \t millis \t detail` (the testable's
        // millis, all its tests together), written as each testable
        // finishes, so a caller reads verdicts by testable rather than matching a suite id in the printed text, and
        // keeps them when the JVM dies (scripts/corpus/run.py). Status PASS, FAIL, ERROR, ROWS, or HUNG (--limit).
        String report = null;
        // --limit=<seconds>: a testable still running after this long is reported HUNG and the JVM halts with exit
        // status 3, so the caller restarts the testables after it. Killing the JVM from outside would lose what it
        // had not yet written, and on Windows would leave java.exe running after its launcher.
        long limitSeconds = 0;
        for (String a : expand(args))
        {
            if (a.startsWith("--testable="))
            {
                testables.add(a.substring("--testable=".length()));
            }
            else if (a.startsWith("--rows="))
            {
                rowsDir = a.substring("--rows=".length());
                Files.createDirectories(Cwd.of(rowsDir));
            }
            else if (a.startsWith("--data="))
            {
                dataOverrides.add(a.substring("--data=".length()));
            }
            else if (a.startsWith("--pmcd-json="))
            {
                pmcdJson = a.substring("--pmcd-json=".length());
            }
            else if (a.startsWith("--report="))
            {
                report = a.substring("--report=".length());
            }
            else if (a.startsWith("--limit="))
            {
                limitSeconds = Long.parseLong(a.substring("--limit=".length()));
            }
            else if (a.startsWith("--dump="))
            {
                dumpDir = a.substring("--dump=".length());
                Files.createDirectories(Cwd.of(dumpDir));
            }
            else
            {
                src.append(Files.readString(Cwd.of(a))).append("\n");
            }
        }

        PureGrammarParser parser = PureGrammarParser.newInstance();
        long t0 = System.nanoTime();
        PureModelContextData pmcd = parser.parseModel(src.toString());
        for (String f : dataOverrides)
        {
            pmcd = override(pmcd, parser.parseModel(Files.readString(Cwd.of(f))), f);
        }
        if (rowsDir != null)
        {
            pmcd = failEveryAssertion(pmcd);
        }
        double parseMs = (System.nanoTime() - t0) / 1e6;
        t0 = System.nanoTime();
        if (pmcdJson != null)
        {
            Files.writeString(Cwd.of(pmcdJson),
                    org.finos.legend.engine.shared.core.ObjectMapperFactory
                            .getNewStandardObjectMapperWithPureProtocolExtensionSupports()
                            .writeValueAsString(pmcd));
            System.out.println("pmcd written: " + pmcdJson);
        }
        PureModel pureModel = Compiler.compile(pmcd, DeploymentMode.TEST, null);
        double compileMs = (System.nanoTime() - t0) / 1e6;

        // ONE TESTABLE AT A TIME, each one's verdicts written to the report as it finishes: a testable that hangs or
        // kills the JVM costs itself, not the testables after it. One that throws out of the framework (a test suite
        // that fails to initialise did, and took a whole batch's results with it) is an ERROR of its own.
        java.io.PrintWriter out = report == null ? null
                : new java.io.PrintWriter(
                        Files.newBufferedWriter(Cwd.of(report), java.nio.charset.StandardCharsets.UTF_8));
        java.util.concurrent.ScheduledExecutorService watchdog = java.util.concurrent.Executors
                .newSingleThreadScheduledExecutor(r ->
                {
                    Thread th = new Thread(r, "testable-limit");
                    th.setDaemon(true);
                    return th;
                });
        TestableRunner runner = new TestableRunner();
        int[] counts = new int[4];   // passed, failed, errored, total
        t0 = System.nanoTime();
        for (String t : testables)
        {
            RunTestsTestableInput in = new RunTestsTestableInput();
            in.testable = t;
            in.unitTestIds = new ArrayList<>();
            long limit = limitSeconds;
            long started = System.nanoTime();
            java.util.concurrent.ScheduledFuture<?> alarm = limit <= 0 ? null
                    : watchdog.schedule(() -> hung(t, limit, out), limit, java.util.concurrent.TimeUnit.SECONDS);
            RunTestsResult res;
            try
            {
                res = runner.doTests(List.of(in), pureModel, pmcd);
            }
            catch (Exception | StackOverflowError e)
            {
                counts[2]++;
                counts[3]++;
                System.out.println("ERROR " + t + "\n      " + oneLine(withCauses(e), 1200));
                write(out, "ERROR\t" + t + "\t\t\t" + (System.nanoTime() - started) / 1_000_000 + "\t"
                        + oneLine(withCauses(e), 900).replace('\t', ' '));
                continue;
            }
            finally
            {
                if (alarm != null)
                {
                    alarm.cancel(false);
                }
                // Every test's connection pools closed before the next testable, as legend-engine's own test helpers
                // do between tests (PureTestHelper, Test_RelationalMFTH2). Left open, the service test runner keeps
                // one pooled DuckDB connection per test and each later test's data setup slows: measured 2026-10-21,
                // nine services sharing stress::RT took 2.4 s to 25 s, one after another, and 40 took over 40 minutes.
                org.finos.legend.engine.plan.execution.stores.relational.connection.ds.state.ConnectionStateManager
                        .getInstance().clearAllConnections();
            }
            long millis = (System.nanoTime() - started) / 1_000_000;
            for (TestResult r : flatten(res))
            {
                counts[3]++;
                record(r, millis, rowsDir, dumpDir, out, counts);
            }
        }
        double runMs = (System.nanoTime() - t0) / 1e6;
        if (out != null)
        {
            out.close();
        }
        System.out.printf("%nparse %.0f ms   compile %.0f ms   run %.0f ms%n", parseMs, compileMs, runMs);
        System.out.printf("%n%d passed, %d failed, %d errored, %d total%n", counts[0], counts[1], counts[2], counts[3]);
        System.exit(0);
    }

    /** A MultiExecution service returns ONE result per execution KEY, wrapped in a MultiExecutionServiceTestResult
     *  rather than a TestExecuted. Casting blindly threw a ClassCastException that killed the JVM mid-run, so every
     *  service after it went unreported -- which looked exactly like the runner hanging. */
    static List<TestResult> flatten(RunTestsResult res)
    {
        List<TestResult> flat = new ArrayList<>();
        for (TestResult r : res.results)
        {
            if (r instanceof org.finos.legend.engine.testable.service.result.MultiExecutionServiceTestResult m)
            {
                m.getKeyIndexedTestResults().forEach((key, inner) ->
                {
                    inner.testable = r.testable;
                    inner.testSuiteId = r.testSuiteId;
                    inner.atomicTestId = r.atomicTestId + " [" + key + "]";
                    flat.add(inner);
                });
            }
            else
            {
                flat.add(r);
            }
        }
        return flat;
    }

    /** One test's verdict, printed and reported with its testable's time; counts[0..2] are passed, failed, errored. */
    static void record(TestResult r, long millis, String rowsDir, String dumpDir, java.io.PrintWriter out, int[] counts)
            throws java.io.IOException
    {
        String id = r.testSuiteId + " / " + r.atomicTestId;
        String row = "\t" + r.testable + "\t" + r.testSuiteId + "\t" + r.atomicTestId + "\t" + millis + "\t";
        if (r instanceof TestError te)
        {
            counts[2]++;
            System.out.println("ERROR " + id + "\n      " + oneLine(te.error, 400));
            write(out, "ERROR" + row + oneLine(te.error, 300).replace('\t', ' '));
            return;
        }
        TestExecuted te = (TestExecuted) r;
        boolean allPass = te.assertStatuses.stream().allMatch(a -> !(a instanceof AssertFail));
        if (allPass)
        {
            counts[0]++;
            System.out.println("PASS  " + id);
            write(out, "PASS" + row);
            return;
        }
        if (rowsDir != null)
        {
            // the rows mode: every assertion fails by construction; the actual is the point
            String base = id.replaceAll("[^A-Za-z0-9_.-]", "_");
            String actual = te.assertStatuses.stream()
                    .filter(a -> a instanceof EqualToJsonAssertFail)
                    .map(a -> ((EqualToJsonAssertFail) a).actual)
                    .findFirst().orElse(null);
            Files.writeString(Cwd.of(rowsDir, base + ".rows.json"), actual == null ? "null" : actual);
            System.out.println("ROWS  " + id + "  -> " + base + ".rows.json");
            write(out, "ROWS" + row);
            return;
        }
        counts[1]++;
        System.out.println("FAIL  " + id);
        write(out, "FAIL" + row);
        for (AssertionStatus a : te.assertStatuses)
        {
            if (a instanceof EqualToJsonAssertFail eq)
            {
                System.out.println("      expected: " + oneLine(eq.expected, 300));
                System.out.println("      actual  : " + oneLine(eq.actual, 300));
                if (dumpDir != null)
                {
                    String base = id.replaceAll("[^A-Za-z0-9_.-]", "_");
                    Files.writeString(Cwd.of(dumpDir, base + ".expected.json"),
                            eq.expected == null ? "null" : eq.expected);
                    Files.writeString(Cwd.of(dumpDir, base + ".actual.json"),
                            eq.actual == null ? "null" : eq.actual);
                    System.out.println("      dumped  : " + base + ".{expected,actual}.json");
                }
            }
            else if (a instanceof AssertFail af)
            {
                System.out.println("      " + oneLine(af.message, 300));
            }
        }
    }

    /** A report line, flushed at once: the watchdog may halt the JVM at any moment. */
    static void write(java.io.PrintWriter out, String line)
    {
        if (out == null)
        {
            return;
        }
        synchronized (out)
        {
            out.println(line);
            out.flush();
        }
    }

    /** The watchdog's alarm: the testable is reported HUNG and the JVM halts, exit status 3. */
    static void hung(String testable, long limitSeconds, java.io.PrintWriter out)
    {
        write(out, "HUNG\t" + testable + "\t\t\t" + limitSeconds * 1000
                + "\tstill running after " + limitSeconds + " s");
        System.out.println("HUNG  " + testable + " still running after " + limitSeconds + " s; halting");
        System.out.flush();
        Runtime.getRuntime().halt(3);
    }

    /** The arguments with each {@code @<file>} replaced by that file's lines, one argument per non-blank line: a
     *  whole corpus and a batch of testables exceed Windows' 32,767-character command line. */
    static List<String> expand(String[] args) throws java.io.IOException
    {
        List<String> out = new ArrayList<>();
        for (String a : args)
        {
            if (a.startsWith("@"))
            {
                for (String line : Files.readAllLines(Cwd.of(a.substring(1))))
                {
                    if (!line.isBlank())
                    {
                        out.add(line.strip());
                    }
                }
            }
            else
            {
                out.add(a);
            }
        }
        return out;
    }

    /** The model with every element of {@code over} replacing the element of the same
     *  path, reported one by one. */
    static PureModelContextData override(PureModelContextData base, PureModelContextData over, String from)
    {
        java.util.Map<String, org.finos.legend.engine.protocol.pure.m3.PackageableElement> byPath =
                new java.util.LinkedHashMap<>();
        for (var e : base.getElements())
        {
            byPath.put(e.getPath(), e);
        }
        for (var e : over.getElements())
        {
            if (e instanceof org.finos.legend.engine.protocol.pure.v1.model.packageableElement.section.SectionIndex)
            {
                continue;   // the override file's own section bookkeeping
            }
            System.out.println((byPath.containsKey(e.getPath()) ? "REPLACED " : "ADDED    ")
                    + e.getPath() + "  (" + from + ")");
            byPath.put(e.getPath(), e);
        }
        return PureModelContextData.newBuilder()
                .withOrigin(base.getOrigin())
                .withSerializer(base.getSerializer())
                .withElements(new ArrayList<>(byPath.values()))
                .build();
    }

    /** Every EqualToJson expectation replaced by a sentinel no result equals, so the
     *  framework reports each test's ACTUAL. The protocol objects are mutated in place;
     *  nothing is written back. */
    static PureModelContextData failEveryAssertion(PureModelContextData pmcd)
    {
        int n = 0;
        for (var e : pmcd.getElements())
        {
            if (!(e instanceof org.finos.legend.engine.protocol.pure.v1.model.packageableElement.service.Service svc)
                    || svc.testSuites == null)
            {
                continue;
            }
            for (var suite : svc.testSuites)
            {
                if (suite.tests == null)
                {
                    continue;
                }
                for (var test : suite.tests)
                {
                    if (!(test instanceof org.finos.legend.engine.protocol.pure.v1.model.packageableElement.service.ServiceTest st)
                            || st.assertions == null)
                    {
                        continue;
                    }
                    for (var a : st.assertions)
                    {
                        if (a instanceof org.finos.legend.engine.protocol.pure.v1.model.test.assertion.EqualToJson eq
                                && eq.expected != null)
                        {
                            eq.expected.data = "[{\"__rows_mode__\":\"no expectation\"}]";
                            n++;
                        }
                    }
                }
            }
        }
        System.out.println("rows mode: " + n + " expectation(s) replaced");
        return pmcd;
    }

    // A throwable and its causes, outermost first. The framework wraps a suite's failure to start ("Error initializing
    // test suite session for 'X_suite'") around the reason it failed, so the outer message alone names no cause
    // (docs/STRESS_CORPUS_PLAN_2026_10_19.md, Phase 1: a quarantine entry needs the reason).
    static String withCauses(Throwable e)
    {
        StringBuilder b = new StringBuilder(String.valueOf(e));
        int depth = 0;
        for (Throwable c = e.getCause(); c != null && depth < 10; c = c.getCause(), depth++)
        {
            b.append(" <- caused by: ").append(c);
        }
        return b.toString();
    }

    static String oneLine(String s, int max)
    {
        if (s == null) { return "null"; }
        s = s.replaceAll("\\s+", " ").trim();
        return s.length() > max ? s.substring(0, max) + " …" : s;
    }
}
