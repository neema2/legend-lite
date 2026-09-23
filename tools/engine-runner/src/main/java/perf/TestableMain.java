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
        for (String a : args)
        {
            if (a.startsWith("--testable="))
            {
                testables.add(a.substring("--testable=".length()));
            }
            else if (a.startsWith("--pmcd-json="))
            {
                pmcdJson = a.substring("--pmcd-json=".length());
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

        List<RunTestsTestableInput> inputs = new ArrayList<>();
        for (String t : testables)
        {
            RunTestsTestableInput in = new RunTestsTestableInput();
            in.testable = t;
            in.unitTestIds = new ArrayList<>();
            inputs.add(in);
        }

        t0 = System.nanoTime();
        RunTestsResult res = new TestableRunner().doTests(inputs, pureModel, pmcd);
        double runMs = (System.nanoTime() - t0) / 1e6;

        int pass = 0, fail = 0, err = 0;
        System.out.printf("parse %.0f ms   compile %.0f ms   run %.0f ms%n%n", parseMs, compileMs, runMs);
        // A MultiExecution service returns ONE result per execution KEY, wrapped in a
        // MultiExecutionServiceTestResult rather than a TestExecuted. Casting blindly
        // threw a ClassCastException that killed the JVM mid-run, so every service after
        // it went unreported -- which looked exactly like the runner hanging.
        List<TestResult> flat = new ArrayList<>();
        for (TestResult r : res.results)
        {
            if (r instanceof org.finos.legend.engine.testable.service.result.MultiExecutionServiceTestResult m)
            {
                m.getKeyIndexedTestResults().forEach((key, inner) ->
                {
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

        for (TestResult r : flat)
        {
            String id = r.testSuiteId + " / " + r.atomicTestId;
            if (r instanceof TestError te)
            {
                err++;
                System.out.println("ERROR " + id + "\n      " + oneLine(te.error, 400));
                continue;
            }
            TestExecuted te = (TestExecuted) r;
            boolean allPass = te.assertStatuses.stream().allMatch(a -> !(a instanceof AssertFail));
            if (allPass) { pass++; System.out.println("PASS  " + id); continue; }
            fail++;
            System.out.println("FAIL  " + id);
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
        System.out.printf("%n%d passed, %d failed, %d errored, %d total%n",
            pass, fail, err, res.results.size());
        System.exit(0);
    }

    static String oneLine(String s, int max)
    {
        if (s == null) { return "null"; }
        s = s.replaceAll("\\s+", " ").trim();
        return s.length() > max ? s.substring(0, max) + " …" : s;
    }
}
