package com.legend.tools.junit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * JUnitMain against Bazel's test protocol (Bazel workplan P1-01), on fixtures a real lane cannot show:
 * an empty selection, a filter that matches nothing, the premature-exit file, the shard split's union on
 * Jupiter and on a flat JUnit 3 suite, and the overrun guard on a PCT-shaped one; and JUnitAction, a run as a
 * build action (P3-01). Each case runs the runner in this JVM with its own environment, as Bazel would set it.
 */
class RunnerTest {

    private static final String FIXTURES = "com.legend.tools.junit.fixtures.";
    private static final Pattern TESTCASE = Pattern.compile("<testcase classname=\"([^\"]*)\" name=\"([^\"]*)\"");

    @TempDir Path dir;

    private int run(Map<String, String> env, String... args) throws Exception {
        return JUnitMain.protocol(args, env::get);
    }

    private Map<String, String> env(String xml) {
        Map<String, String> env = new HashMap<>();
        env.put("XML_OUTPUT_FILE", dir.resolve(xml).toString());
        return env;
    }

    /** classname#name of every testcase in a test.xml, in order (duplicates kept). */
    private List<String> testcases(String xml) throws Exception {
        List<String> ids = new ArrayList<>();
        Matcher m = TESTCASE.matcher(Files.readString(dir.resolve(xml)));
        while (m.find()) {
            ids.add(m.group(1) + "#" + m.group(2));
        }
        return ids;
    }

    @Test
    void testXmlListsEveryTestcase() throws Exception {
        assertEquals(0, run(env("all.xml"), "--select-class=" + FIXTURES + "ProbeTest", "--fail-if-no-tests"));
        List<String> ids = testcases("all.xml");
        assertEquals(9, ids.size(), "5 tests, 3 parameterized invocations, the exit-file probe: " + ids);
        assertFalse(Files.readString(dir.resolve("all.xml")).contains("<properties"),
                "test.xml carries no system-property table (it names the machine)");
    }

    @Test
    void anActionRecordsAFailingPassAndSucceeds() throws Exception {
        Path verdict = dir.resolve("verdict.txt");
        Path log = dir.resolve("pass.log");
        Path ledger = dir.resolve("ledger.tsv");
        JUnitAction.run(new String[] {verdict.toString(), log.toString(), ledger.toString(), "--",
                "--select-class=" + FIXTURES + "FailingTest", "--fail-if-no-tests"});
        assertEquals("1", Files.readString(verdict).trim(), "JUnitMain's exit code for a failed test");
        String text = Files.readString(log);
        assertTrue(text.contains("Failures (1)") && text.contains("the fixture's planted failure"),
                "the log keeps the failure summary its consumer quotes: " + text);
        assertTrue(Files.readString(ledger).startsWith("UNMEASURED: "),
                "an output the pass did not write says so, never a blank file");
    }

    @Test
    void anActionRecordsAPassingPass() throws Exception {
        Path verdict = dir.resolve("verdict.txt");
        JUnitAction.run(new String[] {verdict.toString(), dir.resolve("pass.log").toString(), "--",
                "--select-class=" + FIXTURES + "ProbeTest", "--fail-if-no-tests"});
        assertEquals("0", Files.readString(verdict).trim());
    }

    @Test
    void anEmptySelectionFails() throws Exception {
        assertEquals(2, run(env("empty.xml"), "--select-package=com.legend.nosuchpackage", "--fail-if-no-tests"));
    }

    @Test
    void aFilterThatMatchesNothingFails() throws Exception {
        Map<String, String> env = env("nomatch.xml");
        env.put("TESTBRIDGE_TEST_ONLY", "NoSuchTest");
        assertEquals(2, run(env, "--select-class=" + FIXTURES + "ProbeTest", "--fail-if-no-tests"));
    }

    @Test
    void aFilterRunsOnlyWhatItNames() throws Exception {
        Map<String, String> env = env("one.xml");
        env.put("TESTBRIDGE_TEST_ONLY", "ProbeTest#gamma$");
        assertEquals(0, run(env, "--select-class=" + FIXTURES + "ProbeTest", "--fail-if-no-tests"));
        assertEquals(List.of(FIXTURES + "ProbeTest#gamma()"), testcases("one.xml"));
    }

    @Test
    void anUnknownArgumentFails() {
        assertThrows(IllegalArgumentException.class,
                () -> run(env("x.xml"), "--select-class=" + FIXTURES + "ProbeTest", "--details=summary"));
    }

    @Test
    void thePrematureExitFileExistsOnlyWhileTestsRun() throws Exception {
        Path exit = dir.resolve("premature_exit");
        Map<String, String> env = env("exit.xml");
        env.put("TEST_PREMATURE_EXIT_FILE", exit.toString());
        System.setProperty("runner_test.exit_file", exit.toString());
        try {
            assertEquals(0, run(env, "--select-method=" + FIXTURES + "ProbeTest#exitFileExistsWhileTestsRun"));
        } finally {
            System.clearProperty("runner_test.exit_file");
        }
        assertFalse(Files.exists(exit), "a run that returns removes the file; one that calls System.exit cannot");
    }

    @Test
    void jupiterShardsUnionToTheUnshardedSet() throws Exception {
        assertShardUnion("ProbeTest", 3);
    }

    @Test
    void aFlatJUnit3SuiteShardsUnionToTheUnshardedSet() throws Exception {
        assertShardUnion("FlatSuiteTest", 3);
    }

    @Test
    void aSplitTheVintageEngineCannotHonourFails() throws Exception {
        for (int shard = 0; shard < 2; shard++) {
            Map<String, String> env = shardEnv("nested", shard, 2);
            assertEquals(3, run(env, "--select-class=" + FIXTURES + "NestedSuiteTest", "--fail-if-no-tests"),
                    "shard " + shard + " ran the tests the split gave to the other shard");
        }
    }

    @Test
    void aFilterTheVintageEngineCannotHonourOnlyWarns() throws Exception {
        Map<String, String> env = env("nested_filter.xml");
        env.put("TESTBRIDGE_TEST_ONLY", "testFn_1_1");
        assertEquals(0, run(env, "--select-class=" + FIXTURES + "NestedSuiteTest", "--fail-if-no-tests"));
    }

    private Map<String, String> shardEnv(String name, int shard, int total) {
        Map<String, String> env = env(name + "_" + shard + ".xml");
        env.put("TEST_TOTAL_SHARDS", Integer.toString(total));
        env.put("TEST_SHARD_INDEX", Integer.toString(shard));
        env.put("TEST_SHARD_STATUS_FILE", dir.resolve(name + "_status").toString());
        return env;
    }

    private void assertShardUnion(String fixture, int total) throws Exception {
        String select = "--select-class=" + FIXTURES + fixture;
        assertEquals(0, run(env(fixture + ".xml"), select, "--fail-if-no-tests"));
        TreeSet<String> unsharded = new TreeSet<>(testcases(fixture + ".xml"));
        List<String> union = new ArrayList<>();
        for (int shard = 0; shard < total; shard++) {
            assertEquals(0, run(shardEnv(fixture, shard, total), select, "--fail-if-no-tests"));
            List<String> ran = testcases(fixture + "_" + shard + ".xml");
            assertFalse(ran.isEmpty(), "shard " + shard + " of " + total + " ran nothing: the split is unbalanced");
            union.addAll(ran);
        }
        assertTrue(Files.exists(dir.resolve(fixture + "_status")), "the runner advertises sharding to Bazel");
        assertEquals(unsharded.size(), union.size(), "a test ran in two shards: " + union);
        assertEquals(unsharded, new TreeSet<>(union));
    }
}
