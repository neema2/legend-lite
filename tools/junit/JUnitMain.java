package com.legend.tools.junit;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.management.NotificationEmitter;
import javax.management.openmbean.CompositeData;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import com.sun.management.GarbageCollectionNotificationInfo;

import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.FilterResult;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.discovery.ClassNameFilter;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.engine.discovery.PackageNameFilter;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.EngineFilter;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.PostDiscoveryFilter;
import org.junit.platform.launcher.TagFilter;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import org.junit.platform.reporting.legacy.xml.LegacyXmlReportGeneratingListener;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * The entry point every Bazel test target runs: the JUnit Platform Launcher, driven by
 * the console launcher's selector vocabulary, speaking Bazel's test protocol
 * (https://bazel.build/reference/test-encyclopedia).
 *
 * <p>SELECTION — the BUILD file's {@code select}/{@code exclude_tags}, parsed here with
 * the ConsoleLauncher's exact semantics:
 * <ul>
 *   <li>{@code --select-package=P}, {@code --select-class=C}, {@code --select-method=C#m}: selectors (union);</li>
 *   <li>{@code --include-tag=E}, {@code --exclude-tag=E}: tag expressions ({@link TagFilter});</li>
 *   <li>{@code --exclude-package=P}: a package left out ({@link PackageNameFilter});</li>
 *   <li>{@code --include-classname=R}, {@code --exclude-classname=R}: class-name regexes
 *       ({@link ClassNameFilter}); with no include, the console launcher's default
 *       {@link ClassNameFilter#STANDARD_INCLUDE_PATTERN} applies, as it always did;</li>
 *   <li>{@code --fail-if-no-tests}: an empty selection FAILS.</li>
 * </ul>
 * Any other argument fails the run: a misspelt selector must not widen or empty a lane.
 *
 * <p>THE PROTOCOL.
 * <ul>
 *   <li>{@code XML_OUTPUT_FILE}: the legacy JUnit XML of every engine, merged into one
 *       {@code <testsuites>} document — Bazel's test.xml lists real testcases;</li>
 *   <li>{@code TESTBRIDGE_TEST_ONLY} ({@code --test_filter}): a regex found in
 *       {@code fully.qualified.Class#method} (Bazel's JUnit4 runner's form); a filter
 *       that matches nothing fails the run;</li>
 *   <li>{@code TEST_TOTAL_SHARDS}/{@code TEST_SHARD_INDEX}/{@code TEST_SHARD_STATUS_FILE}:
 *       round-robin over each engine's selected tests in unique-id order;
 *       the empty-selection check counts the UNSHARDED selection, so a shard that drew
 *       no test passes;</li>
 *   <li>{@code TEST_PREMATURE_EXIT_FILE}: created before the run, deleted after it, so a
 *       test that calls {@code System.exit} leaves it behind and Bazel fails the target.</li>
 *   <li>{@code TEST_TMPDIR}: the JVM's temp directory (see pinTempDirectory).</li>
 * </ul>
 *
 * <p>EXIT CODES. 0 all selected tests passed; 1 a test, container or engine failed; 2 nothing was selected
 * ({@code --fail-if-no-tests}, or a {@code --test_filter} that matched nothing); 3 a sharded run whose engine
 * ignored the split; 4 the runner itself failed (a bad argument, a bad filter, an unwritable test.xml).
 *
 * <p>JUNIT 3 SUITES. The vintage engine cannot filter inside a nested or {@code TestSetup}-wrapped
 * suite (the PCT classes), so a test it was told to skip runs anyway. Sharded, that would run the
 * whole class in every shard: the run FAILS instead (the overrun guard). Such a lane is split into
 * one target per class (Bazel workplan D2). Under {@code --test_filter} it is only a warning:
 * running more than asked is harmless.
 *
 * <p>OUTPUTS. A test writes its own outputs under {@code TEST_UNDECLARED_OUTPUTS_DIR}, read from its environment;
 * one pass that hands another its output is a build action ({@link JUnitAction}), never a child JVM of this one
 * (Bazel workplan P3-01).
 */
public final class JUnitMain {

    private JUnitMain() {}

    public static void main(String[] args) throws Exception {
        pinTempDirectory();
        watchHeap();
        int code = 4;
        try {
            code = protocol(args, System::getenv);
        } catch (Exception | Error e) {
            // a bad argument, a bad --test_filter regex, an unwritable test.xml: reported, and the JVM still
            // exits, so a test's non-daemon thread cannot hold it open until the timeout
            e.printStackTrace();
        } finally {
            System.out.flush();
            System.err.flush();
            // Exit explicitly: a test that leaves a non-daemon thread must not hang the run.
            System.exit(code);
        }
    }

    /** One run under Bazel's protocol, its environment read through {@code env} (the runner's own tests
     *  give it one): the premature-exit file exists exactly while the tests run, so a test that calls
     *  {@code System.exit} leaves it behind and Bazel fails the target. */
    static int protocol(String[] args, Function<String, String> env) throws Exception {
        Path exitFile = touch(env.apply("TEST_PREMATURE_EXIT_FILE"));
        try {
            return run(args, env);
        } finally {
            // removed on every way out of the run that is not System.exit, so a runner error is not
            // reported as a premature exit
            if (exitFile != null) {
                Files.deleteIfExists(exitFile);
            }
        }
    }

    // ── selection ────────────────────────────────────────────────────────────────

    static int run(String[] args, Function<String, String> env) throws Exception {
        List<DiscoverySelector> selectors = new ArrayList<>();
        List<String> includeTags = new ArrayList<>();
        List<String> excludeTags = new ArrayList<>();
        List<String> includeNames = new ArrayList<>();
        List<String> excludeNames = new ArrayList<>();
        List<String> excludePackages = new ArrayList<>();
        boolean failIfNoTests = false;
        for (String arg : args) {
            int eq = arg.indexOf('=');
            String key = eq < 0 ? arg : arg.substring(0, eq);
            String value = eq < 0 ? "" : arg.substring(eq + 1);
            switch (key) {
                case "--select-package" -> selectors.add(DiscoverySelectors.selectPackage(value));
                case "--select-class" -> selectors.add(DiscoverySelectors.selectClass(value));
                case "--select-method" -> selectors.add(DiscoverySelectors.selectMethod(value));
                case "--include-tag" -> includeTags.add(value);
                case "--exclude-tag" -> excludeTags.add(value);
                case "--exclude-package" -> excludePackages.add(value);
                case "--include-classname" -> includeNames.add(value);
                case "--exclude-classname" -> excludeNames.add(value);
                case "--fail-if-no-tests" -> failIfNoTests = true;
                default -> throw new IllegalArgumentException("JUnitMain: unknown argument " + arg);
            }
        }
        if (selectors.isEmpty()) {
            throw new IllegalArgumentException("JUnitMain: no --select-* argument");
        }
        String totalShards = env.apply("TEST_TOTAL_SHARDS");
        String shardIndex = env.apply("TEST_SHARD_INDEX");
        if (totalShards != null && Integer.parseInt(totalShards) > 1) {
            touch(env.apply("TEST_SHARD_STATUS_FILE"));
        }
        LauncherDiscoveryRequestBuilder request = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectors)
                .filters(ClassNameFilter.includeClassNamePatterns(includeNames.isEmpty()
                        ? new String[] {ClassNameFilter.STANDARD_INCLUDE_PATTERN}
                        : includeNames.toArray(String[]::new)));
        if (!excludeNames.isEmpty()) {
            request.filters(ClassNameFilter.excludeClassNamePatterns(excludeNames.toArray(String[]::new)));
        }
        if (!excludePackages.isEmpty()) {
            request.filters(PackageNameFilter.excludePackageNames(excludePackages));
        }
        // The Vintage engine runs JUnit 3/4 tests and refuses to start without JUnit 4. Only the targets
        // that run such tests carry JUnit 4 (pct, parser-equivalence and //tools/junit:runner_test take the upstream pool's; a
        // JUnit 4 test cannot compile without it), so on every other target the engine has
        // nothing it could select: left out, not failed. The console launcher's fat jar bundled its own
        // JUnit 4 instead, a second copy beside the upstream one (Bazel workplan P1-01).
        if (!onClasspath("junit.runner.Version")) {
            request.filters(EngineFilter.excludeEngines("junit-vintage"));
        }
        // the tag filters are folded into the Bazel filter, which must see exactly what they keep
        List<PostDiscoveryFilter> tags = new ArrayList<>();
        if (!includeTags.isEmpty()) {
            tags.add(TagFilter.includeTags(includeTags));
        }
        if (!excludeTags.isEmpty()) {
            tags.add(TagFilter.excludeTags(excludeTags));
        }
        BazelFilter bazel = new BazelFilter(tags, env.apply("TESTBRIDGE_TEST_ONLY"),
                totalShards, shardIndex);
        request.filters(bazel);
        return execute(request.build(), bazel, failIfNoTests, env.apply("XML_OUTPUT_FILE"));
    }

    private static int execute(LauncherDiscoveryRequest request, BazelFilter bazel, boolean failIfNoTests,
            String out) throws Exception {
        Path xml = Files.createTempDirectory("junit-xml");
        SummaryGeneratingListener summary = new SummaryGeneratingListener();
        LegacyXmlReportGeneratingListener legacy = new LegacyXmlReportGeneratingListener(xml,
                new PrintWriter(System.err, true));
        Launcher launcher = LauncherFactory.create();
        // A test the filter EXCLUDED that runs anyway: an engine refused the exclusion
        // (vintage cannot filter a nested/TestSetup-wrapped JUnit 3 suite).
        AtomicLong overrun = new AtomicLong();
        TestExecutionListener guard = new TestExecutionListener() {
            @Override
            public void executionStarted(TestIdentifier id) {
                if (id.isTest() && bazel.excluded.contains(id.getUniqueIdObject())) {
                    overrun.incrementAndGet();
                }
            }
        };
        launcher.execute(request, summary, legacy, guard);

        TestExecutionSummary result = summary.getSummary();
        StringWriter text = new StringWriter();
        result.printTo(new PrintWriter(text));
        result.printFailuresTo(new PrintWriter(text), 25);
        System.out.println(text);
        System.out.printf("[bazel] selected %d tests (a parameterized/dynamic container counts once)%s%s%n", bazel.selected(),
                bazel.sharded() ? ", shard " + bazel.index + "/" + bazel.total + " ran " + result.getTestsFoundCount() : "",
                bazel.filter != null ? ", --test_filter=" + bazel.filter : "");

        if (out != null) {
            mergeXml(xml, Path.of(out));
        }
        System.out.println(peakHeap());
        if (overrun.get() > 0) {
            System.err.println("[bazel] " + overrun.get() + " tests ran that the "
                    + (bazel.sharded() ? "shard split" : "--test_filter")
                    + " excluded: an engine could not filter them (a JUnit 3 suite under vintage).");
            if (bazel.sharded()) {
                System.err.println("[bazel] sharding this target would run them in EVERY shard:"
                        + " split it into one target per class instead (Bazel workplan D2).");
                return 3;
            }
        }
        if (result.getTotalFailureCount() > 0) {
            return 1;
        }
        if (bazel.selected() == 0 && (failIfNoTests || bazel.filter != null)) {
            System.err.println(bazel.filter != null
                    ? "[bazel] --test_filter=" + bazel.filter + " matched no test"
                    : "[bazel] the selection found no tests (--fail-if-no-tests)");
            return 2;
        }
        return 0;
    }

    /** The tag filters, TESTBRIDGE_TEST_ONLY and the shard split, as ONE post-discovery
     *  filter, which also counts the selection BEFORE sharding.
     *
     *  <p>SHARDING is round-robin over each engine's selected tests sorted by unique id
     *  (Bazel's own JUnit4 runner's default): balanced even for a handful of tests,
     *  and independent of discovery order. Each engine starts at its own offset so
     *  single-test engines do not all land in shard 0. */
    static final class BazelFilter implements PostDiscoveryFilter {
        private final List<PostDiscoveryFilter> tags;
        final String filter;
        private final Pattern pattern;
        final int total;
        final int index;
        private final AtomicLong selected = new AtomicLong();
        final java.util.Set<UniqueId> excluded = java.util.concurrent.ConcurrentHashMap.newKeySet();
        private final Map<TestDescriptor, Map<UniqueId, Integer>> ordinals = new HashMap<>();

        BazelFilter(List<PostDiscoveryFilter> tags, String filter, String total, String index) {
            this.tags = tags;
            this.filter = filter == null || filter.isEmpty() ? null : filter;
            this.pattern = this.filter == null ? null : Pattern.compile(this.filter);
            this.total = total == null ? 1 : Integer.parseInt(total);
            this.index = index == null ? 0 : Integer.parseInt(index);
        }

        boolean sharded() { return total > 1; }

        long selected() { return selected.get(); }

        /** A unit the filter decides on: a test, or a container that registers its tests at
         *  run time (parameterized, dynamic, template) — whose children a post-discovery
         *  filter never sees, so it is filtered and sharded as one. */
        private static boolean unit(TestDescriptor d) {
            return d.isTest() || d.mayRegisterTests();
        }

        private boolean keeps(TestDescriptor d) {
            for (PostDiscoveryFilter f : tags) {
                if (f.apply(d).excluded()) {
                    return false;
                }
            }
            return pattern == null || pattern.matcher(name(d)).find();
        }

        @Override
        public FilterResult apply(TestDescriptor d) {
            if (!unit(d)) {
                return FilterResult.included("container");
            }
            if (!keeps(d)) {
                if (pattern != null) {
                    excluded.add(d.getUniqueId());
                }
                return FilterResult.excluded("tag filter or --test_filter");
            }
            selected.incrementAndGet();
            if (sharded()) {
                TestDescriptor root = d;
                while (root.getParent().isPresent()) {
                    root = root.getParent().get();
                }
                Integer ordinal = ordinals.computeIfAbsent(root, this::number).get(d.getUniqueId());
                int offset = Math.floorMod(root.getUniqueId().toString().hashCode(), total);
                if (ordinal == null) {
                    // every selected unit was numbered before any was removed: a missing one is a runner bug,
                    // and excluding it would run it in no shard
                    throw new IllegalStateException("[bazel] no shard ordinal for " + d.getUniqueId());
                }
                if ((ordinal + offset) % total != index) {
                    excluded.add(d.getUniqueId());
                    return FilterResult.excluded("another shard");
                }
            }
            return FilterResult.included("selected");
        }

        private Map<UniqueId, Integer> number(TestDescriptor root) {
            List<UniqueId> ids = new ArrayList<>();
            for (TestDescriptor t : root.getDescendants()) {
                if (unit(t) && keeps(t)) {
                    ids.add(t.getUniqueId());
                }
            }
            ids.sort(Comparator.comparing(UniqueId::toString));
            Map<UniqueId, Integer> out = new HashMap<>();
            for (int i = 0; i < ids.size(); i++) {
                out.put(ids.get(i), i);
            }
            return out;
        }

        /** {@code pkg.Class#method} — the form Bazel's own JUnit runner filters on. */
        private static String name(TestDescriptor d) {
            for (TestDescriptor at = d; at != null; at = at.getParent().orElse(null)) {
                TestSource s = at.getSource().orElse(null);
                if (s instanceof MethodSource m) {
                    return m.getClassName() + "#" + m.getMethodName();
                }
                if (s instanceof ClassSource c) {
                    return c.getClassName() + "#" + d.getDisplayName();
                }
            }
            return d.getUniqueId().toString();
        }
    }

    // ── the XML ──────────────────────────────────────────────────────────────────

    /** One {@code <testsuites>} document from the per-engine TEST-*.xml files. */
    private static void mergeXml(Path dir, Path out) throws Exception {
        var builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document merged = builder.newDocument();
        Element root = merged.createElement("testsuites");
        merged.appendChild(root);
        try (Stream<Path> files = Files.list(dir)) {
            for (Path f : files.sorted().toList()) {
                Element suite = builder.parse(f.toFile()).getDocumentElement();
                // every engine's suite, an empty one included: an engine whose root failed (a discovery
                // error) writes no testcase and zero counts, and leaving it out would hide it
                // the JVM's whole system-property table: noise, and it names the machine
                var props = suite.getElementsByTagName("properties");
                while (props.getLength() > 0) {
                    props.item(0).getParentNode().removeChild(props.item(0));
                }
                root.appendChild(merged.importNode(suite, true));
            }
        }
        Files.createDirectories(out.toAbsolutePath().getParent());
        var t = TransformerFactory.newInstance().newTransformer();
        t.setOutputProperty(OutputKeys.ENCODING, StandardCharsets.UTF_8.name());
        t.transform(new DOMSource(merged), new StreamResult(out.toFile()));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    /**
     * The JVM's temp directory is the test's own ({@code TEST_TMPDIR}, which Bazel creates per test and
     * cleans), never the host's /tmp, where files outlive the run and collide across runs. Set before
     * anything creates a temp file: the JDK reads {@code java.io.tmpdir} on first use. A Bazel test
     * always has TEST_TMPDIR; without it this is not a Bazel test.
     */
    private static void pinTempDirectory() {
        String tmp = System.getenv("TEST_TMPDIR");
        if (tmp == null || tmp.isEmpty()) {
            throw new IllegalStateException("TEST_TMPDIR is not set: JUnitMain runs under `bazel test`");
        }
        System.setProperty("java.io.tmpdir", tmp);
    }

    /** The heap's live peak: the most still in use right after a collection, across every collection of
     *  the run. With the most in use right before one, it is the measurement a target's
     *  {@code junit_test(memory_mb = …)} is set from (Bazel workplan P1-21). Each pool's own peak is no
     *  measure: the pools peak at different moments, so their sum exceeds the heap (it read 11 GB for an
     *  8 GB JVM). A collection's before/after usage is one simultaneous snapshot of every pool. */
    private static final AtomicLong LIVE_PEAK = new AtomicLong();
    private static final AtomicLong USED_PEAK = new AtomicLong();

    private static void watchHeap() {
        List<String> heapPools = new ArrayList<>();
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
                heapPools.add(pool.getName());
            }
        }
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (!(gc instanceof NotificationEmitter emitter)) {
                continue;
            }
            emitter.addNotificationListener((notification, handback) -> {
                if (!notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
                    return;
                }
                var info = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData()).getGcInfo();
                LIVE_PEAK.accumulateAndGet(sum(info.getMemoryUsageAfterGc(), heapPools), Math::max);
                USED_PEAK.accumulateAndGet(sum(info.getMemoryUsageBeforeGc(), heapPools), Math::max);
            }, null, null);
        }
    }

    private static long sum(Map<String, MemoryUsage> usage, List<String> pools) {
        long total = 0;
        for (String pool : pools) {
            MemoryUsage u = usage.get(pool);
            if (u != null) {
                total += u.getUsed();
            }
        }
        return total;
    }

    /** A run that never collected has no snapshot: its whole heap in use now stands for both. */
    private static String peakHeap() {
        long now = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        long live = Math.max(LIVE_PEAK.get(), USED_PEAK.get() == 0 ? now : 0);
        long used = Math.max(USED_PEAK.get(), now);
        long max = Runtime.getRuntime().maxMemory();
        return "[bazel] heap: live peak " + (live >> 20) + " MB (in use after a collection), peak "
                + (used >> 20) + " MB before one, of " + (max >> 20) + " MB";
    }

    private static boolean onClasspath(String className) {
        try {
            Class.forName(className, false, JUnitMain.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static Path touch(String file) throws IOException {
        if (file == null || file.isEmpty()) {
            return null;
        }
        Path p = Path.of(file);
        if (Files.exists(p)) {
            Files.setLastModifiedTime(p, FileTime.fromMillis(System.currentTimeMillis()));
        } else {
            Files.createDirectories(p.toAbsolutePath().getParent());
            Files.createFile(p);
        }
        return p;
    }
}
