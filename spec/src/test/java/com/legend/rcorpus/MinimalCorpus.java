// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.rcorpus;

import com.legend.test.PureTestRunner;
import com.legend.test.PureTests;
import com.legend.test.TestObserver;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.model.FunctionDefinition;
import com.legend.model.ImportScope;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
import com.legend.model.StereotypeApplication;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.ValueSpecification;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * THE MINIMAL CORPUS HARNESS (docs/HARNESS_FROM_SCRATCH_AUDIT_2026_09_06.md,
 * rebuilt from the spec in its §1): six pieces and nothing else.
 *
 * <ol>
 * <li><b>find</b> — the engine's {@code core_relational} sources, parsed by
 *     the platform; a test is a {@code <<test.Test>>} function, a setup a
 *     {@code <<test.BeforePackage>>} function; the engine's own exclusion
 *     stereotypes ({@code ToFix}, {@code Ignore}, {@code ExcludeAlloy})
 *     are honoured;</li>
 * <li><b>assemble</b> — ONE model through {@link Compiler#parseSources} /
 *     {@link Compiler#buildModule}, every corpus database bound to one
 *     in-memory connection by an execution overlay;</li>
 * <li><b>seed + session</b> — one database workspace per test PACKAGE
 *     (the engine's grouping), the package's setups run THROUGH THE
 *     PLATFORM once per session, every raw SQL a body executes recorded
 *     for the referee (the platform's {@link
 *     com.legend.sql.dialect.RawSqlBoundary} ledger); a test that carries
 *     its own inline data runs on a private workspace;</li>
 * <li><b>run + judge</b> — the PRODUCT's test runner ({@link PureTestRunner},
 *     upstream boundary batch 7a): sessions, setups, the body through the ONE
 *     production entry with the platform's {@link com.legend.exec.AssertListener};
 *     every assert is the platform's verdict; the harness attaches its referee
 *     and its census through {@link TestObserver} and owns no judgment;</li>
 * <li><b>referee</b> — {@link com.legend.harness.ReplayOracle#INSTANCE}
 *     (golden SQL and plan text brought to rows on the engine's own H2);</li>
 * <li><b>score</b> — PASS / FAIL per test with the failure's reason; the
 *     rosters are the deliverable.</li>
 * </ol>
 *
 * <p>There is no second executor, no recognized "test forms", no census.
 * A body the platform cannot run is a FAIL with the platform's own
 * message as its reason.
 */
public final class MinimalCorpus {

    /** A test's outcome. SKIPPED (Phase 0.3): the body ran without a
     * failure but adjudicated NO verdict and the platform states the
     * program reaches no verdict function — such a test proves nothing
     * and is never counted as a pass. */
    /** ACCEPTED = a DECIDED divergence (USER, 2026-09-08): the test fails,
     * the failure carries the register's witness, and the trace bucket says
     * why the engine's golden is not the spec; never a pass, never hidden. */
    public enum Status { PASS, FAIL, SKIPPED, ACCEPTED }

    /** What a PASS proves (Phase 0.7, audit §3's ladder), derived from the
     * verdict log the runner reports for the test — never from reading its
     * body: DIFFERENTIAL = the referee matched a rows leg against the
     * engine's golden (the strongest witness; whether a literal assert
     * also held is a second census); LITERAL = a value assert was judged
     * with no referee involved; CARDINALITY = only size / emptiness /
     * boolean asserts; SPELLING = every verdict was decided by text
     * (declined); NONE = no verdict (never a pass since batch 128). */
    public enum Strength { DIFFERENTIAL, LITERAL, CARDINALITY, SPELLING, NONE }

    /** One outcome with its reason. {@code verdicts} = assert verdicts
     * the platform reported. */
    public record Result(String fqn, Status status, int verdicts, String reason,
            Strength strength, boolean literalToo) {
        public Result(String fqn, Status status, int verdicts, String reason) {
            this(fqn, status, verdicts, reason, Strength.NONE, false);
        }

        public boolean pass() {
            return status == Status.PASS;
        }
    }

    /** The asserts that pin a COUNT or a boolean, not a value (the catalog's
     * assert family by exact FQN). */
    private static final Set<String> CARDINALITY_ASSERTS = Set.of(
            "meta::pure::functions::asserts::assert",
            "meta::pure::functions::asserts::assertFalse",
            "meta::pure::functions::asserts::assertSize",
            "meta::pure::functions::asserts::assertEmpty",
            "meta::pure::functions::asserts::assertNotEmpty");

    private static final String RUNTIME = "rcorpus::Rt";
    private static final String CONNECTION = "rcorpus::Conn";
    /** Corpus files that are the ENGINE'S IMPLEMENTATION of a platform-owned
     * function family, not test input: loaded, they would redefine the
     * family as user Pure and every test would resolve to the engine's
     * implementation instead of the platform's (measured, batch 134: with
     * this file admitted the 49 {@code lineage::scanRelations} tests inline
     * the engine's {@code scanRelations} and wall on {@code
     * openVariableValues}). The reference checkout is spec, never runtime
     * (user ruling 2026-08-28). Skipped BY NAME and reported — the file
     * defines no test (0 {@code <<test.Test>>}). */
    private static final Map<String, String> ENGINE_IMPLEMENTATION_FILES = Map.of(
            "lineage/scanRelations/scanRelations.pure",
            "the engine's implementation of the platform-owned meta::pure::lineage::scanRelations family");
    /** The exclusion keys, RELATIONAL-relative — for the upstream path manifest
     *  ({@code UpstreamPathManifestTest}): a key that names no file admits the
     *  engine's implementation silently, which is exactly what it must not do. */
    public static java.util.Set<String> engineImplementationFileKeys() {
        return ENGINE_IMPLEMENTATION_FILES.keySet();
    }
    /** The graphFetch domain model (engine-core), an optional LIBRARY input
     *  beside {@link Corpus#M2M_TESTS}. ENGINE_ROOT-relative. */
    public static final String GRAPH_FETCH_DOMAIN =
            "legend-engine-core/legend-engine-core-pure/"
            + "legend-engine-pure-code-compiled-core/"
            + "src/main/resources/core/pure/graphFetch/domain";
    /** The engine-implementation files skipped, with their reason (reported). */
    private final List<String> engineImplementationSkips = new ArrayList<>();
    /** Upstream inputs this run could NOT find — a named SHAPE/LIBRARY file, a
     *  library directory, or an exclusion key that matched no file. LOUD: the
     *  corpus test fails on a non-empty list (upstream boundary program batch
     *  2, 2026-09-10 — until then each of these was a silent {@code continue},
     *  and a starved input passes green: batch 155 traded the prelude's parity
     *  guard for exactly this class of silence). */
    private final List<String> missingInputs = new ArrayList<>();

    public List<String> missingInputs() {
        return List.copyOf(missingInputs);
    }

    public List<String> engineImplementationSkips() {
        return List.copyOf(engineImplementationSkips);
    }

    private final ModelContext ctx;

    /** The corpus's ONE compiled world (libraries, shapes, every corpus
     * file) — the eager-compile probe types every body in it
     * (COMPILE_EVERYTHING_HOMEWORK, P3: "everything compiles when needed"). */
    public ModelContext context() {
        return ctx;
    }

    private final Map<String, String> elementSources;
    private final List<Compiler.ModelSource> sources;

    /** Every source unit the corpus world was parsed from (the eager-compile
     * probe rebuilds the world with more files to measure what they close). */
    public List<Compiler.ModelSource> sources() {
        return sources;
    }

    /** Element FQN → the source unit it was parsed from (the eager-compile
     * probe attributes failures by file). */
    public Map<String, String> elementSources() {
        return elementSources;
    }
    /** Zero-arg functions of the SHARED fixture sources (the corpus-wide
     * setup — relationalSetUp.pure's createTablesAndFillDb family). */
    private final List<String> sharedSetups = new ArrayList<>();
    private final Map<String, ValueSpecification> sharedSetupPrograms = new LinkedHashMap<>();
    /** Library files skipped because they do not parse (reported). */
    private final List<String> libraryWalls = new ArrayList<>();
    /** Elements defined by library sources (model only, never tests). */
    private final Set<String> libraryElements = new LinkedHashSet<>();
    /** The classes and enums admitted from {@link Corpus#SHAPE_FILES}. */
    private final Set<String> shapeElements = new LinkedHashSet<>();

    public List<String> libraryWalls() {
        return List.copyOf(libraryWalls);
    }

    private static final java.util.concurrent.atomic.AtomicInteger SESSION_IDS =
            new java.util.concurrent.atomic.AtomicInteger();

    /** The product's discovery over the corpus world, and its runner. */
    private final PureTests.Discovery discovery;
    private final PureTestRunner runner;

    // ---- FIND + ASSEMBLE --------------------------------------------------

    public MinimalCorpus() throws IOException {
        List<Compiler.ModelSource> shared = sharedSources();
        List<Compiler.ModelSource> all = new ArrayList<>(shared);
        Set<String> seen = new LinkedHashSet<>();
        for (Compiler.ModelSource s : shared) {
            seen.add(s.text());
        }
        java.util.Set<String> matchedExclusionKeys = new java.util.HashSet<>();
        for (Path f : corpusFiles()) {
            // '/' ALWAYS: the keys above are forward-slash relative paths, and
            // Path.toString uses the PLATFORM separator — on Windows nothing
            // matched, so the engine's scanRelations implementation was
            // admitted as user Pure and inlined over the platform's, failing
            // exactly the 49 lineage tests this map exists to protect
            // (Windows CI gate 4, 2026-09-09 — the count matched the batch-134
            // measurement in the comment above to the test).
            String rel = Corpus.RELATIONAL.relativize(f).toString()
                    .replace(java.io.File.separatorChar, '/');
            if (ENGINE_IMPLEMENTATION_FILES.containsKey(rel)) {
                engineImplementationSkips.add(rel + " — " + ENGINE_IMPLEMENTATION_FILES.get(rel));
                matchedExclusionKeys.add(rel);
                continue;
            }
            String text = Files.readString(f);
            if (seen.add(text)) {
                all.add(new Compiler.ModelSource(
                        Corpus.RELATIONAL.relativize(f).toString()
                                .replace(java.io.File.separatorChar, '/'), text));
            }
        }
        // an exclusion key that matched NO file: the engine's implementation
        // it names would have been admitted as user Pure (Windows CI 2026-09-09
        // found this by the separator; a moved upstream file finds it the same
        // way) — reported, never silent
        for (String key : ENGINE_IMPLEMENTATION_FILES.keySet()) {
            if (!matchedExclusionKeys.contains(key)) {
                missingInputs.add("ENGINE_IMPLEMENTATION_FILES key matched no corpus file: "
                        + key + " (under " + Corpus.RELATIONAL + ")");
            }
        }
        // LIBRARY sources are optional inputs (the platform's own M2M test
        // models, the graphFetch domain, two named engine files): one that
        // does not parse is skipped BY NAME — reported, never silent
        for (Path f : libraryFiles()) {
            String text = Files.readString(f);
            if (!seen.add(text)) {
                continue;
            }
            Compiler.ModelSource src = new Compiler.ModelSource(
                    "library/" + f.getFileName(), text);
            List<String> walls = new ArrayList<>();
            Compiler.parseSources(List.of(src), (name, err) -> walls.add(err),
                    com.legend.parser.Dialect.LEGEND_PLATFORM);
            if (walls.isEmpty()) {
                // library sources contribute MODEL only: their own test
                // functions (the platform's M2M suites) are not this corpus
                Compiler.ParsedModule one = Compiler.parseSources(List.of(src),
                        (name, err) -> { }, com.legend.parser.Dialect.LEGEND_PLATFORM);
                refusePlatformNamespace(one.model().elements());
                all.add(src);
                for (PackageableElement el : one.model().elements()) {
                    libraryElements.add(el.qualifiedName());
                }
            } else {
                libraryWalls.add(f.getFileName() + " => " + walls.get(0));
            }
        }
        List<String> parseWalls = new ArrayList<>();
        Compiler.ParsedModule parsed = Compiler.parseSources(all,
                (name, err) -> parseWalls.add(name + " => " + err),
                com.legend.parser.Dialect.LEGEND_PLATFORM);
        // EVERY source (the corpus's own files included) is under the
        // platform-namespace guard: nothing the harness loads may define
        // the platform's stdlib
        refusePlatformNamespace(parsed.model().elements());
        if (!parseWalls.isEmpty()) {
            throw new IllegalStateException("corpus parse walls: " + parseWalls);
        }
        if (!parsed.duplicateElements().isEmpty()) {
            throw new IllegalStateException("corpus duplicate elements: "
                    + parsed.duplicateElements());
        }
        // SHAPE files (Corpus.SHAPE_FILES, batch 155): each named engine
        // file's CLASSES AND ENUMS join the graph — its functions do not.
        // A shape the corpus tree already declares keeps the corpus's own
        // (first definition wins, as parseSources does); a shape the prelude
        // declares yields to the prelude later (Compiler.withoutPreludeShadows,
        // the T4 receipt list). Each element keeps its section's imports.
        parsed = withShapes(parsed);
        Compiler.BuiltModule built = Compiler.buildModule(parsed.model());
        Map<String, String> dbBindings = new LinkedHashMap<>();
        for (PackageableElement el : parsed.model().elements()) {
            if (el instanceof com.legend.model.DatabaseDefinition db) {
                dbBindings.put(db.qualifiedName(), CONNECTION);
            }
        }
        ctx = ((com.legend.compiler.element.PureModelContext) built.context())
                .withExecutionOverlay(
                        new com.legend.model.RuntimeDefinition(RUNTIME, List.of(),
                                dbBindings, List.of()),
                        new com.legend.model.ConnectionDefinition(CONNECTION, null,
                                H2_BACKEND
                                        ? com.legend.model.ConnectionDefinition.DatabaseType.H2
                                        : com.legend.model.ConnectionDefinition.DatabaseType.DuckDB,
                                new com.legend.model.ConnectionSpecification.InMemory(),
                                new com.legend.model.AuthenticationSpec.NoAuth()));
        discovery = PureTests.discover(parsed.model(), libraryElements);
        elementSources = Map.copyOf(parsed.model().elementSources());
        sources = List.copyOf(all);
        // the shared fixture's own zero-arg functions (parsed apart so
        // their FQNs are known without an element→source index)
        Compiler.ParsedModule sharedParsed = Compiler.parseSources(shared,
                (name, err) -> { }, com.legend.parser.Dialect.LEGEND_PLATFORM);
        for (PackageableElement el : sharedParsed.model().elements()) {
            // a TEST is never a setup, whatever its effects; a zero-arg
            // fixture function is a setup only when the PLATFORM says its
            // body has statement effects (Phase 0.8 — the arity rule alone
            // nominated testRuntime(), the type-inference maps, … as
            // "inert setups"; the fact decides, not the arity)
            if (el instanceof FunctionDefinition f && f.parameters().isEmpty()
                    && f.stereotypes().stream().noneMatch(st ->
                            st.stereotypeName().equals("Test"))) {
                ValueSpecification resolved = Compiler.resolveQuery(
                        List.of(new AppliedFunction(f.qualifiedName(), List.of())),
                        new ImportScope(List.of()), ctx);
                if (Compiler.hasStatementEffects(resolved, ctx)) {
                    sharedSetups.add(f.qualifiedName());
                    sharedSetupPrograms.put(f.qualifiedName(), resolved);
                }
            }
        }
        runner = new PureTestRunner(ctx, RUNTIME, MinimalCorpus::openSession, sharedSetups,
                discovery.setupsByPackage(), new CorpusObserver());
        sharedSetupPrograms.forEach(runner::registerSetup);
    }

    /** {@code parsed} plus the classes and enums of every SHAPE file. */
    private Compiler.ParsedModule withShapes(Compiler.ParsedModule parsed) throws IOException {
        Set<String> declared = new LinkedHashSet<>();
        for (PackageableElement el : parsed.model().elements()) {
            declared.add(el.qualifiedName());
        }
        List<PackageableElement> elements = new ArrayList<>(parsed.model().elements());
        Map<String, Integer> offsets = new LinkedHashMap<>(parsed.model().elementOffsets());
        Map<String, ImportScope> imports = new LinkedHashMap<>(parsed.model().elementImports());
        Map<String, String> sources = new LinkedHashMap<>(parsed.model().elementSources());
        for (Path f : Corpus.SHAPE_FILES) {
            if (!Files.isRegularFile(f)) {
                missingInputs.add("SHAPE_FILES entry is not a file: " + f);
                continue;
            }
            String name = "shape/" + f.getFileName();
            List<String> walls = new ArrayList<>();
            Compiler.ParsedModule one = Compiler.parseSources(
                    List.of(new Compiler.ModelSource(name, Files.readString(f))),
                    (n, err) -> walls.add(err), com.legend.parser.Dialect.LEGEND_PLATFORM);
            if (!walls.isEmpty()) {
                libraryWalls.add(f.getFileName() + " => " + walls.get(0));
                continue;
            }
            for (PackageableElement el : one.model().elements()) {
                boolean shape = el instanceof com.legend.model.ClassDefinition
                        || el instanceof com.legend.model.EnumDefinition;
                if (!shape || !declared.add(el.qualifiedName())) {
                    continue;
                }
                elements.add(el);
                Integer off = one.model().elementOffsets().get(el.qualifiedName());
                if (off != null) {
                    offsets.put(el.qualifiedName(), off);
                }
                ImportScope scope = one.model().elementImports().get(el.qualifiedName());
                if (scope != null) {
                    imports.put(el.qualifiedName(), scope);
                }
                sources.put(el.qualifiedName(), name);
                shapeElements.add(el.qualifiedName());
            }
        }
        Map<String, String> texts = new LinkedHashMap<>(parsed.sourceTexts());
        return new Compiler.ParsedModule(
                new com.legend.model.ParsedModel(elements, parsed.model().imports(), null,
                        offsets, imports, sources, parsed.model().unclaimedSections()),
                parsed.duplicateElements(), texts);
    }

    private static List<Compiler.ModelSource> sharedSources() throws IOException {
        List<Compiler.ModelSource> out = new ArrayList<>();
        int i = 0;
        for (String rel : List.of("tests/testModel/simpleTestModel.pure",
                "tests/testModel/inheritanceTestModel.pure",
                "tests/relationalSetUp.pure", "relationalExtension.pure")) {
            out.add(new Compiler.ModelSource("shared-" + i++ + ".pure", Corpus.read(rel)));
        }
        return out;
    }

    /** A platform-independent ordering key: the path with '/' separators.
     *  Sorting Paths directly is CASE-INSENSITIVE on Windows and case-
     *  sensitive on POSIX, which reorders the whole corpus. */
    private static String sortKey(Path p) {
        return p.toString().replace(java.io.File.separatorChar, '/');
    }

    private static List<Path> corpusFiles() throws IOException {
        try (Stream<Path> walk = Files.walk(Corpus.RELATIONAL)) {
            return walk.filter(f -> f.toString().endsWith(".pure") && Files.isRegularFile(f))
                    // ORDER BY THE ID, not the Path: Windows's Path.compareTo
                    // is CASE-INSENSITIVE, so a Path sort gives a DIFFERENT
                    // corpus order there — and this order decides the model
                    // assembly and the dedup below (Windows CI, 2026-09-09)
                    .sorted(java.util.Comparator.comparing(MinimalCorpus::sortKey))
                    .toList();
        }
    }

    private List<Path> libraryFiles() throws IOException {
        List<Path> out = new ArrayList<>();
        Path gfDomain = Corpus.ENGINE_ROOT.resolve(GRAPH_FETCH_DOMAIN);
        for (Path dir : List.of(Corpus.M2M_TESTS, gfDomain)) {
            if (Files.isDirectory(dir)) {
                try (Stream<Path> s = Files.walk(dir)) {
                    out.addAll(s.filter(f -> f.toString().endsWith(".pure"))
                            .sorted(java.util.Comparator.comparing(MinimalCorpus::sortKey))
                            .toList());
                }
            } else {
                missingInputs.add("library directory is not a directory: " + dir);
            }
        }
        for (Path lib : Corpus.LIBRARY_FILES) {
            if (Files.isRegularFile(lib)) {
                out.add(lib);
            } else {
                missingInputs.add("LIBRARY_FILES entry is not a file: " + lib);
            }
        }
        return out;
    }

    /** The runnable tests: every {@code <<test.Test>>} the engine's own
     *  marks do not exclude, in the engine suite's order. */
    public List<PureTests.TestCase> tests() {
        return discovery.runnable();
    }

    /** The DENOMINATOR, re-derived from the model every run (Phase 0.8):
     * {@code declared} = every {@code <<test.Test>>} function the corpus
     * defines, {@code excluded} = those the engine's own stereotypes take
     * out (ToFix / ExcludeAlloy), {@code discovered} = the runnable rest.
     * The run prints the triple and pins it against a comment-stripped
     * text scan of the corpus tree, so a bigger or smaller corpus, or a
     * discovery rule that drops a test, is loud. */
    public record Census(int declared, int excluded, int discovered) {
    }

    public Census census() {
        int declared = discovery.tests().size();
        int runnable = discovery.runnable().size();
        return new Census(declared, declared - runnable, runnable);
    }

    // ---- SESSION (the harness's connection kinds) ---------------------------

    /** {@code -Drcorpus.backend=h2}: the PORTABILITY lane — every session is
     * a fresh in-memory H2 with the engine's session settings instead of a
     * DuckDB workspace; the platform's dialect follows the connection. */
    static final boolean H2_BACKEND =
            "h2".equalsIgnoreCase(System.getProperty("rcorpus.backend", ""));

    private static Connection openSession() throws SQLException {
        if (H2_BACKEND) {
            Connection h2 = DriverManager.getConnection("jdbc:h2:mem:c2s"
                    + SESSION_IDS.getAndIncrement() + com.legend.exec.H2Settings.SETTINGS,
                    "sa", "");
            // the engine's H2 test database carries its extension functions;
            // on this lane the golden runs on THIS session (same-session
            // oracle), so the session carries them too (Phase 0.6: a golden
            // calling legend_h2_extension_lpad was a referee FAULT here)
            try (java.sql.Statement st = h2.createStatement()) {
                for (String alias : com.legend.harness.H2ExtensionFunctions.aliases()) {
                    st.execute(alias);
                }
            }
            return h2;
        }
        return DuckWorkspaces.open();
    }

    public void endSession() {
        runner.endSession();
    }

    /** Verdicts DECIDED BY TEXT, as the platform reported them (Phase 0.6):
     * {@code reason + ' ' + test} → count; the run prints and pins them. */
    private final Map<String, Integer> textDecided = new LinkedHashMap<>();

    public Map<String, Integer> textDecided() {
        return java.util.Collections.unmodifiableMap(textDecided);
    }

    /** Setups the platform derived as INERT (no statement effects) and so
     * never ran — counted and pinned by the run (Phase 0.2). */
    public Set<String> inertSetups() {
        return runner.inertSetups();
    }

    // ---- THE HARNESS'S INSTRUMENTS, attached through the product's seam -----

    /** The referee (the replay oracle with its H2 mirror), the raw-SQL
     * recorder with the session's seed ledger, and the text-decided census —
     * everything the harness adds to a run, and nothing it judges. */
    private final class CorpusObserver implements TestObserver {
        private @com.legend.Nullable Connection mirrorConn;
        /** The session's non-query statements so far — the referee's seed
         * ledger prefix for every later test of the session. */
        private final List<com.legend.sql.dialect.RawSqlBoundary.Raw> seedLedger = new ArrayList<>();
        private @com.legend.Nullable com.legend.sql.dialect.RawSqlBoundary.Recorder recorder;
        private @com.legend.Nullable com.legend.harness.ReplayOracle oracle;
        private com.legend.sql.dialect.RawSqlBoundary.Recorder.@com.legend.Nullable Mark mark;
        private @com.legend.Nullable String currentTest;

        @Override
        public void testStarted(PureTests.TestCase t) {
            // the referee's declines and verdict roster name the test they
            // belong to (display attribution only, no verdict flows through it)
            currentTest = t.fqn();
            com.legend.harness.H2Verify.CURRENT_TEST.set(t.fqn());
            if (System.getenv("LEGEND_LITE_PROGRESS") != null) {
                System.err.println("[corpus2] > " + t.fqn());
            }
        }

        @Override
        public void sessionBegan(String pkg, Connection conn) throws SQLException {
            // the referee's H2 mirror replays goldens beside a DuckDB session; an
            // H2 session IS the oracle's engine and needs no mirror
            if (!H2_BACKEND && com.legend.harness.H2Verify.ready()) {
                mirrorConn = DriverManager.getConnection("jdbc:h2:mem:c2Mirror"
                        + SESSION_IDS.getAndIncrement() + com.legend.exec.H2Settings.SETTINGS,
                        "sa", "");
                com.legend.harness.ReplayOracle.mirrorBegin(mirrorConn);
            }
            seedLedger.clear();
        }

        @Override
        public void sessionEnding() {
            com.legend.harness.ReplayOracle.mirrorEnd();
            if (mirrorConn != null) {
                try {
                    mirrorConn.close();
                } catch (SQLException ignored) {
                    // a mirror that fails to close cannot poison the next
                }
            }
            mirrorConn = null;
        }

        @Override
        public void privateWorkspace(boolean on) {
            com.legend.harness.ReplayOracle.mirrorSuspend(on);
        }

        @Override
        public com.legend.ExecuteOptions options(PureTests.TestCase t, boolean shared) {
            // the raw-SQL ledger of THIS test (Phase 2b): the session's seed
            // prefix, then everything the setups and the body execute; the
            // executor appends through the options, the referee reads it
            recorder = new com.legend.sql.dialect.RawSqlBoundary.Recorder(
                    shared ? seedLedger : List.of());
            oracle = new com.legend.harness.ReplayOracle(recorder);
            // the test-input resource resolver rides the same options (Phase 2d)
            return com.legend.ExecuteOptions.recording(recorder, path -> {
                try {
                    return Files.readString(Corpus.RELATIONAL.getParent().getParent()
                            .resolve(path.startsWith("/") ? path.substring(1) : path));
                } catch (IOException e) {
                    throw new com.legend.error.DataError("test resource '" + path + "'", e);
                }
            });
        }

        @Override
        public com.legend.exec.@com.legend.Nullable SqlReplayOracle oracle(PureTests.TestCase t) {
            return oracle;
        }

        @Override
        public void bodyStarting(Connection conn, boolean effectful) throws SQLException {
            mark = effectful ? java.util.Objects.requireNonNull(oracle).beginAttempt(conn) : null;
        }

        @Override
        public void bodyPassed(Connection conn, boolean effectful) throws SQLException {
            // the session state the body produced is what the engine's run
            // leaves too — kept whether or not the body adjudicated anything
            if (effectful) {
                com.legend.harness.ReplayOracle.commitAttempt(conn);
                mark = null;
            }
        }

        @Override
        public void bodyFailed(Connection conn, boolean effectful) throws SQLException {
            if (effectful && mark != null) {
                java.util.Objects.requireNonNull(oracle).rollbackAttempt(conn, mark);
                mark = null;
            }
        }

        @Override
        public void bodyFinished(PureTests.TestCase t, boolean shared, com.legend.ExecuteOptions options) {
            if (shared && recorder != null) {
                seedLedger.clear();
                for (var stmt : recorder.entries()) {
                    if (!stmt.query()) {
                        seedLedger.add(stmt);
                    }
                }
            }
            com.legend.harness.H2Verify.CURRENT_TEST.remove();
        }

        @Override
        public void declined(String name, String reason) {
            // a text-decided verdict, named by the arm (Phase 0.6)
            textDecided.merge(reason + " " + currentTest, 1, Integer::sum);
        }
    }

    // ---- RUN + SCORE -----------------------------------------------------------

    /** Run one test through the product's runner and score it for the rosters
     *  (the strength ladder is read off the runner's verdict log). */
    public Result run(PureTests.TestCase t) throws SQLException {
        PureTestRunner.Result r = runner.run(t);
        return switch (r.status()) {
            case FAIL -> new Result(t.fqn(), Status.FAIL, r.verdictCount(), r.reason());
            case SKIPPED -> new Result(t.fqn(), Status.SKIPPED, 0, r.reason());
            case PASS -> {
                // the strength ladder (Phase 0.7): counts of what judged the asserts
                // a text-decided assert is the one whose decline preceded its
                // verdict; a refereed one was judged by the referee's rows, not
                // a literal; the rest the platform judged by value or count
                int textDecidedCount = 0;
                int cardinality = 0;
                int literal = 0;
                for (PureTestRunner.Verdict v : r.verdicts()) {
                    if (v.declinedReason() != null) {
                        textDecidedCount++;
                    } else if (v.refereeOutcome() != null) {
                        continue;
                    } else if (CARDINALITY_ASSERTS.contains(v.assertName())) {
                        cardinality++;
                    } else {
                        literal++;
                    }
                }
                Strength strength = r.refereeMatched() ? Strength.DIFFERENTIAL
                        : literal > 0 ? Strength.LITERAL
                        : cardinality > 0 ? Strength.CARDINALITY
                        : Strength.SPELLING;
                yield new Result(t.fqn(), Status.PASS, r.verdictCount(),
                        r.verdictCount() + " verdict(s) " + strength, strength,
                        literal > 0 || cardinality > 0);
            }
        };
    }

    // ---- small structural helpers ------------------------------------------

    static void refusePlatformNamespace(List<? extends PackageableElement> elements) {
        for (PackageableElement el : elements) {
            // the stdlib is FUNCTIONS; a class/enum/association under the
            // package (meta::pure::functions::tests::model::Person — the PCT
            // fixture model) is a fixture, the thing under test (batch 145)
            if (el.qualifiedName().startsWith(PLATFORM_STDLIB_PACKAGE)
                    && (el instanceof FunctionDefinition
                            || el instanceof com.legend.model.NativeFunctionDefinition)) {
                throw new IllegalStateException("platform-namespace library element "
                        + el.qualifiedName() + ": reference checkouts are spec, never runtime");
            }
        }
    }

    private static final String PLATFORM_STDLIB_PACKAGE = "meta::pure::functions::";


    /** The platform's message, WHOLE, on one line ({@link PureTestRunner#whole}). */
    static String whole(@com.legend.Nullable String s) {
        return PureTestRunner.whole(s);
    }
}
