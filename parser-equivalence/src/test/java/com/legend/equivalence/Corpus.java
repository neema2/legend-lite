package com.legend.equivalence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * Where the harness gets real input.
 *
 * <p>Roots are read <b>in place</b> from local checkouts, overridable by system property so CI can
 * point at pinned clones. If a root is missing the harness is skipped rather than silently passing
 * on an empty corpus — see {@code EquivalenceGuards}.
 *
 * <p>The corpora are the ones inventoried in {@code docs/PARSER_DROP_IN_PLAN.md} §3.2. This class
 * covers the <b>file-based</b> tiers (C3, C10); the ~3,191 inline {@code ###} snippets embedded in
 * Java test sources (C2) need a source-level extractor and are deliberately not faked here — a
 * corpus you cannot enumerate is a corpus you cannot report coverage for.
 */
public final class Corpus {

    private Corpus() {
    }

    public static Path engineRoot() {
        return com.legend.testing.ProgramPaths.rootOf("legend.engine.root");
    }

    public static Path pureRoot() {
        return com.legend.testing.ProgramPaths.rootOf("legend.pure.root");
    }

    /** One unit of input: a source file, its text, and where it came from. */
    public record Source(String id, String text, String tier) {
    }

    /**
     * A path rendered with '/' separators, ALWAYS.
     *
     * <p>{@code Path.toString} uses the PLATFORM separator, so every
     * {@code contains("/src/test/")}-style filter in this module matches
     * nothing on Windows — the walk silently yields an empty roster and the
     * census or ratchet reads as a clean sweep rather than a broken one. Route
     * every path-shape test through here (Windows CI, 2026-09-09).
     */
    static String slashed(Path p) {
        return p.toString().replace(java.io.File.separatorChar, '/');
    }

    /** {@code file}'s path within {@code root}, forward-slashed and with a leading slash, so a filter on
     *  "/target/" or "/src/test/" judges the tree's own layout, never where the tree sits on the host. */
    static String within(java.nio.file.Path root, java.nio.file.Path file) {
        return "/" + slashed(root.relativize(file));
    }

    /** Every {@code .pure} under a root, excluding build output. */
    private static List<Path> pureFiles(Path root) {
        return filesWith(root, ".pure");
    }

    private static List<Path> filesWith(Path root, String ext) {
        if (!Files.isDirectory(root)) {
            throw new IllegalStateException("the corpus root " + root + " is not among its inputs: declare it (Bazel workplan P3-14: a missing root failed silently)");
        }
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(p -> p.toString().endsWith(ext))
                    .filter(p -> !within(root, p).contains("/target/"))
                    // ORDER BY THE ID, not the Path: Windows's
                    // Path.compareTo is CASE-INSENSITIVE, so a Path sort
                    // interleaves dataSpaceX and dataspaceY differently
                    // there and the whole manifest pairs up wrong
                    // (missing 5034 / extra 5034 / changed 0 — the same
                    // files in a different order; Windows CI, 2026-09-09)
                    .sorted(java.util.Comparator.comparing(Corpus::slashed))
                    .toList();
        } catch (IOException e) {
            throw new IllegalStateException("cannot walk corpus root " + root, e);
        }
    }

    /** C11 — Pure DOCUMENTS living in {@code .txt} resources (the
     *  persistence test-runner's models and scattered
     *  json/xml/lineage/graphQL fixtures): the first non-blank line is a
     *  {@code ###Section} header or a top-level declaration. 55 files at
     *  enrollment (2026-08-12); the oracle adjudicates each like every
     *  tier. */
    private static void addTxtDocuments(List<Source> out, Path root,
            String tier) {
        for (Path p : filesWith(root, ".txt")) {
            String t;
            try {
                t = Files.readString(p);
            } catch (Exception e) {
                // NOT counted in UNREADABLE: most .txt resources are not
                // Pure and non-UTF8 ones cannot be candidates
                continue;
            }
            String head = t.lines().filter(l -> !l.isBlank()).findFirst()
                    .orElse("").strip();
            if (head.matches("###\\w+")
                    || head.matches("(Class|Enum|Association|Profile"
                            + "|function|import)\\b.*")) {
                // slashed(): the source ID is the corpus KEY — it is matched
                // against the committed manifest and the allowlist ledgers,
                // which are forward-slash. A platform-separator id makes every
                // row miss (Windows CI gate 8, 2026-09-09).
                out.add(new Source(slashed(root.relativize(p)), t, tier));
            }
        }
    }

    /** One load of the corpus: its sources, the files it could not read (COUNTED, never a silent {@code continue};
     *  HARNESS_SIMPLIFICATION_PLAN Phase 6) and the exact-text duplicates it dropped. A value, not process-wide
     *  statics that grew with every load in the JVM (Bazel workplan P3-08). */
    record Loaded(List<Source> sources, List<String> unreadable, int deduped) {}

    private static void add(List<Source> out, Path root, String tier, java.util.function.Predicate<String> accept,
            List<String> unreadable) {
        for (Path p : pureFiles(root)) {
            String t;
            try {
                t = Files.readString(p);
            } catch (Exception e) {
                unreadable.add(slashed(root.relativize(p)) + " :: " + e);
                continue;
            }
            if (accept.test(t)) {
                // slashed(): the source ID is the corpus KEY — it is matched
                // against the committed manifest and the allowlist ledgers,
                // which are forward-slash. A platform-separator id makes every
                // row miss (Windows CI gate 8, 2026-09-09).
                out.add(new Source(slashed(root.relativize(p)), t, tier));
            }
        }
    }

    /**
     * The full corpus.
     *
     * <p><b>C3 — EMIT models.</b> Upstream's multi-file, cross-referencing, user-shaped models; the
     * only tier that exercises cross-file behaviour, and the fastest-growing.
     * <b>C10 — standalone grammar files</b> from both checkouts.
     *
     * <p>{@code m3.pure} is excluded: 3,607 lines of {@code ^Root.children[...]} bootstrap-instance
     * syntax with zero normal declarations, which skews every count.
     */
    /** The first line of the fixture snapshot: the engine release it was
     *  harvested from, INSIDE the file (upstream boundary batch 2), and it must
     *  equal the pinned release ({@link OraclePins#engineRelease()}; checked on
     *  every read below). The filename carried the release too until
     *  2026-09-22: the header is the check that fires, and a fixed name is what
     *  lets the snapshot be an ordinary generated file (the Bazel build writes
     *  it from :gen_fixtures) with its history kept across bumps. */
    public static final String FIXTURE_HEADER_PREFIX = "# engine=";

    /** Hex digits of a fixture's id: 12 (48 bits) — for the ~1,650 fixtures a
     *  collision is ~1e-8, and one fails the read, never shadows a fixture. */
    static final int FIXTURE_ID_HEX = 12;

    /** A fixture's id: {@code engine-fixture#} + the first {@link #FIXTURE_ID_HEX}
     *  hex digits of its source's SHA-256. By CONTENT, not position (2026-09-22):
     *  a positional id renumbered whenever a harvest added, dropped or reordered a
     *  fixture, and every ledger row or comment naming one silently pointed at a
     *  different fixture. Now an id names the same source for as long as it exists. */
    static String fixtureId(String source) {
        try {
            byte[] h = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(source.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return "engine-fixture#" + java.util.HexFormat.of().formatHex(h).substring(0, FIXTURE_ID_HEX);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The committed fixture snapshot for the pinned release. */
    static java.nio.file.Path engineFixturesFile() {
        // a generator in a bump reads the NEW snapshot, the harvest's output,
        // named explicitly (-Dlegend.engine.fixtures, :gen_manifest/:gen_roster)
        String named = System.getProperty("legend.engine.fixtures");
        if (named != null) {
            return java.nio.file.Path.of(named);
        }
        // a test: the committed snapshot, by the runfiles path its target passes (Bazel workplan P1-05)
        return com.legend.testing.Runfile.property("pe.engine.fixtures");
    }

    /** C6: the committed engine-fixture snapshot (see the harvest note
     *  in {@link #all()}). LOUD (batch 2): an ABSENT snapshot, or one whose
     *  in-file header names another release than release.MODULE.bazel's pins,
     *  fails — until 2026-09-10 the reader returned an empty list and tier C6
     *  (1,552 sources) vanished from every gate without a word. */
    static List<Source> engineFixtures() {
        List<Source> out = new ArrayList<>();
        java.nio.file.Path p = engineFixturesFile();
        if (!java.nio.file.Files.exists(p)) {
            throw new IllegalStateException("engine fixture snapshot for the pinned release "
                    + OraclePins.engineRelease() + " is missing: " + p
                    + " — regenerate it: bazel run //:update_generated");
        }
        try {
            var om = new com.fasterxml.jackson.databind.ObjectMapper();
            java.util.Map<String, String> ids = new java.util.HashMap<>();
            boolean first = true;
            for (String line : java.nio.file.Files.readAllLines(p)) {
                if (first) {
                    first = false;
                    if (!line.startsWith(FIXTURE_HEADER_PREFIX)) {
                        throw new IllegalStateException("fixture snapshot " + p + " has no '"
                                + FIXTURE_HEADER_PREFIX + "<release>' header line");
                    }
                    String harvested = line.substring(FIXTURE_HEADER_PREFIX.length()).strip();
                    if (!harvested.equals(OraclePins.engineRelease())) {
                        throw new IllegalStateException("fixture snapshot " + p + " was harvested from"
                                + " engine " + harvested + " but release.MODULE.bazel pins "
                                + OraclePins.engineRelease() + " — re-harvest");
                    }
                    continue;
                }
                String text = om.readTree(line).get("source").asText();
                String id = fixtureId(text);
                String clash = ids.put(id, text);
                if (clash != null) {
                    throw new IllegalStateException("two fixtures share the id " + id
                            + " — lengthen FIXTURE_ID_HEX");
                }
                out.add(new Source(id, text, "C6 engine-fixtures"));
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return out;
    }

    public static List<Source> all() {
        return load().sources();
    }

    /** The corpus with its load's counts. */
    static Loaded load() {
        List<Source> out = new ArrayList<>();
        List<String> unreadable = new ArrayList<>();
        // the WHOLE engine checkout — every module's .pure, not a curated subset
        // (the three-root cut left 1,161 files in other xts modules invisible)
        add(out, engineRoot(), "C3/C10 engine", t -> true, unreadable);
        add(out, pureRoot(), "C10 pure", t -> true, unreadable);
        // C4/C5 — Pure snippets embedded in upstream Java TEST sources; the reference
        // parser adjudicates every candidate, so extraction is tolerant by design
        out.addAll(InlineSnippets.extract(engineRoot(), "C4 engine-inline"));
        // C12 — section-only documents in test Java (no domain decl, so C4's
        // pinned pattern never saw them: the DB-flavor/auth scope hole)
        out.addAll(InlineSnippets.extract(engineRoot(),
                "C12 section-doc-inline", InlineSnippets.SECTION_DOC));
        out.addAll(InlineSnippets.extract(pureRoot(), "C5 pure-inline"));
        // C6 — the engine's own grammar-test fixtures, harvested by
        // EXECUTION (ZEngineFixtureHarvest: the 4.138.2 tests-jars run
        // under recording shims; snapshot committed). The per-production
        // coverage the static tiers cannot see — the Binding transformer
        // hid here. Adjudicated like every tier: the oracle decides.
        out.addAll(engineFixtures());
        // C11 — Pure documents in .txt resources (resource sweep,
        // 2026-08-12); legend-pure has none (measured)
        addTxtDocuments(out, engineRoot(), "C11 resource-txt");
        out.removeIf(s -> s.id().toLowerCase(Locale.ROOT).endsWith("grammar/m3.pure"));
        // DEDUPE by exact text, first occurrence wins (tier order above):
        // the reported count IS the distinct count — the audit measured a
        // 6.6% inflation from fixture rows duplicating inline snippets
        // verbatim (HARNESS_SIMPLIFICATION_PLAN Phase 6)
        java.util.Set<String> seen = new java.util.HashSet<>();
        int before = out.size();
        out.removeIf(s -> !seen.add(s.text()));
        int deduped = before - out.size();
        String only = com.legend.diagnostics.Diagnostics.value("corpus-containing");
        if (only != null) {
            // ITERATION ONLY — a section leg's inner loop. The ratchet gate is
            // the FULL sweep; a filtered run cannot raise it (the test's own
            // MIN_ELEMENTS_COMPARED floor fails long before it could).
            out.removeIf(s -> !s.text().contains(only));
        }
        return new Loaded(out, unreadable, deduped);
    }
}
