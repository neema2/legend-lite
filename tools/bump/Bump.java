package com.legend.tools.bump;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * THE BUMP — move upstream to ONE new release, mechanically:
 *
 * <pre>
 *   bazel run //tools/bump -- &lt;engine release&gt;           e.g. 4.146.0
 *   bazel run //tools/bump -- &lt;engine release&gt; --pins    phases 0-1 only
 * </pre>
 *
 * <ol>
 *   <li>DECIDE — the release must be PUBLISHED on Maven Central (tags and Central
 *       disagree: 4.142.0 is tagged and unpublished); the pure release is DERIVED
 *       from that engine release's own pom (nobody types a pure version); the tag
 *       COMMITS come from GitHub's ref advertisement over HTTPS (what {@code git
 *       ls-remote} reads; no host git); the third-party versions the engine's root
 *       pom manages are read from it.</li>
 *   <li>MOVE — the PINS block of release.MODULE.bazel, rewritten whole (the two releases, the tag commits,
 *       both source archives' integrity computed from the download, the engine-managed third-party versions);
 *       everything else reads them (the jar pools, the archives, the tests' oracle pins); then every jar pool
 *       keyed on the engine release is repinned.</li>
 *   <li>REGENERATE and CHECK — {@code bazel run //:update_generated} (every
 *       generated file, from the new pins; a generator that REFUSES — "the new
 *       thing we cannot parse yet" — stops the bump: fix the platform first, then
 *       re-run, it is idempotent), then {@code bazel test //...}. Bazel is {@code $BAZEL_REAL} (the version
 *       bazelisk pinned and runs this under), else {@code bazel}: the one host program besides the network
 *       (workplan D19).</li>
 * </ol>
 *
 * What it does NOT do is the judgement half: read the diff (that IS the upstream
 * change, made legible), re-pin every ratchet the gates report moved (each with a
 * reason; ledgers shrink-only), commit.
 */
public final class Bump {

    private static final String CENTRAL = "https://repo1.maven.org/maven2";

    /** The file whose PINS block a bump rewrites (MODULE.bazel includes it). */
    static final String SEGMENT = "release.MODULE.bazel";

    /** The PINS block's names, in its order: the block is written whole from these. */
    static final List<String> PINS = List.of(
            "LEGEND_ENGINE_RELEASE", "LEGEND_PURE_RELEASE",
            "LEGEND_ENGINE_REPO", "LEGEND_ENGINE_SHA", "LEGEND_ENGINE_SRC_INTEGRITY",
            "LEGEND_PURE_REPO", "LEGEND_PURE_SHA", "LEGEND_PURE_SRC_INTEGRITY",
            "HIKARICP_VERSION", "COMMONS_LANG3_VERSION", "HTTPCORE_VERSION", "JUNIT4_VERSION", "GUAVA_VERSION");

    private static final String BEGIN = "# ── PINS:";
    private static final String END = "# ── END PINS ──";

    /** The jar pools keyed on LEGEND_ENGINE_RELEASE in release.MODULE.bazel: each is repinned by a bump. */
    private static final List<String> RELEASE_POOLS = List.of("maven_upstream", "maven_runner");

    private final Path ws;

    private Bump(Path ws) {
        this.ws = ws;
    }

    public static void main(String[] args) throws Exception {
        String workspace = System.getenv("BUILD_WORKSPACE_DIRECTORY");
        if (workspace == null) {
            throw new IllegalStateException("run me with `bazel run //tools/bump -- <engine release>`");
        }
        if (args.length < 1 || args.length > 2 || (args.length == 2 && !args[1].equals("--pins"))) {
            throw new IllegalArgumentException("usage: bazel run //tools/bump -- <engine release> [--pins]");
        }
        new Bump(Path.of(workspace)).run(args[0], args.length == 2);
    }

    private void run(String release, boolean pinsOnly) throws Exception {
        HttpClient http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(40)).build();

        step("phase 0: decide — " + release + " must be published, pure derived, tag commits resolved");
        Path segment = ws.resolve(SEGMENT);
        String segmentText = Files.readString(segment, StandardCharsets.UTF_8);
        Map<String, String> pins = readPins(segmentText);
        System.out.println("   from " + pins.get("LEGEND_ENGINE_RELEASE") + " / " + pins.get("LEGEND_PURE_RELEASE"));
        String enginePom = get(http, CENTRAL + "/org/finos/legend/engine/legend-engine/" + release
                + "/legend-engine-" + release + ".pom",
                "legend-engine " + release + " is not on Maven Central (tags and Central disagree; pick a PUBLISHED release)");
        String pure = property(enginePom, "legend.pure.version", release);
        get(http, CENTRAL + "/org/finos/legend/pure/legend-pure-m3-core/" + pure + "/legend-pure-m3-core-" + pure + ".pom",
                "legend-pure " + pure + " (engine " + release + "'s own pairing) is not on Maven Central");
        System.out.println("   to   " + release + " / " + pure + " (derived from engine " + release + "'s pom)");
        String engineTag = "legend-engine-" + release;
        String pureTag = "legend-pure-" + pure;
        String engineSha = tagCommit(http, pins.get("LEGEND_ENGINE_REPO"), engineTag);
        String pureSha = tagCommit(http, pins.get("LEGEND_PURE_REPO"), pureTag);
        System.out.println("   " + engineTag + " = " + engineSha);
        System.out.println("   " + pureTag + " = " + pureSha);
        Map<String, String> managed = new LinkedHashMap<>();
        managed.put("HIKARICP_VERSION", property(enginePom, "hikaricp.version", release));
        managed.put("COMMONS_LANG3_VERSION", property(enginePom, "commons-lang3.version", release));
        managed.put("HTTPCORE_VERSION", property(enginePom, "httpcore.version", release));
        managed.put("JUNIT4_VERSION", property(enginePom, "junit.version", release));
        managed.put("GUAVA_VERSION", property(enginePom, "guava.version", release));
        System.out.println("   engine-managed at " + release + ": " + managed);

        step("phase 1: move — " + SEGMENT + "'s pins, the release's jar pools");
        String engineArchive = "https://github.com/" + pins.get("LEGEND_ENGINE_REPO") + "/archive/refs/tags/"
                + engineTag + ".tar.gz";
        String pureArchive = "https://github.com/" + pins.get("LEGEND_PURE_REPO") + "/archive/refs/tags/"
                + pureTag + ".tar.gz";
        Map<String, String> moved = new LinkedHashMap<>(pins);
        moved.put("LEGEND_ENGINE_RELEASE", release);
        moved.put("LEGEND_PURE_RELEASE", pure);
        moved.put("LEGEND_ENGINE_SHA", engineSha);
        moved.put("LEGEND_ENGINE_SRC_INTEGRITY", integrity(http, engineArchive));
        moved.put("LEGEND_PURE_SHA", pureSha);
        moved.put("LEGEND_PURE_SRC_INTEGRITY", integrity(http, pureArchive));
        moved.putAll(managed);
        Files.writeString(segment, writePins(segmentText, moved), StandardCharsets.UTF_8);
        System.out.println("   " + SEGMENT + " -> " + release + " / " + pure + " (+ tag commits, archives,"
                + " engine-managed versions)");

        // every jar pool whose artifacts or BOM name the engine release (MODULE.bazel: maven_upstream,
        // maven_runner). MODULE.bazel sets fail_if_repin_required on every pool, so a pool left
        // unpinned here fails the build instead of resolving stale jars silently.
        for (String pool : RELEASE_POOLS) {
            bazel(Map.of("REPIN", "1"), "the " + pool + " jar pool could not be repinned at " + release,
                    "run", "@" + pool + "//:pin");
        }
        if (pinsOnly) {
            step("pins only — stopping before regeneration: `git status --short` shows what moved");
            return;
        }

        step("phase 2: regenerate every generated file from the new pins");
        bazel(Map.of(), "a generator REFUSED — the new thing we cannot parse yet: fix the platform,"
                + " then re-run (the bump is idempotent: every step rewrites from the pins)",
                "run", "//:update_generated");

        step("phase 3: every gate, and every generated file checked against its generator");
        bazel(Map.of(), "the pins moved and every file is regenerated, but gates are red: that is the"
                + " judgement half — read the diff, adjudicate each failure, re-pin every moved ratchet"
                + " with a reason (ledgers shrink-only)",
                "test", "//...");

        step("done — the upstream change, made legible by `git status --short` and `git diff --stat`");
        System.out.println("""

                NEXT (the judgement half):
                  1. read the diff — prelude.pure / Pure.java / native-*.tsv / DynaFn.java /
                     corpus-manifest.tsv / protocol-roster.tsv / the fixture snapshot: that IS the
                     upstream change;
                  2. re-pin every ratchet the gates reported moved, each with a reason; ledgers shrink-only;
                  3. commit the named files and push; CI runs the same gates from the pins.""");
    }

    // ------------------------------------------------------------------

    /** The PINS block of {@code segment}: one {@code NAME = "value"} per line between the markers, exactly
     *  {@link #PINS}' names, in order. Anything else in the block is refused: it is written by the bump only. */
    static Map<String, String> readPins(String segment) {
        List<String> lines = block(segment);
        Map<String, String> out = new LinkedHashMap<>();
        for (String line : lines) {
            int eq = line.indexOf(" = \"");
            if (eq <= 0 || !line.endsWith("\"") || line.length() < eq + 5) {
                throw new IllegalStateException(SEGMENT + "'s PINS block holds a line that is no NAME = \"value\": "
                        + line);
            }
            out.put(line.substring(0, eq), line.substring(eq + 4, line.length() - 1));
        }
        if (!List.copyOf(out.keySet()).equals(PINS)) {
            throw new IllegalStateException(SEGMENT + "'s PINS block names " + out.keySet() + "; the bump writes "
                    + PINS);
        }
        return out;
    }

    /** {@code segment} with its PINS block written whole from {@code pins} ({@link #PINS}' names, in order);
     *  every other line as it was. */
    static String writePins(String segment, Map<String, String> pins) {
        if (!List.copyOf(pins.keySet()).equals(PINS)) {
            throw new IllegalArgumentException("pins " + pins.keySet() + " are not " + PINS);
        }
        block(segment);  // the block is well formed before it is replaced
        StringBuilder out = new StringBuilder();
        boolean inBlock = false;
        for (String line : segment.split("\n", -1)) {
            if (line.startsWith(BEGIN)) {
                out.append(line).append('\n');
                for (String k : PINS) {
                    out.append(k).append(" = \"").append(pins.get(k)).append("\"\n");
                }
                inBlock = true;
            } else if (line.equals(END)) {
                inBlock = false;
                out.append(line).append('\n');
            } else if (!inBlock) {
                out.append(line).append('\n');
            }
        }
        return out.substring(0, out.length() - 1);
    }

    /** The lines between the PINS markers: each marker exactly once, in order. */
    private static List<String> block(String segment) {
        List<String> lines = List.of(segment.split("\n", -1));
        List<Integer> begins = new ArrayList<>();
        List<Integer> ends = new ArrayList<>();
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).startsWith(BEGIN)) {
                begins.add(i);
            } else if (lines.get(i).equals(END)) {
                ends.add(i);
            }
        }
        if (begins.size() != 1 || ends.size() != 1 || ends.get(0) < begins.get(0)) {
            throw new IllegalStateException(SEGMENT + " must hold one PINS block (" + BEGIN + " ... " + END + ")");
        }
        return lines.subList(begins.get(0) + 1, ends.get(0));
    }

    /** A pinned download's {@code integrity} (Subresource Integrity), as Bazel's http rules check it. */
    static String integrity(byte[] sha256) {
        return "sha256-" + Base64.getEncoder().encodeToString(sha256);
    }

    /** The commit a tag names in a git ref advertisement: the peeled ref of an annotated tag, else the ref
     *  itself (upstream's tags are lightweight since the 4.14x release workflow); null when there is none. */
    static String tagCommit(String advertisement, String tag) {
        String ref = " refs/tags/" + Pattern.quote(tag);
        for (String suffix : List.of("\\^\\{\\}", "")) {
            // a pkt-line is <4 hex length><40 hex sha> <ref>, ending at the newline (or, on the first, a NUL)
            Matcher m = Pattern.compile("([0-9a-f]{40})" + ref + suffix + "(?=[\\n\\x00])").matcher(advertisement);
            if (m.find()) {
                return m.group(1);
            }
        }
        return null;
    }

    private static void step(String s) {
        System.out.println();
        System.out.println("== " + s);
    }

    private static String get(HttpClient http, String url, String whenMissing) throws Exception {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(40)).build(),
                HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() != 200) {
            throw new IllegalStateException(whenMissing + " (" + url + " -> HTTP " + r.statusCode() + ")");
        }
        return r.body();
    }

    private static String property(String pom, String name, String release) {
        Matcher m = Pattern.compile("<" + Pattern.quote(name) + ">([^<]*)</" + Pattern.quote(name) + ">").matcher(pom);
        if (!m.find() || m.group(1).isBlank()) {
            throw new IllegalStateException("engine " + release + "'s pom declares no " + name);
        }
        return m.group(1).trim();
    }

    private static String integrity(HttpClient http, String url) throws Exception {
        HttpResponse<InputStream> r = http.send(HttpRequest.newBuilder(URI.create(url)).build(),
                HttpResponse.BodyHandlers.ofInputStream());
        if (r.statusCode() != 200) {
            throw new IllegalStateException("cannot download " + url + " -> HTTP " + r.statusCode());
        }
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        try (InputStream in = r.body()) {
            byte[] buf = new byte[1 << 16];
            for (int n; (n = in.read(buf)) > 0; ) {
                md.update(buf, 0, n);
            }
        }
        return integrity(md.digest());
    }

    /** The commit {@code tag} names on GitHub: the smart-HTTP ref advertisement, the list {@code git ls-remote}
     *  reads, fetched over HTTPS so the bump needs no host git. */
    private static String tagCommit(HttpClient http, String repo, String tag) throws Exception {
        String url = "https://github.com/" + repo + ".git/info/refs?service=git-upload-pack";
        String sha = tagCommit(get(http, url, "cannot list " + repo + "'s refs"), tag);
        if (sha == null) {
            throw new IllegalStateException("no tag " + tag + " on " + repo);
        }
        return sha;
    }

    private void bazel(Map<String, String> env, String whenItFails, String... args) throws Exception {
        String real = System.getenv("BAZEL_REAL");
        List<String> cmd = new ArrayList<>(List.of(real == null || real.isBlank() ? "bazel" : real));
        cmd.addAll(List.of(args));
        System.out.println("   $ " + String.join(" ", cmd));
        ProcessBuilder pb = new ProcessBuilder(cmd).directory(ws.toFile()).inheritIO();
        pb.environment().putAll(env);
        int rc = pb.start().waitFor();
        if (rc != 0) {
            throw new IllegalStateException("BUMP STOPPED at `" + String.join(" ", cmd) + "` (exit " + rc + "): "
                    + whenItFails);
        }
    }
}
