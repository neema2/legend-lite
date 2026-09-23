package com.legend.tools.bump;

import java.io.IOException;
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
import java.util.HexFormat;
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
 *       COMMITS come from {@code git ls-remote}; the third-party versions the
 *       engine's root pom manages are read from it.</li>
 *   <li>MOVE — MODULE.bazel (the two releases, both source archives' sha256
 *       computed from the download, the engine-managed third-party versions) and
 *       tools/oracle-pins.env; then the upstream jar pool is repinned.</li>
 *   <li>REGENERATE and CHECK — {@code bazel run //:update_generated} (every
 *       generated file, from the new pins; a generator that REFUSES — "the new
 *       thing we cannot parse yet" — stops the bump: fix the platform first, then
 *       re-run, it is idempotent), then {@code bazel test //...}.</li>
 * </ol>
 *
 * What it does NOT do is the judgement half: read the diff (that IS the upstream
 * change, made legible), re-pin every ratchet the gates report moved (each with a
 * reason; ledgers shrink-only), commit.
 */
public final class Bump {

    private static final String CENTRAL = "https://repo1.maven.org/maven2";

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
        Map<String, String> pins = readPins();
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
        String engineSha = peeled(pins.get("LEGEND_ENGINE_REPO"), engineTag);
        String pureSha = peeled(pins.get("LEGEND_PURE_REPO"), pureTag);
        System.out.println("   " + engineTag + " = " + engineSha);
        System.out.println("   " + pureTag + " = " + pureSha);
        Map<String, String> managed = new LinkedHashMap<>();
        managed.put("com.zaxxer:HikariCP", property(enginePom, "hikaricp.version", release));
        managed.put("org.apache.commons:commons-lang3", property(enginePom, "commons-lang3.version", release));
        managed.put("org.apache.httpcomponents:httpcore", property(enginePom, "httpcore.version", release));
        managed.put("junit:junit", property(enginePom, "junit.version", release));
        managed.put("com.google.guava:guava", property(enginePom, "guava.version", release));
        System.out.println("   engine-managed at " + release + ": " + managed);

        step("phase 1: move — MODULE.bazel, tools/oracle-pins.env, the upstream jar pool");
        String engineArchive = "https://github.com/" + pins.get("LEGEND_ENGINE_REPO") + "/archive/refs/tags/"
                + engineTag + ".tar.gz";
        String pureArchive = "https://github.com/" + pins.get("LEGEND_PURE_REPO") + "/archive/refs/tags/"
                + pureTag + ".tar.gz";
        String engineArchiveSha = sha256(http, engineArchive);
        String pureArchiveSha = sha256(http, pureArchive);
        System.out.println("   " + engineArchive + " sha256 " + engineArchiveSha);
        System.out.println("   " + pureArchive + " sha256 " + pureArchiveSha);

        Path module = ws.resolve("MODULE.bazel");
        String m = Files.readString(module, StandardCharsets.UTF_8);
        m = replaceOne(m, "(?m)^LEGEND_ENGINE_RELEASE = \"[^\"]*\"", "LEGEND_ENGINE_RELEASE = \"" + release + "\"");
        m = replaceOne(m, "(?m)^LEGEND_PURE_RELEASE = \"[^\"]*\"", "LEGEND_PURE_RELEASE = \"" + pure + "\"");
        m = replaceOne(m, "(name = \"legend_engine_src\",[^)]*?sha256 = \")[0-9a-f]{64}\"", "$1" + engineArchiveSha + "\"");
        m = replaceOne(m, "(name = \"legend_pure_src\",[^)]*?sha256 = \")[0-9a-f]{64}\"", "$1" + pureArchiveSha + "\"");
        for (Map.Entry<String, String> e : managed.entrySet()) {
            m = replaceOne(m, "\"" + Pattern.quote(e.getKey()) + ":[^\"]+\"", "\"" + e.getKey() + ":" + e.getValue() + "\"");
        }
        Files.writeString(module, m, StandardCharsets.UTF_8);
        System.out.println("   MODULE.bazel -> " + release + " / " + pure + " (+ archives, engine-managed versions)");

        Path pinsFile = ws.resolve("tools/oracle-pins.env");
        String p = Files.readString(pinsFile, StandardCharsets.UTF_8);
        p = replaceOne(p, "(?m)^LEGEND_ENGINE_RELEASE=.*$", "LEGEND_ENGINE_RELEASE=" + release);
        p = replaceOne(p, "(?m)^LEGEND_PURE_RELEASE=.*$", "LEGEND_PURE_RELEASE=" + pure);
        p = replaceOne(p, "(?m)^LEGEND_ENGINE_SHA=.*$", "LEGEND_ENGINE_SHA=" + engineSha);
        p = replaceOne(p, "(?m)^LEGEND_ENGINE_DESCRIBE=.*$", "LEGEND_ENGINE_DESCRIBE=" + engineTag);
        p = replaceOne(p, "(?m)^LEGEND_PURE_SHA=.*$", "LEGEND_PURE_SHA=" + pureSha);
        p = replaceOne(p, "(?m)^LEGEND_PURE_DESCRIBE=.*$", "LEGEND_PURE_DESCRIBE=" + pureTag);
        Files.writeString(pinsFile, p, StandardCharsets.UTF_8);
        System.out.println("   tools/oracle-pins.env -> " + release + " / " + pure);

        bazel(Map.of("REPIN", "1"), "the upstream jar pool could not be repinned at " + release,
                "run", "@maven_upstream//:pin");
        if (pinsOnly) {
            step("pins only — stopping before regeneration");
            git("status", "--short");
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

        step("done — the upstream change, made legible:");
        git("status", "--short");
        git("diff", "--stat");
        System.out.println("""

                NEXT (the judgement half):
                  1. read the diff above — prelude.pure / Pure.java / native-*.tsv / DynaFn.java /
                     corpus-manifest.tsv / protocol-roster.tsv / the fixture snapshot: that IS the
                     upstream change;
                  2. re-pin every ratchet the gates reported moved, each with a reason; ledgers shrink-only;
                  3. commit the named files and push; CI runs the same gates from the pins.""");
    }

    // ------------------------------------------------------------------

    private static void step(String s) {
        System.out.println();
        System.out.println("== " + s);
    }

    private Map<String, String> readPins() throws IOException {
        Map<String, String> out = new LinkedHashMap<>();
        for (String line : Files.readAllLines(ws.resolve("tools/oracle-pins.env"), StandardCharsets.UTF_8)) {
            int eq = line.indexOf('=');
            if (!line.startsWith("#") && eq > 0) {
                out.put(line.substring(0, eq).trim(), line.substring(eq + 1).trim());
            }
        }
        for (String k : List.of("LEGEND_ENGINE_RELEASE", "LEGEND_PURE_RELEASE", "LEGEND_ENGINE_REPO", "LEGEND_PURE_REPO")) {
            if (!out.containsKey(k)) {
                throw new IllegalStateException("tools/oracle-pins.env has no " + k);
            }
        }
        return out;
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

    private static String sha256(HttpClient http, String url) throws Exception {
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
        return HexFormat.of().formatHex(md.digest());
    }

    /** The commit a tag names: the peeled ref of an annotated tag, else the ref itself
     *  (upstream's tags are lightweight since the 4.14x release workflow). */
    private String peeled(String repo, String tag) throws Exception {
        String url = "https://github.com/" + repo;
        String sha = firstSha(capture("git", "ls-remote", "--tags", url, "refs/tags/" + tag + "^{}"));
        if (sha == null) {
            sha = firstSha(capture("git", "ls-remote", "--tags", url, "refs/tags/" + tag));
        }
        if (sha == null) {
            throw new IllegalStateException("no tag " + tag + " on " + repo);
        }
        return sha;
    }

    private static String firstSha(String lsRemote) {
        for (String line : lsRemote.split("\n")) {
            String[] f = line.trim().split("\\s+");
            if (f.length >= 1 && f[0].matches("[0-9a-f]{40}")) {
                return f[0];
            }
        }
        return null;
    }

    /** Rewrites a pin that appears EXACTLY once — none means the file moved on
     *  without the bump, two would leave one behind. */
    private static String replaceOne(String text, String regex, String replacement) {
        Pattern pattern = Pattern.compile(regex);
        Matcher count = pattern.matcher(text);
        int n = 0;
        while (count.find()) {
            n++;
        }
        if (n != 1) {
            throw new IllegalStateException("pin must appear exactly once, found " + n + ": " + regex);
        }
        return pattern.matcher(text).replaceFirst(replacement);
    }

    private String capture(String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd).directory(ws.toFile()).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (p.waitFor() != 0) {
            throw new IllegalStateException(String.join(" ", cmd) + " failed:\n" + out);
        }
        return out;
    }

    private void bazel(Map<String, String> env, String whenItFails, String... args) throws Exception {
        List<String> cmd = new ArrayList<>(List.of("bazel"));
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

    private void git(String... args) throws Exception {
        List<String> cmd = new ArrayList<>(List.of("git"));
        cmd.addAll(List.of(args));
        new ProcessBuilder(cmd).directory(ws.toFile()).inheritIO().start().waitFor();
    }
}
