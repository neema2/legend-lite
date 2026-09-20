package com.legend;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The planner stays compilable AHEAD OF TIME, to WebAssembly.
 *
 * <p>{@link PlannerNeedsOnlyJavaBaseTest} guards a different and
 * WEAKER property. It bans JDBC, and JDBC is a whole module — but an
 * ahead-of-time class library is narrower than {@code java.base}, so a
 * planner that passes that test can still fail to compile. Every API
 * banned below lives INSIDE {@code java.base}; {@code jdeps} is
 * perfectly happy with all of them, and so is every other test.
 *
 * <p>This is not theory. On 2026-09-20 the planner was compiled to a
 * 4.2 MB WASM module (TeaVM 0.15, {@code research/wasm}) and checked
 * against the JVM over a 69-query differential: byte-identical SQL,
 * byte-identical refusals. Getting there needed exactly five changes,
 * and each banned pattern below is one of them. Without this test
 * nothing fails when they are reverted — the build is green, the
 * suite is green, and the property is silently gone until somebody
 * tries to build the module again months later.
 *
 * <h2>Why this scans everything, with no package allowlist</h2>
 *
 * <p>The first draft of this test carried a planner-package list
 * copied from its sibling, and MUTATION TESTING caught it letting two
 * of the five regressions straight through. {@code Typer} sits in
 * {@code compiler/spec}, and matching on one path segment never saw a
 * SUBpackage; {@code CanonicalDivergence} sits in {@code exec}, which
 * did not look like planner code but is reachable all the same,
 * because {@code Compiler} is a facade over compile AND execute.
 *
 * <p>Both misses were the same mistake: deciding by hand what counts
 * as "the planner". So the allowlist is gone. Every ban below names a
 * replacement that is no worse on a JVM, which makes a module-wide
 * ban free — and free is cheaper than a judgement call that was
 * already wrong twice. If a future toolchain gains one of these,
 * delete that row; do not add exemptions.
 */
class PlannerSurvivesAotCompileTest {

    /** pattern → why it cannot survive, and what to use instead. */
    private static final Map<String, String> BANNED = new LinkedHashMap<>();

    static {
        BANNED.put("\\bjava\\.security\\.",
                "no crypto provider exists ahead of time — use"
                + " com.legend.cache.Sha256, which Sha256Test pins"
                + " byte-identical to MessageDigest");
        BANNED.put("Thread\\.dumpStack\\(\\)",
                "drags in the whole Thread surface, and every call site so"
                + " far was temporary debugging behind an env flag — delete it");
        BANNED.put("\\.newKeySet\\(\\)",
                "ConcurrentHashMap.newKeySet is absent — use"
                + " Collections.newSetFromMap(new ConcurrentHashMap<>())");
        BANNED.put("\\.lines\\(\\)",
                "String.lines() is absent — use ElementParser.linesOf, which"
                + " ElementParserTest pins against String.lines(). Do NOT"
                + " reach for split(\"\\\\n\"): that is a regex, and the"
                + " parser's regex-site count is frozen at zero");
    }

    private static final Pattern APPEND_REPLACEMENT =
            Pattern.compile("\\bappend(Replacement|Tail)\\s*\\(");
    private static final Pattern STRING_BUILDER = Pattern.compile("\\bStringBuilder\\b");

    /** Source with comments stripped, so prose ABOUT a ban is not a ban. */
    private static String code(Path p) throws IOException {
        return Files.readString(p)
                .replaceAll("(?s)/\\*.*?\\*/", "")
                .replaceAll("//[^\n]*", "");
    }

    private static List<Path> sources() throws IOException {
        Path root = Path.of("src/main/java/com/legend");
        if (!Files.isDirectory(root)) {
            root = Path.of("core/src/main/java/com/legend");
        }
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(p -> p.toString().endsWith(".java")).toList();
        }
    }

    @Test
    void noShippedClassUsesAnApiTheAotClassLibraryLacks() throws IOException {
        List<Path> sources = sources();
        assertTrue(sources.size() > 200,
                "census rotted: only " + sources.size() + " sources found");

        List<String> strays = new ArrayList<>();
        for (Path p : sources) {
            String src = code(p);
            for (Map.Entry<String, String> ban : BANNED.entrySet()) {
                if (Pattern.compile(ban.getKey()).matcher(src).find()) {
                    strays.add(p.getFileName() + " uses " + ban.getKey()
                            + " — " + ban.getValue());
                }
            }
        }
        assertTrue(strays.isEmpty(),
                "these break the ahead-of-time build (see research/wasm and"
                + " HANDOFF-2026-09-20 §10); every one is inside java.base,"
                + " so no other test will catch it:\n  "
                + String.join("\n  ", strays));
    }

    @Test
    void matcherAppendUsesTheUniversallyAvailableOverload() throws IOException {
        // Matcher.appendReplacement/appendTail took a StringBuffer until
        // Java 9 added StringBuilder overloads. The AOT class library
        // carries only the original pair, and the synchronisation costs
        // nothing on a local buffer that never escapes.
        List<String> strays = new ArrayList<>();
        for (Path p : sources()) {
            String src = code(p);
            if (APPEND_REPLACEMENT.matcher(src).find()
                    && STRING_BUILDER.matcher(src).find()) {
                strays.add(p.getFileName().toString());
            }
        }
        assertTrue(strays.isEmpty(),
                "a file calling Matcher.append*() must build into a"
                + " StringBuffer, not a StringBuilder — the StringBuilder"
                + " overloads are Java 9 additions the ahead-of-time class"
                + " library does not carry. (This check is deliberately"
                + " blunt: it flags any StringBuilder in such a file. If one"
                + " ever legitimately needs both, narrow the check rather"
                + " than exempting the file.) " + strays);
    }
}
