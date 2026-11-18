// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE COMPATIBILITY SPELLINGS AS THE BYTECODE HOLDS THEM (the GUARD stream, step 2's C.6; the edge audit of 2026-11-17,
 * section C.6: "a constant-pool scan of the product jars (catches concatenation and borrowed constants); per-class
 * pins"). {@link CompatSpellingGuardTest} reads core's main SOURCES: a literal split in two ({@code "TDS" + "Null"},
 * folded by javac into one constant), a constant borrowed from another class (inlined into the user's pool), or a
 * product library outside core's sources ({@code //base}, {@code //json}, the cells) is not seen there. Here every class
 * of the product's jars (the edge's aside) is read as its constant pool ({@link ConstantPools}), matched as the source
 * scan matches: a {@link CompatSpellingGuardTest#LITERALS literal} inside a STRING CONSTANT (a {@code ^...$} one the
 * whole constant), a {@link CompatSpellingGuardTest#SYMBOLS symbol} as a whole word in a NAME or descriptor (never inside
 * a string). A hit is the register's to count only when its class's own source writes it: a literal hit, when that
 * file has the spelling's register row AND one of the file's own literals holds the constant's text (so a borrowed or
 * folded use in a file that also writes the spelling is still found); a symbol hit, when the file has the row. Every
 * other hit is pinned here by class, exact ({@link #POOL_ONLY}), with its dated reason: a new one fails.
 *
 * <p>Not read here (named): a resource's text (prelude.pure, the .tsv tables, service files: C.8's scan), and the hosts'
 * JVM libraries (//wasm, //native, //sdlc-server/grammar) -- no constant-pool scan reads them until C.5's read of the
 * hosts' doors lands (the review of 5911e970e1, 2026-11-17: C.5 ships after this piece).
 */
@Tag("guardrail")
class CompatConstantPoolTest {

    private static final String MAIN = "core/src/main/";

    private static final String JAVA_ROOT = "core/src/main/java/com/legend/";

    private static final String BORROWED_TDS_NULL_FQN = "PlatformTypes.TDS_NULL_FQN borrowed: Pure's class"
            + " meta::pure::tds::TDSNull, LANGUAGE (C5 step C, C-2, 2026-11-17; PlatformTypes' register row STAYS)";

    /** The hits no class's own source accounts for, "top class | spelling" (the register's spelling), each with its
     *  reason. Measured 2026-11-17. */
    static final Map<String, String> POOL_ONLY = Map.ofEntries(
            // the Pure class's name, borrowed (javac inlines PlatformTypes.TDS_NULL_FQN into each user's pool): the
            // register's PlatformTypes row (STAYS) is its one source; each user names the constant, not the spelling
            // (Scalars and VerdictQueries too, whose own TDSNull rows count only their own literals: the review of
            // e62be0f430, 2026-11-17)
            Map.entry("com.legend.compiler.spec.LiteralUnroll | TDSNull", BORROWED_TDS_NULL_FQN),
            Map.entry("com.legend.compiler.spec.TdsDesugars | TDSNull", BORROWED_TDS_NULL_FQN),
            Map.entry("com.legend.compiler.spec.VerdictQueries | TDSNull", BORROWED_TDS_NULL_FQN),
            Map.entry("com.legend.lowering.CollectionLanes | TDSNull", BORROWED_TDS_NULL_FQN),
            Map.entry("com.legend.lowering.InstanceEquality | TDSNull", BORROWED_TDS_NULL_FQN),
            Map.entry("com.legend.lowering.Lowerer | TDSNull", BORROWED_TDS_NULL_FQN),
            Map.entry("com.legend.lowering.Scalars | TDSNull", BORROWED_TDS_NULL_FQN),
            Map.entry("com.legend.testplan.JudgePlans | TDSNull", BORROWED_TDS_NULL_FQN),
            // H2's session statement folded with its own list: "SET NON_KEYWORDS " + NON_KEYWORDS, one constant no
            // single literal holds -- the spelling the register's RULED row counts (S5, 2026-11-16: the user's ruling
            // on H2's lexicon, its session half)
            Map.entry("com.legend.sql.dialect.H2 | SET NON_KEYWORDS", "folded in its own file with H2.NON_KEYWORDS;"
                    + " RULED (S5, 2026-11-16), the register's H2.java row"));

    // ---- the matching ------------------------------------------------------------------------------------------

    /** A register spelling of a literal: a {@code ^...$} one without its anchors, as the register writes it. */
    static String spelling(String literal) {
        return literal.startsWith("^") && literal.endsWith("$") ? literal.substring(1, literal.length() - 1) : literal;
    }

    /** One hit: the class's top class, the register's spelling, and for a literal the string constant holding it. */
    record Hit(String top, String spelling, String constant) {
    }

    private static String top(String n) {
        int dollar = n.indexOf('$');
        return dollar < 0 ? n : n.substring(0, dollar);
    }

    /** Every hit of {@code pools} (binary name -> pool), the edge's classes aside. */
    static List<Hit> hits(Map<String, ConstantPools.Pool> pools) {
        List<Hit> out = new ArrayList<>();
        pools.forEach((name, pool) -> {
            if (name.startsWith("com.legend.edge.")) {
                return;
            }
            for (String s : pool.strings()) {
                for (String lit : CompatSpellingGuardTest.LITERALS) {
                    boolean whole = lit.startsWith("^") && lit.endsWith("$");
                    if (whole ? s.equals(spelling(lit)) : s.contains(lit)) {
                        out.add(new Hit(top(name), spelling(lit), s));
                    }
                }
            }
            for (String u : pool.utf8()) {
                if (pool.strings().contains(u)) {
                    continue;
                }
                for (String sym : CompatSpellingGuardTest.SYMBOLS) {
                    if (ConstantPools.hasWord(u, sym)) {
                        out.add(new Hit(top(name), sym, null));
                    }
                }
            }
        });
        return out;
    }

    /** A source file as the scan reads it: its register key and its string literals, decoded as javac decodes them. */
    record Source(String key, List<String> literals) {
    }

    /** The decoded text of a literal {@link CompatSpellingGuardTest#lex} collected raw: a text block (its raw content
     *  holds a line break) is indent-stripped, then every escape translated. */
    static String decode(String raw) {
        if (raw.indexOf('\n') >= 0) {
            String body = raw.substring(raw.indexOf('\n') + 1);
            return body.stripIndent().translateEscapes();
        }
        return raw.translateEscapes();
    }

    static Source source(String key, String text) {
        List<String> raw = new ArrayList<>();
        CompatSpellingGuardTest.lex(text, raw);
        List<String> decoded = new ArrayList<>();
        for (String r : raw) {
            try {
                decoded.add(decode(r));
            } catch (IllegalArgumentException e) {
                decoded.add(r);
            }
        }
        return new Source(key, decoded);
    }

    /** Whether {@code source} writes the constant {@code constant}'s use of {@code spelling}: the piece of the constant
     *  that holds it (a concatenation's recipe split at its argument marks) stands inside one of the file's literals. */
    static boolean writes(Source source, String spelling, String constant) {
        for (String piece : constant.split("[\\u0001\\u0002]")) {
            if (piece.contains(spelling) && source.literals().stream().anyMatch(l -> l.contains(piece))) {
                return true;
            }
        }
        return false;
    }

    /** The hits of {@code hits} no class's own source accounts for, "top class | spelling". {@code sourceOf} gives a top
     *  class's source, or null when it has none in core. */
    static Set<String> poolOnly(List<Hit> hits, Set<String> registered, Function<String, Source> sourceOf) {
        Set<String> out = new TreeSet<>();
        for (Hit h : hits) {
            Source src = sourceOf.apply(h.top());
            boolean row = src != null && registered.contains(src.key() + " | " + h.spelling());
            boolean accounted = row && (h.constant() == null || writes(src, h.spelling(), h.constant()));
            if (!accounted) {
                out.add(h.top() + " | " + h.spelling());
            }
        }
        return out;
    }

    /** The register's "file | spelling" pairs (every step). */
    static Set<String> registered() {
        Set<String> out = new TreeSet<>();
        for (String row : CompatSpellingGuardTest.REGISTER) {
            String[] p = row.split("\\|");
            out.add(p[0].trim() + " | " + p[1].trim());
        }
        return out;
    }

    /** Core's main sources by top class ({@code com.legend.x.Y} from {@code core/src/main/<root>/com/legend/x/Y.java}),
     *  each with the register key the source scan gives it: under core's java root, else under main. */
    private static Function<String, Source> coreSources() {
        Map<String, Path> byClass = new HashMap<>();
        Set<String> twice = new HashSet<>();
        for (Path f : SourceFiles.under("core/src/main")) {
            String path = SourceFiles.path(f);
            if (!path.endsWith(".java")) {
                continue;
            }
            String rest = path.substring(MAIN.length());
            String cls = rest.substring(rest.indexOf('/') + 1, rest.length() - ".java".length()).replace('/', '.');
            if (byClass.put(cls, f) != null) {
                twice.add(cls);
            }
        }
        Map<String, Source> cache = new HashMap<>();
        return cls -> {
            if (twice.contains(cls)) {
                throw new AssertionError(cls + ": two sources under core/src/main");
            }
            Path f = byClass.get(cls);
            if (f == null) {
                return null;
            }
            return cache.computeIfAbsent(cls, k -> {
                String path = SourceFiles.path(f);
                String key = path.startsWith(JAVA_ROOT) ? path.substring(JAVA_ROOT.length()) : path.substring(MAIN.length());
                try {
                    return source(key, Files.readString(f));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        };
    }

    // ---- the tests ---------------------------------------------------------------------------------------------

    @Test
    void theBytecodeHoldsNoSpellingItsSourceDoesNot() {
        Set<String> found = poolOnly(hits(ConstantPools.poolsOfJars(ConstantPools.jars("legend.product.jars"))),
                registered(), coreSources());
        assertEquals(new TreeSet<>(POOL_ONLY.keySet()), found, "a compatibility spelling in a product class's constant"
                + " pool that its own source does not write (folded, borrowed from another class's constant, or in a"
                + " library outside core's sources): spell it at the edge, or pin it here with its reason; a pin gone is"
                + " deleted");
    }

    /** Every pin names its reason and date. */
    @Test
    void everyPinIsDated() {
        POOL_ONLY.forEach((h, why) -> assertTrue(why.matches(".*\\d{4}-\\d{2}-\\d{2}.*"), h + ": " + why));
    }

    /**
     * Planted (core's test sources, read as bytecode AND as source): {@link PlantedPoolLender} writes "SET NON_KEYWORDS"
     * as its own literal; {@link PlantedPoolBorrower} borrows it, folds "TDS" + "Null", and names a type called
     * {@code Repr}. With a register row for each spelling in BOTH files, the lender's literal is accounted, the
     * borrower's borrowed and folded constants are not (its own literals do not hold them), and the symbol is found in
     * a real pool's names and descriptors -- accounted with its row, found without it.
     */
    @Test
    void theScanBites() {
        Map<String, ConstantPools.Pool> pools = new TreeMap<>();
        for (Class<?> c : List.of(PlantedPoolLender.class, PlantedPoolBorrower.class, PlantedPoolBorrower.Repr.class)) {
            pools.put(c.getName(), ConstantPools.pool(ConstantPools.bytesOf(c)));
        }
        String lender = PlantedPoolLender.class.getName();
        String borrower = PlantedPoolBorrower.class.getName();
        Map<String, Source> sources = Map.of(
                lender, source("Lender.java", read("core/src/test/java/com/legend/PlantedPoolLender.java")),
                borrower, source("Borrower.java", read("core/src/test/java/com/legend/PlantedPoolBorrower.java")));
        Set<String> rows = Set.of("Lender.java | SET NON_KEYWORDS", "Borrower.java | SET NON_KEYWORDS",
                "Borrower.java | TDSNull", "Borrower.java | Repr");
        List<Hit> hits = hits(pools);
        assertEquals(Set.of(borrower + " | SET NON_KEYWORDS", borrower + " | TDSNull"), poolOnly(hits, rows,
                sources::get), "the borrowed and the folded constant are found though their file has rows; the"
                + " lender's own literal and the named symbol are accounted");
        Set<String> withoutSymbolRow = new HashSet<>(rows);
        withoutSymbolRow.remove("Borrower.java | Repr");
        assertTrue(poolOnly(hits, withoutSymbolRow, sources::get).contains(borrower + " | Repr"),
                "a symbol in a real pool's names is found");
        assertTrue(hits.stream().noneMatch(h -> h.spelling().equals("Repr") && h.constant() != null),
                "a symbol is never matched inside a string constant");
        assertEquals("Cannot get ", spelling("^Cannot get $"), "a whole-literal pattern keys as the register writes it");
    }

    private static String read(String path) {
        try {
            return Files.readString(SourceFiles.file(path));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
