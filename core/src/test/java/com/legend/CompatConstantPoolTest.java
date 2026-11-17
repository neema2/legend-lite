// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE COMPATIBILITY SPELLINGS AS THE BYTECODE HOLDS THEM (the GUARD stream, step 2's C.6; the edge audit of 2026-11-17,
 * section C.6: "a constant-pool scan of the product jars (catches concatenation and borrowed constants); per-class
 * pins"). {@link CompatSpellingGuardTest} reads core's main SOURCES: a literal split in two ({@code "TDS" + "Null"},
 * folded by javac into one constant), a constant borrowed from another class (inlined into the user's pool), or a
 * product library outside {@code core/src/main} ({@code //base}, {@code //json}, the cells) is not seen there. Here every
 * class of the product's jars (the edge's aside) is read as its constant pool ({@link ConstantPools}), and each of the
 * register's spellings is matched against it -- a {@link CompatSpellingGuardTest#LITERALS literal} inside any entry (a
 * {@code ^...$} one the whole entry), a {@link CompatSpellingGuardTest#SYMBOLS symbol} as a whole word (a name, a
 * descriptor's class, a constant). A hit whose class's source file has the spelling's register row is the register's to
 * count; every other hit -- the bytecode alone holds it -- is pinned here by class, exact ({@link #POOL_ONLY}), with
 * its reason: a new one fails.
 */
@Tag("guardrail")
class CompatConstantPoolTest {

    private static final String BORROWED_TDS_NULL_FQN = "PlatformTypes.TDS_NULL_FQN borrowed: Pure's class"
            + " meta::pure::tds::TDSNull, LANGUAGE (C5 step C, C-2, 2026-11-17; PlatformTypes' register row STAYS)";

    /** The hits the source register does not hold, "top class | spelling", each with its reason. Measured 2026-11-17. */
    static final Map<String, String> POOL_ONLY = Map.ofEntries(
            // the Pure class's name, borrowed (javac inlines PlatformTypes.TDS_NULL_FQN into each user's pool): the
            // register's PlatformTypes row (STAYS) is its one source; each user names the constant, not the spelling
            Map.entry("com.legend.compiler.spec.LiteralUnroll | TDSNull", BORROWED_TDS_NULL_FQN),
            Map.entry("com.legend.compiler.spec.TdsDesugars | TDSNull", BORROWED_TDS_NULL_FQN),
            Map.entry("com.legend.lowering.CollectionLanes | TDSNull", BORROWED_TDS_NULL_FQN),
            Map.entry("com.legend.lowering.InstanceEquality | TDSNull", BORROWED_TDS_NULL_FQN),
            Map.entry("com.legend.lowering.Lowerer | TDSNull", BORROWED_TDS_NULL_FQN),
            Map.entry("com.legend.testplan.JudgePlans | TDSNull", BORROWED_TDS_NULL_FQN));

    private static String top(String n) {
        int dollar = n.indexOf('$');
        return dollar < 0 ? n : n.substring(0, dollar);
    }

    /** Whether {@code word} stands in {@code text} as a whole word ({@link ConstantPools#hasWord}). */
    static boolean hasWord(String text, String word) {
        return ConstantPools.hasWord(text, word);
    }

    /** The spellings an entry holds: a literal inside it ({@code ^...$}: the whole entry), a symbol as a whole word. */
    static Set<String> spellingsIn(String entry) {
        Set<String> out = new TreeSet<>();
        for (String lit : CompatSpellingGuardTest.LITERALS) {
            boolean whole = lit.startsWith("^") && lit.endsWith("$");
            if (whole ? entry.equals(lit.substring(1, lit.length() - 1)) : entry.contains(lit)) {
                out.add(lit);
            }
        }
        for (String sym : CompatSpellingGuardTest.SYMBOLS) {
            if (hasWord(entry, sym)) {
                out.add(sym);
            }
        }
        return out;
    }

    /** Every hit of {@code pools} (binary name -> its pool), the edge's classes aside, "top class | spelling". */
    static Set<String> hits(Map<String, Set<String>> pools) {
        Set<String> out = new TreeSet<>();
        pools.forEach((name, utf8) -> {
            if (name.startsWith("com.legend.edge.")) {
                return;
            }
            for (String entry : utf8) {
                for (String s : spellingsIn(entry)) {
                    out.add(top(name) + " | " + s);
                }
            }
        });
        return out;
    }

    /** The register's "file | spelling" pairs (every step), the file as the register writes it. */
    static Set<String> registered() {
        Set<String> out = new TreeSet<>();
        for (String row : CompatSpellingGuardTest.REGISTER) {
            String[] p = row.split("\\|");
            out.add(p[0].trim() + " | " + p[1].trim());
        }
        return out;
    }

    /** A core class's source file as the register writes it (under {@code com/legend/}), or null outside core's tree. */
    static String fileOf(String topClass) {
        return topClass.startsWith("com.legend.")
                ? topClass.substring("com.legend.".length()).replace('.', '/') + ".java" : null;
    }

    /** The hits of {@code hits} no register row holds. */
    static Set<String> poolOnly(Set<String> hits, Set<String> registered) {
        Set<String> out = new TreeSet<>();
        for (String h : hits) {
            String[] p = h.split(" \\| ", 2);
            String file = fileOf(p[0]);
            if (file == null || !registered.contains(file + " | " + p[1])) {
                out.add(h);
            }
        }
        return out;
    }

    @Test
    void theBytecodeHoldsNoSpellingTheRegisterDoesNot() {
        Set<String> found = poolOnly(hits(ConstantPools.ofJars(ConstantPools.jars("legend.product.jars"))), registered());
        assertEquals(new TreeSet<>(POOL_ONLY.keySet()), found, "a compatibility spelling in a product class's constant"
                + " pool that the source register does not hold (concatenated, borrowed from another class's constant,"
                + " or in a library outside core/src/main): spell it at the edge, or pin it here with its reason; a pin"
                + " gone is deleted");
    }

    /** Every pin names its reason and date. */
    @Test
    void everyPinIsDated() {
        POOL_ONLY.forEach((h, why) -> assertTrue(why.matches(".*\\d{4}-\\d{2}-\\d{2}.*"), h + ": " + why));
    }

    /** A planted class ({@code PlantedPoolSpelling}) holds a spelling three ways no source scan of its own file reads as
     *  that spelling: a folded concatenation, a constant borrowed from another class, and a symbol named only through a
     *  descriptor. Each is found in its pool, and none has a register row. */
    @Test
    void theScanBites() {
        Map<String, Set<String>> pools = new TreeMap<>();
        for (Class<?> c : List.of(PlantedPoolSpelling.class, PlantedPoolSpelling.Borrowed.class)) {
            pools.put(c.getName(), ConstantPools.utf8(ConstantPools.bytesOf(c)));
        }
        Set<String> found = poolOnly(hits(pools), registered());
        String p = CompatConstantPoolTest.class.getName() + " | "; // a nested class's hits are its top class's
        List<String> want = new ArrayList<>(List.of(p + "TDSNull", p + "SET NON_KEYWORDS"));
        assertTrue(found.containsAll(want), "the pool scan finds the folded and the borrowed constant: " + found);
        assertTrue(hasWord("Lcom/legend/lowering/Repr;", "Repr") && !hasWord("Lcom/legend/lowering/Representation;",
                "Repr"), "a symbol is a whole word in a descriptor");
    }

    /** Spellings the planted class's source writes in no single literal. */
    @SuppressWarnings("unused")
    static final class PlantedPoolSpelling {
        static final String FOLDED = "TDS" + "Null";
        static final String BORROWED = Borrowed.SESSION;

        static final class Borrowed {
            static final String SESSION = "SET NON_" + "KEYWORDS";
        }
    }
}
