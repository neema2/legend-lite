package com.legend.equivalence;

import com.legend.testing.Repo;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.protocol.pure.v1.extension.PureProtocolExtensionLoader;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** PROBE (complete protocol-type census): the FULL Jackson subtype roster
 *  from every protocol jar on the oracle classpath (all @JsonSubTypes,
 *  not a hand-picked walk) + the extension registry; coverage unioned
 *  over BOTH corpuses (engine checkouts AND our own test snippets);
 *  every uncovered tag printed with its full class for triage; the roster
 *  is COMMITTED as docs/protocol-roster.tsv and held equal (batch 6). */
class ProtocolRosterCensusTest {


    @Test
    void completeCensus() throws Exception {
        materializeRoster();
    }

    /** The census body, callable — PmcdReachabilityCensusTest depends
     * on the roster this writes; SELF-SUFFICIENCY (user ruling
     * 2026-08-21: a class that always skips inside a gate is roster
     * theater) means the reader materializes it itself instead of
     * skipping on ordering. */
    static void materializeRoster() throws Exception {
        RosterGenerator.Roster r = RosterGenerator.compute();
        Map<String, Set<String>> tagToClass = r.tagToClass();
        Set<String> seen = r.seen();
        String dump = r.dump();
        java.nio.file.Files.writeString(
                Repo.out("protocol-roster.txt"), dump);
        // THE LEDGER (upstream boundary batch 6): the roster is COMMITTED
        // (docs/protocol-roster.tsv) and held equal — a new upstream tag
        // adds/removes protocol types as a REVIEWED diff, each row marked
        // COVERED (some source in either corpus reaches it) or UNCOVERED.
        // Regenerate: bazel run //:update_generated (RosterGenerator).
        Path ledger = Repo.path("docs", "protocol-roster.tsv");
        String committed = java.nio.file.Files.exists(ledger)
                ? java.nio.file.Files.readString(ledger).lines().filter(l -> !l.startsWith("#"))
                        .collect(java.util.stream.Collectors.joining("\n", "", "\n"))
                : "";
        org.junit.jupiter.api.Assertions.assertEquals(committed, dump,
                "the protocol-type roster moved (new/removed tags, or coverage changed) — review"
                + " the test's protocol-roster.txt against docs/protocol-roster.tsv; regenerate:"
                + " bazel run //:update_generated");
        Set<String> unrostered = new TreeSet<>(seen);
        unrostered.removeAll(tagToClass.keySet());
        System.out.println("@@ seen-but-unrostered: " + unrostered.size()
                + " " + unrostered);
    }
}
