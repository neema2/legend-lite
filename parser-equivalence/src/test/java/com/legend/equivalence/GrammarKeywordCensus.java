package com.legend.equivalence;

import java.io.PrintWriter;

import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** PROBE (keyword-coverage census): every keyword literal in the engine's
 *  73 .g4 grammars, checked against the corpus — a keyword that never
 *  appears in any source BOTH parsers accept marks a grammar arm the
 *  parity harness has never exercised (how the Binding transformer hid).
 *  Diagnostic. A report action of //parser-equivalence:diagnostics_reports (Bazel workplan P3-17), cached until an input changes. */
public final class GrammarKeywordCensus {

    private GrammarKeywordCensus() {}



    /** {@code args[0]}: the directory the report goes in (the action's {@code {OUT_DIR}}). */
    public static void main(String[] args) throws Exception {
        Path outDir = Path.of(args[0]);
        com.legend.testing.Programs.captureConsole(outDir);
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(outDir.resolve("grammar-keyword-census.txt")))) {
            report(out, outDir);
        }
    }

    private static void report(PrintWriter out, Path outDir) throws Exception {
        Path engineRoot = com.legend.testing.ProgramPaths.rootOf("legend.engine.root");
        // keyword literals from lexer/parser grammars: word-shaped, >= 3
        // chars (operators/punctuation can't be checked by text presence)
        Map<String, Set<String>> kwToGrammars = new TreeMap<>();
        try (Stream<Path> s = Files.walk(engineRoot)) {
            for (Path g4 : s.filter(p -> p.toString().endsWith(".g4"))
                    .toList()) {
                String text = Files.readString(g4);
                Matcher m = Pattern
                        .compile("'([A-Za-z_][A-Za-z0-9_]{2,})'")
                        .matcher(text);
                while (m.find()) {
                    kwToGrammars.computeIfAbsent(m.group(1),
                            k -> new TreeSet<>())
                            .add(g4.getFileName().toString()
                                    .replace("ParserGrammar.g4", "")
                                    .replace("LexerGrammar.g4", ""));
                }
            }
        }
        out.println("@@ keywords harvested: " + kwToGrammars.size());

        PureGrammarParser oracle = PureGrammarParser.newInstance();
        Set<String> covered = new HashSet<>();
        int accepted = 0;
        java.util.List<Corpus.Source> universe =
                new java.util.ArrayList<>(Corpus.all());
        universe.addAll(Corpus.engineFixtures());
        for (Corpus.Source src : universe) {
            try {
                oracle.parseModel(src.text());
            } catch (Throwable t) {
                continue;
            }
            try {
                Surfaces.platform(src.text());
            } catch (Throwable t) {
                continue;
            }
            accepted++;
            String txt = src.text();
            for (String kw : kwToGrammars.keySet()) {
                if (covered.contains(kw)) {
                    continue;
                }
                int i = txt.indexOf(kw);
                while (i >= 0) {
                    boolean leftOk = i == 0 || !Character
                            .isLetterOrDigit(txt.charAt(i - 1))
                            && txt.charAt(i - 1) != '_';
                    int e = i + kw.length();
                    boolean rightOk = e >= txt.length() || !Character
                            .isLetterOrDigit(txt.charAt(e))
                            && txt.charAt(e) != '_';
                    if (leftOk && rightOk) {
                        covered.add(kw);
                        break;
                    }
                    i = txt.indexOf(kw, i + 1);
                }
            }
        }
        out.println("@@ both-accepted sources: " + accepted
                + "; keywords covered: " + covered.size() + "/"
                + kwToGrammars.size());
        Map<String, java.util.List<String>> byGrammar = new TreeMap<>();
        for (var e : kwToGrammars.entrySet()) {
            if (covered.contains(e.getKey())) {
                continue;
            }
            for (String g : e.getValue()) {
                byGrammar.computeIfAbsent(g, k -> new java.util.ArrayList<>())
                        .add(e.getKey());
            }
        }
        byGrammar.forEach((g, kws) -> out.println(
                "@@ UNCOVERED [" + g + "] " + String.join(", ", kws)));
    }
}
