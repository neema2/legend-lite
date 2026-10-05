package com.legend.equivalence;

import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Parse-speed measurement: the legend-engine reference parser vs core's
 * {@code PmcdParser.parseDocument} over the SAME corpus (every source the
 * oracle accepts). ON-DEMAND only, and never an action (a timing is the
 * machine's, not the inputs'; Bazel workplan P3-17):
 *
 * <pre>
 * bazel run //parser-equivalence:parse_speed_benchmark
 * </pre>
 *
 * <p>Method: one full UNTIMED pass per parser (JIT/classloading warmup —
 * without it whichever parser runs first pays the JVM's startup tax), then
 * a timed pass each, interleaving avoided so cache effects don't favor the
 * second parser on a per-file basis. Only sources BOTH parsers accept are
 * timed, so the two sides sum over the identical document set.
 */
public final class ParseSpeedBenchmark {

    private ParseSpeedBenchmark() {}


    public static void main(String[] args) {
        List<Corpus.Source> sources = Corpus.all();
        if (sources.isEmpty()) {
            throw new IllegalStateException("no corpus on disk: set -Dlegend.engine.root / -Dlegend.pure.root");
        }

        PureGrammarParser oracle = PureGrammarParser.newInstance();

        // ---- select the common set: sources BOTH parsers accept ----
        List<Corpus.Source> common = new ArrayList<>();
        for (Corpus.Source src : sources) {
            try {
                oracle.parseModel(src.text());
                com.legend.parser.PmcdParser.parseDocument(src.text());
                common.add(src);
            } catch (Throwable t) {
                // rejected by either side — parity's business, not speed's
            }
        }
        // (the selection pass doubles as warmup pass #1 for both parsers)

        // ---- warmup pass #2, per parser ----
        for (Corpus.Source src : common) {
            try {
                oracle.parseModel(src.text());
            } catch (Throwable ignored) { }
        }
        for (Corpus.Source src : common) {
            try {
                com.legend.parser.PmcdParser.parseDocument(src.text());
            } catch (Throwable ignored) { }
        }

        // ---- timed passes (deep-audit §3 fixes: COMPARABLE work — both
        // sides parse to their protocol object graph, core does NOT
        // serialize JSON in the timed region; 3 reps per source taking the
        // MIN — the least-noise estimator for deterministic work; result
        // blackholed so the JIT cannot elide the parse; nothing throws on
        // the common set, so no catch sits inside the timed region) ----
        record Timing(String id, long nanos) { }
        int reps = 3;
        long blackhole = 0;
        List<Timing> oracleTimes = new ArrayList<>(common.size());
        long oracleTotal = 0;
        for (Corpus.Source src : common) {
            long best = Long.MAX_VALUE;
            for (int r = 0; r < reps; r++) {
                long t0 = System.nanoTime();
                blackhole += oracle.parseModel(src.text()).getElements().size();
                best = Math.min(best, System.nanoTime() - t0);
            }
            oracleTotal += best;
            oracleTimes.add(new Timing(src.id(), best));
        }
        List<Timing> coreTimes = new ArrayList<>(common.size());
        long coreTotal = 0;
        for (Corpus.Source src : common) {
            long best = Long.MAX_VALUE;
            for (int r = 0; r < reps; r++) {
                long t0 = System.nanoTime();
                blackhole += com.legend.parser.PmcdParser
                        .parseSections(src.text()).size();
                best = Math.min(best, System.nanoTime() - t0);
            }
            coreTotal += best;
            coreTimes.add(new Timing(src.id(), best));
        }
        System.out.println("blackhole=" + blackhole);

        long totalBytes = common.stream().mapToLong(s -> s.text().length()).sum();
        System.out.println("=== PARSE SPEED: oracle (legend-engine) vs core ===");
        System.out.printf("common corpus       : %,d sources, %,d KB%n",
                common.size(), totalBytes / 1024);
        System.out.printf("oracle total        : %,d ms  (%,.1f us/source, %,.1f MB/s)%n",
                oracleTotal / 1_000_000, oracleTotal / 1000.0 / common.size(),
                totalBytes * 1000.0 / oracleTotal);
        System.out.printf("core   total        : %,d ms  (%,.1f us/source, %,.1f MB/s)%n",
                coreTotal / 1_000_000, coreTotal / 1000.0 / common.size(),
                totalBytes * 1000.0 / coreTotal);
        System.out.printf("ratio (oracle/core) : %.2fx%n",
                (double) oracleTotal / coreTotal);
        oracleTimes.sort(Comparator.comparingLong(Timing::nanos).reversed());
        coreTimes.sort(Comparator.comparingLong(Timing::nanos).reversed());
        System.out.println("slowest 5, oracle:");
        oracleTimes.stream().limit(5).forEach(t -> System.out.printf(
                "  %,8d us  %s%n", t.nanos() / 1000, t.id()));
        System.out.println("slowest 5, core:");
        coreTimes.stream().limit(5).forEach(t -> System.out.printf(
                "  %,8d us  %s%n", t.nanos() / 1000, t.id()));
    }
}
