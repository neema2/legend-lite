package com.legend.equivalence;

import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;

/** One-shot sweep of the sibling's test-corpus fixtures (positive +
 *  negative) against BOTH parsers — prints one TSV row per divergence.
 *  Not a gate; the survivors become battery rows / fixes. Run on demand
 *  (Bazel workplan P3-17): {@code bazel run //parser-equivalence:fixture_sweep -- DIR...}. */
class FixtureSweep {
    static final com.fasterxml.jackson.databind.ObjectMapper M =
            ObjectMapperFactory
                    .getNewStandardObjectMapperWithPureProtocolExtensionSupports();

    public static void main(String[] args) throws Exception {
        int agree = 0;
        for (String dir : args) {
            java.io.File[] files = com.legend.testing.Programs.argument(dir).toFile().listFiles(
                    (d, n) -> n.endsWith(".pure"));
            java.util.Arrays.sort(files);
            for (java.io.File f : files) {
                String src = java.nio.file.Files.readString(f.toPath());
                String e, l;
                try {
                    e = M.writeValueAsString(
                            PureGrammarParser.newInstance().parseModel(src));
                } catch (Throwable t) {
                    e = "REFUSE\t" + t.getClass().getSimpleName() + ": "
                            + String.valueOf(t.getMessage())
                                    .replace('\n', ' ').replace('\t', ' ');
                }
                try {
                    l = com.legend.parser.PmcdParser.parseDocument(src);
                } catch (Throwable t) {
                    l = "REFUSE\t" + t.getClass().getSimpleName() + ": "
                            + String.valueOf(t.getMessage())
                                    .replace('\n', ' ').replace('\t', ' ');
                }
                boolean eAcc = !e.startsWith("REFUSE");
                boolean lAcc = !l.startsWith("REFUSE");
                if (eAcc && lAcc) {
                    if (e.equals(l)) {
                        agree++;
                    } else {
                        int i = 0;
                        while (i < Math.min(e.length(), l.length())
                                && e.charAt(i) == l.charAt(i)) {
                            i++;
                        }
                        System.out.println(f.getName() + "\tBYTE-DIFF\tat "
                                + i + " E..." + snip(e, i) + " L..." + snip(l, i));
                    }
                } else if (eAcc != lAcc) {
                    System.out.println(f.getName() + "\t"
                            + (eAcc ? "ENGINE-ACCEPTS-LITE-REFUSES"
                                    : "ENGINE-REFUSES-LITE-ACCEPTS")
                            + "\t" + (eAcc ? l : e));
                } else {
                    agree++;
                }
            }
        }
        System.out.println("AGREE\t" + agree);
    }

    private static String snip(String s, int i) {
        return s.substring(Math.max(0, i - 40),
                Math.min(s.length(), i + 80));
    }
}
