package perf;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ParseMain's twin, driving legend-lite instead of legend-engine.
 *
 * Deliberately a copy rather than a shared abstraction: the two must agree because both
 * parsers say the same thing about the same bytes, not because they run the same code. A
 * shared driver could hide a divergence by normalising it on the way out.
 *
 * The verdict vocabulary is identical to ParseMain's -- "ok"/"WRONG", "REJECTED: <message>"
 * -- so the two outputs diff directly and the harness that compares them needs no adapter.
 *
 * legend-lite's entry point is PmcdParser.parseDocument, which returns the protocol
 * document as JSON. Whether that JSON MATCHES the engine's is a different and harder
 * question, already answered by parser-equivalence. This asks only the first question, the
 * one the fixture corpus was built for: does it accept, and if not, what does it say?
 */
public class LiteParseMain
{
    public static void main(String[] args) throws Exception
    {
        List<Path> files = new ArrayList<>();
        boolean expectFail = false;
        boolean protocolCheck = false;

        for (String a : args)
        {
            if (a.equals("--expect-fail")) expectFail = true;
            else if (a.equals("--protocol-check")) protocolCheck = true;
            else
            {
                Path p = Cwd.of(a);
                if (Files.isDirectory(p))
                {
                    try (var s = Files.walk(p))
                    {
                        files.addAll(s.filter(f -> f.toString().endsWith(".pure"))
                                .sorted(Comparator.naturalOrder()).collect(Collectors.toList()));
                    }
                }
                else files.add(p);
            }
        }

        if (files.isEmpty())
        {
            System.out.println("NO FIXTURES FOUND -- refusing to report success");
            System.exit(1);
        }

        int wrong = 0;
        for (Path f : files)
        {
            String name = f.getFileName().toString();
            String parseError = null;
            try
            {
                String json = com.legend.parser.PmcdParser.parseDocument(Files.readString(f));
                if (protocolCheck)
                {
                    // Does what lite produced round-trip into the ENGINE's own protocol
                    // classes? This is the question that decides whether an accepted
                    // mis-ordered input is harmless or corrupting. Accepting input the
                    // engine's grammar refuses costs nothing if the resulting document is
                    // still one the engine could have produced; it costs a great deal if
                    // the document is unrepresentable and only fails downstream.
                    org.finos.legend.engine.shared.core.ObjectMapperFactory
                            .getNewStandardObjectMapperWithPureProtocolExtensionSupports()
                            .readValue(json, org.finos.legend.engine.protocol.pure.v1
                                    .model.context.PureModelContextData.class);
                }
            }
            catch (Throwable t)
            {
                // Throwable, not Exception. A parser under construction can fail with
                // StackOverflowError or AssertionError, and reporting those as a crash of
                // the HARNESS rather than a rejection by the PARSER would lose the result.
                parseError = oneLine(t);
            }

            boolean parsed = parseError == null;
            boolean ok = parsed != expectFail;
            if (!ok) wrong++;

            StringBuilder line = new StringBuilder();
            line.append(ok ? "ok   " : "WRONG").append("  ").append(name);
            if (!parsed) line.append("  REJECTED: ").append(parseError);
            else if (expectFail) line.append("  ACCEPTED (expected rejection)");
            System.out.println(line);
        }

        System.out.printf("%n%d files, %d wrong (%s)%n", files.size(), wrong,
                expectFail ? "expected every file to be REJECTED"
                           : "expected every file to PARSE");
        System.exit(wrong);
    }

    private static String oneLine(Throwable t)
    {
        String m = t.getMessage();
        if (m == null || m.length() < 12) m = t.getClass().getSimpleName() + ": " + m;
        m = m.replaceAll("\\s+", " ").trim();
        return m.length() > 220 ? m.substring(0, 220) + "..." : m;
    }
}
