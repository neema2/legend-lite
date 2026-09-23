package perf;

import org.finos.legend.engine.language.pure.compiler.Compiler;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.shared.core.deployment.DeploymentMode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Parses each .pure file INDIVIDUALLY and reports whether the grammar accepted it.
 *
 * TestableMain concatenates its inputs into one document, which is right for a corpus that
 * has to resolve across files and wrong for parser fixtures: one bad fixture would fail the
 * batch and say nothing about which construct broke. Here every file stands alone, so the
 * output is a per-fixture verdict.
 *
 * PARSE and COMPILE are reported separately and that separation is the point. Parser
 * completeness asks only whether the grammar accepts a construct. A fixture that parses and
 * then fails to compile -- an unresolved type, a mapping with no runtime -- is a PASS for
 * this harness, because the parser did its job. Collapsing the two would make the coverage
 * number quietly depend on the compiler, and every fixture would need a full valid model
 * around it just to exercise one keyword.
 *
 *   --expect-fail   invert: every file MUST be rejected. This is the negative harness, and
 *                   it needs its own mode because "the run was green" must not be
 *                   satisfiable by a fixture that failed to load.
 *   --compile       additionally attempt compilation and report it, without affecting the
 *                   verdict.
 *
 * Exit code is the number of files whose verdict was wrong, so a caller can gate on it.
 */
public class ParseMain
{
    public static void main(String[] args) throws Exception
    {
        List<Path> files = new ArrayList<>();
        boolean expectFail = false;
        boolean compile = false;

        for (String a : args)
        {
            if (a.equals("--expect-fail")) expectFail = true;
            else if (a.equals("--compile")) compile = true;
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
            // An empty run is not a green run. Without this a mistyped path reports
            // "0 wrong" and reads as success.
            System.out.println("NO FIXTURES FOUND -- refusing to report success");
            System.exit(1);
        }

        PureGrammarParser parser = PureGrammarParser.newInstance();
        int wrong = 0;

        for (Path f : files)
        {
            String name = f.getFileName().toString();
            String src = Files.readString(f);
            String parseError = null;
            PureModelContextData pmcd = null;
            try
            {
                pmcd = parser.parseModel(src);
            }
            catch (Exception e)
            {
                parseError = oneLine(e);
            }

            boolean parsed = parseError == null;
            boolean ok = parsed != expectFail;
            if (!ok) wrong++;

            StringBuilder line = new StringBuilder();
            line.append(ok ? "ok   " : "WRONG").append("  ").append(name);
            if (!parsed) line.append("  REJECTED: ").append(parseError);
            else if (expectFail) line.append("  ACCEPTED (expected rejection)");

            if (compile && parsed)
            {
                try
                {
                    Compiler.compile(pmcd, DeploymentMode.TEST, null);
                    line.append("  [compiles]");
                }
                catch (Exception e)
                {
                    // Not a failure -- see the class comment. Reported so a fixture that
                    // parses but is semantically nonsense is visible rather than silent.
                    line.append("  [parse-only: ").append(oneLine(e)).append("]");
                }
            }
            System.out.println(line);
        }

        System.out.printf("%n%d files, %d wrong (%s)%n", files.size(), wrong,
                expectFail ? "expected every file to be REJECTED"
                           : "expected every file to PARSE");
        System.exit(wrong);
    }

    /** Legend's parse errors carry the line/column and the offending token; keep that and
     *  drop the stack, which is a page of ANTLR frames identical across every failure. */
    private static String oneLine(Exception e)
    {
        String m = e.getMessage();
        // Some walkers throw with a message of literally "Error", which names nothing.
        // Qualify it, otherwise diagnosing a rejected fixture means re-running by hand.
        if (m == null || m.length() < 12) m = e.getClass().getSimpleName() + ": " + m;
        m = m.replaceAll("\\s+", " ").trim();
        return m.length() > 220 ? m.substring(0, 220) + "..." : m;
    }
}
