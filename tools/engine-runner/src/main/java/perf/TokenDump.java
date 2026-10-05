package perf;

import org.antlr.v4.runtime.Vocabulary;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Enumeration;
import java.util.Locale;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Dumps every literal token the RUNNER can actually lex, grouped by lexer.
 *
 * This exists because the census once measured a different artifact from the one it verified
 * against: keywords.py harvested .g4 files from a legend-engine WORKING COPY at git HEAD while
 * the runner parsed with released jars (4.138.2 then; both are the pinned release now). Five keywords in the census --
 * DataSpace `mappingProvider`, DataQuality `testSuites`/`data`/`tests`/`asserts` -- do not
 * exist in the jars at all, so no fixture could ever cover them, and "100%" would have been
 * a claim about a parser nobody was running.
 *
 * The surface a rewrite must implement is the surface the reference implementation HAS. So
 * take it from the reference implementation: ANTLR bakes a Vocabulary into every generated
 * lexer, and it is the same table the parser uses to produce "Valid alternatives: [...]".
 *
 * Output is one line per lexer:  <SimpleClassName> <TAB> literal <TAB> literal ...
 * with the surrounding single quotes stripped. Consumed by keywords.py.
 *
 * //tools/engine-runner:vocab runs it over the runner's jars (the pinned release) and writes
 * vocab.tsv; `bazel run //tools/engine-runner:update_vocab` puts it in the tree, and //:generated
 * fails when the committed copy is stale (Bazel workplan P2-18).
 *
 *   TokenDump <output file>
 */
public class TokenDump
{
    public static void main(String[] args) throws Exception
    {
        TreeMap<String, TreeSet<String>> out = new TreeMap<>();

        for (String entry : System.getProperty("java.class.path").split(File.pathSeparator))
        {
            if (!entry.endsWith(".jar")) continue;
            try (JarFile jar = new JarFile(entry))
            {
                for (Enumeration<JarEntry> e = jar.entries(); e.hasMoreElements(); )
                {
                    String name = e.nextElement().getName();
                    if (!name.endsWith(".class")) continue;
                    String cls = name.substring(0, name.length() - 6).replace('/', '.');
                    String simple = cls.substring(cls.lastIndexOf('.') + 1);
                    // Generated lexers only. Inner classes and parsers have no VOCABULARY
                    // worth reading -- a parser's vocabulary is its lexer's.
                    if (simple.indexOf('$') >= 0) continue;
                    if (!simple.toLowerCase(Locale.ROOT).endsWith("lexer")
                            && !simple.endsWith("LexerGrammar")) continue;
                    collect(cls, simple, out);
                }
            }
            catch (Exception ignored)
            {
                // A jar we cannot open tells us nothing; the classpath has hundreds and one
                // unreadable entry must not take the dump down.
            }
        }

        if (args.length != 1)
        {
            throw new IllegalArgumentException("usage: TokenDump <output file>");
        }
        StringBuilder text = new StringBuilder();
        for (var e : out.entrySet())
        {
            if (e.getValue().isEmpty()) continue;
            text.append(e.getKey()).append('\t').append(String.join("\t", e.getValue())).append('\n');
        }
        if (text.length() == 0)
        {
            // an empty vocabulary is a classpath that lost the engine's grammars, never a surface
            throw new IllegalStateException("no ANTLR lexer on the classpath: the runner's grammar jars are missing");
        }
        java.nio.file.Files.writeString(java.nio.file.Path.of(args[0]), text.toString(),
                java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void collect(String cls, String simple,
                                TreeMap<String, TreeSet<String>> out)
    {
        Class<?> c;
        try
        {
            c = Class.forName(cls, false, TokenDump.class.getClassLoader());
        }
        catch (Throwable t)
        {
            return;                     // not loadable here; not our surface either
        }

        Field f;
        try
        {
            f = c.getDeclaredField("VOCABULARY");
        }
        catch (NoSuchFieldException n)
        {
            return;                     // not an ANTLR-generated lexer
        }
        f.setAccessible(true);
        Vocabulary vocab;
        try
        {
            // Reading a static field INITIALISES the class, and some lexers on this
            // classpath fail to initialise -- the jars carry a mix of ANTLR 4.8 and 4.13.2
            // generated code, and the mismatched ones throw ExceptionInInitializerError.
            // That is per-class, not fatal: catching Throwable here keeps every lexer that
            // does load, instead of losing the whole dump to one that does not.
            Object v = f.get(null);
            if (!(v instanceof Vocabulary)) return;
            vocab = (Vocabulary) v;
        }
        catch (Throwable t)
        {
            System.err.println("skipped (will not initialise): " + cls);
            return;
        }

        TreeSet<String> literals = out.computeIfAbsent(simple, k -> new TreeSet<>());
        for (int t = 0; t <= vocab.getMaxTokenType(); t++)
        {
            String lit = vocab.getLiteralName(t);
            if (lit == null || lit.length() < 3) continue;
            // ANTLR stores literals quoted; strip the quotes, keep the spelling.
            literals.add(lit.substring(1, lit.length() - 1));
        }
    }
}
