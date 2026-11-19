// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package planoracle.compare;

import com.legend.json.Json;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * legend-engine's answers to the pure/v1 golden requests ({@code planoracle.GoldenRunMain}'s recording) written in the
 * net's own line form, and each golden line set against legend-engine's (slice R, 2026-11-18). Test tooling.
 *
 * <pre>
 *   --engine=&lt;file&gt;        GoldenRunMain's recording, one JSON array [name, status, body] a line
 *   --answers-out=&lt;file&gt;   legend-engine's answers as the net writes lite's: name, status and the body made compact
 *                           by lite's JSON library, ids masked, newlines escaped ({@code PureV1EdgeGoldenTest.answer},
 *                           {@code .line}) -- so a golden line and an engine line compare byte for byte
 *   --golden=&lt;file&gt;        the net's golden (core/src/test/resources/edge/pure_v1_answers.tsv); with it:
 *   --differences-out=&lt;f&gt;  one line per golden line: its name, {@code same} or {@code differs} (or {@code missing},
 *                           a golden the runner has no request for), and where they differ: {@code status}, and each
 *                           JSON path whose value differs ({@code -} only lite's, {@code +} only legend-engine's)
 * </pre>
 *
 * <p>One mask beyond the net's: an error's {@code trace} (legend-engine's {@code ExceptionError.trace}, the stack trace
 * {@code printStackTrace} writes) keeps its first line, the exception, and its frames become {@code <frames>}: they are
 * the in-process run's call path, never the server's, so no run reproduces them. Lite sends no trace; that difference
 * stays visible as the path {@code +$.trace}.
 */
public final class GoldenCompareMain
{
    /** The net's id mask ({@code PureV1EdgeGoldenTest.IDS}). */
    private static final Pattern IDS = Pattern.compile(
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
    private static final int MAX_PATHS = 12;

    private GoldenCompareMain()
    {
    }

    public static void main(String[] args) throws IOException
    {
        Map<String, String> a = new LinkedHashMap<>();
        for (String arg : args)
        {
            int eq = arg.indexOf('=');
            if (!arg.startsWith("--") || eq < 0)
            {
                throw new IllegalArgumentException("unknown argument " + arg);
            }
            a.put(arg.substring(2, eq), arg.substring(eq + 1));
        }
        Map<String, String> engine = new LinkedHashMap<>();
        StringBuilder answers = new StringBuilder();
        for (String l : Files.readAllLines(Path.of(a.get("engine")), StandardCharsets.UTF_8))
        {
            Json.Arr row = (Json.Arr) Json.parse(l);
            String name = ((Json.Str) row.items().get(0)).value();
            String status = ((Json.Str) row.items().get(1)).value();
            String body = ((Json.Str) row.items().get(2)).value();
            if (name.equals("#"))
            {
                answers.append("# ").append(body).append('\n');   // what answered (GoldenRunMain.header)
                continue;
            }
            String line = netLine(status, body);
            engine.put(name, line);
            answers.append(name).append('\t').append(line).append('\n');
        }
        Files.writeString(Path.of(a.get("answers-out")), answers, StandardCharsets.UTF_8);

        if (a.containsKey("golden"))
        {
            StringBuilder out = new StringBuilder();
            int same = 0;
            int differs = 0;
            List<String> seen = new ArrayList<>();
            for (String l : Files.readAllLines(Path.of(a.get("golden")), StandardCharsets.UTF_8))
            {
                int tab = l.indexOf('\t');
                String name = l.substring(0, tab);
                String golden = l.substring(tab + 1);
                seen.add(name);
                String mine = engine.get(name);
                if (mine == null)
                {
                    out.append(name).append("\tmissing\tthe runner made no request of this name\n");
                }
                else if (mine.equals(golden))
                {
                    same++;
                    out.append(name).append("\tsame\t\n");
                }
                else
                {
                    differs++;
                    out.append(name).append("\tdiffers\t").append(String.join(", ", paths(golden, mine))).append('\n');
                }
            }
            for (String name : engine.keySet())
            {
                if (!seen.contains(name))
                {
                    out.append(name).append("\textra\tno golden of this name\n");
                }
            }
            Files.writeString(Path.of(a.get("differences-out")), out, StandardCharsets.UTF_8);
            System.out.printf("engine oracle: %d golden lines the same, %d differ%n", same, differs);
        }
    }

    /** A body as the net writes an answer: compact if it is a JSON object, the trace's frames masked, then the ids
     *  masked and the newlines escaped. */
    static String netLine(String status, String body)
    {
        String json = body;
        try
        {
            Json.Obj o = Json.parseObject(body);
            if (o.fields().get("trace") instanceof Json.Str t)
            {
                int nl = t.value().indexOf('\n');
                LinkedHashMap<String, Json.Node> fields = new LinkedHashMap<>(o.fields());
                fields.put("trace", new Json.Str((nl < 0 ? t.value() : t.value().substring(0, nl)) + "\n<frames>"));
                o = new Json.Obj(fields);
            }
            json = Json.toCompact(o);
        }
        catch (RuntimeException notJson)
        {
            // an answer that is no JSON object is recorded as it is, as the net records lite's
        }
        return IDS.matcher(status + "\t" + json).replaceAll("<id>").replace("\n", "\\n").replace("\r", "");
    }

    /** Where a golden line and legend-engine's differ: the status, then each differing JSON path. */
    static List<String> paths(String golden, String engine)
    {
        List<String> out = new ArrayList<>();
        int g = golden.indexOf('\t');
        int e = engine.indexOf('\t');
        String gs = g < 0 ? golden : golden.substring(0, g);
        String es = e < 0 ? engine : engine.substring(0, e);
        if (!gs.equals(es))
        {
            out.add("status " + gs + "/" + es);
        }
        String gb = g < 0 ? "" : golden.substring(g + 1);
        String eb = e < 0 ? "" : engine.substring(e + 1);
        try
        {
            diff("$", Json.parse(gb), Json.parse(eb), out);
        }
        catch (RuntimeException notJson)
        {
            if (!gb.equals(eb))
            {
                out.add("body (not JSON)");
            }
        }
        if (out.size() > MAX_PATHS)
        {
            int more = out.size() - MAX_PATHS;
            out = new ArrayList<>(out.subList(0, MAX_PATHS));
            out.add("(" + more + " more)");
        }
        return out;
    }

    private static void diff(String path, Json.Node a, Json.Node b, List<String> out)
    {
        if (Json.toCompact(a).equals(Json.toCompact(b)))
        {
            return;
        }
        if (a instanceof Json.Obj oa && b instanceof Json.Obj ob)
        {
            int before = out.size();
            for (Map.Entry<String, Json.Node> f : oa.fields().entrySet())
            {
                Json.Node other = ob.fields().get(f.getKey());
                if (other == null)
                {
                    out.add("-" + path + "." + f.getKey());
                }
                else
                {
                    diff(path + "." + f.getKey(), f.getValue(), other, out);
                }
            }
            for (String k : ob.fields().keySet())
            {
                if (!oa.fields().containsKey(k))
                {
                    out.add("+" + path + "." + k);
                }
            }
            if (out.size() == before)
            {
                out.add(path + " (key order)");
            }
            return;
        }
        if (a instanceof Json.Arr aa && b instanceof Json.Arr ab)
        {
            int n = Math.min(aa.items().size(), ab.items().size());
            if (aa.items().size() != ab.items().size())
            {
                out.add(path + " (" + aa.items().size() + "/" + ab.items().size() + " items)");
            }
            for (int i = 0; i < n; i++)
            {
                diff(path + "[" + i + "]", aa.items().get(i), ab.items().get(i), out);
            }
            return;
        }
        out.add(path);
    }
}
