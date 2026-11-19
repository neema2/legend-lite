// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package planoracle;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.protocol.pure.m3.function.LambdaFunction;
import org.finos.legend.engine.shared.core.api.model.ExecuteInput;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * The pure/v1 golden requests, each planned by legend-engine ({@link EngineRoutes#plan}) and the plan's JSON run by
 * legend-engine ({@link EngineRoutes#execute}) in one JVM (slice R, 2026-11-18). Test tooling.
 *
 * <pre>
 *   --requests=&lt;tsv&gt;   one request a line: its name, a tab, the lambda's grammar (the net's list)
 *   --model=&lt;file&gt;     the model the requests run on, sent as pure/v1 sends it ({"_type":"text"})
 *   --out=&lt;file&gt;       one JSON array a line, [name, status, body]: the execute answer under the request's name,
 *                       the generatePlan answer under "plan." and the name, in the net's order
 *   --duckdb=&lt;file&gt;    a seeded DuckDB file: the model's text has each {@code ${DUCKDB_PATH}} replaced by the path of
 *                       a private copy of it (DuckDB opens a file to write; a build input is read-only)
 * </pre>
 *
 * <p>Each request is the body a legend-engine client sends: the lambda parsed by legend-engine's own grammar
 * ({@code PureGrammarParser.parseLambda(code, sourceId, false)}: no source information, as the net asks lite's
 * grammarToJson), written by the server's mapper, beside the model text, {@code "clientVersion":"vX_X_X"} and a
 * {@code BaseExecutionContext} -- the fields the net's bodies carry ({@code PureV1EdgeGoldenTest.everyRequestsAnswer}).
 * A lambda legend-engine's grammar refuses is recorded with the status {@code PARSE} and the message, on both lines.
 */
public final class GoldenRunMain
{
    private GoldenRunMain()
    {
    }

    public static void main(String[] args) throws Exception
    {
        Path requests = null;
        Path model = null;
        Path out = null;
        Path duckdb = null;
        for (String a : args)
        {
            if (a.startsWith("--requests="))
            {
                requests = Path.of(a.substring("--requests=".length()));
            }
            else if (a.startsWith("--model="))
            {
                model = Path.of(a.substring("--model=".length()));
            }
            else if (a.startsWith("--out="))
            {
                out = Path.of(a.substring("--out=".length()));
            }
            else if (a.startsWith("--duckdb="))
            {
                duckdb = Path.of(a.substring("--duckdb=".length()));
            }
            else
            {
                throw new IllegalArgumentException("unknown argument " + a);
            }
        }
        if (requests == null || model == null || out == null)
        {
            throw new IllegalArgumentException("usage: --requests=<tsv> --model=<file> --out=<file> [--duckdb=<file>]");
        }
        String modelText = Files.readString(model, StandardCharsets.UTF_8);
        String copyPath = null;
        if (duckdb != null)
        {
            Path copy = Files.createTempDirectory("engine-oracle-duckdb").resolve(duckdb.getFileName().toString());
            Files.copy(duckdb, copy, StandardCopyOption.REPLACE_EXISTING);
            copy.toFile().setWritable(true);
            // into a Pure string, which legend-engine's parser unescapes: written with '/', which every platform takes
            copyPath = copy.toAbsolutePath().toString().replace('\\', '/');
            modelText = modelText.replace("${DUCKDB_PATH}", copyPath);
        }
        else if (modelText.contains("${DUCKDB_PATH}"))
        {
            throw new IllegalArgumentException("the model names ${DUCKDB_PATH}: pass --duckdb=<file>");
        }

        EngineRoutes routes = new EngineRoutes();
        long t0 = System.nanoTime();
        int planned = 0;
        int n = 0;
        try (Writer w = Files.newBufferedWriter(out, StandardCharsets.UTF_8))
        {
            // the drivers that answered, as the class path resolved them: the pool lists H2 2.2.224, but
            // legend-engine's H2 execution jar carries H2 2.1.214's classes and is ahead of it
            header(w, "legend-engine 4.145.0; H2 " + h2Version() + "; DuckDB " + duckDbVersion());
            for (String[] r : readRequests(requests))
            {
                n++;
                String name = r[0];
                String body;
                try
                {
                    body = requestBody(r[1], modelText);
                }
                catch (RuntimeException e)
                {
                    EngineAnswer refused = EngineAnswer.of(0, String.valueOf(e.getMessage()));
                    line(w, name, "PARSE", refused, copyPath);
                    line(w, "plan." + name, "PARSE", refused, copyPath);
                    continue;
                }
                EngineAnswer plan = routes.plan(body);
                EngineAnswer run;
                if (plan.status() == 200)
                {
                    planned++;
                    ExecuteInput input = EngineRoutes.SERVER_MAPPER.readValue(body, ExecuteInput.class);
                    run = routes.execute(plan.text(), EngineRoutes.parameterValues(input.parameterValues));
                }
                else
                {
                    // the execute route builds the same plan first (Execute.execLegacy, Execute.java:316-323): a
                    // request legend-engine cannot plan answers execute with the same error
                    run = plan;
                }
                line(w, name, String.valueOf(run.status()), run, copyPath);
                line(w, "plan." + name, String.valueOf(plan.status()), plan, copyPath);
                w.flush();
            }
        }
        System.out.printf("engine oracle: %d requests, %d planned, %.1f s%n", n, planned, (System.nanoTime() - t0) / 1e9);
        // legend-engine's executors leave non-daemon threads (connection pools) behind
        System.exit(0);
    }

    /** The body a legend-engine client sends for a lambda over a model. */
    static String requestBody(String lambda, String modelText) throws IOException
    {
        LambdaFunction function = PureGrammarParser.newInstance().parseLambda(lambda, "", false);
        ObjectNode body = JsonNodeFactory.instance.objectNode();
        body.put("clientVersion", "vX_X_X");
        body.set("function", EngineRoutes.SERVER_MAPPER.valueToTree(function));
        ObjectNode m = body.putObject("model");
        m.put("_type", "text");
        m.put("code", modelText);
        body.putObject("context").put("_type", "BaseExecutionContext");
        return EngineRoutes.SERVER_MAPPER.writeValueAsString(body);
    }

    static List<String[]> readRequests(Path file) throws IOException
    {
        List<String[]> out = new ArrayList<>();
        for (String l : Files.readAllLines(file, StandardCharsets.UTF_8))
        {
            if (l.isBlank() || l.startsWith("#"))
            {
                continue;
            }
            int tab = l.indexOf('\t');
            if (tab < 0)
            {
                throw new IllegalArgumentException("a request line is name TAB lambda: " + l);
            }
            out.add(new String[] {l.substring(0, tab), l.substring(tab + 1)});
        }
        return out;
    }

    /** One answer; the DuckDB copy's path (a fresh temporary directory each run) written back as the placeholder. */
    private static void line(Writer w, String name, String status, EngineAnswer answer, String copyPath)
            throws IOException
    {
        String text = answer.text();
        if (copyPath != null)
        {
            text = text.replace(copyPath, "${DUCKDB_PATH}");
        }
        ArrayNode a = JsonNodeFactory.instance.arrayNode();
        a.add(name);
        a.add(status);
        a.add(text);
        w.write(EngineRoutes.SERVER_MAPPER.writeValueAsString(a));
        w.write('\n');
    }

    /** A first line naming what answered: {@code ["#", "", text]}. */
    private static void header(Writer w, String text) throws IOException
    {
        ArrayNode a = JsonNodeFactory.instance.arrayNode();
        a.add("#");
        a.add("");
        a.add(text);
        w.write(EngineRoutes.SERVER_MAPPER.writeValueAsString(a));
        w.write('\n');
    }

    /** The H2 the class path loads, by its own constant (read reflectively: the oracle names no H2 type). */
    static String h2Version()
    {
        try
        {
            return String.valueOf(Class.forName("org.h2.engine.Constants").getField("VERSION").get(null));
        }
        catch (ReflectiveOperationException e)
        {
            return "absent";
        }
    }

    /** The DuckDB the class path loads, as it answers {@code version()}. */
    static String duckDbVersion()
    {
        try (java.sql.Connection c = java.sql.DriverManager.getConnection("jdbc:duckdb:");
             java.sql.Statement s = c.createStatement();
             java.sql.ResultSet rs = s.executeQuery("SELECT version()"))
        {
            rs.next();
            return rs.getString(1);
        }
        catch (java.sql.SQLException e)
        {
            return "absent";
        }
    }
}
